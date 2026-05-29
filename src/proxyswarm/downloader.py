"""Worker-side per-item logic plus the per-API-key throttle detector.

`Downloader` is shared across all `ThreadPoolExecutor` workers: it dedups by id,
acquires a proxy, issues the use case's request, classifies the response, and
either hands a successful body to the use case or loops to the next proxy on a
proxy-fault outcome. `APIThrottleDetector` watches for streaks of identical
authoritative throttle responses across *different* proxies — a pattern that
implicates the API key, which proxy rotation can't fix.
"""

import threading
import time
from typing import TYPE_CHECKING

import requests
from loguru import logger

from .stats import FetchOutcome, Stats

if TYPE_CHECKING:
    from .config import SwarmConfig
    from .pool import ProxyPool
    from .usecase import UseCase


class APIThrottleDetector:
    """Distinguish per-IP throttling from per-API-key throttling.

    Per-IP throttling is fixed by proxy rotation; per-key throttling isn't.
    If 5+ different proxies in a row all return `limit_exceeded`, the
    constraint is clearly the key, not the IPs — and the operator needs to
    know that before they keep burning proxy quota chasing it.

    State machine
    -------------
    Only `rate_limited` and `auth_error` count toward streaks — both
    implicate the key. Authoritative API answers (zip / not_found /
    bad_query) reset the streak *and* log a recovery message if a warning
    was previously emitted. Proxy-fault outcomes (proxy_bad / proxy_garbage
    / timeouts) are ambiguous and don't touch the counter either way.

    Warnings are de-duped per label: the same label only logs once until
    either the streak clears or it flips (e.g. rate_limited → auth_error).
    """

    def __init__(self, threshold: int) -> None:
        """Initialize with the consecutive-hit threshold that triggers a warning."""
        self.threshold = threshold
        self.streak = 0
        self.warned_label: str | None = None
        self.lock = threading.Lock()

    def observe(self, outcome: FetchOutcome) -> None:
        """Update the key-throttle streak from one outcome; warn on threshold."""
        if outcome == FetchOutcome.RATE_LIMITED:
            label = "rate_limited"
        elif outcome == FetchOutcome.AUTH_ERROR:
            label = "auth_error"
        elif outcome in (
            FetchOutcome.OK,
            FetchOutcome.NOT_FOUND,
            FetchOutcome.BAD_QUERY,
        ):
            # Authoritative API answer → key is being honored. Close the loop on
            # any prior warning so the operator knows the throttle has cleared.
            with self.lock:
                prior_label = self.warned_label
                prior_streak = self.streak
                self.streak = 0
                self.warned_label = None
            if prior_label is not None:
                logger.success(
                    'API throttling cleared (was "{}" after {} consecutive hits)'
                    " — resuming normal operation",
                    prior_label,
                    prior_streak,
                )
            return
        else:
            return
        with self.lock:
            self.streak += 1
            if self.streak >= self.threshold and self.warned_label != label:
                logger.warning(
                    '{} consecutive "{}" responses across different proxies — '
                    "likely API-key throttling, not per-IP. "
                    "Proxy rotation will not fix this.",
                    self.streak,
                    label,
                )
                self.warned_label = label


class Downloader:
    """Worker-side per-item logic: dedup, fetch-and-classify, hand to the use case.

    One instance is shared across all `ThreadPoolExecutor` workers — the
    mutable state it owns (`existing_ids`, throttle detector, shutdown
    event) is locked or atomic where it matters.

    Per-item workflow (`download`):
    1. Bail early if the id is already in `existing_ids` (`_already_have`).
    2. Retry loop: acquire a proxy, issue the use case's request, classify
       the response. Authoritative API outcomes terminate; proxy-fault
       outcomes cool the proxy and loop. Caps at
       `min(max_retry_attempts, max(min_retry_attempts, alive_proxies))`
       attempts (config defaults 40 / 10).

    Shutdown
    --------
    `request_shutdown` flips `_shutdown_event` so in-flight workers bail at
    their next attempt boundary instead of grinding through their full retry
    budget (up to `self.config.max_retry_attempts ×
    self.config.request_timeout_sec` per worker). The Ctrl-C path in
    `analyze_data` calls this before `pool.shutdown(wait=True)`.
    """

    def __init__(
        self,
        session: requests.Session,
        proxy_pool: ProxyPool,
        use_case: UseCase,
        existing_ids: set[str],
        stats: Stats,
        config: SwarmConfig,
    ) -> None:
        """Wire the shared session, pool, use case, dedup set, stats, and config."""
        self.config = config
        self.session = session
        self.proxy_pool = proxy_pool
        self.use_case = use_case
        self.existing_ids = existing_ids
        self.stats = stats
        # Guards check-then-add on existing_ids so concurrent workers can't both
        # decide to download the same item. Plain `in`/`.add()` are atomic under
        # the GIL individually; the compound is not.
        self._existing_lock = threading.Lock()
        self._throttle_detector = APIThrottleDetector(
            threshold=self.config.throttle_streak_threshold
        )
        # Set on Ctrl-C so in-flight `download` calls bail at the next attempt
        # boundary instead of grinding through their full retry loop
        # (up to self.config.max_retry_attempts × self.config.request_timeout_sec
        # per worker). Without this, `pool.shutdown(wait=True)`
        # blocks on workers that have no idea the user wants to quit.
        self._shutdown_event = threading.Event()

    def request_shutdown(self) -> None:
        """Signal in-flight workers to bail at their next attempt boundary."""
        # Idempotent — but log once so the operator gets confirmation Ctrl-C registered.
        if not self._shutdown_event.is_set():
            logger.warning(
                "Shutdown requested — workers will bail at next attempt "
                "boundary (~{}s max)",
                self.config.request_timeout_sec,
            )
        self._shutdown_event.set()

    def _already_have(self, item_id: str) -> bool:
        with self._existing_lock:
            return item_id in self.existing_ids

    def _record_have(self, item_id: str) -> None:
        with self._existing_lock:
            self.existing_ids.add(item_id)

    def _post(
        self, proxy: str | None, item_id: str
    ) -> tuple[requests.Response | None, float | None]:
        # Returns (response, elapsed_ms) on a clean HTTP exchange, or (None, None)
        # on a transport-level failure (timeout / conn refused / HTTP error). The
        # proxy is already credited via mark_bad on the None path. `stream=True`
        # so the body isn't eagerly buffered — `UseCase.classify` reads just
        # enough to classify, then closes the response.
        req = self.use_case.build_request(item_id)
        proxies = {"http": proxy, "https": proxy} if proxy else None
        t0 = time.perf_counter()
        try:
            response = self.session.request(
                req.method,
                req.url,
                data=req.data,
                json=req.json_,
                params=req.params,
                headers=req.headers,
                timeout=self.config.request_timeout_sec,
                allow_redirects=True,
                proxies=proxies,
                stream=True,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            self.stats.bump_outcome("timeout")
            self.proxy_pool.mark_bad(proxy, "timeout")
            return None, None
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", "?")
            # raise_for_status() doesn't close the response, and stream=True
            # leaves the connection checked out of the urllib3 pool. Close
            # explicitly so 429/5xx storms don't slowly drain the pool.
            if e.response is not None:
                e.response.close()
            self.stats.bump_outcome("http_error")
            self.proxy_pool.mark_bad(proxy, f"http {code}")
            return None, None
        except requests.exceptions.RequestException as e:
            self.stats.bump_outcome("conn_error")
            self.proxy_pool.mark_bad(proxy, f"conn: {type(e).__name__}")
            return None, None
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return response, elapsed_ms

    def _acquire_with_backoff(self) -> str | None:
        """Acquire a proxy, backing off while the pool is starved.

        Returns the proxy, or None if shutdown was requested during a backoff
        sleep (the caller should stop). Pool starvation waits here rather than
        burning the per-item HTTP-attempt budget — otherwise a worker in a
        trough eats all its attempts on backoff sleeps without ever sending a
        request.
        """
        proxy = self.proxy_pool.acquire()
        miss_count = 0
        while proxy is None:
            miss_count += 1
            # 0.5, 1, 2, 4, 5, 5, ... seconds. `Event.wait` returns True if
            # shutdown fires during the sleep — bail immediately in that case.
            backoff = min(
                self.config.acquire_retry_sleep_sec * (2 ** min(miss_count - 1, 4)),
                self.config.acquire_backoff_cap_sec,
            )
            if self._shutdown_event.wait(backoff):
                return None
            proxy = self.proxy_pool.acquire()
        return proxy

    def _handle_classified(
        self,
        proxy: str | None,
        item_id: str,
        response: requests.Response,
        elapsed_ms: float | None,
        attempt: int,
    ) -> bool:
        """Classify one response and act on it.

        Returns True if the item is finished (success, or an authoritative API
        answer) and the retry loop should stop; False to try another proxy.
        """
        outcome, detail, body = self.use_case.classify(response)
        self.stats.bump_outcome(outcome)
        self._throttle_detector.observe(outcome)

        if outcome == FetchOutcome.OK:
            if self.use_case.handle_success(item_id, body):
                self._record_have(item_id)
                self.proxy_pool.mark_success(proxy, elapsed_ms)
                logger.debug("Got {} via {} (attempt {})", item_id[:16], proxy, attempt)
                self.stats.bump("success")
                return True
            # Use case rejected the body (e.g. corrupt zip) — proxy delivered
            # partial / wrong data. Blame it and try another.
            self.stats.bump_outcome("proxy_garbage")
            self.proxy_pool.mark_bad(proxy, "corrupt body")
            return False
        if outcome == FetchOutcome.NOT_FOUND:
            self.proxy_pool.mark_success(proxy, elapsed_ms)
            logger.info("Item {} not found upstream", item_id[:16])
            self.stats.bump("not_found")
            return True
        if outcome in (FetchOutcome.BAD_QUERY, FetchOutcome.AUTH_ERROR):
            # Both are authoritative answers: credit the proxy (it answered;
            # the query or API key is what's wrong, not the IP) and stop.
            # Crediting keeps the acquire() contract — every reserved attempt
            # pairs with exactly one mark_*.
            self.proxy_pool.mark_success(proxy, elapsed_ms)
            if outcome == FetchOutcome.BAD_QUERY:
                logger.error("Malformed query for {}: {}", item_id[:16], detail)
            else:
                logger.error(
                    "Auth problem ({}) — check credentials, aborting item {}",
                    detail,
                    item_id[:16],
                )
            self.stats.bump("failed")
            return True
        if outcome == FetchOutcome.RATE_LIMITED:
            self.proxy_pool.mark_exhausted(proxy)
            return False
        # PROXY_BAD or PROXY_GARBAGE — try another proxy. `detail` is always a
        # string on this branch (only OK leaves it None), but coalesce
        # defensively so the type checker stays happy too.
        self.proxy_pool.mark_bad(proxy, detail or "unknown")
        return False

    @logger.catch(reraise=False, message="Unhandled exception in download worker")
    def download(self, item_id: str) -> None:
        """Fetch one item via the use case and persist it.

        Increments exactly one of `_metrics['success' | 'skipped' |
        'not_found' | 'failed']` per call. `_outcomes` may be incremented
        multiple times (one per HTTP attempt).

        The `@logger.catch` decorator swallows any unexpected exception —
        ThreadPoolExecutor would otherwise hold it on the future and we'd
        never see it, since `analyze_data` doesn't call `.result()`.
        """
        if self._already_have(item_id):
            logger.trace("Skipping {} (already complete)", item_id[:16])
            self.stats.bump("skipped")
            return

        # Retry loop: free proxies fail constantly (timeouts, conn-refused, HTML
        # interstitials, captive portals). Any non-OK response means "try the
        # next proxy" rather than dropping this item. We only give up when the
        # upstream answers definitively (NOT_FOUND / BAD_QUERY / AUTH_ERROR) or
        # when the pool is fully drained. `max_attempts` budgets *real HTTP
        # attempts* only — starvation backoff waits inside `_acquire_with_backoff`.
        max_attempts = min(
            self.config.max_retry_attempts,
            max(self.config.min_retry_attempts, self.proxy_pool.alive_count()),
        )
        for attempt in range(1, max_attempts + 1):
            if self._shutdown_event.is_set():
                return
            proxy = self._acquire_with_backoff()
            if proxy is None:
                return  # shutdown requested during backoff
            response, elapsed_ms = self._post(proxy, item_id)
            if response is None:
                continue
            if self._handle_classified(proxy, item_id, response, elapsed_ms, attempt):
                return

        logger.error(
            "Exhausted {} attempts for {} ({} proxies alive)",
            max_attempts,
            item_id[:16],
            self.proxy_pool.alive_count(),
        )
        self.stats.bump("failed")
