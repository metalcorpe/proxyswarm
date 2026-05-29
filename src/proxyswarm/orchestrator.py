"""Runtime wiring, the periodic metrics reporter, and the `run` entry point.

This is the framework's composition root: `_build_runtime` loads proxies (from a
`ProxySource`), builds the pool, optionally warm-seeds it from a pre-flight
health check, runs the use case's `prepare` step, and mounts a tuned HTTP
session. `analyze_data` then drives a `ThreadPoolExecutor` of workers through
every item the use case yields, with bounded in-flight submission and a
graceful Ctrl-C path. `run` ties it together.
"""

import contextlib
import math
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import TYPE_CHECKING

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from tqdm import tqdm

from .config import SwarmConfig
from .downloader import Downloader
from .health import check_proxies
from .pool import ProxyPool
from .proxysource import ProxySource, _load_from_source, default_proxy_source
from .stats import Stats

if TYPE_CHECKING:
    from .usecase import UseCase


def _warm_seed_pool(pool: ProxyPool, latencies: dict[str, float]) -> None:
    """Promote pre-flight-verified proxies into the pool's known-good set.

    Each alive proxy is credited via `mark_success` with its measured latency,
    so the fast lane starts populated and EWMA-ordered rather than cold-walking
    the slow lane to rediscover proxies the health check already confirmed.
    """
    for proxy, latency_ms in latencies.items():
        pool.mark_success(proxy, elapsed_ms=latency_ms)


def _maybe_warm_seed(pool: ProxyPool, config: SwarmConfig) -> None:
    """Pre-flight liveness-check the pool's proxies and warm-seed the live ones.

    No-op when `config.health_check_enabled` is False. Otherwise validates every
    loaded proxy concurrently (see `proxyswarm.health.check_proxies`) and feeds
    the confirmed-live ones — with their measured latency — into the fast lane.
    """
    if not config.health_check_enabled:
        return
    latencies = check_proxies(
        pool.proxies,
        test_url=config.health_check_url,
        concurrency=config.health_check_concurrency,
        connect_timeout=config.health_check_connect_timeout_sec,
        read_timeout=config.health_check_read_timeout_sec,
        target_alive=config.health_check_target_alive,
    )
    _warm_seed_pool(pool, latencies)


_SECONDS_PER_MINUTE = 60
_SECONDS_PER_HOUR = 3600


def _fmt_eta(seconds: float) -> str:
    if seconds <= 0 or math.isnan(seconds):  # 0 or NaN
        return "?"
    if seconds < _SECONDS_PER_MINUTE:
        return f"{int(seconds)}s"
    if seconds < _SECONDS_PER_HOUR:
        return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"
    h = int(seconds // _SECONDS_PER_HOUR)
    m = int((seconds % _SECONDS_PER_HOUR) // _SECONDS_PER_MINUTE)
    return f"{h}h{m:02d}m"


def _emit_metrics_tick(
    proxy_pool: ProxyPool,
    stats: Stats,
    threatcount: int,
    start_ts: float,
    last: float,
    last_total: int,
) -> tuple[float, int]:
    """Emit one metrics + outcomes line; return the (time, total) next baseline."""
    now = time.time()
    m, o = stats.snapshot()
    total = sum(m.values())
    rate = (total - last_total) / max(now - last, 1e-6)
    snap = proxy_pool.snapshot_metrics()
    # ETA from cumulative average rate (more stable than the per-interval
    # rate when the pool is bouncing around). `total` only counts samples
    # this analyze_data invocation has processed since the reporter was
    # given the post-baseline snapshot — but it includes baseline carry-
    # over from prior invocations, which slightly under-estimates ETA.
    # Acceptable: ETA is a hint, not a contract.
    avg_rate = total / max(now - start_ts, 1e-6)
    remaining = max(threatcount - total, 0)
    eta_sec = remaining / avg_rate if avg_rate > 0 else 0.0
    pct = (total / threatcount * 100) if threatcount > 0 else 0.0
    logger.info(
        "{:.0f}% ({}/{}) eta={} | rate={:.1f}/s | "
        "ok={} skip={} 404={} fail={} | "
        "fast_lane={} expl={} (med={:.0f}ms p90={:.0f}ms) cooled={} | "
        "gate drain={} prob={} merit={} rej={}",
        pct,
        total,
        threatcount,
        _fmt_eta(eta_sec),
        rate,
        m["success"],
        m["skipped"],
        m["not_found"],
        m["failed"],
        snap.fast_lane,
        snap.lane_explorers,
        snap.lane_med_ms,
        snap.lane_p90_ms,
        snap.cooled,
        snap.gate["drain"],
        snap.gate["probation"],
        snap.gate["merit"],
        snap.gate["reject"],
    )
    # Per-attempt outcomes — surfaces *why* attempts fail. `rate_limited` here
    # is the canary for API-key throttling (vs IP throttling); see
    # APIThrottleDetector.
    logger.info(
        "outcomes | ok={} 404={} bad={} auth={} rate_lim={} "
        "proxy_bad={} garbage={} timeout={} http={} conn={}",
        o["ok"],
        o["not_found"],
        o["bad_query"],
        o["auth_error"],
        o["rate_limited"],
        o["proxy_bad"],
        o["proxy_garbage"],
        o["timeout"],
        o["http_error"],
        o["conn_error"],
    )
    return now, total


def _metrics_reporter(
    config: SwarmConfig,
    stop_event: threading.Event,
    proxy_pool: ProxyPool,
    stats: Stats,
    threatcount: int,
    start_ts: float,
) -> None:
    last = time.time()
    last_total = 0
    while not stop_event.wait(config.metrics_interval_sec):
        # Daemon thread — logger.catch swallows + logs any tick error (bad
        # format-arg, pool mutation mid-snapshot) so it survives to the next
        # interval instead of dying silently for the rest of the run. On error
        # the (last, last_total) baseline is left unchanged.
        with logger.catch(message="metrics-reporter iteration failed; continuing"):
            last, last_total = _emit_metrics_tick(
                proxy_pool, stats, threatcount, start_ts, last, last_total
            )


def analyze_data(
    downloader: Downloader,
    use_case: UseCase,
    config: SwarmConfig,
    *,
    dry_run: bool = False,
) -> None:
    """Drive the worker pool through every item the use case yields.

    With `dry_run=True`, log the item count and return without launching the
    pool — useful for previewing a use case's work set before committing
    proxy + disk to it.

    Drives a `ThreadPoolExecutor` of `config.workers` workers with bounded
    in-flight submission (~4× worker count) to keep memory flat regardless
    of the use case's cardinality. On Ctrl-C, signals workers to bail,
    flushes the proxy state, logs an interrupted-run summary, and re-raises
    so the caller sees `KeyboardInterrupt`.
    """
    items = list(use_case.iter_items())
    # Shuffle so restarts and concurrent workers don't keep hammering the same
    # clustered slice. Source-ordered work (e.g. a CSV sorted by first-seen) tends
    # to correlate with shared upstream conditions; random order amortises that.
    random.shuffle(items)
    itemcount = len(items)
    logger.info('{} items to fetch for "{}"', itemcount, use_case.name)
    if dry_run:
        logger.info("--dry-run: skipping download phase")
        return
    logger.info(
        "Starting parallel download of {} items with {} threads",
        itemcount,
        config.workers,
    )

    # Snapshot counters at the start so the end-of-run summary reflects *this*
    # run's contribution, not the lifetime totals (matters if analyze_data is
    # invoked more than once in a single process).
    stats = downloader.stats
    baseline, _ = stats.snapshot()
    start_ts = time.time()

    stop_metrics = threading.Event()
    reporter = threading.Thread(
        target=_metrics_reporter,
        args=(config, stop_metrics, downloader.proxy_pool, stats, itemcount, start_ts),
        name="metrics-reporter",
        daemon=True,
    )
    reporter.start()

    # Bounded submission: keep ~4x worker count in flight rather than queuing all
    # 100k+ futures up front (which holds the full items list + closures in memory
    # and gives no scheduling benefit).
    pool = ThreadPoolExecutor(max_workers=config.workers)
    in_flight = set()
    it = iter(items)
    interrupted = False
    try:
        for _ in range(min(config.workers * 4, itemcount)):
            try:
                in_flight.add(pool.submit(downloader.download, next(it)))
            except StopIteration:
                break
        with tqdm(total=itemcount) as pbar:
            while in_flight:
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for _ in done:
                    pbar.update(1)
                    with contextlib.suppress(StopIteration):
                        in_flight.add(pool.submit(downloader.download, next(it)))
    except KeyboardInterrupt:
        # Signal workers to bail at their next attempt boundary, then let
        # `finally` perform the bounded wait. The `raise` here re-raises *after*
        # `finally` runs, so the caller still sees KeyboardInterrupt.
        interrupted = True
        logger.warning(
            "Interrupted — signaling workers to bail and flushing proxy state"
        )
        downloader.request_shutdown()
        raise
    finally:
        stop_metrics.set()
        # With `_shutdown_event` set, in-flight workers exit within ~one HTTP
        # timeout (config.request_timeout_sec) instead of grinding all
        # config.max_retry_attempts. `cancel_futures` drops
        # anything still queued. On the clean path (no interrupt) there's
        # nothing queued or running, so this is effectively a no-op.
        pool.shutdown(cancel_futures=True, wait=True)
        if interrupted:
            downloader.proxy_pool.save_state()
        # End-of-run summary — counts only this invocation's progress. Useful
        # on both clean exit and Ctrl-C (the `raise` above runs *after* finally,
        # so the operator sees the summary right before the traceback).
        elapsed = time.time() - start_ts
        m_now, _ = stats.snapshot()
        m = {k: m_now[k] - baseline[k] for k in m_now}
        processed = sum(m.values())
        rate = processed / max(elapsed, 1e-6)
        level = logger.warning if interrupted else logger.success
        level(
            '{} "{}" in {:.1f}s ({:.1f}/s avg) — '
            "ok={} 404={} fail={} skipped={} | processed={}/{}",
            "Interrupted" if interrupted else "Finished",
            use_case.name,
            elapsed,
            rate,
            m["success"],
            m["not_found"],
            m["failed"],
            m["skipped"],
            processed,
            itemcount,
        )


def _log_startup_config(config: SwarmConfig) -> None:
    # One-shot banner so the operator can sanity-check tuning without grepping
    # the source. Mirrors the knobs at the top of this file.
    logger.info(
        "Config: workers={} top_k={} (explore={:.0%}, max_attempts<{}) timeout={}s "
        "probation={} drain<{:.0%} gate=p{:.0f} streak={} retry_sleep={}s "
        "refresh_every={} slow_budget={} ip_cap={}/day",
        config.workers,
        config.top_k_fast_lane,
        config.exploration_fraction,
        config.exploration_max_attempts,
        config.request_timeout_sec,
        config.probation_threshold,
        config.lane_drain_fraction,
        config.lane_gate_percentile * 100,
        config.throttle_streak_threshold,
        config.acquire_retry_sleep_sec,
        config.refresh_interval,
        config.slow_lane_budget,
        config.daily_per_ip_limit,
    )


def _build_runtime(
    use_case: UseCase,
    config: SwarmConfig,
    proxy_source: ProxySource | None = None,
) -> Downloader:
    """Wire up and return the `Downloader` runtime.

    Loads proxies (from `proxy_source`, defaulting to the file→scrape chain for
    `config.proxies_file`), builds the pool, runs the use case's `prepare` step,
    snapshots its `existing_ids`, and mounts a tuned HTTP session with the use
    case's headers. Logs the startup config banner as a side effect so the
    first thing in the log file is the tuning state.
    """
    _log_startup_config(config)
    if proxy_source is None:
        proxy_source = default_proxy_source(config.proxies_file)
    proxy_pool = ProxyPool(
        _load_from_source(proxy_source), config, state_file=config.proxy_state_file
    )
    _maybe_warm_seed(proxy_pool, config)

    use_case.prepare()
    existing_ids = use_case.existing_ids()

    # Shared session. Each worker talks to a different proxy upstream, so the connection
    # pool needs headroom beyond `config.workers` — otherwise urllib3 keeps closing and
    # reopening sockets as workers rotate proxies.
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=config.workers * 4, pool_maxsize=config.workers * 4
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(use_case.session_headers())

    return Downloader(session, proxy_pool, use_case, existing_ids, Stats(), config)


def run(
    use_case: UseCase,
    config: SwarmConfig | None = None,
    *,
    proxy_source: ProxySource | None = None,
    dry_run: bool = False,
) -> None:
    """Entry point for running a use case.

    Pass a custom `proxy_source` (any object implementing `ProxySource`) to
    supply proxies from somewhere other than the default file→scrape chain.
    """
    if config is None:
        config = SwarmConfig()
    downloader = _build_runtime(use_case, config, proxy_source)
    with contextlib.suppress(KeyboardInterrupt):
        analyze_data(downloader, use_case, config, dry_run=dry_run)
