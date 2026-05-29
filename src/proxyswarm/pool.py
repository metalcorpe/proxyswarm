"""Thread-safe proxy scheduler with a fast/slow-lane discovery model.

Free proxy lists are mostly garbage (timeouts, captive portals, dead hosts) with
a small reliable tail. The pool keeps a **fast lane** — a deque of the top-K
proxies by score (EWMA latency / EWMA success rate) — and falls back to a
bounded **slow lane** scan for discovery. A configurable fraction of the fast
lane is reserved for **exploration** (under-sampled proxies that get a chance
to climb into the scored top), so the pool self-heals as conditions change
instead of being locked to a one-shot verdict on each proxy. State is persisted
to disk so a restart skips the cold-start cost of rediscovering which proxies work.
"""

import threading
import time
from collections import deque
from typing import TYPE_CHECKING, ClassVar, NamedTuple, TypedDict

from loguru import logger

from .state import (
    ProxyStateStore,
    ProxyStats,
    _next_utc_midnight_ts,
    _proxy_key,
    _today_utc,
)

if TYPE_CHECKING:
    from .config import SwarmConfig


class GateCounters(TypedDict):
    """Cumulative opportunistic-insert gate tallies (see field comments)."""

    # Cumulative tally of which branch admitted/rejected each opportunistic
    # insert (see `ProxyPool._can_opportunistic_insert`). A fixed key set so the
    # reporter's bracket reads (`gate["drain"]` etc.) are type-checked.
    drain: int
    probation: int
    merit: int
    reject: int


class MetricsSnapshot(NamedTuple):
    """Periodic-reporter view of the pool (see field comments)."""

    # Periodic-reporter view of the pool. Named so the reporter doesn't have to
    # remember a 5-tuple's positional order — and so future fields can land
    # without breaking every caller.
    fast_lane: int
    lane_explorers: int  # count of lane slots currently holding exploration candidates
    lane_med_ms: float
    lane_p90_ms: float
    cooled: int
    gate: GateCounters


class ProxyPool:
    """Thread-safe proxy scheduler with a fast/slow-lane discovery model.

    Concurrency model
    -----------------
    A single `self.lock` guards all mutable pool state — `state`, `good*`,
    `idx`, `explorer_idx`, `exhausted`, `_gate_counters`, the update counter,
    and the starvation trackers. Methods with a leading underscore expect the
    caller to already hold the lock; public methods take it themselves.

    Fast lane
    ---------
    `good` is a deque of up to `self.config.top_k_fast_lane` proxies ordered by score
    (`_score`, lower=better). `acquire` rotates through it in O(K). The lane
    is rebuilt periodically from `good_candidates` (the full known-good
    superset) every `self.config.refresh_interval` successes, and on-demand by `acquire`
    when the lane drains mid-rotation.

    Exploration slots
    -----------------
    `self.config.exploration_fraction` of the lane is filled by `_pick_explorers` with
    under-sampled proxies (untested first, then `attempts <
    self.config.exploration_max_attempts`) instead of by score. Workers rotate through
    them the same way as exploitation slots, so an explorer that succeeds
    gets promoted into `good_candidates` via `mark_success` and competes for
    the scored top on the next refresh. Without this, a proxy that failed
    once early gets a permanently-bad EWMA and never gets re-tested.

    Slow lane
    ---------
    If the fast lane finds nothing, a bounded round-robin walk of length
    `self.config.slow_lane_budget` over `self.proxies` looks for a usable entry. Bounded
    because the full list is ~305k and the hit rate is ~0.15% — unbounded
    scans would burn lock time. When this also fails, `acquire` returns None
    and the caller is expected to sleep + retry.

    Cooldown vs retirement
    ----------------------
    `mark_bad` puts a proxy on exponential-backoff cooldown — recoverable.
    `mark_exhausted` permanently retires it for the run (hit per-IP daily
    cap). The persisted state file uses the cooldown timestamp + a long-term
    failure-rate heuristic (`_in_cooldown`) so a flaky proxy can rejoin
    after 24h idle.
    """

    def __init__(
        self,
        proxies: list[str | None],
        config: SwarmConfig,
        state_file: str | None = None,
    ) -> None:
        """Build the pool, load+prune persisted state, and seed the fast lane."""
        self.config = config
        # None represents "no proxy" (your real IP) — drop it if you don't want that.
        self.proxies = list(proxies) or [None]
        self.exhausted: set[str | None] = (
            set()
        )  # hard rate-limit retirements (mark_exhausted only)
        # Fast lane: deque of the top-K known-good proxies by score (lower=faster &
        # more reliable). `good_candidates` is the full pool of ever-succeeded
        # proxies; `_refresh_good` rebuilds the deque from the top-K of that pool
        # every self.config.refresh_interval successes. `good_set` is an O(1)
        # membership guard.
        self.good: deque[str | None] = deque()
        self.good_set: set[str | None] = set()
        self.good_candidates: set[str | None] = set()
        self._update_counter = 0
        # Cumulative counts of which gate branch admitted (or rejected) each
        # opportunistic insert. Bumped under self.lock from `_can_opportunistic_insert`.
        # If `reject` stays near zero, the gate isn't filtering; if `probation`
        # dominates indefinitely, EWMAs aren't converging — both are signals to retune.
        self._gate_counters: GateCounters = {
            "drain": 0,
            "probation": 0,
            "merit": 0,
            "reject": 0,
        }
        self.lock = threading.Lock()
        self.idx = 0
        # Independent cursor for the exploration walk in `_refresh_good`. Advances
        # each refresh so successive rebuilds sample fresh slices of the (shuffled)
        # proxy list instead of re-hitting the same prefix.
        self.explorer_idx = 0
        # Starvation tracking: if `acquire()` returns None continuously for
        # self.config.starvation_warn_sec, the pool is effectively dead and the operator
        # needs to know (proxies cooled, key throttled, etc). One warning per
        # starvation episode — cleared on the next successful acquire.
        self._last_acquire_success_ts = time.time()
        self._starve_warned = False
        # Reverse mapping so _seed_good_from_state can iterate the (small) persisted
        # state instead of walking the (~305k) full proxy list.
        self._key_to_proxy = {_proxy_key(p): p for p in self.proxies}

        # Load + prune persisted state, then hand the shared dict to the store.
        valid_keys = set(self._key_to_proxy)
        raw = ProxyStateStore.load(state_file)
        # json.load gives untyped dicts. Coerce each onto a fresh _empty_stats so
        # the "every key present, right type" invariant the bracket-access read
        # sites rely on is actually true — a stale/hand-edited/older-schema
        # proxy_state.json can't smuggle in a missing or wrong-typed key that
        # would later KeyError inside a worker. (The previous `cast` was a no-op
        # at runtime and enforced nothing.)
        self.state: dict[str, ProxyStats] = {
            k: self._coerce_stats(v)
            for k, v in raw.items()
            if k in valid_keys and isinstance(v, dict)
        }
        self._store = ProxyStateStore(
            state_file, self.state, self.lock, config=self.config
        )

        self._seed_good_from_state()
        if state_file:
            now = time.time()
            cooled = sum(
                1 for s in self.state.values() if s.get("cooldown_until", 0) > now
            )
            logger.info(
                "Loaded persisted state for {} proxies "
                "({} still on cooldown, {} seeded into fast-lane)",
                len(self.state),
                cooled,
                len(self.good),
            )
            self._store.start_background_saver()

    def save_state(self) -> None:
        """Flush persisted proxy state to disk (public alias for the store)."""
        # Public alias preserved for callers (e.g. KeyboardInterrupt handler).
        self._store.save()

    def _seed_good_from_state(self) -> None:
        # Treat the persisted state as a free smoke test: any proxy that has
        # succeeded at least once and isn't currently cooling joins the candidate
        # pool. The fast lane gets the top-K by score.
        now = time.time()
        for key, s in self.state.items():
            p = self._key_to_proxy[key]  # prune in __init__ guarantees this exists
            if s.get("last_success_ts", 0) > 0 and s.get("cooldown_until", 0) <= now:
                self.good_candidates.add(p)
        self._refresh_good()

    def _score(self, proxy: str | None) -> float:
        # Lower is better. Untested proxies get a moderate penalty so timed ones
        # rank above them. Floor the success rate so a momentarily-flaky proxy
        # isn't divided into oblivion.
        s = self._get(proxy)
        ms = s.get("ewma_ms", 0.0) or self.config.untested_latency_ms
        succ = max(s.get("ewma_success", 1.0), 0.05)
        return ms / succ

    def _refresh_good(self) -> None:
        # Caller holds the lock. Splits self.config.top_k_fast_lane between
        # exploitation (top
        # by score from good_candidates) and exploration (under-sampled proxies
        # from the wider pool — see `_pick_explorers`). Any unused exploitation
        # budget (good_candidates smaller than the budget) rolls into exploration
        # so the lane stays full while the pool is still being discovered.
        now = time.time()
        usable = [
            p for p in self.good_candidates if not self._in_cooldown(self._get(p), now)
        ]
        usable.sort(key=self._score)
        exploration_slots = int(
            self.config.top_k_fast_lane * self.config.exploration_fraction
        )
        exploitation_budget = self.config.top_k_fast_lane - exploration_slots
        exploitation = usable[:exploitation_budget]
        leftover = exploitation_budget - len(exploitation)
        explorers = self._pick_explorers(exploration_slots + leftover, now)
        lane = exploitation + explorers
        self.good = deque(lane)
        self.good_set = set(lane)

    def _pick_explorers(self, n: int, now: float) -> list[str | None]:
        # Caller holds the lock. Walks the (shuffled) proxy list from
        # `explorer_idx`, collecting up to `n` entries that are under-sampled and
        # not currently cooled. `attempts < self.config.exploration_max_attempts` lets a
        # proxy that failed once early get re-rolled — the user's "give touched
        # proxies a chance to escalate" path. Walk is bounded so a late-run pool
        # (everything touched, everything cooled) doesn't iterate all ~305k under
        # the lock.
        if n <= 0 or not self.proxies:
            return []
        out: list[str | None] = []
        walked = 0
        budget = min(n * self.config.exploration_walk_multiplier, len(self.proxies))
        while len(out) < n and walked < budget:
            p = self.proxies[self.explorer_idx]
            self.explorer_idx = (self.explorer_idx + 1) % len(self.proxies)
            walked += 1
            if p in self.exhausted or p in self.good_candidates:
                continue
            # `state.get` (not `_get`) so the walk doesn't mint empty entries for
            # every untested proxy — those would persist into proxy_state.json.
            s = self.state.get(_proxy_key(p))
            if s is not None:
                if s["attempts"] >= self.config.exploration_max_attempts:
                    continue
                if self._in_cooldown(s, now):
                    continue
            out.append(p)
        return out

    def _lane_score_threshold(self) -> float | None:
        # Caller holds the lock. Returns the score at
        # self.config.lane_gate_percentile of the
        # current lane — the bar a candidate must beat to earn an opportunistic
        # spot. Returns None when the lane is empty (caller treats as "no bar").
        if not self.good:
            return None
        scores = sorted(self._score(p) for p in self.good)
        idx = max(0, int(self.config.lane_gate_percentile * len(scores)) - 1)
        return scores[idx]

    def _can_opportunistic_insert(self, proxy: str | None) -> bool:
        # Caller holds the lock. Admit on any of:
        #   (a) Drain mode — lane is depleted; throughput beats purity.
        #   (b) Probation — under-sampled proxies need a few rides for their
        #       EWMA to converge before the score-gate can rank them fairly.
        #       Without this, a once-timed slow proxy would never get re-tested.
        #   (c) Meritocratic — score beats the lane's p90.
        if len(self.good) < int(
            self.config.top_k_fast_lane * self.config.lane_drain_fraction
        ):
            self._gate_counters["drain"] += 1
            return True
        s = self._get(proxy)
        if s["attempts"] - s["failures"] < self.config.probation_threshold:
            self._gate_counters["probation"] += 1
            return True
        threshold = self._lane_score_threshold()
        if threshold is None or self._score(proxy) <= threshold:
            self._gate_counters["merit"] += 1
            return True
        self._gate_counters["reject"] += 1
        return False

    @staticmethod
    def _empty_stats() -> ProxyStats:
        return {
            "attempts": 0,
            "failures": 0,
            "last_reason": None,
            "last_failure_ts": 0,
            "consecutive_failures": 0,
            "cooldown_until": 0,
            "usage_today": 0,
            "usage_date": "",
            "last_success_ts": 0,
            "ewma_ms": 0.0,  # 0 = never measured
            "ewma_success": 1.0,  # optimistic; decays toward 0 on failure
        }

    # Accepted runtime types per ProxyStats field, for coercing disk-loaded data.
    # Numeric fields accept int|float because timestamps start as int 0 in
    # _empty_stats but become floats once written (time.time()); JSON round-trips
    # both. last_reason is str|None.
    _STATS_FIELD_TYPES: ClassVar[dict[str, tuple[type, ...]]] = {
        "attempts": (int,),
        "failures": (int,),
        "last_reason": (str,),
        "last_failure_ts": (int, float),
        "consecutive_failures": (int,),
        "cooldown_until": (int, float),
        "usage_today": (int,),
        "usage_date": (str,),
        "last_success_ts": (int, float),
        "ewma_ms": (int, float),
        "ewma_success": (int, float),
    }

    @classmethod
    def _coerce_stats(cls, raw: dict) -> ProxyStats:
        # Merge a loaded entry onto a fresh _empty_stats, copying over only keys
        # present with an accepted type. Missing keys keep their default; wrong-
        # typed or extra keys are dropped. `bool` is rejected for int fields
        # (bool is an int subclass, but a JSON `true` is never a valid count).
        base = cls._empty_stats()
        for field, types in cls._STATS_FIELD_TYPES.items():
            value = raw.get(field)
            if value is None or isinstance(value, bool):
                continue
            if isinstance(value, types):
                # `field` is a dynamic str, but it's always a real ProxyStats key
                # (it comes from _STATS_FIELD_TYPES, whose keys mirror the
                # TypedDict). The checker can't prove that for a non-literal key.
                base[field] = value  # ty: ignore[invalid-key]
        return base

    def _get(self, proxy: str | None) -> ProxyStats:
        key = _proxy_key(proxy)
        s = self.state.get(key)
        if s is None:
            s = self._empty_stats()
            self.state[key] = s
        return s

    def _update_ewma(
        self, stat: ProxyStats, sample_ms: float | None = None, *, success: bool = True
    ) -> None:
        # Latency EWMA: seed directly on first observation; the 0.0 sentinel
        # would otherwise pull the EWMA toward zero.
        if sample_ms is not None and sample_ms > 0:
            cur_ms = stat.get("ewma_ms", 0.0)
            stat["ewma_ms"] = (
                sample_ms
                if cur_ms <= 0
                else (1 - self.config.ewma_alpha_latency) * cur_ms
                + self.config.ewma_alpha_latency * sample_ms
            )
        cur_succ = stat.get("ewma_success", 1.0)
        target = 1.0 if success else 0.0
        stat["ewma_success"] = (
            1 - self.config.ewma_alpha_success
        ) * cur_succ + self.config.ewma_alpha_success * target

    def _cooldown_for(self, reason: str, consecutive: int) -> float:
        # Daily-cap / rate-limit → cool until UTC rollover. Assumes the upstream's
        # quota window resets at UTC midnight (true for most rate-limited APIs).
        # Plugins with a non-daily reset cadence should keep rate_limited proxies
        # in their own classify path rather than relying on this default.
        if reason in ("daily cap", "rate limit hit"):
            return _next_utc_midnight_ts()
        # Exponential backoff per consecutive failure, capped at 24h.
        return time.time() + min(
            self.config.cooldown_max_sec,
            self.config.cooldown_base_sec * (2 ** max(0, consecutive - 1)),
        )

    def _in_cooldown(self, s: ProxyStats, now: float) -> bool:
        if s["cooldown_until"] > now:
            return True
        # Long-term bad: high failure rate after enough attempts and the last failure
        # was within the last day → keep cooling. Lets a flaky proxy recover
        # after 24h idle.
        if s["attempts"] >= self.config.min_attempts_for_rate:
            rate = s["failures"] / s["attempts"]
            if (
                rate >= self.config.high_failure_rate
                and (now - s["last_failure_ts"]) < self.config.cooldown_max_sec
            ):
                return True
        return False

    def _reset_daily(self, s: ProxyStats) -> None:
        today = _today_utc()
        if s["usage_date"] != today:
            s["usage_date"] = today
            s["usage_today"] = 0

    def _reserve(self, p: str | None, now: float) -> str | None:
        # Caller holds the lock. Returns p if it can be used, else None.
        if p in self.exhausted:
            return None
        s = self._get(p)
        self._reset_daily(s)
        if self._in_cooldown(s, now):
            return None
        if s["usage_today"] >= self.config.daily_per_ip_limit:
            s["cooldown_until"] = _next_utc_midnight_ts()
            s["last_reason"] = "daily cap"
            self._store.mark_dirty()
            return None
        s["attempts"] += 1
        s["usage_today"] += 1
        self._store.mark_dirty()
        return p

    def _try_fast_lane(self, now: float) -> str | None:
        # Caller holds the lock. Rotates through the fast-lane deque, returning the
        # first proxy that reserves successfully. Proxies that fail to reserve fall
        # out of the deque — they're only re-added by `mark_success`.
        for _ in range(len(self.good)):
            p = self.good.popleft()
            self.good_set.discard(p)
            if self._reserve(p, now) is not None:
                self.good.append(p)
                self.good_set.add(p)
                return p
        return None

    def acquire(self) -> str | None:
        """Reserve a proxy for one request, or return None if nothing is usable.

        On success the proxy's `attempts` and `usage_today` are pre-incremented;
        the caller must follow up with exactly one of `mark_success`,
        `mark_bad`, or `mark_exhausted` (the `_post` + `_classify_response`
        path in `Downloader.download` does this).

        Lock strategy: the fast lane runs under one acquire/release. The slow
        lane releases and re-acquires between self.config.slow_lane_batch-sized
        chunks so
        50 workers don't serialize on a 100-iter discovery walk — a worker
        holding the lock for the full scan blocks 49 other workers' fast-lane
        rotations entirely. Three lock acquires on a full miss; one or two on
        success.
        """
        # Fast lane: usually a hit on the first call.
        with self.lock:
            now = time.time()
            p = self._try_fast_lane(now)
            if p is None and self.good_candidates:
                # Refill from `good_candidates` (the full known-good pool, ~500
                # entries) and try again. Sorting 500 items is microseconds;
                # walking 305k under lock is not.
                self._refresh_good()
                p = self._try_fast_lane(now)
            if p is not None:
                self._last_acquire_success_ts = now
                self._starve_warned = False
                return p

        # Slow lane: bounded discovery walk in batches that release the lock
        # between chunks. ~0.15% hit rate on the full list means an unbounded
        # scan is wasted time — and holding the lock through 100 iterations
        # stalls every other worker's fast-lane rotation.
        remaining = self.config.slow_lane_budget
        while remaining > 0:
            with self.lock:
                now = time.time()
                batch = min(self.config.slow_lane_batch, remaining, len(self.proxies))
                for _ in range(batch):
                    cand = self.proxies[self.idx]
                    self.idx = (self.idx + 1) % len(self.proxies)
                    if self._reserve(cand, now) is not None:
                        self._last_acquire_success_ts = now
                        self._starve_warned = False
                        return cand
                remaining -= batch

        # Total miss — emit a single starvation warning per episode.
        starve_for: float | None = None
        with self.lock:
            elapsed = time.time() - self._last_acquire_success_ts
            if elapsed > self.config.starvation_warn_sec and not self._starve_warned:
                self._starve_warned = True
                starve_for = elapsed
        if starve_for is not None:
            logger.warning(
                "Proxy pool starvation: no acquirable proxy for {:.0f}s — "
                "all proxies cooled/exhausted",
                starve_for,
            )
        return None

    def mark_success(self, proxy: str | None, elapsed_ms: float | None = None) -> None:
        """Credit a clean exchange.

        Clears cooldown, updates EWMAs, and possibly promotes the proxy into
        the fast lane (subject to the gate).
        """
        with self.lock:
            s = self._get(proxy)
            s["consecutive_failures"] = 0
            s["cooldown_until"] = 0
            s["last_success_ts"] = time.time()
            self._update_ewma(s, sample_ms=elapsed_ms, success=True)
            self.good_candidates.add(proxy)
            # Opportunistic insert: if there's room in the fast lane AND the
            # candidate clears the score-gate (or is in drain/probation), take
            # the slot. The gate stops slow proxies from polluting the lane
            # between periodic refreshes — see `_can_opportunistic_insert`.
            if (
                proxy not in self.good_set
                and len(self.good) < self.config.top_k_fast_lane
                and self._can_opportunistic_insert(proxy)
            ):
                self.good.append(proxy)
                self.good_set.add(proxy)
            self._update_counter += 1
            if self._update_counter >= self.config.refresh_interval:
                self._update_counter = 0
                self._refresh_good()
            self._store.mark_dirty()

    def mark_exhausted(self, proxy: str | None) -> None:
        """Permanently retire a proxy for the rest of the run.

        Triggered by hitting the upstream's per-IP / per-key daily cap. Cools
        until UTC midnight in the persisted state so it rejoins automatically
        tomorrow.
        """
        with self.lock:
            if proxy not in self.exhausted:
                self.exhausted.add(proxy)
                s = self._get(proxy)
                s["last_reason"] = "rate limit hit"
                s["cooldown_until"] = _next_utc_midnight_ts()
                self._store.mark_dirty()
                logger.warning(
                    "Proxy {} retired (rate limited at {} reqs today)",
                    proxy,
                    s["usage_today"],
                )

    def mark_bad(self, proxy: str | None, reason: str) -> None:
        """Cool a proxy after a transport- or proxy-fault outcome.

        Cooldown only — don't permanently retire. A transient timeout
        shouldn't kill a usable proxy for the rest of the session;
        `_in_cooldown` + the exponential backoff in `_cooldown_for` already
        gates reuse. `reason` is free-form text stored on the stats entry
        for postmortem inspection of `proxy_state.json`.
        """
        with self.lock:
            s = self._get(proxy)
            s["failures"] += 1
            s["consecutive_failures"] += 1
            s["last_reason"] = reason
            s["last_failure_ts"] = time.time()
            s["cooldown_until"] = self._cooldown_for(reason, s["consecutive_failures"])
            self._update_ewma(s, success=False)
            self._store.mark_dirty()
            logger.trace(
                "Proxy {} cooled {}s: {} (consec={}, rate={:.0%})",
                proxy,
                int(s["cooldown_until"] - time.time()),
                reason,
                s["consecutive_failures"],
                s["failures"] / max(s["attempts"], 1),
            )

    def alive_count(self) -> int:
        """Return the number of proxies not permanently retired."""
        with self.lock:
            return len(self.proxies) - len(self.exhausted)

    def snapshot_metrics(self) -> MetricsSnapshot:
        """Periodic-reporter view of the pool.

        Computed under the lock so the reporter doesn't have to reach into
        pool internals. The `gate` field is a *cumulative* counter, not a
        per-interval delta — the reporter shows trends rather than rates.
        """
        with self.lock:
            now = time.time()
            fast_lane = len(self.good)
            # Match what `_refresh_good` actually filters out — both the explicit
            # cooldown_until and the long-term high-failure-rate heuristic. Just
            # checking cooldown_until would under-count what's being held back.
            cooled = sum(1 for s in self.state.values() if self._in_cooldown(s, now))
            lanes_ms = sorted(self._get(p).get("ewma_ms", 0.0) for p in self.good)
            if lanes_ms:
                lane_med = lanes_ms[len(lanes_ms) // 2]
                p90_idx = max(0, int(0.9 * len(lanes_ms)) - 1)
                lane_p90 = lanes_ms[p90_idx]
            else:
                lane_med = 0.0
                lane_p90 = 0.0
            # Count lane slots currently holding under-sampled proxies. Reading
            # state without `_get` so we don't materialize entries here either.
            lane_explorers = 0
            for p in self.good:
                s = self.state.get(_proxy_key(p))
                if s is None or s["attempts"] < self.config.exploration_max_attempts:
                    lane_explorers += 1
            gate: GateCounters = {
                "drain": self._gate_counters["drain"],
                "probation": self._gate_counters["probation"],
                "merit": self._gate_counters["merit"],
                "reject": self._gate_counters["reject"],
            }
        return MetricsSnapshot(
            fast_lane, lane_explorers, lane_med, lane_p90, cooled, gate
        )
