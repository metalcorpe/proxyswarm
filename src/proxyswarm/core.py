import json
import atexit
import random
import argparse
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from enum import StrEnum
from typing import Iterator, NamedTuple, Protocol, TypedDict
from tqdm import tqdm
from loguru import logger
import os
import time
import requests
from requests.adapters import HTTPAdapter

from .config import SwarmConfig

"""Free-proxy-pool bulk fetcher with a pluggable use case.

Architecture
------------
The framework owns three pieces: a self-tuning `ProxyPool`, a generic
`Downloader` worker, and an orchestrator (`analyze_data`). Everything
API- and content-specific lives in a `UseCase` plugin — what work items
to fetch, how to build the HTTP request, how to classify the response,
and what to do with a successful body. The shipped `MalwareBazaarUseCase`
implements the original behaviour: fetch sample SHAs from a Bazaar CSV
index, POST to Bazaar's API, and extract the AES zip.

1. `UseCase.iter_items` yields work item ids; `UseCase.existing_ids` seeds
   the resume/dedup set.
2. `analyze_data` materialises and dispatches each id to a thread-pool of
   `Downloader.download` workers.
3. Each worker borrows a proxy from `ProxyPool`, posts the request from
   `UseCase.build_request`, hands the response to `UseCase.classify`, and
   either calls `UseCase.handle_success` on the body or loops to the next
   proxy on a proxy-fault outcome.

To add a new use case, implement the `UseCase` protocol (see its docstring),
then run it with `run(my_use_case, config=...)`. See `examples/malware_bazaar.py`
for a CLI wrapper (`main`/`_parse_args`) you can model your own entry point on.

ProxyPool design
----------------
Free proxy lists are mostly garbage (timeouts, captive portals, dead hosts) with
a small reliable tail. The pool keeps a **fast lane** — a deque of the top-K
proxies by score (EWMA latency / EWMA success rate) — and falls back to a
bounded **slow lane** scan for discovery. A configurable fraction of the fast
lane is reserved for **exploration** (under-sampled proxies that get a chance
to climb into the scored top), so the pool self-heals as conditions change
instead of being locked to a one-shot verdict on each proxy. State is persisted
to disk so a restart skips the cold-start cost of rediscovering which proxies work.

A single per-proxy lock guards both the in-memory state and a background
flush thread that writes `proxy_state.json` every `self.config.save_interval_sec`. The
state dict's shape is pinned by the `ProxyStats` TypedDict.

Throttling vs proxy fault
-------------------------
Many APIs (Bazaar included) return HTTP 200 + an in-band error payload on
app-level errors, so transport success != API success. The use case's
`classify` method decodes the response into a `FetchOutcome` so the
framework can distinguish three cases:
  * **API answered authoritatively** (ok / not_found / bad_query / auth_error)
    — finish, don't retry.
  * **API throttled this proxy** (rate_limited)
    — retire it for the day, retry on a different one.
  * **Proxy itself was the problem** (proxy_bad / proxy_garbage / timeout /
    conn_error / http_error) — cool the proxy on backoff, retry.

`APIThrottleDetector` watches for streaks of identical authoritative
throttle responses across different proxies — that pattern implicates the
API *key*, which proxy rotation can't fix.

Operational signals
-------------------
* Startup banner logs every tuning knob — see `_log_startup_config`.
* `metrics-reporter` daemon prints throughput + lane health every
  `self.config.metrics_interval_sec`.
* `proxy-state-saver` daemon flushes state every `self.config.save_interval_sec`.
* End-of-run summary line is emitted from `analyze_data`'s `finally` block on
  both clean exit and Ctrl-C, with per-invocation (not lifetime) counters.
"""

# ---------------------------------------------------------------------------


# Outcome class for every HTTP attempt. OK / NOT_FOUND / BAD_QUERY / AUTH_ERROR
# are authoritative API answers — the loop is finished. RATE_LIMITED retires
# the proxy and retries. PROXY_BAD / PROXY_GARBAGE cool the proxy and retry.
#
# `StrEnum` lets members double as dict keys (each `.value` is a `Stats`
# outcome-counter key — see `Stats._OUTCOME_KEYS`, which is derived from this
# enum) and as direct equality targets in `if outcome == ...` checks.
class FetchOutcome(StrEnum):
    OK = "ok"  # body is the payload — call handle_success
    NOT_FOUND = "not_found"  # upstream doesn't have this item
    BAD_QUERY = "bad_query"  # malformed query; no point retrying
    AUTH_ERROR = "auth_error"  # API key / auth problem; no point retrying
    RATE_LIMITED = "rate_limited"  # this proxy hit a quota; retire and retry
    PROXY_BAD = "proxy_bad"  # unknown/non-definitive status; retry
    PROXY_GARBAGE = (
        "proxy_garbage"  # body wasn't protocol-shaped (HTML / captive portal); retry
    )


class Stats:
    """Per-run counters shared across workers under a single lock.

    `metrics` is per-sample: one bump per completed `download()` call. `outcomes`
    is per-attempt: a single sample may bump 40 times across retries. The two
    live together because they're always bumped/snapshotted from the same code
    paths, and one lock serializes faster than two for these microscopic
    critical sections.

    Encapsulating these (vs. module-level dicts + lock) means `analyze_data` can
    be invoked multiple times in a single process with clean per-run baselines,
    and the reporter / Downloader get a single object to pass around instead of
    module-global state.
    """

    _METRIC_KEYS: tuple[str, ...] = ("success", "skipped", "not_found", "failed")
    # Transport-level outcomes the framework decides directly in `Downloader._post`
    # (the use case never returns these as a FetchOutcome): timeout (requests.Timeout),
    # http_error (non-2xx), conn_error (DNS / ECONNREFUSED / SSL / other).
    _TRANSPORT_KEYS: tuple[str, ...] = ("timeout", "http_error", "conn_error")
    # Every FetchOutcome value is a counter key, plus the transport keys above.
    # Deriving from the enum keeps the two in lockstep — add a FetchOutcome member
    # and its counter appears automatically, with no second list to update.
    _OUTCOME_KEYS: tuple[str, ...] = tuple(o.value for o in FetchOutcome) + _TRANSPORT_KEYS

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.metrics: dict[str, int] = dict.fromkeys(self._METRIC_KEYS, 0)
        self.outcomes: dict[str, int] = dict.fromkeys(self._OUTCOME_KEYS, 0)

    def bump(self, key: str) -> None:
        with self._lock:
            if key not in self.metrics:
                logger.error("Unknown metric key {!r} — not counted", key)
                return
            self.metrics[key] += 1

    def bump_outcome(self, key: str) -> None:
        # Guard unknown keys instead of letting `dict[key] += 1` raise KeyError.
        # A plugin `UseCase.classify` can return a FetchOutcome the framework has
        # no counter for; without this guard the KeyError escapes `download` into
        # its `@logger.catch` and the item vanishes silently — no metric bumped,
        # never recorded in existing_ids, and the acquired proxy left unpaired
        # with a mark_* (breaking the acquire() accounting contract).
        with self._lock:
            if key not in self.outcomes:
                logger.error("Unknown outcome key {!r} — not counted", key)
                return
            self.outcomes[key] += 1

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._lock:
            return dict(self.metrics), dict(self.outcomes)


def _today_utc() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _next_utc_midnight_ts() -> float:
    now = time.time()
    return now + (86400 - (now % 86400))


def _proxy_key(proxy: str | None) -> str:
    return proxy if proxy is not None else "__direct__"


class ProxyStateStore:
    """File-backed persistence for ProxyPool stats.

    Owns the file path, dirty flag, and background flush thread. The state
    dict itself lives on the ProxyPool (passed in by reference) so the pool's
    lock can guard both reads/writes uniformly.
    """

    def __init__(
        self,
        state_file: str | None,
        state: dict[str, ProxyStats],
        pool_lock: threading.Lock,
        config: SwarmConfig,
    ):
        self.config = config
        self.state_file = state_file
        self.state = state
        self._lock = pool_lock
        self._dirty = False

    @staticmethod
    def load(state_file: str | None) -> dict:
        if not state_file or not os.path.exists(state_file):
            return {}
        try:
            with open(state_file) as f:
                data = json.load(f)
        except (OSError, ValueError) as e:
            # OSError: unreadable/missing file. ValueError: malformed JSON
            # (JSONDecodeError subclasses it). Narrow so a programming error
            # (e.g. a bug in surrounding code) isn't silently swallowed as
            # "starting fresh". The data isn't lost — the file is read-only here.
            logger.warning("Could not read proxy state ({}), starting fresh", e)
            return {}
        return data if isinstance(data, dict) else {}

    def mark_dirty(self) -> None:
        # Caller holds pool_lock.
        self._dirty = True

    def save(self) -> None:
        if not self.state_file:
            return
        with self._lock:
            if not self._dirty:
                return
            payload = json.dumps(self.state)
            self._dirty = False
        # Per-call tmp suffix so concurrent saves (background thread + atexit +
        # explicit save_state() on Ctrl-C) don't clobber each other's tmp file
        # before the atomic rename lands.
        tmp = f"{self.state_file}.tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            with open(tmp, "w") as f:
                f.write(payload)
            os.replace(tmp, self.state_file)
        except OSError as e:
            # Narrow to I/O failures (disk full, EROFS, bad path). A transient
            # write failure must not crash a long run — the state is warm-start
            # scoring data, reconstructable on the next run. `json.dumps` runs
            # under the lock above (outside this try), so a serialization bug
            # still propagates instead of being mislabeled "could not save".
            logger.warning("Could not save proxy state: {}", e)
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    def start_background_saver(self) -> None:
        if not self.state_file:
            return
        threading.Thread(
            target=self._saver_loop, name="proxy-state-saver", daemon=True
        ).start()
        atexit.register(self.save)

    def _saver_loop(self) -> None:
        # Daemon thread — if it dies the file silently stops getting flushed and
        # you don't notice until restart. Catch and continue so a transient
        # error (disk full, EROFS) doesn't kill the loop. The daemon thread
        # dies with the process; atexit handles the final flush.
        while True:
            time.sleep(self.config.save_interval_sec)
            try:
                self.save()
            except Exception:
                logger.exception("proxy-state-saver iteration failed; continuing")


class GateCounters(TypedDict):
    # Cumulative tally of which branch admitted/rejected each opportunistic
    # insert (see `ProxyPool._can_opportunistic_insert`). A fixed key set so the
    # reporter's bracket reads (`gate["drain"]` etc.) are type-checked.
    drain: int
    probation: int
    merit: int
    reject: int


class MetricsSnapshot(NamedTuple):
    # Periodic-reporter view of the pool. Named so the reporter doesn't have to
    # remember a 5-tuple's positional order — and so future fields can land
    # without breaking every caller.
    fast_lane: int
    lane_explorers: int  # count of lane slots currently holding exploration candidates
    lane_med_ms: float
    lane_p90_ms: float
    cooled: int
    gate: GateCounters


class ProxyStats(TypedDict):
    # Per-proxy state, persisted to disk via ProxyStateStore. Every key is
    # initialized by `ProxyPool._empty_stats`, so consumers can use bracket
    # access (no `.get` fallback) — the type checker now verifies key names
    # across every read site.
    attempts: int
    failures: int
    last_reason: str | None
    last_failure_ts: float
    consecutive_failures: int
    cooldown_until: float
    usage_today: int
    usage_date: str
    last_success_ts: float
    ewma_ms: float  # EWMA of per-request latency; 0 = never measured
    ewma_success: float  # EWMA of success rate; 1.0 = optimistic seed


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
    ):
        self.config = config
        # None represents "no proxy" (your real IP) — drop it if you don't want that.
        self.proxies = list(proxies) or [None]
        self.exhausted: set[str | None] = (
            set()
        )  # hard rate-limit retirements (mark_exhausted only)
        # Fast lane: deque of the top-K known-good proxies by score (lower=faster &
        # more reliable). `good_candidates` is the full pool of ever-succeeded
        # proxies; `_refresh_good` rebuilds the deque from the top-K of that pool
        # every self.config.refresh_interval successes. `good_set` is an O(1) membership guard.
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
                "Loaded persisted state for {} proxies ({} still on cooldown, {} seeded into fast-lane)",
                len(self.state),
                cooled,
                len(self.good),
            )
            self._store.start_background_saver()

    def save_state(self) -> None:
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
        # Caller holds the lock. Splits self.config.top_k_fast_lane between exploitation (top
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
        # Caller holds the lock. Returns the score at self.config.lane_gate_percentile of the
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
    _STATS_FIELD_TYPES: dict[str, tuple[type, ...]] = {
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
                base[field] = value  # type: ignore[literal-required]
        return base

    def _get(self, proxy: str | None) -> ProxyStats:
        key = _proxy_key(proxy)
        s = self.state.get(key)
        if s is None:
            s = self._empty_stats()
            self.state[key] = s
        return s

    def _update_ewma(
        self, stat: ProxyStats, sample_ms: float | None = None, success: bool = True
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
        # was within the last day → keep cooling. Lets a flaky proxy recover after 24h idle.
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
        lane releases and re-acquires between self.config.slow_lane_batch-sized chunks so
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
        """Credit a clean exchange. Clears cooldown, updates EWMAs, possibly
        promotes the proxy into the fast lane (subject to the gate)."""
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
        """Permanently retire a proxy for the rest of the run (hit the
        upstream's per-IP / per-key daily cap). Cools until UTC midnight in
        the persisted state so it rejoins automatically tomorrow."""
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


def _classify_proxy_scheme(port: str) -> str:
    # Heuristic: port 1080 is conventionally SOCKS5; everything else assume HTTP.
    return "socks5h" if port == "1080" else "http"


def _load_proxies(path: str) -> list[str | None]:
    if not os.path.exists(path):
        logger.warning("No proxies file at {}, running without proxies", path)
        return [None]
    out = []
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "://" not in line:
                host, _, port = line.partition(":")
                # Filter obviously bogus entries.
                if (
                    not host
                    or not port
                    or host in ("0.0.0.0",)
                    or host.startswith("127.")
                ):
                    continue
                line = f"{_classify_proxy_scheme(port)}://{host}:{port}"
            out.append(line)
    # Dedup, then shuffle. Shuffling spreads the slow-lane discovery walk across
    # the file's contents — if the file is sorted (by country / ASN / scraper
    # source), an unshuffled walk would hit correlated batches and either find
    # everything fast or nothing at all. Shuffle once at load is sufficient;
    # the walk itself is round-robin against this fixed order.
    seen: set[str] = set()
    deduped: list[str | None] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    random.shuffle(deduped)
    logger.info("Loaded {} proxies ({} unique, shuffled)", len(out), len(deduped))
    return deduped


class RequestSpec(NamedTuple):
    """How to call the upstream for one work item. Returned by `UseCase.build_request`.

    The framework wraps this with the chosen proxy, the shared session
    headers, `stream=True`, `allow_redirects=True`, and `self.config.request_timeout_sec`
    before issuing — so the plugin doesn't need to think about transport.
    `json_` is named with a trailing underscore to avoid shadowing the
    `json` stdlib module imported at the top of this file.
    """

    url: str
    method: str = "POST"
    data: dict | None = None
    json_: dict | None = None
    params: dict | None = None
    headers: dict | None = None


class UseCase(Protocol):
    """Plugin contract for a fetch-loop use case.

    The framework owns the proxy pool, retry loop, stats, throttle
    detector, and shutdown signalling. A use case owns three things:

    * **Work source** — `iter_items` yields ids to fetch; `existing_ids`
      returns a starting set so resumed runs skip already-complete ids.
    * **Fetch + classify** — `build_request` turns an id into a
      `RequestSpec`; `classify` turns the response into a `FetchOutcome`
      plus an optional detail string and the body bytes the framework
      passes back to `handle_success`.
    * **Result handler** — `handle_success` persists the body (or whatever)
      and returns True on success. Returning False signals "body looked
      right but was corrupt" — the framework cools the proxy and retries.

    Lifecycle: `add_arguments` is called once during CLI parsing,
    `from_args` constructs the instance, `prepare` runs before the worker
    pool starts (mkdirs, partial cleanup, fetching a CSV index, etc),
    `session_headers` is mounted on the shared `requests.Session`.

    The `name` attribute is used in log messages so different invocations
    can be told apart at a glance.
    """

    name: str

    @classmethod
    def add_arguments(cls, p: argparse.ArgumentParser) -> None: ...

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "UseCase": ...

    def prepare(self) -> None: ...

    def session_headers(self) -> dict[str, str]: ...

    def iter_items(self) -> Iterator[str]: ...

    def existing_ids(self) -> set[str]: ...

    def build_request(self, item_id: str) -> RequestSpec: ...

    def classify(
        self, response: requests.Response
    ) -> tuple[FetchOutcome, str | None, bytes]: ...

    def handle_success(self, item_id: str, body: bytes) -> bool: ...


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

    def __init__(self, threshold: int):
        self.threshold = threshold
        self.streak = 0
        self.warned_label: str | None = None
        self.lock = threading.Lock()

    def observe(self, outcome: FetchOutcome) -> None:
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
                    'API throttling cleared (was "{}" after {} consecutive hits) — resuming normal operation',
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
                    "likely API-key throttling, not per-IP. Proxy rotation will not fix this.",
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
    budget (up to `self.config.max_retry_attempts × self.config.request_timeout_sec` per worker). The Ctrl-C path in
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
    ):
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
        # (up to self.config.max_retry_attempts × self.config.request_timeout_sec per worker). Without this, `pool.shutdown(wait=True)`
        # blocks on workers that have no idea the user wants to quit.
        self._shutdown_event = threading.Event()

    def request_shutdown(self) -> None:
        # Idempotent — but log once so the operator gets confirmation Ctrl-C registered.
        if not self._shutdown_event.is_set():
            logger.warning(
                "Shutdown requested — workers will bail at next attempt boundary (~{}s max)",
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
        # when the pool is fully drained.
        max_attempts = min(
            self.config.max_retry_attempts,
            max(self.config.min_retry_attempts, self.proxy_pool.alive_count()),
        )
        # `max_attempts` budgets *real HTTP attempts* only. Pool starvation (every
        # proxy cooled / exhausted) waits in an inner loop instead of burning the
        # budget — otherwise a worker in a trough eats all 40 attempts on backoff
        # sleeps (~3 min) and fails the item without ever sending a request.
        miss_count = 0
        for attempt in range(1, max_attempts + 1):
            if self._shutdown_event.is_set():
                return
            proxy = self.proxy_pool.acquire()
            while proxy is None:
                miss_count += 1
                # 0.5, 1, 2, 4, 5, 5, ... seconds. `Event.wait` returns True if
                # shutdown fires during the sleep — bail immediately in that case.
                backoff = min(
                    self.config.acquire_retry_sleep_sec * (2 ** min(miss_count - 1, 4)),
                    self.config.acquire_backoff_cap_sec,
                )
                if self._shutdown_event.wait(backoff):
                    return
                proxy = self.proxy_pool.acquire()
            miss_count = 0

            response, elapsed_ms = self._post(proxy, item_id)
            if response is None:
                continue

            outcome, detail, body = self.use_case.classify(response)
            self.stats.bump_outcome(outcome)
            self._throttle_detector.observe(outcome)

            if outcome == FetchOutcome.OK:
                if self.use_case.handle_success(item_id, body):
                    self._record_have(item_id)
                    self.proxy_pool.mark_success(proxy, elapsed_ms)
                    logger.debug(
                        "Got {} via {} (attempt {})", item_id[:16], proxy, attempt
                    )
                    self.stats.bump("success")
                    return
                # Use case rejected the body (e.g. corrupt zip) — proxy delivered
                # partial / wrong data. Blame it and try another.
                self.stats.bump_outcome("proxy_garbage")
                self.proxy_pool.mark_bad(proxy, "corrupt body")
                continue
            if outcome == FetchOutcome.NOT_FOUND:
                self.proxy_pool.mark_success(proxy, elapsed_ms)
                logger.info("Item {} not found upstream", item_id[:16])
                self.stats.bump("not_found")
                return
            if outcome == FetchOutcome.BAD_QUERY:
                self.proxy_pool.mark_success(proxy, elapsed_ms)
                logger.error("Malformed query for {}: {}", item_id[:16], detail)
                self.stats.bump("failed")
                return
            if outcome == FetchOutcome.AUTH_ERROR:
                # Credit the proxy — it answered authoritatively, the API key
                # is what's broken. Without this, `attempts` is incremented by
                # `_reserve` but never paired with a mark_*, drifting the
                # proxy's bookkeeping (see `acquire()` contract).
                self.proxy_pool.mark_success(proxy, elapsed_ms)
                logger.error(
                    "Auth problem ({}) — check credentials, aborting item {}",
                    detail,
                    item_id[:16],
                )
                self.stats.bump("failed")
                return
            if outcome == FetchOutcome.RATE_LIMITED:
                self.proxy_pool.mark_exhausted(proxy)
                continue
            # PROXY_BAD or PROXY_GARBAGE — try another proxy. `detail` is always
            # a string on this branch (only the OK outcome leaves it None), but
            # coalesce defensively so the type checker stays happy too.
            self.proxy_pool.mark_bad(proxy, detail or "unknown")

        logger.error(
            "Exhausted {} attempts for {} ({} proxies alive)",
            max_attempts,
            item_id[:16],
            self.proxy_pool.alive_count(),
        )
        self.stats.bump("failed")


def _fmt_eta(seconds: float) -> str:
    if seconds <= 0 or seconds != seconds:  # 0 or NaN
        return "?"
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m{int(seconds % 60):02d}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h{m:02d}m"


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
        # Daemon thread — wrap each tick so a bad format-arg or pool mutation
        # during snapshot doesn't kill the reporter silently for the rest of
        # the run. Survives to the next interval and tries again.
        try:
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
                "{:.0f}% ({}/{}) eta={} | rate={:.1f}/s | ok={} skip={} 404={} fail={} | "
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
            # is the canary for API-key throttling (vs IP throttling); see APIThrottleDetector.
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
            last, last_total = now, total
        except Exception:
            logger.exception("metrics-reporter iteration failed; continuing")


def analyze_data(
    downloader: Downloader,
    use_case: UseCase,
    config: SwarmConfig,
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
                    try:
                        in_flight.add(pool.submit(downloader.download, next(it)))
                    except StopIteration:
                        pass
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
        # timeout (config.request_timeout_sec) instead of grinding all config.max_retry_attempts. `cancel_futures` drops
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


def _build_runtime(use_case: UseCase, config: SwarmConfig) -> Downloader:
    """Wire up the runtime: load proxies, build the pool, run the use case's
    `prepare` step, snapshot its `existing_ids`, mount a tuned HTTP session
    with the use case's headers, return the `Downloader`. Logs the startup
    config banner as a side effect so the first thing in the log file is
    the tuning state."""
    _log_startup_config(config)
    proxy_pool = ProxyPool(
        _load_proxies(config.proxies_file), config, state_file=config.proxy_state_file
    )

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
    use_case: UseCase, config: SwarmConfig | None = None, dry_run: bool = False
) -> None:
    """Entry point for running a use case."""
    if config is None:
        config = SwarmConfig()
    downloader = _build_runtime(use_case, config)
    try:
        analyze_data(downloader, use_case, config, dry_run=dry_run)
    except KeyboardInterrupt:
        pass
