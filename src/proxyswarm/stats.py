"""Per-attempt outcome classification and per-run counters.

`FetchOutcome` is the framework's closed vocabulary for "what happened on one
HTTP attempt"; a use case's `classify` returns one. `Stats` aggregates both
per-sample metrics and per-attempt outcomes behind a single lock so workers and
the metrics reporter share one consistent view.
"""

import threading
from enum import StrEnum

from loguru import logger


# Outcome class for every HTTP attempt. OK / NOT_FOUND / BAD_QUERY / AUTH_ERROR
# are authoritative API answers — the loop is finished. RATE_LIMITED retires
# the proxy and retries. PROXY_BAD / PROXY_GARBAGE cool the proxy and retry.
#
# `StrEnum` lets members double as dict keys (each `.value` is a `Stats`
# outcome-counter key — see `Stats._OUTCOME_KEYS`, which is derived from this
# enum) and as direct equality targets in `if outcome == ...` checks.
class FetchOutcome(StrEnum):
    """Classification of a single HTTP attempt; see the comment above."""

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
    _OUTCOME_KEYS: tuple[str, ...] = (
        tuple(o.value for o in FetchOutcome) + _TRANSPORT_KEYS
    )

    def __init__(self) -> None:
        """Initialize the lock and zeroed metric/outcome counters."""
        self._lock = threading.Lock()
        self.metrics: dict[str, int] = dict.fromkeys(self._METRIC_KEYS, 0)
        self.outcomes: dict[str, int] = dict.fromkeys(self._OUTCOME_KEYS, 0)

    def bump(self, key: str) -> None:
        """Increment a per-sample metric counter; log-and-skip unknown keys."""
        with self._lock:
            if key not in self.metrics:
                logger.error("Unknown metric key {!r} — not counted", key)
                return
            self.metrics[key] += 1

    def bump_outcome(self, key: str) -> None:
        """Increment a per-attempt outcome counter; log-and-skip unknown keys."""
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
        """Return a consistent copy of (metrics, outcomes) under the lock."""
        with self._lock:
            return dict(self.metrics), dict(self.outcomes)
