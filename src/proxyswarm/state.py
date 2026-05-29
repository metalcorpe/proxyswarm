"""File-backed persistence for proxy stats, plus UTC time and shape helpers.

Holds the `ProxyStats` shape the stats are pinned to and the UTC time helpers
the pool's cooldown math depends on.

`ProxyStateStore` owns the file path, dirty flag, and background flush thread.
The state dict itself lives on the `ProxyPool` (passed in by reference) so the
pool's lock can guard both reads/writes and the periodic flush uniformly.
"""

import atexit
import json
import os
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

from loguru import logger

if TYPE_CHECKING:
    from .config import SwarmConfig


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
    ) -> None:
        """Bind the file path, shared state dict, pool lock, and config."""
        self.config = config
        self.state_file = state_file
        self.state = state
        self._lock = pool_lock
        self._dirty = False

    @staticmethod
    def load(state_file: str | None) -> dict[str, object]:
        """Read and JSON-decode the state file, returning {} on any failure."""
        if not state_file or not Path(state_file).exists():
            return {}
        try:
            with Path(state_file).open(encoding="utf-8") as f:
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
        """Flag that in-memory state has changed and needs a flush."""
        # Caller holds pool_lock.
        self._dirty = True

    def save(self) -> None:
        """Atomically write state to disk via a temp file + rename, if dirty."""
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
        dest = Path(self.state_file)
        tmp = dest.with_name(f"{dest.name}.tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                f.write(payload)
            tmp.replace(dest)
        except OSError as e:
            # Narrow to I/O failures (disk full, EROFS, bad path). A transient
            # write failure must not crash a long run — the state is warm-start
            # scoring data, reconstructable on the next run. `json.dumps` runs
            # under the lock above (outside this try), so a serialization bug
            # still propagates instead of being mislabeled "could not save".
            logger.warning("Could not save proxy state: {}", e)
            tmp.unlink(missing_ok=True)

    def start_background_saver(self) -> None:
        """Launch the periodic-flush daemon thread and register an atexit flush."""
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
            # logger.catch swallows + logs any error so a transient fault
            # (disk full, EROFS) doesn't kill the daemon loop.
            with logger.catch(message="proxy-state-saver iteration failed; continuing"):
                self.save()


class ProxyStats(TypedDict):
    """Per-proxy persisted state; shape pinned for bracket-access reads."""

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
