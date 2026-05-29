"""Backward-compatible facade for the proxyswarm framework.

The framework was split out of this single module into focused ones:

* `stats`        — `FetchOutcome`, `Stats`
* `state`        — `ProxyStateStore`, `ProxyStats`, the UTC time helpers
* `pool`         — `ProxyPool`, `GateCounters`, `MetricsSnapshot`
* `proxysource`  — proxy parsing/validation + the pluggable `ProxySource`s
* `usecase`      — the `UseCase` protocol + `RequestSpec`
* `downloader`   — `Downloader`, `APIThrottleDetector`
* `orchestrator` — `analyze_data`, `run`, and the runtime wiring

This module re-exports all of them so existing imports
(`from proxyswarm.core import X`) keep working. `import time` is retained so
code/tests that monkeypatch `core.time` still target the shared `time` module.
"""

import time  # noqa: F401  — re-exported so `monkeypatch.setattr(core.time, ...)` still works

from .downloader import APIThrottleDetector, Downloader
from .orchestrator import (
    _build_runtime,
    _emit_metrics_tick,
    _fmt_eta,
    _log_startup_config,
    _maybe_warm_seed,
    _metrics_reporter,
    _warm_seed_pool,
    analyze_data,
    check_proxies,
    run,
)
from .pool import GateCounters, MetricsSnapshot, ProxyPool
from .proxysource import (
    ChainedProxySource,
    FileProxySource,
    ProxySource,
    ScrapingProxySource,
    _is_bogus_proxy_host,
    _is_parseable_proxy,
    _load_from_source,
    _load_proxies,
    _normalize_proxy_line,
    _read_proxy_file,
    _scrape_proxies_to_file,
    default_proxy_source,
    scrape_proxies,
)
from .state import (
    ProxyStateStore,
    ProxyStats,
    _next_utc_midnight_ts,
    _proxy_key,
    _today_utc,
)
from .stats import FetchOutcome, Stats
from .usecase import RequestSpec, UseCase

__all__ = [
    "APIThrottleDetector",
    "ChainedProxySource",
    "Downloader",
    "FetchOutcome",
    "FileProxySource",
    "GateCounters",
    "MetricsSnapshot",
    "ProxyPool",
    "ProxySource",
    "ProxyStateStore",
    "ProxyStats",
    "RequestSpec",
    "ScrapingProxySource",
    "Stats",
    "UseCase",
    "_build_runtime",
    "_emit_metrics_tick",
    "_fmt_eta",
    "_is_bogus_proxy_host",
    "_is_parseable_proxy",
    "_load_from_source",
    "_load_proxies",
    "_log_startup_config",
    "_maybe_warm_seed",
    "_metrics_reporter",
    "_next_utc_midnight_ts",
    "_normalize_proxy_line",
    "_proxy_key",
    "_read_proxy_file",
    "_scrape_proxies_to_file",
    "_today_utc",
    "_warm_seed_pool",
    "analyze_data",
    "check_proxies",
    "default_proxy_source",
    "run",
    "scrape_proxies",
]
