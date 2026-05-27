"""proxyswarm — free-proxy-pool bulk fetcher with a pluggable use-case API."""

from .config import SwarmConfig
from .core import (
    Downloader,
    FetchOutcome,
    ProxyPool,
    RequestSpec,
    UseCase,
    analyze_data,
    run,
)

__all__ = [
    "Downloader",
    "FetchOutcome",
    "ProxyPool",
    "RequestSpec",
    "SwarmConfig",
    "UseCase",
    "analyze_data",
    "run",
]
