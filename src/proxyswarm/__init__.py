"""proxyswarm — free-proxy-pool bulk fetcher with a pluggable use-case API."""

from .config import SwarmConfig
from .downloader import Downloader
from .orchestrator import analyze_data, run
from .pool import ProxyPool
from .proxysource import (
    ChainedProxySource,
    FileProxySource,
    ProxySource,
    ScrapingProxySource,
    default_proxy_source,
)
from .stats import FetchOutcome
from .usecase import RequestSpec, UseCase

__all__ = [
    "ChainedProxySource",
    "Downloader",
    "FetchOutcome",
    "FileProxySource",
    "ProxyPool",
    "ProxySource",
    "RequestSpec",
    "ScrapingProxySource",
    "SwarmConfig",
    "UseCase",
    "analyze_data",
    "default_proxy_source",
    "run",
]
