"""Proxy acquisition: parsing/validation/normalization + pluggable sources.

Two layers live here:

* **Parsing** — `_read_proxy_file` and the `_normalize_*` / `_is_*` helpers turn
  raw lines (`host:port`, `scheme://host:port`, comments, junk) into a clean,
  routability-filtered list of `scheme://host:port` strings.
* **Sources** — the `ProxySource` protocol abstracts *where* the raw proxy list
  comes from. `FileProxySource` reads a file, `ScrapingProxySource` scrapes
  public endpoints (and caches the result to a file), and `ChainedProxySource`
  tries each in order and returns the first non-empty result. `run` accepts a
  custom `ProxySource`, so the framework no longer hardwires "file, else scrape"
  — a caller can supply proxies from Redis, an API, a database, anything.

`_load_from_source` applies the shared post-processing (dedup + shuffle, and the
no-proxies → `[None]` degrade) that every source's output needs before it feeds
the pool, so a custom source only has to return raw strings.
"""

import ipaddress
import random
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

from loguru import logger

from .scraper import scrape_proxies


def _classify_proxy_scheme(port: str) -> str:
    # Heuristic: port 1080 is conventionally SOCKS5; everything else assume HTTP.
    return "socks5h" if port == "1080" else "http"


def _is_bogus_proxy_host(host: str) -> bool:
    # A public proxy must live at a globally-routable IP. `not is_global`
    # rejects unspecified (0.0.0.0/::), loopback, link-local, multicast,
    # reserved, and private (RFC1918) ranges in one IPv4/IPv6-aware check.
    # Hostnames aren't IP literals — let them through for DNS to resolve later.
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return not ip.is_global


def _is_parseable_proxy(proxy: str) -> bool:
    """Return True if `proxy` yields a host and an in-range port via `urlsplit`.

    `SplitResult.port` validates lazily and raises ValueError on a port outside
    0-65535 or a non-numeric port. An entry that can't produce a usable
    host:port is dropped at load so it can't crash a consumer that parses it
    later (the pre-flight health check, the downloader).
    """
    try:
        parsed = urlsplit(proxy)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        return False
    return bool(host) and port is not None


def _normalize_proxy_line(line: str) -> str | None:
    """Normalise one stripped proxy line to `scheme://host:port`, or None to skip.

    Skips blanks and `#` comments; schemes bare `host:port` entries (see
    `_classify_proxy_scheme`) and drops them if the host is a non-routable IP
    literal (see `_is_bogus_proxy_host`). Structural port-range validation is
    left to the caller so malformed entries can be counted separately.
    """
    if not line or line.startswith("#"):
        return None
    if "://" not in line:
        host, _, port = line.partition(":")
        if not host or not port or _is_bogus_proxy_host(host):
            return None
        line = f"{_classify_proxy_scheme(port)}://{host}:{port}"
    return line


def _read_proxy_file(path: str) -> list[str]:
    """Parse a proxy file into normalised `scheme://host:port` entries.

    Blank lines and `#` comments are skipped. Bare `host:port` entries gain a
    scheme and non-routable IP literals are dropped (see `_normalize_proxy_line`).
    Entries whose host:port can't be parsed back out are dropped and counted
    (see `_is_parseable_proxy`). A missing or empty file yields an empty list.
    """
    if not Path(path).exists() or Path(path).stat().st_size == 0:
        return []

    out: list[str] = []
    malformed = 0
    with Path(path).open(encoding="utf-8") as f:
        for raw in f:
            line = _normalize_proxy_line(raw.strip())
            if line is None:
                continue
            if not _is_parseable_proxy(line):
                malformed += 1
                continue
            out.append(line)
    if malformed:
        logger.warning(
            "Dropped {} malformed proxy entries (unparseable host:port) from {}",
            malformed,
            path,
        )
    return out


def _scrape_proxies_to_file(path: str) -> list[str]:
    """Scrape public sources, persist them to `path`, and re-read the result.

    Re-reading routes scraped proxies through the same classification and
    routability filters as a user-supplied file, so there's no validation
    bypass. Returns an empty list if scraping yields nothing or the write fails.
    """
    logger.warning(
        "No proxies found in {}, attempting to scrape from public sources...", path
    )
    scraped = scrape_proxies()
    if not scraped:
        return []
    try:
        with Path(path).open("w", encoding="utf-8") as f:
            f.writelines(p + "\n" for p in scraped)
    except OSError as e:
        logger.error("Failed to write scraped proxies to {}: {}", path, e)
        return []
    logger.info("Saved {} scraped proxies to {}", len(scraped), path)
    return _read_proxy_file(path)


class ProxySource(Protocol):
    """Where raw proxies come from.

    `load` returns normalised `scheme://host:port` strings (or an empty list if
    this source has nothing). The framework handles dedup, shuffling, and the
    no-proxies degrade in `_load_from_source`, so an implementation only owns
    *acquisition* — read a file, hit an API, query a DB, etc. Implement this
    protocol and pass it to `run(use_case, proxy_source=...)`.
    """

    def load(self) -> list[str]:
        """Return normalised proxy strings for this source (may be empty)."""
        ...


class FileProxySource:
    """Read proxies from a local file (see `_read_proxy_file`)."""

    def __init__(self, path: str) -> None:
        """Bind the file path to read on `load`."""
        self.path = path

    def load(self) -> list[str]:
        """Read and normalise the file; empty list if missing/empty."""
        return _read_proxy_file(self.path)


class ScrapingProxySource:
    """Scrape public endpoints, caching the raw result to `path` for reuse.

    Caching to disk means a later `FileProxySource(path)` (or a restart) reuses
    the scrape instead of hammering the public endpoints again.
    """

    def __init__(self, path: str) -> None:
        """Bind the file path the scrape result is cached to."""
        self.path = path

    def load(self) -> list[str]:
        """Scrape, persist to `path`, and return the normalised result."""
        return _scrape_proxies_to_file(self.path)


class ChainedProxySource:
    """Try each source in order; return the first non-empty result.

    Models the framework's default "use the file, else fall back to scraping"
    policy as composition rather than a hardcoded branch — extend it by adding
    sources to the chain.
    """

    def __init__(self, sources: list[ProxySource]) -> None:
        """Bind the ordered list of sources to try."""
        self.sources = sources

    def load(self) -> list[str]:
        """Return the first source's non-empty result, else an empty list."""
        for source in self.sources:
            out = source.load()
            if out:
                return out
        return []


def default_proxy_source(path: str) -> ProxySource:
    """Build the framework default: read `path`, else fall back to a cached scrape."""
    return ChainedProxySource([FileProxySource(path), ScrapingProxySource(path)])


def _dedup_and_shuffle(out: list[str]) -> list[str | None]:
    # Dedup, then shuffle. Shuffling spreads the slow-lane discovery walk across
    # the source's contents — if the list is sorted (by country / ASN / scraper
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


def _load_from_source(source: ProxySource) -> list[str | None]:
    """Materialise a `ProxySource` into the pool's proxy list.

    Applies the shared post-processing every source needs: dedup + shuffle, and
    degrade to the single-`None` sentinel (run without proxies) when the source
    is empty.
    """
    out = source.load()
    if not out:
        logger.warning("Proxy source yielded nothing, running without proxies")
        return [None]
    return _dedup_and_shuffle(out)


def _load_proxies(path: str) -> list[str | None]:
    """Load proxies for `path` via the default file→scrape chain.

    Backward-compatible convenience wrapper around
    `_load_from_source(default_proxy_source(path))`, kept because callers and
    tests reference it directly.
    """
    return _load_from_source(default_proxy_source(path))
