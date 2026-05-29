"""Async pre-flight liveness check for scraped proxies.

`check_proxies` validates a batch of `scheme://host:port` proxies concurrently
and returns only the live ones, mapped to their measured latency in
milliseconds. Run before a fetch job, its output is used to *warm-seed* the
`ProxyPool` (via `mark_success`) so the fast lane starts hot instead of
discovering good proxies the slow way during real work.

Two liveness bars, by scheme:
- ``http`` / ``https`` — full validation: open the connection (TCP check), send
  an **absolute-form** ``GET`` (how you speak to an HTTP proxy) to a tiny
  204-no-content endpoint, and require a ``204`` back. A transparent or broken
  proxy that returns an error/redirect page fails this cleanly.
- ``socks*`` — TCP-connect only. A SOCKS handshake isn't implemented yet, so a
  successful connect is the bar; the pool's slow-lane discovery still vets these
  against the real target at runtime.

Pure stdlib `asyncio` — no extra dependencies. Liveness is ephemeral (free
proxies die and revive within minutes), so timeouts are deliberately short: a
fast, recent, shallow check beats a slow, stale, thorough one.
"""

import asyncio
import contextlib
import sys
import time
from urllib.parse import urlsplit

from loguru import logger

if sys.platform == "win32":  # pragma: no cover - no POSIX fd limits to raise
    _resource = None
else:
    import resource as _resource

#: Default validation endpoint: a tiny global 204-no-content responder.
DEFAULT_TEST_URL = "http://www.gstatic.com/generate_204"

_USER_AGENT = "proxyswarm-healthcheck/1.0"

#: fds reserved for stdio, the event loop, log files, etc. — kept free so a
#: concurrency cap derived from the soft limit can't starve the rest of the run.
_FD_HEADROOM = 128


def _resolve_concurrency(desired: int) -> int:
    """Cap concurrency to the file-descriptor budget, raising the soft limit.

    Each in-flight check holds one socket, so concurrency above the fd soft
    limit makes `open_connection` fail with OSError — silently misread as a dead
    proxy. Raise the soft limit toward the hard limit and cap concurrency below
    it (minus headroom). On platforms without `resource` (Windows), trust the
    requested value.
    """
    if _resource is None:
        return desired
    # `resource` + RLIMIT_NOFILE are POSIX-guaranteed; ty checks under
    # `python-platform = "all"` (incl. Windows) where typeshed marks them
    # possibly-absent, but the `_resource is None` guard above means this only
    # runs where they exist.
    soft, hard = _resource.getrlimit(_resource.RLIMIT_NOFILE)  # ty: ignore[possibly-missing-attribute]
    want = min(desired + _FD_HEADROOM, hard)
    if soft < want:
        with contextlib.suppress(OSError, ValueError):
            _resource.setrlimit(_resource.RLIMIT_NOFILE, (want, hard))  # ty: ignore[possibly-missing-attribute]
            soft = want
    return max(1, min(desired, soft - _FD_HEADROOM))


def _elapsed_ms(start: float) -> float:
    return (time.monotonic() - start) * 1000.0


async def _close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


async def _probe_http(
    host: str,
    port: int,
    *,
    test_url: str,
    connect_timeout: float,
    read_timeout: float,
) -> float | None:
    """Validate an HTTP proxy end-to-end; return latency ms or None."""
    target = urlsplit(test_url).hostname or ""
    request = (
        f"GET {test_url} HTTP/1.1\r\n"
        f"Host: {target}\r\n"
        f"User-Agent: {_USER_AGENT}\r\n"
        f"Accept: */*\r\n"
        f"Connection: close\r\n\r\n"
    ).encode()

    start = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=connect_timeout
        )
    except OSError, TimeoutError:
        return None

    try:
        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=read_timeout)
        status_line = await asyncio.wait_for(reader.readline(), timeout=read_timeout)
    except OSError, TimeoutError:
        return None
    finally:
        await _close(writer)

    # Status line: b"HTTP/1.1 204 No Content\r\n" -> second token is the code.
    # Require exactly 204: a transparent/broken proxy returning a 200 error page
    # or a redirect must not count as a working pass-through.
    match status_line.split():
        case [_version, b"204", *_rest]:
            return _elapsed_ms(start)
        case _:
            return None


async def _probe_tcp(host: str, port: int, *, connect_timeout: float) -> float | None:
    """Validate reachability by TCP connect alone; return latency ms or None."""
    start = time.monotonic()
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=connect_timeout
        )
    except OSError, TimeoutError:
        return None
    await _close(writer)
    return _elapsed_ms(start)


async def _check_one(
    proxy: str,
    *,
    test_url: str,
    connect_timeout: float,
    read_timeout: float,
) -> float | None:
    """Liveness-check one proxy; dispatch on scheme. Returns latency ms or None."""
    parsed = urlsplit(proxy)
    try:
        host, port = parsed.hostname, parsed.port
    except ValueError:
        # `.port` validates lazily and raises on an out-of-range/non-numeric
        # port — a malformed proxy is just another dead one, not a fatal error.
        return None
    if not host or port is None:
        return None
    if parsed.scheme in ("http", "https"):
        return await _probe_http(
            host,
            port,
            test_url=test_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
    return await _probe_tcp(host, port, connect_timeout=connect_timeout)


async def _check_all(
    proxies: list[str],
    *,
    test_url: str,
    concurrency: int,
    connect_timeout: float,
    read_timeout: float,
    target_alive: int,
) -> dict[str, float]:
    """Check proxies via a bounded worker pool; return alive -> latency ms.

    A fixed pool of `concurrency` workers drains a single shared iterator, so at
    most that many checks (and sockets) are ever in flight regardless of list
    size — and no second copy of the list is built (cf. a pre-filled queue).
    `next()` on a list iterator has no `await` inside it, so concurrent workers
    can't draw the same item. When `target_alive` is positive, the pool stops as
    soon as that many live proxies are found — the remaining workers exit after
    their current check, so the overshoot is at most `concurrency - 1`.
    `target_alive=0` checks everything.
    """
    pending = iter(proxies)
    results: dict[str, float] = {}
    enough = asyncio.Event()

    async def worker() -> None:
        for proxy in pending:
            if enough.is_set():
                return
            latency = await _check_one(
                proxy,
                test_url=test_url,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
            )
            if latency is not None:
                results[proxy] = latency
                if target_alive and len(results) >= target_alive:
                    enough.set()
                    return

    pool_size = min(concurrency, len(proxies))
    await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(pool_size)))
    return results


def check_proxies(
    proxies: list[str | None],
    *,
    test_url: str = DEFAULT_TEST_URL,
    concurrency: int = 1000,
    connect_timeout: float = 3.0,
    read_timeout: float = 4.0,
    target_alive: int = 0,
) -> dict[str, float]:
    """Concurrently liveness-check `proxies`; return alive ones -> latency ms.

    `None` entries (the "no proxy" sentinel) are skipped. With `target_alive > 0`
    the check stops early once that many live proxies are found — enough to
    warm-seed the pool without validating a (mostly-dead) full list. Concurrency
    is capped to the file-descriptor budget. Synchronous wrapper around the
    asyncio batch checker so callers in the sync codebase can use it directly.
    """
    candidates = [p for p in proxies if p is not None]
    if not candidates:
        return {}

    effective = _resolve_concurrency(concurrency)
    logger.info(
        "Health-checking {} proxies (concurrency={}, target_alive={})...",
        len(candidates),
        effective,
        target_alive or "all",
    )
    results = asyncio.run(
        _check_all(
            candidates,
            test_url=test_url,
            concurrency=effective,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            target_alive=target_alive,
        )
    )

    logger.success("Health check complete: {} live proxies found", len(results))
    return results
