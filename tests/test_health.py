"""Tests for the async pre-flight proxy liveness checker (`proxyswarm.health`).

The checker validates a proxy by talking raw HTTP to it (absolute-form request
line) and asserting a 204 from a known endpoint. These tests stand up local
asyncio servers that *impersonate* a proxy with configurable behaviour, so the
checks run hermetically against 127.0.0.1 with no real network.
"""

import asyncio
import contextlib
import socket
import threading
from collections.abc import Awaitable, Callable
from typing import Self

from proxyswarm import health
from proxyswarm.health import check_proxies

# A handler is the asyncio.start_server callback: (reader, writer) -> awaitable.
Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


class FakeProxy:
    """Run an asyncio server impersonating a proxy on its own background loop.

    The checker calls the synchronous `check_proxies` (which uses `asyncio.run`)
    on the main thread; this server gets its own loop on a daemon thread so the
    two never share an event loop. Yields `host`/`port` and a `url` helper.
    """

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self.host = ""
        self.port = 0

    def __enter__(self) -> Self:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            msg = "fake proxy server did not start"
            raise RuntimeError(msg)
        return self

    def __exit__(self, *_exc: object) -> None:
        # Stop the loop, then join the thread so its graceful shutdown (cancel
        # lingering handler tasks, close the server) finishes before the test
        # moves on — otherwise a GC'd transport raises PytestUnraisableException.
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        server = self._loop.run_until_complete(
            asyncio.start_server(self._handler, "127.0.0.1", 0)
        )
        self.host, self.port = server.sockets[0].getsockname()[:2]
        self._ready.set()
        self._loop.run_forever()  # blocks until __exit__ stops the loop
        # Graceful shutdown: cancel in-flight handlers and close transports
        # before closing the loop, so nothing is destroyed-while-pending.
        server.close()
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        self._loop.run_until_complete(
            asyncio.gather(server.wait_closed(), *pending, return_exceptions=True)
        )
        self._loop.close()

    def url(self, scheme: str = "http") -> str:
        return f"{scheme}://{self.host}:{self.port}"


def _free_port() -> int:
    """Return a port number with no listener (for connection-refused tests)."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _respond_204(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    await reader.readline()  # consume the absolute-form request line
    writer.write(b"HTTP/1.1 204 No Content\r\n\r\n")
    await writer.drain()
    writer.close()


async def _respond_500(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    await reader.readline()
    writer.write(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
    await writer.drain()
    writer.close()


async def _accept_then_idle(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    # Accept the connection but never send an HTTP response. Drain until the
    # peer disconnects (read-to-EOF) so the handler ends promptly at teardown
    # instead of lingering as a pending task.
    with contextlib.suppress(OSError):
        await reader.read()
    writer.close()


def _check(
    proxies: list[str | None],
    *,
    concurrency: int = 10,
    connect_timeout: float = 2.0,
    read_timeout: float = 2.0,
    target_alive: int = 0,
) -> dict[str, float]:
    return check_proxies(
        proxies,
        test_url="http://probe.invalid/generate_204",
        concurrency=concurrency,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        target_alive=target_alive,
        show_progress=False,
    )


def test_working_http_proxy_reported_alive_with_positive_latency() -> None:
    with FakeProxy(_respond_204) as fp:
        result = _check([fp.url()])
    assert set(result) == {fp.url()}
    assert result[fp.url()] > 0.0


def test_dead_proxy_with_no_listener_is_omitted() -> None:
    dead = f"http://127.0.0.1:{_free_port()}"
    assert _check([dead]) == {}


def test_tcp_open_but_non_204_response_is_omitted() -> None:
    with FakeProxy(_respond_500) as fp:
        assert _check([fp.url()]) == {}


def test_http_proxy_that_never_responds_times_out_and_is_omitted() -> None:
    with FakeProxy(_accept_then_idle) as fp:
        assert _check([fp.url()], read_timeout=0.3) == {}


def test_socks_proxy_passes_on_tcp_connect_alone() -> None:
    # SOCKS proxies aren't HTTP-validated (no handshake yet) — a successful TCP
    # connect is the bar, so an accepting server counts as alive.
    with FakeProxy(_accept_then_idle) as fp:
        result = _check([fp.url(scheme="socks5h")])
    assert set(result) == {fp.url(scheme="socks5h")}


def test_none_entry_is_skipped() -> None:
    assert _check([None]) == {}


def test_proxy_with_out_of_range_port_is_omitted() -> None:
    # urlsplit(...).port raises ValueError for ports outside 0-65535; one
    # malformed entry must be treated as dead, not crash the whole batch.
    assert _check(["http://1.2.3.4:99999"]) == {}


def test_mixed_batch_returns_only_alive() -> None:
    dead = f"http://127.0.0.1:{_free_port()}"
    with FakeProxy(_respond_204) as fp:
        result = _check([fp.url(), dead, None])
    assert set(result) == {fp.url()}


# --- early stop / scale -------------------------------------------------------


def test_early_stop_returns_exactly_target_at_serial_concurrency(monkeypatch) -> None:
    # With concurrency=1 the walk is serial, so the check stops the instant the
    # target is met — no in-flight overshoot. All proxies are "alive" here.
    async def always_alive(_proxy: str, **_kw: object) -> float:
        return 1.0

    monkeypatch.setattr(health, "_check_one", always_alive)
    proxies: list[str | None] = [f"http://10.0.0.{i}:80" for i in range(100)]
    result = _check(proxies, concurrency=1, target_alive=3)
    assert len(result) == 3


def test_target_alive_zero_checks_every_proxy(monkeypatch) -> None:
    async def always_alive(_proxy: str, **_kw: object) -> float:
        return 1.0

    monkeypatch.setattr(health, "_check_one", always_alive)
    proxies: list[str | None] = [f"http://10.0.0.{i}:80" for i in range(12)]
    result = _check(proxies, concurrency=4, target_alive=0)
    assert len(result) == 12


def test_resolve_concurrency_stays_within_request_and_positive() -> None:
    resolved = health._resolve_concurrency(50)
    assert 1 <= resolved <= 50
