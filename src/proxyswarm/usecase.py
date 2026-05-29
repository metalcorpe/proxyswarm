"""The `UseCase` plugin contract and the `RequestSpec` it builds.

A use case owns everything API- and content-specific: what work items to fetch,
how to build the HTTP request, how to classify the response, and what to do with
a successful body. The framework (proxy pool, retry loop, stats) depends only on
this protocol — concrete use cases live outside the package and point inward.
"""

from typing import TYPE_CHECKING, NamedTuple, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator

    import requests

    from .stats import FetchOutcome


class RequestSpec(NamedTuple):
    """How to call the upstream for one work item. Returned by `UseCase.build_request`.

    The framework wraps this with the chosen proxy, the shared session
    headers, `stream=True`, `allow_redirects=True`, and
    `self.config.request_timeout_sec`
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

    Construction is the caller's concern, not the framework's: `run` takes an
    already-built instance. The shipped examples parse argv into one with a
    per-use-case `main`/`_parse_args` pair (see `examples/`), but that's a CLI
    convention, not part of this contract.

    Lifecycle: `prepare` runs once before the worker pool starts — whatever
    one-time setup the use case owns (mkdirs, partial cleanup, fetching a CSV
    index, etc); `session_headers` is mounted on the shared `requests.Session`;
    then `iter_items`/`build_request`/`classify`/`handle_success` drive each
    item. The framework provides the proxy swarm and retry loop only — where a
    fetched body is persisted is entirely the use case's concern.

    The `name` attribute is used in log messages so different invocations
    can be told apart at a glance.
    """

    name: str

    def prepare(self) -> None:
        """Run any one-time setup before the worker pool starts."""
        ...

    def session_headers(self) -> dict[str, str]:
        """Return headers to mount on the shared requests session."""
        ...

    def iter_items(self) -> Iterator[str]:
        """Yield the work-item ids to fetch."""
        ...

    def existing_ids(self) -> set[str]:
        """Return ids already complete, so resumed runs can skip them."""
        ...

    def build_request(self, item_id: str) -> RequestSpec:
        """Turn a work-item id into a `RequestSpec`."""
        ...

    def classify(
        self, response: requests.Response
    ) -> tuple[FetchOutcome, str | None, bytes]:
        """Decode a response into (outcome, detail, body)."""
        ...

    def handle_success(self, item_id: str, body: bytes) -> bool:
        """Persist a successful body; return False if it was corrupt."""
        ...
