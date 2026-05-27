"""Worked example of the `proxyswarm` framework: NVD CVE bulk fetcher.

This file is a runnable use case, not the framework. Like
`examples/malware_bazaar.py` it implements the `proxyswarm.UseCase` protocol
end to end — see `proxyswarm.core` for the framework internals.

It exists as a *deliberately different* second example: where MalwareBazaar
exercises the exotic corners of the protocol (POST form data, an API that
returns HTTP 200 + an in-band JSON error for everything, a binary AES-zip
body), `NvdCveUseCase` exercises the common REST shape:

  * **GET** requests with query params (no body to build).
  * **Generated** work items — CVE IDs from a numeric range, no input file.
  * **HTTP-status-driven** flow handled by the *framework*, not the use case
    (see the big note on `classify` below).
  * A **text/JSON** body saved straight to disk (no extraction).

Why proxies at all? The NVD API is rate-limited *per source IP*: ~5 requests
per rolling 30s window without an API key (~50/30s with one). Spreading the
requests across a pool of free proxies is the literal reason you'd reach for
this tool — each proxy is a fresh IP with its own quota.

Run it::

    # Fetch CVE-2024-0001 .. CVE-2024-2000 into ./nvd_store
    python -m examples.nvd_cve --year 2024 --count 2000
    export NVD_API_KEY=...        # optional, lifts the per-IP rate limit
    python -m examples.nvd_cve --year 2023 --start 1 --count 5000 --api-key $NVD_API_KEY

To build your own use case, implement the `UseCase` protocol and write a
`main()`/`_parse_args` pair modelled on the ones below, substituting your
class for `NvdCveUseCase`, then call `proxyswarm.run(your_use_case)`.
"""

import os
import json
import argparse
import threading
from typing import Iterator

import requests
from loguru import logger

from proxyswarm import SwarmConfig, FetchOutcome, RequestSpec, UseCase, run


# ---------------------------------------------------------------------------
# NVD CVE use case. A worked example of the UseCase protocol for a plain
# rate-limited REST/JSON API.
# ---------------------------------------------------------------------------

# Single CVE lookup endpoint. `?cveId=CVE-YYYY-NNNN` returns one record.
NVD_CVE_ENDPOINT = "https://services.nvd.nist.gov/rest/json/cves/2.0"

# The API key is optional and read from NVD_API_KEY (or --api-key). With a key
# the per-IP rate limit rises from ~5 to ~50 requests per 30s. Never hardcode a
# key here — this file lives in a public repo. Request one at
# https://nvd.nist.gov/developers/request-an-api-key.
DEFAULT_NVD_API_KEY = os.environ.get("NVD_API_KEY")
# CWD-relative default so the example is runnable anywhere; override per-operator.
DEFAULT_NVD_STORE = "nvd_store"

# A single NVD CVE record is small (a few KB to low tens of KB). Anything past
# this cap on a 200 response is not a CVE — almost always a captive-portal proxy
# that answered 200 with megabytes of HTML. Bound the read so one bad proxy
# can't make us buffer its whole interstitial page. Plugin-local: only this use
# case's `classify` references it.
MAX_BODY_BYTES = 5 * 1024 * 1024


class NvdCveUseCase:
    """Bulk-fetch CVE records from the NVD 2.0 REST API.

    Work items are CVE IDs generated from a `--year` and a numeric range
    (`--start`, `--count`) — no input file. The NVD API answers a
    syntactically valid but nonexistent CVE with `HTTP 200` and
    `totalResults: 0`, so gaps in the numbering are handled gracefully as
    `NOT_FOUND` rather than errors. Each hit is saved as `<cve-id>.json`.
    """

    name = "nvd-cve"

    @classmethod
    def add_arguments(cls, p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--year",
            type=int,
            required=True,
            help="CVE year to fetch, e.g. 2024. Item IDs are CVE-<year>-<n>.",
        )
        p.add_argument(
            "--start",
            type=int,
            default=1,
            help="First sequence number in the CVE range. Default: 1.",
        )
        p.add_argument(
            "--count",
            type=int,
            default=1000,
            help="How many sequence numbers to fetch from --start. Default: 1000.",
        )
        p.add_argument(
            "--store",
            default=DEFAULT_NVD_STORE,
            help=f"Destination folder for fetched CVE JSON. Default: {DEFAULT_NVD_STORE}.",
        )
        p.add_argument(
            "--api-key",
            default=DEFAULT_NVD_API_KEY,
            help="NVD API key (optional). Defaults to the NVD_API_KEY environment variable.",
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "NvdCveUseCase":
        if args.count < 1:
            raise SystemExit(f"--count must be >= 1, got {args.count}")
        if args.start < 1:
            raise SystemExit(f"--start must be >= 1, got {args.start}")
        return cls(args.year, args.start, args.count, args.store, args.api_key)

    def __init__(
        self,
        year: int,
        start: int,
        count: int,
        store_folder: str,
        api_key: str | None,
    ):
        self.year = year
        self.start = start
        self.count = count
        self.store_folder = store_folder
        self.api_key = api_key
        # Instance-level `name` (shadows class attr) so concurrent or sequential
        # runs against different years are disambiguated in the logs.
        self.name = f"nvd-cve:{year}"

    def prepare(self) -> None:
        os.makedirs(self.store_folder, exist_ok=True)
        # Sweep stale partials left over from a previously-killed write. They're
        # dot-prefixed so they don't pollute the dedup set, but they're wasted disk.
        stale = 0
        for name in os.listdir(self.store_folder):
            if name.startswith(".partial."):
                try:
                    os.unlink(os.path.join(self.store_folder, name))
                    stale += 1
                except OSError:
                    pass
        if stale:
            logger.info("Cleaned {} stale partial files in {}", stale, self.store_folder)

    def session_headers(self) -> dict[str, str]:
        # NVD reads the key from the `apiKey` header. Omit it entirely when
        # unset — sending an empty key is treated as malformed by some edges.
        return {"apiKey": self.api_key} if self.api_key else {}

    def iter_items(self) -> Iterator[str]:
        """Yield generated CVE IDs `CVE-<year>-<n>` for n in the configured range.

        Sequence numbers are zero-padded to the canonical minimum width of 4
        digits (`CVE-2024-0001`); numbers past 9999 simply use more digits.
        No dedup set is needed — a contiguous range is unique by construction.
        """
        for n in range(self.start, self.start + self.count):
            yield f"CVE-{self.year}-{n:04d}"

    def existing_ids(self) -> set[str]:
        # Hits are saved as `<cve-id>.json`, so strip the `.json` suffix to get
        # the bare ID the per-item dedup check compares against. Skip
        # dot-prefixed names so in-progress `.partial.*` files don't show up.
        return {
            name[: -len(".json")]
            for name in os.listdir(self.store_folder)
            if name.endswith(".json") and not name.startswith(".")
        }

    def build_request(self, item_id: str) -> RequestSpec:
        # GET with the CVE ID as a query param. The framework adds the proxy,
        # session headers, stream=True, and the timeout — see RequestSpec.
        return RequestSpec(
            url=NVD_CVE_ENDPOINT,
            method="GET",
            params={"cveId": item_id},
        )

    def classify(
        self, response: requests.Response
    ) -> tuple[FetchOutcome, str | None, bytes]:
        """Classify a 2xx NVD response into an outcome + body.

        IMPORTANT — what this method does *not* handle, and why:

        The framework calls `response.raise_for_status()` in
        `Downloader._post` *before* this method runs, so `classify` only ever
        sees **2xx** responses. A `403`/`429` (NVD's per-IP rate-limit
        signal) or a `404` never arrives here — it's raised as an
        `HTTPError`, counted as `http_error`, and the proxy is cooled and
        retried on a *different* IP. For a per-IP rate limit that is exactly
        the right remedy, so there is deliberately no RATE_LIMITED branch
        below: adding one would be dead code.

        That leaves three things to tell apart among the 200s:
          * a real CVE record           → OK
          * "valid ID, doesn't exist"   → NOT_FOUND  (NVD: 200 + totalResults 0)
          * a 200 that isn't NVD JSON   → PROXY_GARBAGE (captive portal / HTML)

        Returns `(outcome, detail, body)`. `body` is the full response for OK
        (it's what `handle_success` writes); for the rest it's the bounded
        prefix used to classify.
        """
        try:
            # Bounded streaming read: accumulate up to MAX_BODY_BYTES, then stop.
            # A misbehaving proxy can answer 200 with megabytes of HTML; we never
            # need more than a CVE record's worth to classify or to save.
            buf = bytearray()
            oversized = False
            try:
                for chunk in response.iter_content(chunk_size=65536):
                    buf.extend(chunk)
                    if len(buf) > MAX_BODY_BYTES:
                        oversized = True
                        break
            except requests.exceptions.RequestException as e:
                # Mid-stream errors (ChunkedEncodingError, ConnectionError after
                # headers arrived, etc.) — blame the proxy and let the caller loop.
                return (
                    FetchOutcome.PROXY_GARBAGE,
                    f"stream error: {type(e).__name__}",
                    bytes(buf),
                )

            if oversized:
                return FetchOutcome.PROXY_GARBAGE, "oversized body", bytes(buf)

            body = bytes(buf)
            if not body:
                return FetchOutcome.PROXY_GARBAGE, "empty body", body
            try:
                data = json.loads(body)
            except ValueError:
                # 200 but not JSON — a captive portal or proxy error page.
                return FetchOutcome.PROXY_GARBAGE, f"non-json ({len(body)}B)", body
            # A genuine NVD response always carries `totalResults`. Its absence
            # means this 200 came from something other than NVD (a proxy that
            # rewrote the response, an unexpected redirect target, etc.).
            if not isinstance(data, dict) or "totalResults" not in data:
                return FetchOutcome.PROXY_GARBAGE, "not an NVD response", body
            if data.get("totalResults", 0) >= 1 and data.get("vulnerabilities"):
                return FetchOutcome.OK, None, body
            # Valid query, but NVD has no such CVE — authoritative, don't retry.
            return FetchOutcome.NOT_FOUND, "totalResults=0", body
        finally:
            response.close()

    def handle_success(self, item_id: str, body: bytes) -> bool:
        """Persist a CVE record as `<item_id>.json`, written atomically.

        Returns False if the body doesn't parse as JSON after all — `classify`
        already validated it, so this is a belt-and-braces guard against a
        proxy that swapped the body between the classify read and here. A
        False return tells the framework to treat it as a proxy fault and
        retry on a different one (mirrors the corrupt-zip path in the Bazaar
        example).
        """
        try:
            json.loads(body)
        except ValueError:
            logger.warning("Body for {} did not parse as JSON ({}B)", item_id, len(body))
            return False

        target = os.path.join(self.store_folder, f"{item_id}.json")
        # Write to a hidden per-thread partial and rename atomically. Guarantees:
        #   1. A killed mid-write never leaves a partial `<id>.json` the dedup
        #      check would treat as complete — partials are dot-prefixed, and
        #      existing_ids skips dot-prefixed names.
        #   2. Concurrent workers fetching the same ID can't see a half-written
        #      file — os.replace is atomic.
        partial = os.path.join(
            self.store_folder,
            f".partial.{os.getpid()}.{threading.get_ident()}.{item_id}.json",
        )
        try:
            with open(partial, "wb") as f:
                f.write(body)
            os.replace(partial, target)
        except OSError as e:
            logger.warning("Could not write {}: {}", target, e)
            if os.path.exists(partial):
                try:
                    os.unlink(partial)
                except OSError:
                    pass
            return False
        return True


def _parse_args(
    use_case_cls: type[UseCase], argv: list[str] | None = None
) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"Free-proxy-pool bulk fetcher (use case: {use_case_cls.name}).",
    )
    # Framework-level flags. The use case appends its own.
    p.add_argument(
        "--workers",
        type=int,
        default=100,
        help="ThreadPoolExecutor size. Default: 100.",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path. Rotates at 100MB, retains 5.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Count + log items without launching the download pool.",
    )
    use_case_cls.add_arguments(p)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for the NVD CVE use case.

    Parses the framework flags plus `NvdCveUseCase`'s own arguments
    (`add_arguments` extends the parser, `from_args` builds the instance),
    constructs a `SwarmConfig`, optionally configures a rotating log file,
    then hands off to `proxyswarm.run`.
    """
    args = _parse_args(NvdCveUseCase, argv)
    config = SwarmConfig(workers=args.workers)
    if args.log_file:
        # `enqueue=True` so log writes happen on a background thread and the
        # workers don't serialize on disk I/O.
        logger.add(
            args.log_file,
            rotation="100 MB",
            retention=5,
            enqueue=True,
            backtrace=False,
            diagnose=False,
        )
    use_case = NvdCveUseCase.from_args(args)
    run(use_case, config=config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
