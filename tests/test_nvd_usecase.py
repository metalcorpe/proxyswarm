"""NvdCveUseCase: response classification and atomic JSON persistence.

`classify` is the 200-body decision the retry loop hinges on (real record vs.
NVD's "doesn't exist" 200 vs. a captive-portal 200); `handle_success` does an
atomic, corrupt-safe write. Non-2xx handling lives in the framework
(`Downloader._post` raises before classify runs), so it isn't tested here.
"""

import json

import nvd_cve as nv  # from examples/, via conftest sys.path insert
from proxyswarm import FetchOutcome


class FakeResponse:
    """Minimal duck-typed stand-in: classify only uses iter_content + close."""

    def __init__(self, *chunks: bytes) -> None:
        self._chunks = list(chunks)
        self.closed = False

    def iter_content(self, chunk_size: int = 1):
        # Honor chunk_size like requests does, so classify's bounded read
        # behaves realistically.
        data = b"".join(self._chunks)
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    def close(self) -> None:
        self.closed = True


def make_use_case(store: str) -> "nv.NvdCveUseCase":
    return nv.NvdCveUseCase(2024, 1, 10, store, api_key=None)


def classify(*chunks: bytes):
    uc = make_use_case("unused")
    resp = FakeResponse(*chunks)
    outcome, detail, body = uc.classify(resp)
    assert resp.closed, "classify must close the response in its finally"
    return outcome, detail, body


def _nvd_hit() -> bytes:
    return json.dumps(
        {"totalResults": 1, "vulnerabilities": [{"cve": {"id": "CVE-2024-0001"}}]}
    ).encode()


# --- classify ----------------------------------------------------------------


def test_record_with_results_is_ok() -> None:
    outcome, _, body = classify(_nvd_hit())
    assert outcome == FetchOutcome.OK
    assert json.loads(body)["totalResults"] == 1


def test_zero_results_is_not_found() -> None:
    outcome, detail, _ = classify(
        json.dumps({"totalResults": 0, "vulnerabilities": []}).encode()
    )
    assert outcome == FetchOutcome.NOT_FOUND
    assert "totalResults=0" in (detail or "")


def test_non_json_is_garbage() -> None:
    outcome, detail, _ = classify(b"<html>captive portal</html>")
    assert outcome == FetchOutcome.PROXY_GARBAGE
    assert "non-json" in (detail or "")


def test_json_without_total_results_is_garbage() -> None:
    # A 200 whose JSON isn't an NVD response (e.g. a proxy rewrote it).
    outcome, detail, _ = classify(json.dumps({"unexpected": "shape"}).encode())
    assert outcome == FetchOutcome.PROXY_GARBAGE
    assert "NVD" in (detail or "")


def test_empty_body_is_garbage() -> None:
    outcome, detail, _ = classify(b"")
    assert outcome == FetchOutcome.PROXY_GARBAGE
    assert "empty" in (detail or "")


def test_oversized_body_is_garbage_and_bounded() -> None:
    # A multi-MB 200 (captive portal flood) is rejected without buffering it all.
    big = b"x" * (nv.MAX_BODY_BYTES + 1024)
    outcome, detail, body = classify(big)
    assert outcome == FetchOutcome.PROXY_GARBAGE
    assert "oversized" in (detail or "")
    # Bounded read: stops within one chunk (65536) past the cap, not the full body.
    assert len(body) <= nv.MAX_BODY_BYTES + 65536


# --- handle_success ----------------------------------------------------------


def test_handle_success_writes_record(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    uc = make_use_case(str(store))
    body = _nvd_hit()
    assert uc.handle_success("CVE-2024-0001", body) is True
    assert (store / "CVE-2024-0001.json").read_bytes() == body


def test_handle_success_rejects_non_json_and_leaves_no_partial(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    uc = make_use_case(str(store))
    assert uc.handle_success("CVE-2024-0001", b"not json at all") is False
    assert not list(store.glob(".partial.*"))
    assert not list(store.iterdir())  # nothing committed


def test_existing_ids_strips_json_and_skips_partials(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    (store / "CVE-2024-0001.json").write_bytes(_nvd_hit())
    (store / ".partial.1.2.CVE-2024-0002.json").write_bytes(b"in-progress")
    uc = make_use_case(str(store))
    assert uc.existing_ids() == {"CVE-2024-0001"}


def test_iter_items_generates_padded_range() -> None:
    uc = nv.NvdCveUseCase(2024, 8, 3, "unused", api_key=None)
    assert list(uc.iter_items()) == ["CVE-2024-0008", "CVE-2024-0009", "CVE-2024-0010"]
