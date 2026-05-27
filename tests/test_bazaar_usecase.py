"""MalwareBazaarUseCase: response classification and AES-zip extraction.

`classify` is the throttle-vs-proxy-fault decision the retry loop hinges on;
`_extract_zip` extracts attacker-influenced archives, so the basename
path-traversal guard and atomic, corrupt-safe writes are security-relevant.
"""

import io
import json
from typing import TYPE_CHECKING, cast

import pyzipper

import malware_bazaar as mb  # from examples/, via conftest sys.path insert
from proxyswarm import FetchOutcome

if TYPE_CHECKING:
    import requests


class FakeResponse:
    """Minimal duck-typed stand-in: classify only uses iter_content + close."""

    def __init__(self, *chunks: bytes) -> None:
        self._chunks = list(chunks)
        self.closed = False

    def iter_content(self, chunk_size: int = 1):
        # Honor chunk_size like requests does, so classify's bounded-peek break
        # (which fires once the buffer reaches chunk_size) behaves realistically.
        data = b"".join(self._chunks)
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    def close(self) -> None:
        self.closed = True


def make_use_case(store: str) -> mb.MalwareBazaarUseCase:
    return mb.MalwareBazaarUseCase("exe", store, "index.csv", "test-key")


def classify(*chunks: bytes):
    uc = make_use_case("unused")
    resp = FakeResponse(*chunks)
    # FakeResponse duck-types only what classify touches (iter_content/close).
    outcome, detail, body = uc.classify(cast("requests.Response", resp))
    assert resp.closed, "classify must close the response in its finally"
    return outcome, detail, body


# --- classify ----------------------------------------------------------------


def test_zip_magic_is_ok() -> None:
    outcome, _, body = classify(b"PK\x03\x04" + b"\x00" * 200)
    assert outcome == FetchOutcome.OK
    assert body.startswith(b"PK\x03\x04")


def test_json_file_not_found() -> None:
    outcome, detail, _ = classify(
        json.dumps({"query_status": "file_not_found"}).encode()
    )
    assert outcome == FetchOutcome.NOT_FOUND
    assert detail == "file_not_found"


def test_json_limit_exceeded_is_rate_limited() -> None:
    outcome, _, _ = classify(json.dumps({"query_status": "limit_exceeded"}).encode())
    assert outcome == FetchOutcome.RATE_LIMITED


def test_json_unauthorized_is_auth_error() -> None:
    outcome, _, _ = classify(json.dumps({"query_status": "unauthorized"}).encode())
    assert outcome == FetchOutcome.AUTH_ERROR


def test_unknown_status_is_proxy_bad() -> None:
    outcome, detail, _ = classify(json.dumps({"query_status": "weird"}).encode())
    assert outcome == FetchOutcome.PROXY_BAD
    assert "weird" in (detail or "")


def test_invalid_json_is_garbage() -> None:
    outcome, _, _ = classify(b"<html>captive portal</html>")
    assert outcome == FetchOutcome.PROXY_GARBAGE


def test_short_body_is_garbage() -> None:
    outcome, detail, _ = classify(b"PK")  # < 4 bytes, undecidable
    assert outcome == FetchOutcome.PROXY_GARBAGE
    assert "short" in (detail or "")


def test_non_zip_peek_is_bounded() -> None:
    # A multi-chunk non-zip body must stop reading near CLASSIFY_PEEK_BYTES
    # rather than buffering everything.
    big = b"x" * (mb.CLASSIFY_PEEK_BYTES * 4)
    _, _, body = classify(b"not-a-zip", big)
    assert len(body) <= mb.CLASSIFY_PEEK_BYTES + len(b"not-a-zip")


# --- _extract_zip / handle_success ------------------------------------------


def _aes_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with pyzipper.AESZipFile(
        buf, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES
    ) as zf:
        zf.setpassword(b"infected")
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def test_extract_writes_member(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    uc = make_use_case(str(store))
    assert uc.handle_success("sha", _aes_zip({"sample.exe": b"payload"})) is True
    assert (store / "sample.exe").read_bytes() == b"payload"


def test_extract_path_traversal_collapsed_to_basename(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    uc = make_use_case(str(store))
    uc.handle_success("sha", _aes_zip({"../../escape.exe": b"evil"}))
    # The traversal-prefixed member lands inside the store as its basename only.
    assert (store / "escape.exe").is_file()
    assert not (tmp_path / "escape.exe").exists()
    assert not (tmp_path.parent / "escape.exe").exists()


def test_corrupt_zip_returns_false_and_leaves_no_partial(tmp_path) -> None:
    store = tmp_path / "store"
    store.mkdir()
    uc = make_use_case(str(store))
    assert uc.handle_success("sha", b"PK\x03\x04 not really a zip") is False
    assert not list(store.glob(".partial.*"))
    assert not list(store.iterdir())  # nothing committed
