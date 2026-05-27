"""Stats key guards and the Downloader per-call accounting contract.

The guard test locks down the silent-item-black-hole fix: an unknown outcome
key must be logged-and-skipped, never raise into download()'s @logger.catch.
The download tests lock down "exactly one metric per call" plus pairing every
acquire() with a mark_*.
"""

import requests

from proxyswarm import Downloader, FetchOutcome, ProxyPool, RequestSpec, SwarmConfig
from proxyswarm.core import Stats

P1 = "http://1.1.1.1:8080"


# --- Stats key guards --------------------------------------------------------


def test_bump_unknown_metric_key_does_not_raise() -> None:
    stats = Stats()
    stats.bump("typo")  # must not raise
    metrics, _ = stats.snapshot()
    assert sum(metrics.values()) == 0


def test_bump_unknown_outcome_key_does_not_raise() -> None:
    stats = Stats()
    stats.bump_outcome("bogus_outcome")  # the plugin black-hole guard
    _, outcomes = stats.snapshot()
    assert sum(outcomes.values()) == 0


def test_bump_known_keys_count() -> None:
    stats = Stats()
    stats.bump("success")
    stats.bump_outcome("ok")
    metrics, outcomes = stats.snapshot()
    assert metrics["success"] == 1
    assert outcomes["ok"] == 1


# --- Downloader accounting ---------------------------------------------------


class FakeUseCase:
    name = "fake"

    def __init__(self, outcome: FetchOutcome, handle_ok: bool = True) -> None:
        self._outcome = outcome
        self._handle_ok = handle_ok

    def build_request(self, item_id: str) -> RequestSpec:
        return RequestSpec(url="http://example.invalid/")

    def classify(self, response):
        return self._outcome, None, b"body"

    def handle_success(self, item_id: str, body: bytes) -> bool:
        return self._handle_ok


def make_downloader(use_case, monkeypatch, existing=None) -> Downloader:
    cfg = SwarmConfig()
    pool = ProxyPool([P1], cfg, state_file=None)
    dl = Downloader(
        requests.Session(), pool, use_case, existing or set(), Stats(), cfg
    )
    # Replace the transport with a canned clean exchange so no socket is opened.
    monkeypatch.setattr(dl, "_post", lambda proxy, item_id: (object(), 100.0))
    return dl


def test_download_ok_bumps_success_once_and_pairs_proxy(monkeypatch) -> None:
    dl = make_downloader(FakeUseCase(FetchOutcome.OK), monkeypatch)
    dl.download("item1")
    metrics, _ = dl.stats.snapshot()
    assert metrics == {"success": 1, "skipped": 0, "not_found": 0, "failed": 0}
    # The acquired proxy was credited (paired with mark_success).
    s = dl.proxy_pool.state[P1]
    assert s["attempts"] == 1
    assert s["failures"] == 0
    assert s["last_success_ts"] > 0


def test_download_not_found_bumps_once(monkeypatch) -> None:
    dl = make_downloader(FakeUseCase(FetchOutcome.NOT_FOUND), monkeypatch)
    dl.download("item1")
    metrics, _ = dl.stats.snapshot()
    assert metrics["not_found"] == 1
    assert sum(metrics.values()) == 1


def test_download_skips_already_have(monkeypatch) -> None:
    dl = make_downloader(
        FakeUseCase(FetchOutcome.OK), monkeypatch, existing={"item1"}
    )
    dl.download("item1")
    metrics, _ = dl.stats.snapshot()
    assert metrics["skipped"] == 1
    assert sum(metrics.values()) == 1
    # Skipped before acquiring — no proxy touched.
    assert dl.proxy_pool.state.get(P1, {"attempts": 0})["attempts"] == 0
