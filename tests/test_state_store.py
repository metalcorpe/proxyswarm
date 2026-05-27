"""ProxyStateStore: load/save round-trip, atomic persistence, unknown-key
pruning, and corrupt-file handling (the persistence boundary)."""

import json

from proxyswarm import ProxyPool, SwarmConfig
from proxyswarm.core import ProxyStateStore

P1 = "http://1.1.1.1:8080"
P2 = "http://2.2.2.2:8080"


def test_load_missing_file_returns_empty(tmp_path) -> None:
    assert ProxyStateStore.load(str(tmp_path / "nope.json")) == {}


def test_load_corrupt_file_returns_empty(tmp_path) -> None:
    f = tmp_path / "state.json"
    f.write_text("{ this is not valid json")
    assert ProxyStateStore.load(str(f)) == {}


def test_load_non_dict_returns_empty(tmp_path) -> None:
    f = tmp_path / "state.json"
    f.write_text("[1, 2, 3]")
    assert ProxyStateStore.load(str(f)) == {}


def test_save_load_round_trip(tmp_path) -> None:
    f = str(tmp_path / "state.json")
    pool = ProxyPool([P1, P2], SwarmConfig(), state_file=f)
    p = pool.acquire()
    pool.mark_success(p, elapsed_ms=123.0)
    pool.save_state()

    restored = ProxyPool([P1, P2], SwarmConfig(), state_file=f)
    assert p in restored.state
    assert restored.state[p]["last_success_ts"] > 0
    assert restored.state[p]["ewma_ms"] == 123.0
    # A succeeded, non-cooled proxy is seeded back into the fast lane on load.
    assert p in restored.good_candidates


def test_unknown_keys_pruned_on_load(tmp_path) -> None:
    f = tmp_path / "state.json"
    payload = {
        P1: dict(ProxyPool._empty_stats()),
        "http://9.9.9.9:1": dict(ProxyPool._empty_stats()),  # not in proxy list
    }
    f.write_text(json.dumps(payload))
    pool = ProxyPool([P1, P2], SwarmConfig(), state_file=str(f))
    assert P1 in pool.state
    assert "http://9.9.9.9:1" not in pool.state


def test_save_is_noop_when_not_dirty(tmp_path) -> None:
    f = tmp_path / "state.json"
    store = ProxyStateStore(str(f), {}, __import__("threading").Lock(), SwarmConfig())
    store.save()  # not dirty → must not create the file
    assert not f.exists()


def test_save_atomic_replace(tmp_path) -> None:
    f = tmp_path / "state.json"
    state = {P1: dict(ProxyPool._empty_stats())}
    import threading

    store = ProxyStateStore(str(f), state, threading.Lock(), SwarmConfig())
    store.mark_dirty()
    store.save()
    assert json.loads(f.read_text())[P1]["ewma_success"] == 1.0
    # No leftover tmp files from the atomic rename.
    assert not list(tmp_path.glob("*.tmp.*"))
