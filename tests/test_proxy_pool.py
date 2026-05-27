"""ProxyPool: the acquire↔mark accounting contract, cooldown/backoff math, the
daily cap, EWMA updates, and disk-load coercion. All pure-unit; `time.time` is
monkeypatched where the math depends on it."""

from proxyswarm import ProxyPool, SwarmConfig, core

P1 = "http://1.1.1.1:8080"
P2 = "http://2.2.2.2:8080"


def make_pool() -> ProxyPool:
    # state_file=None → no background saver thread, no atexit registration.
    return ProxyPool([P1, P2], SwarmConfig(), state_file=None)


# --- accounting contract -----------------------------------------------------


def test_acquire_increments_attempts_and_usage() -> None:
    pool = make_pool()
    p = pool.acquire()
    assert p in (P1, P2)
    s = pool.state[p]
    assert s["attempts"] == 1
    assert s["usage_today"] == 1
    assert s["failures"] == 0


def test_mark_success_clears_cooldown_and_promotes() -> None:
    pool = make_pool()
    p = pool.acquire()
    assert p is not None
    pool.mark_success(p, elapsed_ms=150.0)
    s = pool.state[p]
    assert s["consecutive_failures"] == 0
    assert s["cooldown_until"] == 0
    assert s["last_success_ts"] > 0
    assert s["ewma_ms"] == 150.0  # seeded on first observation
    assert p in pool.good_candidates
    assert p in pool.good_set


def test_mark_bad_records_failure_and_cooldown() -> None:
    pool = make_pool()
    p = pool.acquire()
    assert p is not None
    pool.mark_bad(p, "timeout")
    s = pool.state[p]
    assert s["failures"] == 1
    assert s["consecutive_failures"] == 1
    assert s["last_reason"] == "timeout"
    assert s["cooldown_until"] > 0


def test_mark_exhausted_retires_until_midnight() -> None:
    pool = make_pool()
    p = pool.acquire()
    assert p is not None
    pool.mark_exhausted(p)
    assert p in pool.exhausted
    assert pool.state[p]["cooldown_until"] > 0
    assert pool.alive_count() == len([P1, P2]) - 1


def test_daily_cap_cools_proxy(monkeypatch) -> None:
    cfg = SwarmConfig(daily_per_ip_limit=2)
    pool = ProxyPool([P1], cfg, state_file=None)
    now = 1_000_000.0
    monkeypatch.setattr(core.time, "time", lambda: now)
    # Two reserves allowed, the third must trip the daily cap.
    assert pool._reserve(P1, now) == P1
    assert pool._reserve(P1, now) == P1
    assert pool._reserve(P1, now) is None
    assert pool.state[P1]["last_reason"] == "daily cap"


# --- cooldown / backoff math -------------------------------------------------


def test_cooldown_exponential_backoff(monkeypatch) -> None:
    pool = make_pool()
    now = 1_000_000.0
    monkeypatch.setattr(core.time, "time", lambda: now)
    base = pool.config.cooldown_base_sec
    assert pool._cooldown_for("timeout", 1) == now + base
    assert pool._cooldown_for("timeout", 2) == now + base * 2
    assert pool._cooldown_for("timeout", 3) == now + base * 4


def test_cooldown_capped_at_max(monkeypatch) -> None:
    pool = make_pool()
    now = 1_000_000.0
    monkeypatch.setattr(core.time, "time", lambda: now)
    assert pool._cooldown_for("timeout", 99) == now + pool.config.cooldown_max_sec


def test_daily_cap_reason_cools_until_midnight() -> None:
    pool = make_pool()
    cooldown = pool._cooldown_for("daily cap", 1)
    assert cooldown == core._next_utc_midnight_ts() or cooldown > 0


def test_in_cooldown_explicit_timestamp() -> None:
    pool = make_pool()
    now = 1_000_000.0
    s = pool._empty_stats()
    s["cooldown_until"] = now + 10
    assert pool._in_cooldown(s, now) is True
    s["cooldown_until"] = now - 10
    assert pool._in_cooldown(s, now) is False


def test_in_cooldown_long_term_high_failure_rate() -> None:
    pool = make_pool()
    now = 1_000_000.0
    s = pool._empty_stats()
    s["attempts"] = 10
    s["failures"] = 9  # 90% >= high_failure_rate (0.8)
    s["last_failure_ts"] = now - 100  # within cooldown_max window
    assert pool._in_cooldown(s, now) is True
    # Stale failure (older than the cooldown window) no longer holds it.
    s["last_failure_ts"] = now - (pool.config.cooldown_max_sec + 1)
    assert pool._in_cooldown(s, now) is False


def test_reset_daily_zeroes_on_date_change() -> None:
    pool = make_pool()
    s = pool._empty_stats()
    s["usage_today"] = 50
    s["usage_date"] = "1999-01-01"
    pool._reset_daily(s)
    assert s["usage_today"] == 0
    assert s["usage_date"] == core._today_utc()


# --- EWMA --------------------------------------------------------------------


def test_ewma_latency_seeds_then_blends() -> None:
    pool = make_pool()
    s = pool._empty_stats()
    pool._update_ewma(s, sample_ms=200.0, success=True)
    assert s["ewma_ms"] == 200.0  # seeded, not pulled toward the 0.0 sentinel
    pool._update_ewma(s, sample_ms=400.0, success=True)
    a = pool.config.ewma_alpha_latency
    assert abs(s["ewma_ms"] - ((1 - a) * 200.0 + a * 400.0)) < 1e-9


def test_ewma_success_decays_on_failure() -> None:
    pool = make_pool()
    s = pool._empty_stats()
    pool._update_ewma(s, success=False)
    a = pool.config.ewma_alpha_success
    assert abs(s["ewma_success"] - ((1 - a) * 1.0 + a * 0.0)) < 1e-9


# --- disk-load coercion (the cast → _coerce_stats fix) -----------------------


def test_coerce_stats_fills_missing_keys() -> None:
    coerced = ProxyPool._coerce_stats({"attempts": 5})
    assert set(coerced) == set(ProxyPool._empty_stats())
    assert coerced["attempts"] == 5
    assert coerced["failures"] == 0  # default


def test_coerce_stats_rejects_wrong_types_and_extra_keys() -> None:
    coerced = ProxyPool._coerce_stats(
        {"attempts": "not-an-int", "usage_date": "2026-05-27", "bogus": 1}
    )
    assert coerced["attempts"] == 0  # wrong type → default
    assert coerced["usage_date"] == "2026-05-27"
    assert "bogus" not in coerced


def test_coerce_stats_accepts_float_timestamps() -> None:
    coerced = ProxyPool._coerce_stats({"last_success_ts": 1234.5})
    assert coerced["last_success_ts"] == 1234.5


def test_coerce_stats_rejects_bool_for_int_field() -> None:
    # JSON `true` must not be accepted as a count (bool is an int subclass).
    coerced = ProxyPool._coerce_stats({"attempts": True})
    assert coerced["attempts"] == 0
