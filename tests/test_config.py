"""SwarmConfig.__post_init__ validation — illegal configs must fail loudly at
construction rather than as a negative slice or ThreadPoolExecutor error deep
into a run."""

import pytest

from proxyswarm import SwarmConfig


def test_defaults_construct() -> None:
    cfg = SwarmConfig()
    assert cfg.workers == 100
    assert 0.0 <= cfg.exploration_fraction <= 1.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"exploration_fraction": 2.0},
        {"exploration_fraction": -0.1},
        {"lane_drain_fraction": 1.5},
        {"lane_gate_percentile": 2.0},
        {"ewma_alpha_latency": -1.0},
        {"ewma_alpha_success": 1.01},
        {"high_failure_rate": 5.0},
    ],
)
def test_fraction_fields_must_be_unit_interval(kwargs: dict) -> None:
    with pytest.raises(ValueError, match="must be in \\[0, 1\\]"):
        SwarmConfig(**kwargs)


def test_workers_must_be_positive() -> None:
    with pytest.raises(ValueError, match="workers must be >= 1"):
        SwarmConfig(workers=0)


def test_top_k_must_be_positive() -> None:
    with pytest.raises(ValueError, match="top_k_fast_lane must be >= 1"):
        SwarmConfig(top_k_fast_lane=0)


def test_retry_attempt_ordering() -> None:
    with pytest.raises(ValueError, match="min_retry_attempts"):
        SwarmConfig(min_retry_attempts=100, max_retry_attempts=10)


def test_cooldown_ordering() -> None:
    with pytest.raises(ValueError, match="cooldown_base_sec"):
        SwarmConfig(cooldown_base_sec=99_999_999, cooldown_max_sec=10)


def test_health_check_concurrency_must_be_positive() -> None:
    with pytest.raises(ValueError, match="health_check_concurrency must be >= 1"):
        SwarmConfig(health_check_concurrency=0)


def test_health_check_target_alive_must_be_non_negative() -> None:
    with pytest.raises(ValueError, match="health_check_target_alive must be >= 0"):
        SwarmConfig(health_check_target_alive=-1)


def test_health_check_target_alive_zero_is_allowed() -> None:
    # 0 = exhaustive (no early stop); a legal, supported configuration.
    assert SwarmConfig(health_check_target_alive=0).health_check_target_alive == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"health_check_connect_timeout_sec": 0.0},
        {"health_check_connect_timeout_sec": -1.0},
        {"health_check_read_timeout_sec": 0.0},
        {"health_check_read_timeout_sec": -2.0},
    ],
)
def test_health_check_timeouts_must_be_positive(kwargs: dict) -> None:
    with pytest.raises(ValueError, match="must be > 0"):
        SwarmConfig(**kwargs)
