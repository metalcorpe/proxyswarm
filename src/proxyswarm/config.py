"""Runtime tuning knobs for the proxy swarm, with construction-time validation."""

import dataclasses


@dataclasses.dataclass
class SwarmConfig:
    """Tunable parameters for `ProxyPool`, `Downloader`, and the orchestrator.

    All fields have working defaults; `__post_init__` rejects out-of-range
    values (negative counts, fractions outside [0, 1], inverted min/max pairs)
    so misconfiguration fails at construction rather than deep into a run.
    """

    workers: int = 100
    request_timeout_sec: int = 4
    max_retry_attempts: int = 40
    min_retry_attempts: int = 10

    proxies_file: str = "proxies.txt"
    proxy_state_file: str = "proxy_state.json"

    daily_per_ip_limit: int = 1900

    cooldown_base_sec: int = 300
    cooldown_max_sec: int = 86400
    save_interval_sec: int = 60
    min_attempts_for_rate: int = 5
    high_failure_rate: float = 0.8

    top_k_fast_lane: int = 100
    ewma_alpha_latency: float = 0.3
    ewma_alpha_success: float = 0.1
    refresh_interval: int = 50
    untested_latency_ms: float = 5000.0
    slow_lane_budget: int = 100
    slow_lane_batch: int = 20
    starvation_warn_sec: int = 30
    acquire_retry_sleep_sec: float = 0.5
    acquire_backoff_cap_sec: float = 5.0
    probation_threshold: int = 3
    lane_drain_fraction: float = 0.5
    lane_gate_percentile: float = 0.9
    throttle_streak_threshold: int = 5

    exploration_fraction: float = 0.1
    exploration_max_attempts: int = 10
    exploration_walk_multiplier: int = 100

    metrics_interval_sec: int = 30

    # Fields constrained to the [0, 1] interval. Out-of-range values silently
    # corrupt the lane slot math (e.g. exploration_fraction > 1 makes
    # exploitation_budget negative and `usable[:negative]` a wrong slice).
    _FRACTION_FIELDS = (
        "exploration_fraction",
        "lane_drain_fraction",
        "lane_gate_percentile",
        "ewma_alpha_latency",
        "ewma_alpha_success",
        "high_failure_rate",
    )

    def __post_init__(self) -> None:
        """Validate ranges, raising ValueError on any illegal field."""
        # Validate at construction so misconfiguration fails loudly and early
        # rather than as a negative slice or a ThreadPoolExecutor ValueError
        # deep into a run.
        for field in self._FRACTION_FIELDS:
            value = getattr(self, field)
            if not 0.0 <= value <= 1.0:
                msg = f"{field} must be in [0, 1], got {value}"
                raise ValueError(msg)
        if self.workers < 1:
            msg = f"workers must be >= 1, got {self.workers}"
            raise ValueError(msg)
        if self.top_k_fast_lane < 1:
            msg = f"top_k_fast_lane must be >= 1, got {self.top_k_fast_lane}"
            raise ValueError(msg)
        if self.min_retry_attempts > self.max_retry_attempts:
            msg = (
                f"min_retry_attempts ({self.min_retry_attempts}) must be "
                f"<= max_retry_attempts ({self.max_retry_attempts})"
            )
            raise ValueError(msg)
        if self.cooldown_base_sec > self.cooldown_max_sec:
            msg = (
                f"cooldown_base_sec ({self.cooldown_base_sec}) must be "
                f"<= cooldown_max_sec ({self.cooldown_max_sec})"
            )
            raise ValueError(msg)
