"""APIThrottleDetector: the streak / dedup / recovery state machine that tells
an operator when they're hitting key-level (not IP-level) throttling."""

from proxyswarm.core import APIThrottleDetector, FetchOutcome


def test_streak_warns_at_threshold() -> None:
    d = APIThrottleDetector(threshold=3)
    d.observe(FetchOutcome.RATE_LIMITED)
    d.observe(FetchOutcome.RATE_LIMITED)
    assert d.warned_label is None  # below threshold
    d.observe(FetchOutcome.RATE_LIMITED)
    assert d.streak == 3
    assert d.warned_label == "rate_limited"


def test_proxy_faults_do_not_affect_streak() -> None:
    d = APIThrottleDetector(threshold=3)
    d.observe(FetchOutcome.RATE_LIMITED)
    d.observe(FetchOutcome.PROXY_BAD)
    d.observe(FetchOutcome.PROXY_GARBAGE)
    assert d.streak == 1  # only the rate_limited counted


def test_authoritative_answer_resets_streak() -> None:
    d = APIThrottleDetector(threshold=2)
    d.observe(FetchOutcome.RATE_LIMITED)
    d.observe(FetchOutcome.RATE_LIMITED)
    assert d.warned_label == "rate_limited"
    d.observe(FetchOutcome.OK)
    assert d.streak == 0
    assert d.warned_label is None


def test_warning_is_deduped_until_cleared() -> None:
    d = APIThrottleDetector(threshold=2)
    for _ in range(5):
        d.observe(FetchOutcome.RATE_LIMITED)
    # Still warned once for the same label; streak keeps climbing.
    assert d.warned_label == "rate_limited"
    assert d.streak == 5


def test_label_flip_rewarns() -> None:
    d = APIThrottleDetector(threshold=2)
    d.observe(FetchOutcome.RATE_LIMITED)
    d.observe(FetchOutcome.RATE_LIMITED)
    assert d.warned_label == "rate_limited"
    # auth_error also implicates the key and continues the streak, but the label
    # flips, so the detector must surface the new label.
    d.observe(FetchOutcome.AUTH_ERROR)
    assert d.warned_label == "auth_error"
