import random

import pytest

from notify_queue.domain.backoff import backoff_delay


@pytest.mark.parametrize(("attempt", "ceiling"), [(1, 2), (2, 4), (3, 8), (4, 16), (10, 60)])
def test_delay_stays_within_equal_jitter_bounds(attempt: int, ceiling: float) -> None:
    rng = random.Random(7)
    for _ in range(200):
        delay = backoff_delay(attempt, base=2, cap=60, rng=rng)
        assert ceiling / 2 <= delay <= ceiling


def test_delay_grows_with_each_attempt() -> None:
    # The lower bound of attempt n+1 equals the upper bound of attempt n.
    assert backoff_delay(3, base=2, cap=600, rng=random.Random(0)) >= backoff_delay(
        2, base=2, cap=600, rng=random.Random(0)
    )


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError):
        backoff_delay(0, base=2, cap=60)
