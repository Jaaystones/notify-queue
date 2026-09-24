import random


def backoff_delay(
    attempt: int, *, base: float, cap: float, rng: random.Random | None = None
) -> float:
    """Delay before retrying after failed attempt number ``attempt`` (1-based).

    Exponential with "equal jitter": the ceiling is ``min(cap, base * 2**(attempt-1))``
    and the delay is drawn from ``[ceiling/2, ceiling]``. Half the ceiling guarantees
    the delay really grows with each attempt; the random half spreads out retries
    so jobs that failed together (e.g. during a provider outage) don't all retry in
    the same instant.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    ceiling = min(cap, base * 2 ** (attempt - 1))
    return ceiling / 2 + (rng or random).uniform(0, ceiling / 2)
