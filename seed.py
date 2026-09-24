"""Seed the running API with a demo workload.

    uv run python seed.py [--api-url http://localhost:8000]

Submits through the public API (not the database) so the seed also exercises
validation and idempotency. The mix is chosen so every feature shows up in
/v1/metrics, /v1/dead-letters and /mock/webhooks:

  * immediate jobs across all channels and priorities
  * delayed jobs (send_at and delay_seconds)
  * a burst to one recipient that exceeds RATE_LIMIT_PER_HOUR (default 10)
  * repeated idempotency keys (replays) and one conflicting reuse (409)
  * a permanently undeliverable job and a poison message (both end in the DLQ)
"""

import argparse
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

CHANNELS = ["email", "sms", "push"]
PRIORITIES = ["low", "normal", "high", "critical"]


def payload_for(channel: str, n: int) -> dict[str, Any]:
    if channel == "email":
        return {"subject": f"Your listing #{n} got a new enquiry", "body": "Log in to reply."}
    if channel == "sms":
        return {"text": f"Viewing #{n} confirmed for tomorrow 10:00."}
    return {"title": "Price drop", "body": f"A saved property (#{n}) dropped 5%."}


def build_requests() -> list[tuple[str, dict[str, Any], dict[str, str]]]:
    """(label, body, headers) for every request the seed sends."""
    rng = random.Random(42)
    requests: list[tuple[str, dict[str, Any], dict[str, str]]] = []

    for n in range(1, 25):
        channel = CHANNELS[n % 3]
        requests.append(
            (
                "immediate",
                {
                    "recipient": f"user{n}@example.com",
                    "channel": channel,
                    "payload": payload_for(channel, n),
                    "priority": rng.choice(PRIORITIES),
                },
                {},
            )
        )

    for n in range(1, 4):
        requests.append(
            (
                "delayed (delay_seconds)",
                {
                    "recipient": f"later{n}@example.com",
                    "channel": "email",
                    "payload": payload_for("email", 100 + n),
                    "delay_seconds": 20 * n,
                },
                {},
            )
        )
    send_at = (datetime.now(UTC) + timedelta(minutes=2)).isoformat()
    requests.append(
        (
            "delayed (send_at)",
            {
                "recipient": "later-send-at@example.com",
                "channel": "sms",
                "payload": payload_for("sms", 200),
                "send_at": send_at,
                "priority": "high",
            },
            {},
        )
    )

    for n in range(1, 15):
        requests.append(
            (
                "rate-limited burst",
                {
                    "recipient": "busy-agent@example.com",
                    "channel": "push",
                    "payload": payload_for("push", 300 + n),
                },
                {},
            )
        )

    for n in range(1, 4):
        body = {
            "recipient": f"order{n}@example.com",
            "channel": "email",
            "payload": {"subject": f"Receipt for order {n}"},
            "priority": "high",
        }
        headers = {"Idempotency-Key": f"order-{n}-receipt"}
        requests.append(("idempotent (first)", body, headers))
        requests.append(("idempotent (replay)", body, headers))
    requests.append(
        (
            "idempotent (conflicting reuse)",
            {"recipient": "someone-else@example.com", "channel": "email", "payload": {}},
            {"Idempotency-Key": "order-1-receipt"},
        )
    )

    requests.append(
        (
            "permanent failure",
            {
                "recipient": "bounced@invalid.example",
                "channel": "email",
                "payload": {"simulate": "permanent_failure", "subject": "Hello"},
            },
            {},
        )
    )
    requests.append(
        (
            "poison message",
            {
                "recipient": "user-poison@example.com",
                "channel": "push",
                "payload": {"simulate": "poison"},
                "max_attempts": 3,
            },
            {},
        )
    )
    return requests


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--api-url", default="http://localhost:8000")
    args = parser.parse_args()

    outcomes: Counter[tuple[str, int]] = Counter()
    with httpx.Client(base_url=args.api_url, timeout=10) as client:
        for label, body, headers in build_requests():
            response = client.post("/v1/jobs", json=body, headers=headers)
            outcomes[(label, response.status_code)] += 1

    print(f"Seeded {sum(outcomes.values())} requests against {args.api_url}\n")
    print(f"{'request':34} {'HTTP':>5} {'count':>6}")
    for (label, status), count in sorted(outcomes.items()):
        print(f"{label:34} {status:>5} {count:>6}")
    print(
        "\nWatch progress:  curl -s localhost:8000/v1/metrics"
        "\nDead letters:    curl -s localhost:8000/v1/dead-letters"
        "\nWebhooks:        curl -s localhost:8000/mock/webhooks"
    )


if __name__ == "__main__":
    main()
