"""boto3 helpers: a client factory plus a counter that exposes botocore's *hidden* internal retries.

By default botocore silently retries throttles/timeouts (up to 4 times for S3) before the caller sees anything, so a
monitor that only counts its own retries under-reports flakiness. RetryCounter hooks the client's event system:
every HTTP attempt fires `request-created`, every logical API call fires `before-call`; the difference is the number
of internal retries.
"""
from __future__ import annotations

import threading


class RetryCounter:
    def __init__(self):
        self._lock, self._attempts, self._calls = threading.Lock(), 0, 0

    def attach(self, client) -> "RetryCounter":
        client.meta.events.register("request-created", self._on_attempt)
        client.meta.events.register("before-call", self._on_call)
        return self

    def _on_attempt(self, **_):
        with self._lock:
            self._attempts += 1

    def _on_call(self, **_):
        with self._lock:
            self._calls += 1

    def value(self) -> int:
        """Cumulative internal retries (attempts beyond the first for each call)."""
        with self._lock:
            return max(0, self._attempts - self._calls)


def make_s3_client(region: str | None = None):
    """Returns (s3_client, RetryCounter) with sane timeouts so a stalled connection cannot hang the agent."""
    import boto3
    from botocore.config import Config

    client = boto3.client("s3", region_name=region, config=Config(
        connect_timeout=5, read_timeout=60, retries={"mode": "standard", "max_attempts": 5},
        user_agent_extra="cloudpulse/2"))
    return client, RetryCounter().attach(client)
