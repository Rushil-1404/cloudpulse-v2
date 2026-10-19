"""Deterministic synthetic traffic so the platform can be demoed and tested without touching AWS.

The data deliberately contains the patterns CloudPulse is meant to find: a diurnal load curve, a recurring
nightly S3 throttling problem on large uploads, and a recent upload degradation episode.
"""
from __future__ import annotations

import math
import random
import time

from .models import MIB, TransferRecord

SIZE_CLASSES = ((0.45, 10 * 1024, 1 * MIB), (0.40, 1 * MIB, 100 * MIB),
                (0.14, 100 * MIB, 1024 * MIB), (0.01, 1024 * MIB, 4096 * MIB))
ERRORS = (("RequestTimeout", 0.35), ("EndpointConnectionError", 0.25), ("SlowDown", 0.20),
          ("InternalError", 0.10), ("AccessDenied", 0.10))
NON_RETRYABLE = {"AccessDenied"}


def _pick(rng: random.Random, weighted):
    r, acc = rng.random(), 0.0
    for item, w in weighted:
        acc += w
        if r <= acc:
            return item
    return weighted[-1][0]


def generate(days: int = 7, per_day: int = 300, seed: int = 42, now: float | None = None,
             bucket: str = "cloudpulse-demo", instances=("i-0a1b2c3d4e5f60001", "i-0a1b2c3d4e5f60002"),
             degradation_hours: float = 2.0) -> list[TransferRecord]:
    rng, now = random.Random(seed), time.time() if now is None else now
    out: list[TransferRecord] = []
    for _ in range(days * per_day):
        # diurnal load: rejection-sample the start time (busy 08:00-20:00 UTC)
        while True:
            ts = now - rng.uniform(0, days * 86400) - 1
            hour = (ts % 86400) / 3600
            if rng.random() < 0.35 + 0.65 * max(0.0, math.sin((hour - 6) / 24 * 2 * math.pi)):
                break
        direction = "upload" if rng.random() < 0.55 else "download"
        _, lo, hi = _pick(rng, [(c, c[0]) for c in SIZE_CLASSES])
        size = int(math.exp(rng.uniform(math.log(lo), math.log(hi))))
        degraded = direction == "upload" and ts >= now - degradation_hours * 3600
        bw = max(4.0, rng.gauss(60 if direction == "upload" else 90, 10)) * (0.35 if degraded else 1.0)
        latency = rng.lognormvariate(math.log(60), 0.45) * (2.5 if degraded else 1.0)

        fail_p = 0.02
        if direction == "upload" and size >= 100 * MIB and int((ts % 86400) // 3600) == 2:
            fail_p = 0.30  # recurring nightly throttling pattern
        failed = rng.random() < fail_p
        code = None
        if failed:
            code = "SlowDown" if fail_p > 0.1 else _pick(rng, ERRORS)
        retries = 0
        if failed:
            retries = 0 if code in NON_RETRYABLE else 3
        elif rng.random() < 0.05:
            retries = rng.choice([1, 1, 2, 3])
        backoff = sum(0.5 * 2 ** i for i in range(retries))
        expected = latency / 1000 + (size / MIB) / bw
        duration = expected * (rng.uniform(0.2, 0.9) if failed else 1.0) + backoff
        tp = (size / MIB) / duration
        out.append(TransferRecord(
            transfer_id=f"sim-{rng.getrandbits(64):016x}", direction=direction, bucket=bucket,
            key=f"data/{rng.getrandbits(32):08x}.bin", file_size_bytes=size, started_at=ts,
            completed_at=ts + duration, status="failed" if failed else "success",
            latency_ms=None if failed else round(latency, 2), retries=retries, error_code=code,
            error_message=f"simulated {code}" if code else None, instance_id=rng.choice(instances),
            cpu_percent=round(min(100, max(1, rng.gauss(12 + tp * 0.35, 4))), 1),
            memory_percent=round(min(100, max(5, rng.gauss(45, 6))), 1),
            network_mbps=round(tp * rng.uniform(1.0, 1.1), 3),
        ))
    return sorted(out, key=lambda r: r.started_at)
