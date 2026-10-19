"""Real-traffic generator for demos/load tests: random-size files uploaded then downloaded through the monitor."""
from __future__ import annotations

import os
import random
import tempfile
import time
import uuid
from typing import Callable

from ..models import MIB, TransferRecord


def _write_random(path: str, size: int) -> None:
    with open(path, "wb") as fh:
        left = size
        while left > 0:
            n = min(left, MIB)
            fh.write(os.urandom(n))
            left -= n


def run_loadgen(monitor, bucket: str, count: int = 20, min_mb: float = 0.1, max_mb: float = 25.0,
                interval_s: float = 1.0, prefix: str = "cloudpulse-loadgen/", seed: int | None = None,
                sleep: Callable[[float], None] = time.sleep) -> list[TransferRecord]:
    """Upload `count` random files and download each back. Returns every TransferRecord produced."""
    rng, records = random.Random(seed), []
    with tempfile.TemporaryDirectory() as tmp:
        for i in range(count):
            size = int(rng.uniform(min_mb, max_mb) * MIB)
            src, dst = os.path.join(tmp, f"up-{i}.bin"), os.path.join(tmp, f"down-{i}.bin")
            _write_random(src, size)
            key = f"{prefix}{uuid.uuid4().hex[:12]}-{i}.bin"
            up = monitor.upload(src, bucket, key)
            records.append(up)
            if up.ok:
                records.append(monitor.download(bucket, key, dst))
            for p in (src, dst):
                if os.path.exists(p):
                    os.remove(p)
            if i < count - 1:
                sleep(interval_s)
    return records
