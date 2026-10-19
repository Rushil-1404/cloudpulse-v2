"""Host resource sampling (CPU / memory / network) around a transfer, plus EC2 instance-id discovery."""
from __future__ import annotations

import socket

from ..models import MIB

try:  # psutil is optional: metrics are simply reported as None without it
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


def _cpu_snapshot() -> tuple[float, float]:
    """(total, idle) CPU seconds. guest time is already counted inside user time, so it is subtracted."""
    t = psutil.cpu_times()
    total = sum(t) - getattr(t, "guest", 0.0) - getattr(t, "guest_nice", 0.0)
    return total, t.idle + getattr(t, "iowait", 0.0)


def cpu_percent_between(a: tuple[float, float], b: tuple[float, float]) -> float | None:
    """Busy % between two (total, idle) snapshots; None if no CPU time elapsed."""
    d_total, d_idle = b[0] - a[0], b[1] - a[1]
    if d_total <= 0:
        return None
    return round(max(0.0, min(100.0, 100.0 * (d_total - d_idle) / d_total)), 1)


def _nic_bytes() -> int:
    """Bytes sent+received on real interfaces. Loopback is excluded: the agent talks to a local API over it."""
    return sum(c.bytes_sent + c.bytes_recv for name, c in psutil.net_io_counters(pernic=True).items()
               if name != "lo" and not name.startswith("lo"))


class ResourceSampler:
    """Snapshot-delta sampler (no background thread, negligible overhead).

    Each instance keeps its own snapshots, so concurrent transfers do not corrupt each other (psutil.cpu_percent()
    shares one global timer, which made V1's CPU figure wrong under concurrency). CPU % is system-wide busy time
    during the transfer; network MB/s is the NIC byte delta over the duration (all traffic on the host).
    """

    def start(self) -> None:
        self._cpu0 = self._net0 = None
        if psutil:
            self._cpu0, self._net0 = _cpu_snapshot(), _nic_bytes()

    def stop(self, duration_s: float) -> dict:
        if not psutil or self._cpu0 is None:
            return {"cpu_percent": None, "memory_percent": None, "network_mbps": None}
        net = None
        if duration_s > 0:
            net = round(max(0, _nic_bytes() - self._net0) / MIB / duration_s, 3)
        return {"cpu_percent": cpu_percent_between(self._cpu0, _cpu_snapshot()),
                "memory_percent": round(psutil.virtual_memory().percent, 1), "network_mbps": net}


def get_instance_id(timeout: float = 0.3) -> str:
    """EC2 instance id via IMDSv2; falls back to the hostname when not running on EC2."""
    try:
        import requests

        base = "http://169.254.169.254/latest"
        token = requests.put(f"{base}/api/token", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
                             timeout=timeout).text
        r = requests.get(f"{base}/meta-data/instance-id", headers={"X-aws-ec2-metadata-token": token}, timeout=timeout)
        if r.ok and r.text.startswith("i-"):
            return r.text
    except Exception:
        pass
    return socket.gethostname()
