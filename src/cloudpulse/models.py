"""Domain models shared by the agent, storage, analytics and API layers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

MIB = 1024 * 1024  # "MB" in this project always means MiB (1,048,576 bytes)
DIRECTIONS = ("upload", "download")
STATUSES = ("success", "failed")


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


@dataclass
class TransferRecord:
    """One EC2<->S3 transfer attempt (including its retries)."""

    transfer_id: str
    direction: str  # "upload" (EC2 -> S3) | "download" (S3 -> EC2)
    bucket: str
    key: str
    file_size_bytes: int
    started_at: float  # epoch seconds (UTC)
    completed_at: float
    status: str  # "success" | "failed"
    latency_ms: float | None = None  # time to first byte of the final attempt
    retries: int = 0
    error_code: str | None = None
    error_message: str | None = None
    instance_id: str = "local"
    cpu_percent: float | None = None
    memory_percent: float | None = None
    network_mbps: float | None = None

    def validate(self) -> "TransferRecord":
        if self.direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}")
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}")
        if self.file_size_bytes < 0 or self.retries < 0:
            raise ValueError("file_size_bytes and retries must be >= 0")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must be >= started_at")
        return self

    @property
    def ok(self) -> bool:
        return self.status == "success"

    @property
    def duration_s(self) -> float:
        return max(0.0, self.completed_at - self.started_at)

    @property
    def throughput_mbps(self) -> float | None:
        """Effective MiB/s for successful, non-empty transfers (includes retry back-off)."""
        if not self.ok or self.file_size_bytes <= 0 or self.duration_s <= 0:
            return None
        return (self.file_size_bytes / MIB) / self.duration_s

    def to_dict(self, derived: bool = False) -> dict:
        d = asdict(self)
        if derived:
            d.update(
                duration_s=round(self.duration_s, 4),
                throughput_mbps=None if self.throughput_mbps is None else round(self.throughput_mbps, 4),
                started_iso=iso(self.started_at),
                completed_iso=iso(self.completed_at),
            )
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "TransferRecord":
        names = cls.__dataclass_fields__.keys()
        return cls(**{k: v for k, v in d.items() if k in names})


@dataclass
class Alert:
    rule: str  # transfer_failed | low_throughput | high_latency | excessive_retries
    severity: str  # critical | warning
    message: str
    created_at: float
    transfer_id: str | None = None
    dedupe_key: str = ""
    acknowledged: bool = False
    id: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["created_iso"] = iso(self.created_at)
        return d
