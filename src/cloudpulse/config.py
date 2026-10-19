"""Runtime configuration, loaded from environment variables (12-factor style)."""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass

VALID_ROLES = {"admin", "viewer", "agent"}


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def parse_api_keys(raw: str) -> dict[str, str]:
    """Parse ``"key1:admin,key2:viewer"`` into ``{key: role}`` and validate roles."""
    keys: dict[str, str] = {}
    for part in filter(None, (p.strip() for p in raw.split(","))):
        key, sep, role = part.rpartition(":")
        if key.startswith("sha256$") and len(key) != 7 + 64:
            raise ValueError("hashed API key must be sha256$<64 hex chars> (see `cloudpulse genkey`)")
        if not sep or not key or role not in VALID_ROLES:
            raise ValueError(
                f"Invalid CLOUDPULSE_API_KEYS entry (expected <key>:<{'|'.join(sorted(VALID_ROLES))}>)"
            )
        keys[key] = role
    return keys


@dataclass(frozen=True)
class Settings:
    db_path: str = "cloudpulse.db"
    api_keys: dict | None = None  # {api_key: role}
    auth_disabled: bool = False
    api_url: str = "http://localhost:8000"
    agent_api_key: str = ""
    aws_region: str | None = None
    cw_namespace: str = "CloudPulse"
    sns_topic_arn: str | None = None
    min_throughput_mbps: float = 5.0
    max_latency_ms: float = 1000.0
    max_retries: int = 3
    alert_cooldown_s: int = 300
    instance_id: str = ""
    spool_path: str = "cloudpulse-spool.jsonl"
    cache_ttl_s: float = 10.0
    retention_days: int = 90  # 0 = keep forever
    max_future_skew_s: int = 600  # reject transfers that "finished" more than this far in the future
    max_duration_s: int = 7 * 86400

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.getenv
        return cls(
            db_path=env("CLOUDPULSE_DB_PATH", "cloudpulse.db"),
            api_keys=parse_api_keys(env("CLOUDPULSE_API_KEYS", "")),
            auth_disabled=_bool("CLOUDPULSE_AUTH_DISABLED"),
            api_url=env("CLOUDPULSE_API_URL", "http://localhost:8000").rstrip("/"),
            agent_api_key=env("CLOUDPULSE_AGENT_API_KEY", ""),
            aws_region=env("AWS_REGION") or env("AWS_DEFAULT_REGION"),
            cw_namespace=env("CLOUDPULSE_CW_NAMESPACE", "CloudPulse"),
            sns_topic_arn=env("CLOUDPULSE_SNS_TOPIC_ARN") or None,
            min_throughput_mbps=float(env("CLOUDPULSE_MIN_THROUGHPUT_MBPS", "5")),
            max_latency_ms=float(env("CLOUDPULSE_MAX_LATENCY_MS", "1000")),
            max_retries=int(env("CLOUDPULSE_MAX_RETRIES", "3")),
            alert_cooldown_s=int(env("CLOUDPULSE_ALERT_COOLDOWN_S", "300")),
            instance_id=env("CLOUDPULSE_INSTANCE_ID", ""),
            spool_path=env("CLOUDPULSE_SPOOL_PATH", "cloudpulse-spool.jsonl"),
            cache_ttl_s=float(env("CLOUDPULSE_CACHE_TTL_S", "10")),
            retention_days=int(env("CLOUDPULSE_RETENTION_DAYS", "90")),
        )

    def resolved_instance_id(self) -> str:
        return self.instance_id or socket.gethostname()
