"""Application service: every behaviour of the API, independent of the web framework.

api.py is a thin router over this class, so ingestion rules, validation, caching, windows and report logic are all
unit-tested without needing FastAPI.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import Callable

from . import __version__
from . import analytics as an
from .alerts import AlertManager
from .config import Settings
from .models import TransferRecord
from .reports import build_report, render_markdown
from .storage import SQLiteStore

log = logging.getLogger("cloudpulse.service")
EARLIEST_PLAUSIBLE = 946_684_800.0  # 2000-01-01
MAX_OBJECT_BYTES = 5 * 1024 ** 4  # S3's maximum object size (5 TiB)
MAX_BUCKET_POINTS = 2000


class ServiceError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


class TTLCache:
    """Thread-safe TTL cache with a size bound (a viewer cycling through arbitrary query params must not grow memory)."""

    def __init__(self, ttl_s: float, maxsize: int = 256):
        self.ttl, self.maxsize, self._d, self._lock = ttl_s, maxsize, {}, threading.Lock()

    def get_or_compute(self, key, fn):
        now = time.monotonic()
        with self._lock:
            hit = self._d.get(key)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        value = fn()
        with self._lock:
            self._d[key] = (now, value)
            while len(self._d) > self.maxsize:
                self._d.pop(min(self._d, key=lambda k: self._d[k][0]))
        return value

    def clear(self) -> None:
        with self._lock:
            self._d.clear()

    def __len__(self) -> int:
        return len(self._d)


class CloudPulseService:
    def __init__(self, store: SQLiteStore, settings: Settings, manager: AlertManager,
                 clock: Callable[[], float] = time.time):
        self.store, self.settings, self.manager, self.clock = store, settings, manager, clock
        self.cache = TTLCache(settings.cache_ttl_s)
        self._last_purge = 0.0

    # ---- housekeeping ------------------------------------------------------
    def maybe_purge(self) -> dict | None:
        """Enforce retention at most once an hour (called on start-up and from ingest)."""
        days, now = self.settings.retention_days, self.clock()
        if days <= 0 or now - self._last_purge < 3600:
            return None
        self._last_purge = now
        res = self.store.purge_older_than(now - days * 86400)
        if res["transfers"] or res["alerts"]:
            log.info("retention purge removed %s", res)
            self.cache.clear()
        return res

    def close(self) -> None:
        self.manager.close()
        self.store.close()

    def health(self) -> dict:
        try:
            self.store.ping()
        except Exception as exc:  # noqa: BLE001
            log.error("health check failed: %s", exc)
            raise ServiceError(503, "database unavailable")
        return {"status": "ok", "version": __version__}  # nothing sensitive: this endpoint is unauthenticated

    # ---- ingestion -----------------------------------------------------------
    def _sanity(self, d: dict, now: float) -> str | None:
        """Semantic checks pydantic cannot express. Returns a rejection reason or None."""
        s, c = d["started_at"], d["completed_at"]
        if not (math.isfinite(s) and math.isfinite(c)):
            return "timestamps must be finite"
        if s < EARLIEST_PLAUSIBLE:
            return "started_at is implausibly old"
        if c > now + self.settings.max_future_skew_s:
            return "completed_at is in the future"
        if c < s:
            return "completed_at precedes started_at"
        if c - s > self.settings.max_duration_s:
            return "duration exceeds the maximum"
        if not 0 <= d["file_size_bytes"] <= MAX_OBJECT_BYTES:
            return "file_size_bytes out of range"
        return None

    def ingest(self, items: list[dict]) -> dict:
        now, accepted, rejected = self.clock(), [], []
        for d in items:
            reason = self._sanity(d, now)
            if reason:
                rejected.append({"transfer_id": d.get("transfer_id"), "reason": reason})
                continue
            try:
                accepted.append(TransferRecord.from_dict(d).validate())
            except ValueError as exc:
                rejected.append({"transfer_id": d.get("transfer_id"), "reason": str(exc)})
        if items and not accepted:
            raise ServiceError(422, f"all {len(rejected)} transfer(s) rejected: {rejected[:3]}")
        new = self.store.insert_transfers(accepted)
        raised = self.manager.process(new)
        if new:
            self.cache.clear()
        self.maybe_purge()
        return {"received": len(items), "inserted": len(new), "duplicates": len(accepted) - len(new),
                "rejected": rejected, "alerts_raised": len(raised)}

    # ---- queries -------------------------------------------------------------
    def _window(self, range_: str) -> tuple[float, float, int]:
        try:
            span = an.parse_range(range_)
        except ValueError as exc:
            raise ServiceError(422, str(exc))
        now = self.clock()
        return now, now - span, span

    def _load(self, range_: str, direction: str | None):
        now, since, _ = self._window(range_)
        return now, self.store.query_transfers(since=since, until=now, direction=direction)

    def transfers(self, range_: str, direction, status, limit: int, offset: int) -> dict:
        now, since, _ = self._window(range_)
        rows = self.store.query_transfers(since, now, direction, status, limit, offset, descending=True)
        return {"total": self.store.count_transfers(since, now, direction, status), "limit": limit,
                "offset": offset, "items": [r.to_dict(derived=True) for r in rows]}

    def summary(self, range_: str, direction: str | None) -> dict:
        def compute():
            _, recs = self._load(range_, direction)
            return {"range": range_, "summary": an.summarize(recs), "by_direction": an.summarize_by_direction(recs),
                    "throughput_by_size": an.throughput_by_size(recs)}
        return self.cache.get_or_compute(("summary", range_, direction), compute)

    def timeseries(self, range_: str, direction: str | None, bucket: int | None) -> dict:
        _, _, span = self._window(range_)
        b = bucket or an.auto_bucket(span)
        if span / b > MAX_BUCKET_POINTS:
            raise ServiceError(422, f"bucket too small for this range (max {MAX_BUCKET_POINTS} points)")

        def compute():
            _, recs = self._load(range_, direction)
            return {"range": range_, "bucket_seconds": b, "points": an.timeseries(recs, b)}
        return self.cache.get_or_compute(("series", range_, direction, b), compute)

    def reliability(self, range_: str, direction: str | None) -> dict:
        def compute():
            now, recs = self._load(range_, direction)
            patterns = an.failure_patterns(recs)
            anomalies = an.detect_anomalies(recs)
            # Degradation compares the last hour with the 24 h before it, whatever range is on screen.
            lookback = self.store.query_transfers(since=now - an.DEGRADATION_LOOKBACK_S, until=now,
                                                  direction=direction)
            return {"range": range_, "reliability": an.reliability_score(recs), "failure_patterns": patterns,
                    "anomaly_count": len(anomalies), "anomalies": anomalies[:25],
                    "degradation": an.detect_degradation(lookback, now),
                    "recommendations": an.recommendations(recs, patterns=patterns)}
        return self.cache.get_or_compute(("rel", range_, direction), compute)

    def alerts(self, range_: str, open_only: bool, limit: int) -> dict:
        _, since, _ = self._window(range_)
        return {"items": [a.to_dict() for a in self.store.list_alerts(since, open_only, limit)]}

    def ack(self, alert_id: int) -> dict:
        if not self.store.ack_alert(alert_id):
            raise ServiceError(404, "Alert not found")
        return {"acknowledged": alert_id}

    def report(self, kind: str, fmt: str = "json"):
        try:
            rep = build_report(self.store, kind, now=self.clock())
        except ValueError as exc:
            raise ServiceError(422, str(exc))
        return render_markdown(rep) if fmt == "markdown" else rep

