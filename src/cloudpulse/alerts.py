"""Alerting service: threshold rules, de-duplication (cool-down), pluggable notifiers (log / SNS)."""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from .models import MIB, Alert, TransferRecord
from .storage import SQLiteStore

log = logging.getLogger("cloudpulse.alerts")


@dataclass(frozen=True)
class AlertThresholds:
    min_throughput_mbps: float = 5.0
    max_latency_ms: float = 1000.0
    max_retries: int = 3
    # Throughput of tiny objects is dominated by latency, so it is only judged above this size.
    min_size_bytes_for_throughput: int = 1 * MIB


def evaluate_transfer(rec: TransferRecord, th: AlertThresholds) -> list[Alert]:
    """Apply the four PRD alert conditions (FR-5) to one completed transfer."""
    base = f"{rec.direction}:{rec.instance_id}"
    when, tid = rec.completed_at, rec.transfer_id
    out: list[Alert] = []
    if not rec.ok:
        out.append(Alert("transfer_failed", "critical",
                         f"{rec.direction} of s3://{rec.bucket}/{rec.key} failed after {rec.retries} retries "
                         f"({rec.error_code or 'Unknown'}: {rec.error_message or 'no detail'})",
                         when, tid, f"transfer_failed:{base}:{rec.error_code or ''}"))
    tp = rec.throughput_mbps
    if tp is not None and rec.file_size_bytes >= th.min_size_bytes_for_throughput and tp < th.min_throughput_mbps:
        out.append(Alert("low_throughput", "warning",
                         f"{rec.direction} throughput {tp:.2f} MB/s is below the {th.min_throughput_mbps} MB/s threshold "
                         f"(s3://{rec.bucket}/{rec.key})", when, tid, f"low_throughput:{base}"))
    if rec.latency_ms is not None and rec.latency_ms > th.max_latency_ms:
        out.append(Alert("high_latency", "warning",
                         f"{rec.direction} latency {rec.latency_ms:.0f} ms exceeds the {th.max_latency_ms:.0f} ms threshold",
                         when, tid, f"high_latency:{base}"))
    if rec.retries >= th.max_retries:
        out.append(Alert("excessive_retries", "warning",
                         f"{rec.direction} needed {rec.retries} retries (threshold {th.max_retries})",
                         when, tid, f"excessive_retries:{base}"))
    return out


class Notifier(Protocol):
    def notify(self, alert: Alert) -> None: ...


class LogNotifier:
    def notify(self, alert: Alert) -> None:
        log.warning("ALERT [%s/%s] %s", alert.severity, alert.rule, alert.message)


class NullNotifier:
    def notify(self, alert: Alert) -> None:
        pass


class SNSNotifier:
    """Publishes alerts to an Amazon SNS topic (email/SMS/Slack-webhook subscribers)."""

    def __init__(self, topic_arn: str, region: str | None = None, client=None):
        if client is None:
            import boto3  # lazy: keeps the core importable without boto3
            client = boto3.client("sns", region_name=region)
        self._client, self._topic = client, topic_arn

    def notify(self, alert: Alert) -> None:
        self._client.publish(
            TopicArn=self._topic,
            Subject=f"[CloudPulse {alert.severity.upper()}] {alert.rule}"[:100],
            Message=alert.message,
            MessageAttributes={"rule": {"DataType": "String", "StringValue": alert.rule}},
        )


class AsyncNotifier:
    """Delivers notifications on background threads so a slow/unavailable SNS never delays ingestion."""

    def __init__(self, inner: Notifier, workers: int = 2, queue_size: int = 1000):
        self._inner, self._q, self.dropped = inner, queue.Queue(queue_size), 0
        self._threads = [threading.Thread(target=self._work, daemon=True, name="cp-notify") for _ in range(workers)]
        for t in self._threads:
            t.start()

    def notify(self, alert: Alert) -> None:
        try:
            self._q.put_nowait(alert)
        except queue.Full:
            self.dropped += 1
            log.error("notification queue full; dropped alert %s", alert.rule)

    def _work(self) -> None:
        while True:
            alert = self._q.get()
            try:
                if alert is None:
                    return
                self._inner.notify(alert)
            except Exception:  # noqa: BLE001
                log.exception("notifier %s failed", type(self._inner).__name__)
            finally:
                self._q.task_done()

    def close(self, timeout: float = 10.0) -> None:
        for _ in self._threads:
            self._q.put(None)
        for t in self._threads:
            t.join(timeout)


class AlertManager:
    """Evaluates records, suppresses duplicates within a cool-down, persists and notifies."""

    COOLDOWN_OVERRIDES = {"transfer_failed": 60}  # failures are time-critical: keep the window short

    def __init__(self, store: SQLiteStore, thresholds: AlertThresholds | None = None,
                 notifiers: list[Notifier] | None = None, cooldown_s: int = 300,
                 max_notify_age_s: float | None = 3600, clock: Callable[[], float] = time.time):
        self.store, self.th, self.cooldown_s = store, thresholds or AlertThresholds(), cooldown_s
        self.notifiers = notifiers if notifiers is not None else [LogNotifier()]
        self.max_notify_age_s, self.clock = max_notify_age_s, clock

    def process(self, records: list[TransferRecord]) -> list[Alert]:
        """Store (and normally notify) alerts for new records.

        * Records older than `max_notify_age_s` (e.g. replayed from an agent's disk spool after an outage) are stored
          for the audit trail, tagged "[late arrival]", but NOT notified - nobody wants a page about last night.
        * The cool-down is symmetric, so out-of-order arrival cannot defeat de-duplication.
        """
        raised: list[Alert] = []
        for rec in sorted(records, key=lambda r: r.completed_at):
            late = self.max_notify_age_s is not None and self.clock() - rec.completed_at > self.max_notify_age_s
            for alert in evaluate_transfer(rec, self.th):
                window = self.COOLDOWN_OVERRIDES.get(alert.rule, self.cooldown_s)
                last = self.store.last_alert_at(alert.dedupe_key)
                if last is not None and abs(alert.created_at - last) < window:
                    continue
                if late:
                    alert.message = "[late arrival] " + alert.message
                self.store.add_alert(alert)
                raised.append(alert)
                if late:
                    continue
                for n in self.notifiers:
                    try:
                        n.notify(alert)
                    except Exception:  # a broken notifier must never break ingestion
                        log.exception("notifier %s failed", type(n).__name__)
        return raised

    def close(self) -> None:
        for n in self.notifiers:
            if hasattr(n, "close"):
                n.close()
