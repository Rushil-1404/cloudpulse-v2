"""Where the agent sends records: the CloudPulse API (with an on-disk spool) and Amazon CloudWatch."""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
from datetime import datetime, timezone

from ..models import TransferRecord

log = logging.getLogger("cloudpulse.sinks")

# HTTP statuses that mean "try again later" (outage, throttling, or a credential the operator can still fix).
RETRY_LATER = {401, 403, 408, 425, 429}


class ApiSink:
    """POSTs records to the CloudPulse API. Anything that cannot be delivered is spooled to a JSONL file and replayed
    (oldest first) on the next emit, so no event is lost during an outage.

    Failure handling (V2):
      * network error / 5xx / 429 / 401 / 403 -> keep in the spool and retry later.
      * other 4xx (the server judged the data invalid) -> the batch is re-sent record by record to isolate the bad
        one(s); bad records go to `<spool>.rejected` instead of blocking the queue forever (V1 "poison pill" bug).
    """

    def __init__(self, api_url: str, api_key: str, spool_path: str = "cloudpulse-spool.jsonl", timeout: float = 5.0,
                 max_spool: int = 10_000, batch_size: int = 500, session=None):
        if session is None:
            import requests
            session = requests.Session()
        self.url = api_url.rstrip("/") + "/api/v1/transfers/batch"
        self.spool_path, self.dead_letter_path = spool_path, spool_path + ".rejected"
        self.headers, self.timeout = {"X-API-Key": api_key}, timeout
        self.max_spool, self.batch_size, self.session = max_spool, batch_size, session
        self._lock = threading.Lock()

    # ---- public --------------------------------------------------------------
    def emit(self, record: TransferRecord) -> None:
        with self._lock:
            pending = self._read_spool() + [record.to_dict()]
            leftover = self._deliver(pending)
            if len(leftover) > self.max_spool:
                log.error("spool full: dropping %d oldest record(s)", len(leftover) - self.max_spool)
            self._write_spool(leftover[-self.max_spool:])

    # ---- delivery ------------------------------------------------------------
    def _post(self, items: list[dict]) -> str:
        """-> 'ok' | 'retry' | 'reject'."""
        try:
            resp = self.session.post(self.url, json={"transfers": items}, headers=self.headers, timeout=self.timeout)
        except Exception as exc:  # noqa: BLE001 - connection refused, DNS, timeout...
            log.warning("API unreachable (%s)", exc)
            return "retry"
        code = resp.status_code
        if 200 <= code < 300:
            return "ok"
        if code >= 500 or code in RETRY_LATER:
            level = log.error if code in (401, 403) else log.warning
            level("API returned HTTP %s; will retry later%s", code, " - check CLOUDPULSE_AGENT_API_KEY" if code in (401, 403) else "")
            return "retry"
        return "reject"

    def _deliver(self, pending: list[dict]) -> list[dict]:
        """Send everything possible; return the records that must stay in the spool."""
        i = 0
        while i < len(pending):
            chunk = pending[i:i + self.batch_size]
            outcome = self._post(chunk)
            if outcome == "ok":
                i += len(chunk)
            elif outcome == "retry":
                return pending[i:]
            else:  # reject: isolate the offender(s)
                for j, item in enumerate(chunk):
                    one = self._post([item])
                    if one == "retry":
                        return chunk[j:] + pending[i + len(chunk):]
                    if one == "reject":
                        self._dead_letter(item)
                i += len(chunk)
        return []

    def _dead_letter(self, item: dict) -> None:
        log.error("API rejected record %s permanently; saved to %s", item.get("transfer_id"), self.dead_letter_path)
        with open(self.dead_letter_path, "a") as fh:
            fh.write(json.dumps(item) + "\n")

    # ---- spool file ------------------------------------------------------------
    def _read_spool(self) -> list[dict]:
        if not os.path.exists(self.spool_path):
            return []
        items = []
        with open(self.spool_path) as fh:
            for line in fh:
                try:
                    if line.strip():
                        items.append(json.loads(line))
                except json.JSONDecodeError:  # a torn write after a crash must not wedge the agent
                    log.error("skipping corrupt spool line")
        return items

    def _write_spool(self, items: list[dict]) -> None:
        if not items:
            if os.path.exists(self.spool_path):
                os.remove(self.spool_path)
            return
        tmp = self.spool_path + ".tmp"
        with open(tmp, "w") as fh:
            fh.writelines(json.dumps(i) + "\n" for i in items)
        os.replace(tmp, self.spool_path)  # atomic


def build_metric_data(rec: TransferRecord) -> list[dict]:
    """CloudWatch datapoints for one transfer (pure function; unit tested)."""
    dims = [{"Name": "Direction", "Value": rec.direction}, {"Name": "InstanceId", "Value": rec.instance_id}]
    ts = datetime.fromtimestamp(rec.completed_at, tz=timezone.utc)

    def m(name: str, value: float, unit: str) -> dict:
        return {"MetricName": name, "Dimensions": dims, "Timestamp": ts, "Value": float(value), "Unit": unit}

    data = [m("TransferSuccess", 1 if rec.ok else 0, "Count"), m("TransferFailure", 0 if rec.ok else 1, "Count"),
            m("Retries", rec.retries, "Count"), m("FileSizeBytes", rec.file_size_bytes, "Bytes")]
    if rec.ok:
        data.append(m("DurationSeconds", rec.duration_s, "Seconds"))
        if rec.throughput_mbps is not None:
            data.append(m("ThroughputMBps", rec.throughput_mbps, "Megabytes/Second"))
        if rec.latency_ms is not None:
            data.append(m("LatencyMs", rec.latency_ms, "Milliseconds"))
    for name, value, unit in (("CpuUtilization", rec.cpu_percent, "Percent"),
                              ("MemoryUtilization", rec.memory_percent, "Percent"),
                              ("NetworkMBps", rec.network_mbps, "Megabytes/Second")):
        if value is not None:
            data.append(m(name, value, unit))
    return data


class CloudWatchSink:
    def __init__(self, namespace: str = "CloudPulse", region: str | None = None, client=None):
        if client is None:
            import boto3
            client = boto3.client("cloudwatch", region_name=region)
        self.client, self.namespace = client, namespace

    def emit(self, record: TransferRecord) -> None:
        self.client.put_metric_data(Namespace=self.namespace, MetricData=build_metric_data(record))


_STOP = object()


class BackgroundSink:
    """Runs a slow sink on a worker thread so reporting never adds latency to the transfer path.

    V1 called the API synchronously (5 s timeout) after every transfer, so an API outage slowed the very transfers
    being measured. Call close() before the process exits to flush the queue.
    """

    def __init__(self, inner, maxsize: int = 1000):
        self.inner, self.dropped, self._q = inner, 0, queue.Queue(maxsize)
        self._t = threading.Thread(target=self._run, daemon=True, name="cp-sink")
        self._t.start()

    def emit(self, record: TransferRecord) -> None:
        try:
            self._q.put_nowait(record)
        except queue.Full:
            self.dropped += 1
            log.error("sink queue full; dropped record %s", record.transfer_id)

    def _run(self) -> None:
        while True:
            item = self._q.get()
            try:
                if item is _STOP:
                    return
                self.inner.emit(item)
            except Exception:  # noqa: BLE001
                log.exception("sink %s failed", type(self.inner).__name__)
            finally:
                self._q.task_done()

    def close(self, timeout: float = 30.0) -> None:
        self._q.put(_STOP)
        self._t.join(timeout)
        if self._t.is_alive():
            log.error("sink %s did not flush within %.0fs", type(self.inner).__name__, timeout)
