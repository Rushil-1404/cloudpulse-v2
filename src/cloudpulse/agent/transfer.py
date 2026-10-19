"""Monitoring agent: wraps boto3 S3 transfers, measures them, retries transient failures, emits records."""
from __future__ import annotations

import logging
import os
import random
import time
import uuid
from typing import Callable, Iterable, Protocol

from ..models import TransferRecord
from .resources import ResourceSampler

log = logging.getLogger("cloudpulse.agent")

# Retrying these cannot succeed: fail fast instead of burning the retry budget.
NON_RETRYABLE = {
    "AccessDenied", "403", "404", "NoSuchBucket", "NoSuchKey", "InvalidAccessKeyId", "SignatureDoesNotMatch",
    "InvalidBucketName", "FileNotFoundError", "IsADirectoryError", "PermissionError",
}


class Sink(Protocol):
    def emit(self, record: TransferRecord) -> None: ...


def error_code(exc: BaseException) -> str:
    resp = getattr(exc, "response", None)  # botocore.exceptions.ClientError
    if isinstance(resp, dict):
        code = resp.get("Error", {}).get("Code")
        if code:
            return str(code)
    return type(exc).__name__


class TransferMonitor:
    """Usage::

        monitor = TransferMonitor(boto3.client("s3"), sinks=[api_sink, cw_sink], instance_id="i-0abc")
        record = monitor.upload("/data/report.csv", "my-bucket", "reports/report.csv")
    """

    def __init__(self, s3_client, sinks: Iterable[Sink] = (), *, instance_id: str = "local", max_retries: int = 3,
                 backoff_base_s: float = 0.5, backoff_max_s: float = 20.0, transfer_config=None,
                 sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic, jitter: Callable[[], float] = random.random,
                 sampler_factory=ResourceSampler, retry_counter=None):
        self.s3, self.sinks, self.instance_id = s3_client, list(sinks), instance_id
        self.max_retries, self.backoff_base_s, self.backoff_max_s = max_retries, backoff_base_s, backoff_max_s
        self.transfer_config = transfer_config
        self._sleep, self._clock, self._mono, self._jitter = sleep, clock, monotonic, jitter
        self._sampler_factory = sampler_factory
        self._retry_counter = retry_counter  # optional agent.aws.RetryCounter: makes botocore's internal retries visible

    def close(self) -> None:
        """Flush and stop background sinks. Call before the process exits."""
        for sink in self.sinks:
            if hasattr(sink, "close"):
                sink.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- public API ------------------------------------------------------
    def upload(self, path: str, bucket: str, key: str) -> TransferRecord:
        def op(ctx: dict, cb):
            ctx["size"] = os.path.getsize(path)  # FileNotFoundError is recorded as a (non-retryable) failure
            self.s3.upload_file(path, bucket, key, Callback=cb, **self._cfg())
        return self._run("upload", bucket, key, op)

    def download(self, bucket: str, key: str, path: str) -> TransferRecord:
        def op(ctx: dict, cb):
            ctx["size"] = int(self.s3.head_object(Bucket=bucket, Key=key)["ContentLength"])
            self.s3.download_file(bucket, key, path, Callback=cb, **self._cfg())
        return self._run("download", bucket, key, op)

    # ---- internals -------------------------------------------------------
    def _cfg(self) -> dict:
        return {"Config": self.transfer_config} if self.transfer_config is not None else {}

    def _backoff(self, attempt: int) -> float:
        """Capped exponential back-off with equal jitter."""
        cap = min(self.backoff_max_s, self.backoff_base_s * (2 ** attempt))
        return cap / 2 + self._jitter() * cap / 2

    def _run(self, direction: str, bucket: str, key: str, op) -> TransferRecord:
        started, t0 = self._clock(), self._mono()
        hidden0 = self._retry_counter.value() if self._retry_counter else 0
        sampler = self._sampler_factory()
        sampler.start()
        ctx: dict = {"size": 0}
        retries, latency_ms, status, error = 0, None, "failed", None

        for attempt in range(self.max_retries + 1):
            attempt_t0 = self._mono()
            first_byte: list[float] = []

            def progress(nbytes: int, _t0=attempt_t0, _fb=first_byte):
                if nbytes > 0 and not _fb:
                    _fb.append(self._mono() - _t0)

            try:
                op(ctx, progress)
                status, error = "success", None
                latency_ms = (first_byte[0] if first_byte else self._mono() - attempt_t0) * 1000
                break
            except Exception as exc:  # noqa: BLE001 - every failure must be recorded, never raised
                error = exc
                if error_code(exc) in NON_RETRYABLE or attempt == self.max_retries:
                    break
                retries += 1
                log.warning("%s s3://%s/%s failed (%s); retry %d/%d", direction, bucket, key,
                            error_code(exc), retries, self.max_retries)
                self._sleep(self._backoff(attempt))

        duration = self._mono() - t0
        usage = sampler.stop(duration)
        if self._retry_counter:  # retries performed silently inside botocore
            retries += max(0, self._retry_counter.value() - hidden0)
        record = TransferRecord(
            transfer_id=uuid.uuid4().hex, direction=direction, bucket=bucket, key=key,
            file_size_bytes=int(ctx["size"]), started_at=started, completed_at=started + duration,
            status=status, latency_ms=None if status == "failed" else round(latency_ms, 3), retries=retries,
            error_code=error_code(error) if error else None,
            error_message=str(error)[:500] if error else None,
            instance_id=self.instance_id, **usage,
        ).validate()
        for sink in self.sinks:
            try:
                sink.emit(record)
            except Exception:  # noqa: BLE001 - observability must never break the transfer itself
                log.exception("sink %s failed", type(sink).__name__)
        return record
