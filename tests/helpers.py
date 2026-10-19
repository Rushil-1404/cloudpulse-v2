from cloudpulse.models import MIB, TransferRecord


def rec(i=0, *, direction="upload", size=10 * MIB, start=1_000_000.0, duration=1.0, status="success", latency=50.0,
        retries=0, code=None, instance="i-1", **kw):
    return TransferRecord(
        transfer_id=f"t{i}", direction=direction, bucket="b", key=f"k{i}", file_size_bytes=size, started_at=start,
        completed_at=start + duration, status=status, latency_ms=None if status == "failed" else latency,
        retries=retries, error_code=code, instance_id=instance, **kw)
