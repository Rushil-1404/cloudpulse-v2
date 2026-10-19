"""Analytics engine: KPIs, time series, reliability score, failure patterns, anomalies, recommendations.

Pure functions over lists of TransferRecord, so they are trivial to unit test and to swap to a different store.
Anomaly detection is statistical (robust modified z-score), deliberately not ML (ML is out of scope per the PRD).
"""
from __future__ import annotations

import math
import re
import statistics
from collections import Counter, defaultdict

from .models import MIB, TransferRecord, iso

_RANGE_RE = re.compile(r"^(\d+)([mhd])$")
_UNIT_S = {"m": 60, "h": 3600, "d": 86400}
SIZE_BUCKETS = (("small", 0, 1 * MIB), ("medium", 1 * MIB, 100 * MIB), ("large", 100 * MIB, 1024 * MIB), ("xlarge", 1024 * MIB, math.inf))
THROTTLE_CODES = {"SlowDown", "RequestLimitExceeded", "ThrottlingException", "503", "Throttling"}


MAX_RANGE_S = 366 * 86400
DEGRADATION_RECENT_S = 3600
DEGRADATION_BASELINE_S = 86400
DEGRADATION_LOOKBACK_S = DEGRADATION_RECENT_S + DEGRADATION_BASELINE_S


def parse_range(value: str) -> int:
    """'30m' / '24h' / '7d' -> seconds (1 minute to 366 days)."""
    m = _RANGE_RE.match(value.strip().lower())
    if not m or int(m.group(1)) <= 0:
        raise ValueError("range must look like 30m, 24h or 7d")
    seconds = int(m.group(1)) * _UNIT_S[m.group(2)]
    if seconds > MAX_RANGE_S:
        raise ValueError("range must be at most 366d")
    return seconds


def auto_bucket(range_s: int) -> int:
    """Pick a chart bucket size giving roughly 24-100 points."""
    for b in (60, 300, 900, 3600, 6 * 3600):
        if range_s / b <= 100:
            return b
    return 86400


def size_bucket(size_bytes: int) -> str:
    for name, lo, hi in SIZE_BUCKETS:
        if lo <= size_bytes < hi:
            return name
    return "xlarge"


def percentile(values: list[float], pct: float) -> float | None:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * pct / 100
    lo, hi = math.floor(k), math.ceil(k)
    return vals[lo] if lo == hi else vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def _avg(values) -> float | None:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _r(v: float | None, nd: int = 3) -> float | None:
    return None if v is None else round(v, nd)


# --------------------------------------------------------------------------- KPIs
def summarize(records: list[TransferRecord]) -> dict:
    total = len(records)
    ok = [r for r in records if r.ok]
    failed = total - len(ok)
    thr = [r.throughput_mbps for r in ok if r.throughput_mbps is not None]
    lat = [r.latency_ms for r in records if r.latency_ms is not None]
    dur = [r.duration_s for r in ok]
    retries = sum(r.retries for r in records)
    return {
        "total_transfers": total,
        "successful": len(ok),
        "failed": failed,
        "success_rate_pct": _r(len(ok) / total * 100, 2) if total else None,
        "failure_rate_pct": _r(failed / total * 100, 2) if total else None,
        "total_bytes": sum(r.file_size_bytes for r in records if r.ok),
        "avg_throughput_mbps": _r(_avg(thr)),
        "p50_throughput_mbps": _r(percentile(thr, 50)),
        "p5_throughput_mbps": _r(percentile(thr, 5)),
        "avg_latency_ms": _r(_avg(lat), 2),
        "p95_latency_ms": _r(percentile(lat, 95), 2),
        "avg_duration_s": _r(_avg(dur)),
        "p95_duration_s": _r(percentile(dur, 95)),
        "total_retries": retries,
        "retry_rate": _r(retries / total, 4) if total else None,  # retries per transfer
        "transfers_with_retry_pct": _r(sum(1 for r in records if r.retries) / total * 100, 2) if total else None,
        "avg_cpu_percent": _r(_avg(r.cpu_percent for r in records), 1),
        "avg_memory_percent": _r(_avg(r.memory_percent for r in records), 1),
        "avg_network_mbps": _r(_avg(r.network_mbps for r in records), 2),
    }


def summarize_by_direction(records: list[TransferRecord]) -> dict:
    return {d: summarize([r for r in records if r.direction == d]) for d in ("upload", "download")}


def throughput_by_size(records: list[TransferRecord]) -> dict:
    groups: dict[str, list[float]] = defaultdict(list)
    for r in records:
        if r.throughput_mbps is not None:
            groups[size_bucket(r.file_size_bytes)].append(r.throughput_mbps)
    return {name: {"count": len(v), "avg_throughput_mbps": _r(_avg(v))} for name, v in groups.items()}


def timeseries(records: list[TransferRecord], bucket_s: int) -> list[dict]:
    """Per (time bucket, direction) rows, ascending. Empty buckets are omitted."""
    groups: dict[tuple[int, str], list[TransferRecord]] = defaultdict(list)
    for r in records:
        groups[(int(r.started_at // bucket_s) * bucket_s, r.direction)].append(r)
    rows = []
    for (ts, direction), recs in sorted(groups.items()):
        s = summarize(recs)
        rows.append({
            "bucket_start": ts, "bucket_iso": iso(ts), "direction": direction, "count": s["total_transfers"],
            "failures": s["failed"], "success_rate_pct": s["success_rate_pct"],
            "avg_throughput_mbps": s["avg_throughput_mbps"], "avg_latency_ms": s["avg_latency_ms"],
            "p95_latency_ms": s["p95_latency_ms"], "retries": s["total_retries"],
        })
    return rows


# ------------------------------------------------------------------ reliability
def data_rate_mbps(r: TransferRecord) -> float | None:
    """Throughput excluding time-to-first-byte (size / (duration - latency)); None for files < 1 MiB.

    Plain throughput of small objects is dominated by per-request latency, so it says little about stability.
    """
    if not r.ok or r.file_size_bytes < MIB:
        return None
    transfer_s = r.duration_s - (r.latency_ms or 0) / 1000
    return (r.file_size_bytes / MIB) / transfer_s if transfer_s > 0.001 else None


def reliability_score(records: list[TransferRecord]) -> dict:
    """0-100 score = 60% success rate + 20% retry health + 20% throughput stability.

    * success   : 100 - 10 x failure-rate points, floored at 0 (99% -> 90, 95% -> 50, <= 90% -> 0).
    * retry     : 100 - 50 * retries-per-transfer, floored at 0 (2 retries/transfer => 0).
    * stability : spread of the latency-adjusted data rate (see data_rate_mbps) within each (direction, size bucket),
                  as a count-weighted coefficient of variation: CV <= 0.15 scores 100, CV >= 0.75 scores 0, linear
                  in between. No eligible data (all files < 1 MiB) scores 100.
    """
    if not records:
        return {"score": None, "grade": "N/A", "components": {}}
    s = summarize(records)
    # Each percentage point of failures costs 10 points: 99% -> 90, 95% -> 50, <= 90% -> 0. A linear mapping would let a
    # system that fails 10% of its transfers still score a "B".
    success = max(0.0, 100 - 10 * (100 - s["success_rate_pct"]))
    retry = max(0.0, 100 - 50 * s["retry_rate"])
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in records:
        rate = data_rate_mbps(r)
        if rate is not None:
            groups[(r.direction, size_bucket(r.file_size_bytes))].append(rate)
    weighted, n = 0.0, 0
    for vals in groups.values():
        if len(vals) >= 3 and statistics.mean(vals) > 0:
            weighted += (statistics.pstdev(vals) / statistics.mean(vals)) * len(vals)
            n += len(vals)
    cv = weighted / n if n else 0.0
    stability = max(0.0, min(100.0, 100 * (0.75 - cv) / 0.60))
    score = 0.6 * success + 0.2 * retry + 0.2 * stability
    grade = next(g for g, t in (("A", 95), ("B", 90), ("C", 80), ("D", 70), ("F", 0)) if score >= t)
    return {
        "score": round(score, 1), "grade": grade,
        "components": {"success": round(success, 1), "retry": round(retry, 1), "stability": round(stability, 1)},
    }


def failure_patterns(records: list[TransferRecord], min_occurrences: int = 3) -> dict:
    failed = [r for r in records if not r.ok]
    code = lambda r: r.error_code or "Unknown"  # noqa: E731
    hour = lambda r: int((r.started_at % 86400) // 3600)  # UTC hour of day  # noqa: E731
    by_code = Counter(code(r) for r in failed)
    recurring = []

    def add(desc: str, count: int, **extra):
        if count >= min_occurrences:
            recurring.append({"pattern": desc, "count": count,
                              "share_of_failures_pct": round(count / len(failed) * 100, 1), **extra})

    for c, n in by_code.items():
        add(f"{c} errors", n, kind="error_code")
    for (c, d, b), n in Counter((code(r), r.direction, size_bucket(r.file_size_bytes)) for r in failed).items():
        add(f"{c} on {b} {d}s", n, kind="error_code+direction+size")
    for (c, h), n in Counter((code(r), hour(r)) for r in failed).items():
        add(f"{c} around {h:02d}:00 UTC", n, kind="error_code+hour")
    recurring.sort(key=lambda p: -p["count"])
    return {
        "total_failures": len(failed),
        "by_error_code": dict(by_code.most_common()),
        "by_direction": dict(Counter(r.direction for r in failed)),
        "by_size_bucket": dict(Counter(size_bucket(r.file_size_bytes) for r in failed)),
        "by_hour_utc": {f"{h:02d}": n for h, n in sorted(Counter(hour(r) for r in failed).items())},
        "recurring": recurring[:15],
    }


# ---------------------------------------------------------------------- anomalies
def _robust_baseline(vals: list[float]) -> tuple[float, float | None]:
    """(median, scale) computed ONCE per group so anomaly scoring stays O(n log n).

    scale is MAD / 0.6745 (so z ~ standard deviations); falls back to the mean absolute deviation when MAD is 0.
    """
    med = statistics.median(vals)
    mad = statistics.median(abs(v - med) for v in vals)
    if mad > 0:
        return med, mad / 0.6745
    mean_ad = statistics.mean(abs(v - med) for v in vals)
    return med, (1.2533 * mean_ad) if mean_ad > 0 else None


def detect_anomalies(records: list[TransferRecord], threshold: float = 3.5, min_samples: int = 10) -> list[dict]:
    """Flag unusually low throughput / high latency vs the (direction, size bucket) baseline."""
    groups: dict[tuple[str, str], list[TransferRecord]] = defaultdict(list)
    for r in records:
        if r.ok:
            groups[(r.direction, size_bucket(r.file_size_bytes))].append(r)
    found = []
    for (direction, bucket), recs in groups.items():
        if len(recs) < min_samples:
            continue
        for metric, getter, sign in (("throughput_mbps", lambda r: r.throughput_mbps, -1),
                                     ("latency_ms", lambda r: r.latency_ms, 1)):
            pairs = [(r, getter(r)) for r in recs if getter(r) is not None]
            if len(pairs) < min_samples:
                continue
            vals = [v for _, v in pairs]
            med, scale = _robust_baseline(vals)
            if scale is None:
                continue
            for r, v in pairs:
                z = (v - med) / scale
                if sign * z > threshold:
                    found.append({"transfer_id": r.transfer_id, "metric": metric, "direction": direction,
                                  "size_bucket": bucket, "value": round(v, 3), "baseline_median": round(med, 3),
                                  "z_score": round(z, 2), "started_at": r.started_at, "started_iso": iso(r.started_at)})
    return sorted(found, key=lambda a: -a["started_at"])


def detect_degradation(records: list[TransferRecord], now: float, recent_s: int = DEGRADATION_RECENT_S,
                       baseline_s: int = DEGRADATION_BASELINE_S,
                       throughput_drop: float = 0.25, latency_rise: float = 0.5,
                       min_recent: int = 5, min_baseline: int = 20) -> list[dict]:
    """Compare the recent window with the preceding baseline window per (direction, size bucket).

    `records` must cover DEGRADATION_LOOKBACK_S (25 h) before `now`, regardless of the range a caller is displaying;
    otherwise there is no baseline to compare against.
    """
    recent: dict[tuple, list[TransferRecord]] = defaultdict(list)
    base: dict[tuple, list[TransferRecord]] = defaultdict(list)
    for r in records:
        if not r.ok:
            continue
        key = (r.direction, size_bucket(r.file_size_bytes))
        if r.started_at >= now - recent_s:
            recent[key].append(r)
        elif r.started_at >= now - recent_s - baseline_s:
            base[key].append(r)
    out = []
    for key, rec in recent.items():
        bas = base.get(key, [])
        if len(rec) < min_recent or len(bas) < min_baseline:
            continue
        for metric, getter, worse in (("throughput_mbps", lambda r: r.throughput_mbps, "down"),
                                      ("latency_ms", lambda r: r.latency_ms, "up")):
            rv = [getter(r) for r in rec if getter(r) is not None]
            bv = [getter(r) for r in bas if getter(r) is not None]
            if not rv or not bv:
                continue
            rm, bm = statistics.median(rv), statistics.median(bv)
            if bm <= 0:
                continue
            change = (rm - bm) / bm
            if (worse == "down" and change <= -throughput_drop) or (worse == "up" and change >= latency_rise):
                out.append({"direction": key[0], "size_bucket": key[1], "metric": metric,
                            "recent_median": round(rm, 3), "baseline_median": round(bm, 3),
                            "change_pct": round(change * 100, 1), "recent_samples": len(rv)})
    return sorted(out, key=lambda d: -abs(d["change_pct"]))


# ----------------------------------------------------------------- recommendations
def recommendations(records: list[TransferRecord], summary: dict | None = None, patterns: dict | None = None) -> list[dict]:
    """Rule-based, explainable optimisation advice. Every rule cites the evidence that triggered it."""
    if not records:
        return []
    s = summary or summarize(records)
    p = patterns or failure_patterns(records)
    recs: list[dict] = []

    def add(severity: str, title: str, detail: str):
        recs.append({"severity": severity, "title": title, "detail": detail})

    codes = p["by_error_code"]
    throttled = sum(n for c, n in codes.items() if c in THROTTLE_CODES)
    denied = sum(n for c, n in codes.items() if c in {"AccessDenied", "403", "InvalidAccessKeyId"})
    if s["success_rate_pct"] is not None and s["success_rate_pct"] < 99:
        top = next(iter(codes), "Unknown")
        add("high", "Success rate is below the 99% target",
            f"{s['failed']} of {s['total_transfers']} transfers failed ({s['failure_rate_pct']}%); most common error: {top}.")
    if throttled:
        add("high", "S3 request throttling detected",
            f"{throttled} failures were throttling errors. Spread keys across more prefixes (S3 scales per prefix), "
            "use exponential back-off with jitter, and avoid bursts of parallel small requests.")
    if denied:
        add("high", "Permission errors on transfers",
            f"{denied} failures were access errors. Review the instance role's s3:GetObject / s3:PutObject / "
            "s3:ListBucket permissions and bucket policy; these errors are not retryable.")
    if (s["retry_rate"] or 0) > 0.1:
        add("medium", "High retry rate",
            f"{s['retry_rate']} retries per transfer ({s['transfers_with_retry_pct']}% of transfers retried). "
            "Check network path stability, use a same-Region bucket and an S3 gateway VPC endpoint.")
    if (s["avg_latency_ms"] or 0) > 500:
        add("medium", "High time-to-first-byte latency",
            f"Average latency is {s['avg_latency_ms']} ms. Keep the instance and bucket in the same Region, add an S3 "
            "gateway endpoint, and reuse connections (a single long-lived boto3 client).")
    by_size = throughput_by_size(records)
    large = by_size.get("large", {}).get("avg_throughput_mbps")
    xlarge = by_size.get("xlarge", {}).get("avg_throughput_mbps")
    big = [v for v in (large, xlarge) if v is not None]
    if big and min(big) < 30:
        add("medium", "Large-file throughput is low",
            f"Large objects average {round(min(big), 1)} MB/s. Tune multipart transfers (TransferConfig: larger "
            "multipart_chunksize, higher max_concurrency) and check the instance's network baseline bandwidth "
            "(consider a network-optimised instance type).")
    small_share = sum(1 for r in records if r.file_size_bytes < MIB) / len(records)
    if small_share > 0.5 and (by_size.get("small", {}).get("avg_throughput_mbps") or 99) < 5:
        add("low", "Many small objects dominate",
            f"{round(small_share * 100)}% of transfers are under 1 MiB, where per-request latency dominates. "
            "Batch small files (archives) or transfer them concurrently.")
    if (s["avg_cpu_percent"] or 0) > 80:
        add("medium", "CPU saturation during transfers",
            f"Average CPU is {s['avg_cpu_percent']}%. Lower transfer concurrency or move to a larger instance.")
    if (s["avg_memory_percent"] or 0) > 85:
        add("medium", "Memory pressure during transfers",
            f"Average memory is {s['avg_memory_percent']}%. Reduce multipart concurrency/chunk size or add memory.")
    if not recs:
        add("info", "No action needed", "Reliability and performance are within the expected targets.")
    order = {"high": 0, "medium": 1, "low": 2, "info": 3}
    return sorted(recs, key=lambda r: order[r["severity"]])
