"""Daily / weekly report generation (JSON + Markdown)."""
from __future__ import annotations

import json
import time
from collections import Counter
from pathlib import Path

from . import analytics as an
from .models import MIB, iso
from .storage import SQLiteStore

PERIODS = {"daily": 86400, "weekly": 7 * 86400}


def _delta(cur, prev):
    return None if cur is None or prev is None else round(cur - prev, 3)


def build_report(store: SQLiteStore, kind: str, now: float | None = None) -> dict:
    if kind not in PERIODS:
        raise ValueError(f"kind must be one of {sorted(PERIODS)}")
    now = time.time() if now is None else now
    span = PERIODS[kind]
    start, prev_start = now - span, now - 2 * span
    recs = store.query_transfers(since=start, until=now)
    prev = store.query_transfers(since=prev_start, until=start - 1e-6)
    summary, prev_summary = an.summarize(recs), an.summarize(prev)
    patterns = an.failure_patterns(recs)
    alerts = store.list_alerts(since=start, limit=10_000)
    anomalies = an.detect_anomalies(recs)
    return {
        "kind": kind, "generated_at": iso(now), "period_start": iso(start), "period_end": iso(now),
        "summary": summary,
        "trend_vs_previous_period": {
            "success_rate_pct": _delta(summary["success_rate_pct"], prev_summary["success_rate_pct"]),
            "avg_throughput_mbps": _delta(summary["avg_throughput_mbps"], prev_summary["avg_throughput_mbps"]),
            "avg_latency_ms": _delta(summary["avg_latency_ms"], prev_summary["avg_latency_ms"]),
            "total_transfers": _delta(summary["total_transfers"], prev_summary["total_transfers"]),
        },
        "by_direction": an.summarize_by_direction(recs),
        "throughput_by_size": an.throughput_by_size(recs),
        "reliability": an.reliability_score(recs),
        "failure_patterns": patterns,
        "anomaly_count": len(anomalies), "anomalies": anomalies[:10],
        "degradation": an.detect_degradation(
            store.query_transfers(since=now - an.DEGRADATION_LOOKBACK_S, until=now), now),
        "alerts": {"total": len(alerts), "by_rule": dict(Counter(a.rule for a in alerts)),
                   "by_severity": dict(Counter(a.severity for a in alerts))},
        "recommendations": an.recommendations(recs, summary, patterns),
    }


def _f(v, unit: str = "", nd: int = 2) -> str:
    return "n/a" if v is None else f"{v:,.{nd}f}{unit}"


def _signed(v, unit: str = "") -> str:
    return "n/a" if v is None else f"{v:+,.2f}{unit}"


def render_markdown(rep: dict) -> str:
    s, rel, t = rep["summary"], rep["reliability"], rep["trend_vs_previous_period"]
    L = [f"# CloudPulse {rep['kind'].capitalize()} Report",
         f"_Period: {rep['period_start']} to {rep['period_end']} (generated {rep['generated_at']})_", "",
         "## Performance Summary", "",
         "| KPI | Value | vs previous period |", "|---|---|---|",
         f"| Transfers | {s['total_transfers']} | {_signed(t['total_transfers'])} |",
         f"| Data moved | {_f(s['total_bytes'] / MIB / 1024, ' GiB')} | |",
         f"| Avg throughput | {_f(s['avg_throughput_mbps'], ' MB/s')} | {_signed(t['avg_throughput_mbps'], ' MB/s')} |",
         f"| P5 throughput | {_f(s['p5_throughput_mbps'], ' MB/s')} | |",
         f"| Avg latency | {_f(s['avg_latency_ms'], ' ms', 1)} | {_signed(t['avg_latency_ms'], ' ms')} |",
         f"| P95 latency | {_f(s['p95_latency_ms'], ' ms', 1)} | |",
         f"| Avg duration | {_f(s['avg_duration_s'], ' s')} | |",
         f"| Avg CPU / memory | {_f(s['avg_cpu_percent'], '%', 1)} / {_f(s['avg_memory_percent'], '%', 1)} | |",
         "", "| Direction | Transfers | Avg throughput | Avg latency | Success |", "|---|---|---|---|---|"]
    for d, ds in rep["by_direction"].items():
        L.append(f"| {d} | {ds['total_transfers']} | {_f(ds['avg_throughput_mbps'], ' MB/s')} | "
                 f"{_f(ds['avg_latency_ms'], ' ms', 1)} | {_f(ds['success_rate_pct'], '%')} |")
    L += ["", "## Reliability Summary", "",
          f"**Reliability score: {_f(rel['score'], '', 1)} / 100 (grade {rel['grade']})**", "",
          f"- Success rate: {_f(s['success_rate_pct'], '%')} ({s['failed']} failed of {s['total_transfers']}) "
          f"[{_signed(t['success_rate_pct'], ' pp')} vs previous]",
          f"- Retries: {s['total_retries']} total, {_f(s['retry_rate'], '', 3)} per transfer",
          f"- Alerts raised: {rep['alerts']['total']} {rep['alerts']['by_rule'] or ''}",
          f"- Statistical anomalies: {rep['anomaly_count']}"]
    fp = rep["failure_patterns"]
    if fp["recurring"]:
        L += ["", "### Recurring failure patterns", ""] + [f"- {p['pattern']}: {p['count']} failures" for p in fp["recurring"][:8]]
    if rep["degradation"]:
        L += ["", "### Performance degradation (recent vs baseline)", ""] + [
            f"- {d['direction']} {d['size_bucket']} {d['metric']}: {d['change_pct']:+.1f}% "
            f"({d['baseline_median']} -> {d['recent_median']})" for d in rep["degradation"]]
    L += ["", "## Recommendations", ""] + [f"- **[{r['severity'].upper()}] {r['title']}** - {r['detail']}" for r in rep["recommendations"]]
    return "\n".join(L) + "\n"


def save_report(rep: dict, out_dir: str | Path) -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = f"cloudpulse-{rep['kind']}-{rep['generated_at'][:10]}"
    j, m = out / f"{stem}.json", out / f"{stem}.md"
    j.write_text(json.dumps(rep, indent=2))
    m.write_text(render_markdown(rep))
    return j, m
