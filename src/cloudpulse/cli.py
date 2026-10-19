"""``cloudpulse`` command line: serve | dashboard | seed | purge | genkey | upload | download | loadgen | report."""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from .config import Settings


def _build_monitor(settings: Settings, args):
    from .agent.aws import make_s3_client
    from .agent.resources import get_instance_id
    from .agent.sinks import ApiSink, BackgroundSink, CloudWatchSink
    from .agent.transfer import TransferMonitor

    sinks = []
    if not args.no_api:
        if not settings.agent_api_key:
            raise SystemExit("CLOUDPULSE_AGENT_API_KEY is not set (use --no-api to skip the CloudPulse API)")
        sinks.append(BackgroundSink(ApiSink(settings.api_url, settings.agent_api_key, settings.spool_path)))
    if not args.no_cloudwatch:
        sinks.append(BackgroundSink(CloudWatchSink(settings.cw_namespace, settings.aws_region)))
    s3, counter = make_s3_client(settings.aws_region)
    return TransferMonitor(s3, sinks, instance_id=settings.instance_id or get_instance_id(),
                           max_retries=settings.max_retries, retry_counter=counter)


def _print_record(rec) -> int:
    tp = f"{rec.throughput_mbps:.2f} MB/s" if rec.throughput_mbps is not None else "n/a"
    print(f"{rec.status.upper()} {rec.direction} s3://{rec.bucket}/{rec.key} size={rec.file_size_bytes}B "
          f"duration={rec.duration_s:.2f}s throughput={tp} latency={rec.latency_ms}ms retries={rec.retries}"
          + (f" error={rec.error_code}" if rec.error_code else ""))
    return 0 if rec.ok else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="cloudpulse", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the API server")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)

    d = sub.add_parser("dashboard", help="run the Streamlit dashboard")
    d.add_argument("--port", type=int, default=8501)
    d.add_argument("--host", default="127.0.0.1")

    sd = sub.add_parser("seed", help="load synthetic demo data (no AWS needed)")
    sd.add_argument("--days", type=int, default=7)
    sd.add_argument("--per-day", type=int, default=300)
    sd.add_argument("--seed", type=int, default=42)
    sd.add_argument("--keep", action="store_true", help="keep earlier demo data instead of replacing it")

    pg = sub.add_parser("purge", help="delete transfers/alerts older than N days")
    pg.add_argument("--days", type=int, default=None, help="default: CLOUDPULSE_RETENTION_DAYS")

    gk = sub.add_parser("genkey", help="generate an API key and its hashed config entry")
    gk.add_argument("--role", choices=["admin", "viewer", "agent"], default="viewer")

    for name in ("upload", "download"):
        t = sub.add_parser(name, help=f"monitored S3 {name}")
        if name == "upload":
            t.add_argument("path"), t.add_argument("bucket"), t.add_argument("key")
        else:
            t.add_argument("bucket"), t.add_argument("key"), t.add_argument("path")
        t.add_argument("--no-api", action="store_true", help="do not send to the CloudPulse API")
        t.add_argument("--no-cloudwatch", action="store_true", help="do not publish to CloudWatch")

    lg = sub.add_parser("loadgen", help="generate real monitored upload+download traffic against a bucket")
    lg.add_argument("bucket")
    lg.add_argument("--count", type=int, default=20)
    lg.add_argument("--min-mb", type=float, default=0.1)
    lg.add_argument("--max-mb", type=float, default=25.0)
    lg.add_argument("--interval", type=float, default=1.0, help="seconds between files")
    lg.add_argument("--no-api", action="store_true")
    lg.add_argument("--no-cloudwatch", action="store_true")

    r = sub.add_parser("report", help="generate a daily/weekly report")
    r.add_argument("kind", choices=["daily", "weekly"])
    r.add_argument("--out", default="reports")

    args = p.parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "serve":
        import uvicorn
        uvicorn.run("cloudpulse.api:create_app", factory=True, host=args.host, port=args.port)
        return 0
    if args.cmd == "dashboard":
        app_path = Path(__file__).parent / "dashboard" / "app.py"
        return subprocess.call([sys.executable, "-m", "streamlit", "run", str(app_path), "--server.address", args.host,
                                "--server.port", str(args.port), "--browser.gatherUsageStats", "false"])
    if args.cmd == "seed":
        from .alerts import AlertManager, AlertThresholds, NullNotifier
        from .simulator import generate
        from .storage import SQLiteStore
        store = SQLiteStore(settings.db_path)
        if not args.keep:  # demo ids are deterministic: without this a re-seed would be ignored and go stale
            store.delete_simulated()
        recs = generate(args.days, args.per_day, args.seed)
        new = store.insert_transfers(recs)
        alerts = AlertManager(store, AlertThresholds(settings.min_throughput_mbps, settings.max_latency_ms,
                                                     settings.max_retries), [NullNotifier()],
                              settings.alert_cooldown_s, max_notify_age_s=None).process(new)
        print(f"Seeded {len(new)} transfers and {len(alerts)} alerts into {settings.db_path}")
        return 0
    if args.cmd == "purge":
        from .storage import SQLiteStore
        days = settings.retention_days if args.days is None else args.days
        if days <= 0:
            raise SystemExit("retention is disabled (days <= 0)")
        print("Purged", SQLiteStore(settings.db_path).purge_older_than(time.time() - days * 86400))
        return 0
    if args.cmd == "genkey":
        from .auth import generate_key, hash_key
        raw = generate_key()
        print(f"API key (give to the {args.role}; shown once): {raw}")
        print(f"CLOUDPULSE_API_KEYS entry (store this):        {hash_key(raw)}:{args.role}")
        return 0
    if args.cmd in ("upload", "download"):
        with _build_monitor(settings, args) as monitor:  # the context manager flushes background sinks
            rec = monitor.upload(args.path, args.bucket, args.key) if args.cmd == "upload" \
                else monitor.download(args.bucket, args.key, args.path)
        return _print_record(rec)
    if args.cmd == "loadgen":
        from .agent.loadgen import run_loadgen
        with _build_monitor(settings, args) as monitor:
            recs = run_loadgen(monitor, args.bucket, args.count, args.min_mb, args.max_mb, args.interval)
        failed = sum(1 for r in recs if not r.ok)
        print(f"loadgen finished: {len(recs)} transfers, {failed} failed")
        return 1 if failed else 0
    if args.cmd == "report":
        from .reports import build_report, save_report
        from .storage import SQLiteStore
        j, m = save_report(build_report(SQLiteStore(settings.db_path), args.kind), args.out)
        print(f"Wrote {j} and {m}")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
