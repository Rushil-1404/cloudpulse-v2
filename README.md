# CloudPulse v2: Performance & Reliability Analyzer for EC2 ↔ S3 Transfers

> **v2.0** fixes 24 defects found in an audit of v1 (data loss, hidden retries, a too-lenient score, security
> hardening, a Streamlit forward-compatibility break and more). See [CHANGELOG.md](CHANGELOG.md).

CloudPulse measures every file transfer between Amazon EC2 and Amazon S3, stores it, analyses throughput, latency and
reliability, raises alerts, and shows everything on a live dashboard. It implements the PRD (FR-1 … FR-6, NFRs, KPIs)
as a small, testable Python system.

```
 EC2 instance                                                         
┌─────────────────────────────┐   per-transfer record    ┌──────────────────────────┐
│ Monitoring agent            │ ───── HTTPS + API key ──▶│ API (FastAPI)            │
│  boto3 upload/download      │   (spooled to disk if    │  ingest · RBAC · cache   │
│  timing · retries · psutil  │    the API is down)      │  ├─ Alerting service ─────────▶ SNS → email/SMS
└──────────┬──────────────────┘                          │  ├─ Analytics engine     │
           │ put_metric_data                             │  └─ Reports (daily/weekly)│
           ▼                                             └────────┬─────────────────┘
   Amazon CloudWatch                                              │ SQL
   (namespace CloudPulse)                                         ▼
                                                         SQLite (default) ◀── Dashboard (Streamlit + Plotly,
   S3 bucket ◀── the transfers being measured                                  auto-refresh every 30 s)
```

## Quick start (no AWS account needed, about 2 minutes)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]"

cloudpulse seed                                   # 7 days of synthetic traffic with planted problems
CLOUDPULSE_AUTH_DISABLED=true cloudpulse serve    # API on :8000, docs at /docs (dev only)
CLOUDPULSE_AUTH_DISABLED=true cloudpulse dashboard   # second terminal, UI on http://localhost:8501
cloudpulse report daily                           # writes reports/*.md and *.json
python -m pytest -q                               # run the test suite
cloudpulse genkey --role viewer                   # generate an API key + its hashed config entry
```

With Docker: `cp .env.example .env`, edit the keys, `docker compose up --build`, then
`docker compose run --rm api seed`.

## Running on real AWS

1. **Deploy** `infra/cloudformation.yaml` (parameters: VPC, public subnet, your CIDR, alert email, git URL of this repo).
   It creates the EC2 host (IMDSv2, encrypted disk, no SSH, SSM access), a private encrypted S3 bucket (TLS-only), an SNS
   topic, and a least-privilege role (bucket read/write, `PutMetricData` limited to the `CloudPulse` namespace,
   `sns:Publish`). nginx terminates TLS on :443.
2. **Open the dashboard** at the `DashboardUrl` output (self-signed cert; use ACM/ALB or Let's Encrypt for production).
   Secrets are split per consumer under `/etc/cloudpulse/` (root-only): `api.env` holds only key *hashes*;
   `dashboard.env` the viewer key, `agent.env` the agent key, `admin.env` the admin key for humans. Read one via
   Session Manager, e.g. `sudo cat /etc/cloudpulse/admin.env`. A **CloudWatch dashboard** and native failure alarms are
   created too (they keep alerting if the CloudPulse API is down).
3. **Generate or monitor traffic** on the instance:
   ```bash
   sudo bash -c 'set -a; . /etc/cloudpulse/agent.env; set +a
     /opt/cloudpulse/bin/cloudpulse loadgen <bucket> --count 30 --max-mb 50
     /opt/cloudpulse/bin/cloudpulse upload ./file.csv <bucket> reports/file.csv
     /opt/cloudpulse/bin/cloudpulse download <bucket> reports/file.csv /tmp/file.csv'
   ```
   Embed in your own code:
   ```python
   monitor = TransferMonitor(boto3.client("s3"), [ApiSink(url, key), CloudWatchSink()], instance_id=get_instance_id())
   record = monitor.upload("/data/x.csv", "bucket", "x.csv")   # returns the TransferRecord; never raises
   ```

## What gets measured

| Metric | Definition |
|---|---|
| Throughput (MB/s) | `size / duration`, successful non-empty transfers. MB = MiB (1,048,576 B). Duration includes retry back-off, so it is the effective rate users experience |
| Latency (ms) | Time from the start of the final attempt to the first progress callback (≈ time to first byte) |
| Duration / Upload / Download | `completed_at - started_at`, split by direction |
| Success / failure rate | successful (failed) ÷ total transfers |
| Retries | the agent's own retries (capped exponential back-off with jitter; fails fast on AccessDenied, NoSuchKey, …) **plus** botocore's silent internal retries, counted via event hooks |
| CPU / memory / network | `psutil`: system CPU busy % during the transfer (per-sampler `cpu_times` deltas, safe under concurrency), memory % at the end, real-NIC bytes (loopback excluded) ÷ duration |

**Reliability score (0-100)** = 60% success (`100 - 10 × failure-points`: 99% → 90, 95% → 50, ≤ 90% → 0) + 20% retry health (`100 - 50 × retries/transfer`) + 20% throughput
stability (spread of the *latency-adjusted* data rate within direction × size bucket, files ≥ 1 MiB). Grades: A ≥ 95, B ≥ 90, C ≥ 80, D ≥ 70.

**Analytics** (all deterministic and explainable, no ML): recurring failure patterns (error code × direction × size ×
hour), robust modified-z-score anomalies per direction × size bucket, recent-vs-baseline degradation detection, and
rule-based recommendations that cite the evidence that triggered them.

**Alerts (FR-5):** failed transfer (critical), throughput below threshold (files ≥ 1 MiB only, since latency dominates tiny
objects), latency above threshold, retries ≥ threshold. Alerts are evaluated at ingestion (near real time),
de-duplicated with a cool-down (60 s for failures, 300 s otherwise), stored, and published to SNS when configured.

## API (`/docs` has the full OpenAPI UI)

| Endpoint | Role | Purpose |
|---|---|---|
| `POST /api/v1/transfers`, `/transfers/batch` | agent, admin | Idempotent ingest (by `transfer_id`), max 1000 per batch; invalid records are returned in `rejected` with reasons |
| `GET /api/v1/transfers` | viewer, admin | Paged history with filters |
| `GET /api/v1/metrics/summary`, `/timeseries` | viewer, admin | KPIs, per-direction and per-size stats, chart series |
| `GET /api/v1/reliability` | viewer, admin | Score, failure patterns, anomalies, degradation, recommendations |
| `GET /api/v1/alerts`, `POST /alerts/{id}/ack` | viewer / admin | Alert feed and acknowledgement |
| `GET /api/v1/reports/{daily\|weekly}?format=markdown` | viewer, admin | Reports |
| `GET /health`, `GET /api/v1/whoami` | public / any | Liveness; caller's role |

Auth is an `X-API-Key` header. Keys and roles come from `CLOUDPULSE_API_KEYS=key:admin,key:viewer,key:agent`.
The API refuses to start with no keys unless `CLOUDPULSE_AUTH_DISABLED=true`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CLOUDPULSE_API_KEYS` | (required) | `key:role` list; keys may be hashed as `sha256$<hex>` (`cloudpulse genkey`) |
| `CLOUDPULSE_RETENTION_DAYS` | 90 | Data older than this is purged hourly (0 = keep forever; `cloudpulse purge`) |
| `CLOUDPULSE_DB_PATH` | `cloudpulse.db` | SQLite file |
| `CLOUDPULSE_API_URL` / `CLOUDPULSE_AGENT_API_KEY` | `http://localhost:8000` / | Where the agent sends records |
| `CLOUDPULSE_DASHBOARD_API_KEY` | | Key the dashboard uses (viewer by default; admin enables alert ack) |
| `CLOUDPULSE_SNS_TOPIC_ARN` | | Enables SNS notifications |
| `CLOUDPULSE_MIN_THROUGHPUT_MBPS` / `_MAX_LATENCY_MS` / `_MAX_RETRIES` | 5 / 1000 / 3 | Alert thresholds |
| `CLOUDPULSE_ALERT_COOLDOWN_S` | 300 | Alert de-duplication window |
| `CLOUDPULSE_CW_NAMESPACE` | `CloudPulse` | CloudWatch namespace |

## Requirement traceability

| PRD | Where |
|---|---|
| FR-1 Transfer monitoring | `agent/transfer.py` (start/end, size, direction, status) |
| FR-2 Metrics | `agent/transfer.py`, `agent/resources.py`, `agent/sinks.py` (CloudWatch), `analytics.summarize` |
| FR-3 Reliability | `analytics.reliability_score`, `failure_patterns`, retry stats |
| FR-4 Dashboard | `dashboard/app.py` (widgets, history, throughput/latency charts, score panel, alerts) |
| FR-5 Alerting | `alerts.py` + SNS |
| FR-6 Reporting | `reports.py`, `cloudpulse report`, `/api/v1/reports/*` |
| NFR performance | 30 s refresh, 10 s query cache; `PerformanceTests` analyses 30 days × 1000/day in well under 3 s |
| NFR security | IAM least privilege, TLS via nginx (rate-limited, security headers), API-key RBAC with hashed keys, per-service secrets, IMDSv2, no SSH, hardened systemd units, input sanity checks |
| CloudWatch dashboards (PRD §7) | `infra/cloudformation.yaml` (dashboard + native alarms) |
| NFR scalability | ≥ 1000 records/day (tested at 30 000 records); idempotent batch ingest |
| Risks | Incomplete metrics → disk spool + idempotent replay; network instability → retries; dashboard load → cache |

## Design decisions

* **CloudWatch and a database.** CloudWatch gets the aggregate metrics (alarms, native dashboards, retention). Per-transfer
  records live in the database because failure patterns, percentiles and history tables need event-level data that
  CloudWatch cannot give cheaply.
* **SQLite by default**, for zero-setup demos at the PRD's scale (1000 records/day). `SQLiteStore` is the only module that
  touches SQL; PostgreSQL or DynamoDB means implementing the same ~9 methods.
* **The agent never raises and never blocks on observability.** Sink failures are logged, API outages go to a local
  JSONL spool and are replayed in order. A transfer that fails is still a recorded data point.
* **Idempotent ingest** (`INSERT OR IGNORE` on `transfer_id`) makes at-least-once delivery safe.
* **Analytics are pure functions** over records, which is why they are easy to test and to move into Lambda later.
* **Framework-free service layer.** `service.py` / `auth.py` hold all API behaviour; `api.py` only routes. Everything that matters is testable without FastAPI installed.
* **Reporting is off the hot path.** Sinks and SNS run on background workers with bounded queues; call `monitor.close()` (or use `with`) to flush.

## Testing (PRD Phase 6)

`python -m pytest -q` (or `python -m unittest discover -s tests`; 64 tests run without FastAPI, +8 API tests with it). `tests/test_v2.py` holds a regression test for each v2 fix. Coverage: models and validation, KPI maths, reliability score,
failure patterns, anomaly and degradation detection, recommendations, alert rules and cool-down, storage idempotency,
agent retry/back-off/non-retryable/missing-file paths (fake S3 client), API spool and replay, CloudWatch datapoints,
simulator-driven end-to-end reports, a performance budget test, and API RBAC/validation tests (needs `fastapi` and `httpx`).

## Known limitations

* CPU and network figures are host-wide (psutil), so concurrent non-transfer work inflates them. `RetryCounter` deltas assume one monitor per boto3 client.
* botocore's internal retries are counted but their individual error codes are not visible.
* Latency is time-to-first-progress-callback, a proxy for TTFB rather than a packet-level measurement.
* Single-writer SQLite; use PostgreSQL if several API replicas are needed.
* The self-signed certificate in the template is for demos only.
* Out of scope per the PRD: multi-cloud, ML forecasting, automated remediation. See PRD §12 for the roadmap.
