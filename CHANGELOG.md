# Changelog

## 2.0.0: defect-fix and hardening release

An audit of V1 found 24 defects (below). Each one has a regression test in `tests/test_v2.py`, except where marked
**(infra)**, which can only be validated by deploying. Severity reflects the impact on a real deployment.

### Data loss and correctness

| # | Sev | Defect in V1 | Fix in V2 |
|---|---|---|---|
| 1 | High | **Poison-pill spool.** If the API answered 4xx for one record, the whole spool was retried forever, so no later record was ever delivered (and the spool then silently dropped the oldest data when full). | A 4xx rejection triggers record-by-record isolation; permanently bad records go to `<spool>.rejected`, good ones flow. 401/403 (fixable key) and 5xx/429 stay queued. |
| 2 | High | **Reporting slowed the transfers it measured.** The API sink ran synchronously (5 s timeout) after every transfer, contradicting the README's "never blocks" claim. | `BackgroundSink` worker thread + `TransferMonitor.close()` flush (used by all CLI commands). |
| 3 | High | **Degradation detection was blind** for any range under 25 h and in daily reports, because the baseline window was outside the loaded records. | The degradation check always loads its own 25 h look-back (service and reports). |
| 4 | High | **Reliability score was too lenient.** A system failing 10% of transfers still scored a "B". | Success component is `100 - 10 x failure-points`: 99% -> 90, 95% -> 50, <= 90% -> 0. |
| 5 | Med | **Hidden retries.** botocore retries throttles internally, invisible to the `retries` metric, so flakiness was under-reported. | `RetryCounter` hooks botocore's `request-created` / `before-call` events and adds internal retries to the record. |
| 6 | Med | **CPU figure wrong under concurrency** (psutil's global timer) and **network inflated** by loopback traffic to the local API. | Per-sampler `cpu_times()` deltas (guest time excluded); loopback excluded from NIC counters. |
| 7 | Med | **Seeding went stale.** Demo ids are deterministic, so re-running `seed` inserted nothing and the data aged out of the dashboard window. | `seed` replaces previous demo data (`--keep` to append). |
| 8 | Low | A torn spool line after a crash would wedge the agent; a missing agent key silently spooled 401s forever. | Corrupt lines are skipped; the CLI fails fast without `CLOUDPULSE_AGENT_API_KEY`. |

### Alerting

| # | Sev | Defect in V1 | Fix in V2 |
|---|---|---|---|
| 9 | Med | **Stale pages.** Records replayed after an outage fired SNS notifications for hours-old failures. | Alerts for records older than 1 h are stored, tagged `[late arrival]`, and not notified. |
| 10 | Med | **SNS publish ran inside the ingest request**, so a slow SNS delayed the agent's POST. | `AsyncNotifier` (bounded queue, worker threads, drop-and-log when full). |
| 11 | Med | **Cool-down was one-sided**, so out-of-order arrivals defeated de-duplication. | Symmetric (`abs`) cool-down. |

### Security and robustness

| # | Sev | Defect in V1 | Fix in V2 |
|---|---|---|---|
| 12 | Med | NaN/inf, future timestamps, year-1970 data, week-long durations and 10^18-byte sizes were accepted (polluting windows, causing 500s on SQLite overflow). | Pydantic `allow_inf_nan=False`, size <= 5 TiB; service rejects future (> 10 min), implausibly old or over-long transfers per record and reports reasons. |
| 13 | Med | **DoS vectors:** unbounded `range` values, tiny `bucket` sizes (hundreds of thousands of buckets), and an unbounded query cache. | Range <= 366 d, <= 2000 buckets, size-bounded cache. |
| 14 | Low | Unauthenticated `/health` disclosed the stored-record count and never checked the database. | Returns status + version only; pings the DB (503 on failure). |
| 15 | Med | No retention: the database grew forever. | `CLOUDPULSE_RETENTION_DAYS` (default 90), hourly enforcement, `cloudpulse purge`. |
| 16 | Med | **Secrets.** One env file with every key was handed to every service; keys sat in plaintext on the server. | **(infra)** One file per consumer; the API stores only SHA-256 hashes (`sha256$...`, `cloudpulse genkey`). |
| 17 | Low | **(infra)** nginx had no rate limit or security headers; systemd units unhardened. | `limit_req`, body-size cap, HSTS/nosniff/frame headers, `NoNewPrivileges`, `ProtectSystem=strict`, `PrivateTmp`. |

### Completeness, deployment and UX

| # | Sev | Defect in V1 | Fix in V2 |
|---|---|---|---|
| 18 | Med | **PRD gap:** CloudWatch dashboards were required but not delivered. | **(infra)** CloudWatch dashboard + native failure alarms (they keep working if the CloudPulse API is down). |
| 19 | Low | **(infra)** Instance reached S3 over the internet path, which the tool's own latency recommendation warns against. | Optional S3 gateway VPC endpoint (`RouteTableId` parameter). |
| 20 | High | **Dashboard would break on current Streamlit:** `use_container_width` is deprecated and will be removed in favour of `width="stretch"`. | Version-agnostic detection of the supported argument. |
| 21 | Low | Dashboard regenerated both reports on every rerun; timestamps unlabelled. | 60 s cache; "UTC" labelled. |
| 22 | Med | **Docker:** the API health check was inherited by the dashboard container, so it was permanently "unhealthy". | Per-service health checks; the dashboard waits for a healthy API. |

### Engineering

| # | Sev | Defect in V1 | Fix in V2 |
|---|---|---|---|
| 23 | Med | **The API layer was untestable without FastAPI**, so its logic had no tests in constrained environments. | Logic moved to `service.py` and `auth.py` (framework-free, fully tested); `api.py` is a thin router. |
| 24 | Low | Version/behaviour docs drifted (README claimed behaviours the code did not have). | README, formulas and traceability updated to match. |

### Not verified in the build environment (no network)

* `api.py` routing, the Streamlit dashboard, `cloudformation.yaml` against AWS, and the botocore event names used by
  `RetryCounter` were reviewed and syntax/structure-checked but **not executed**. Their logic is covered through the
  service layer and fakes; run `python -m pytest -q` with the dev extras installed (it also runs `tests/test_api.py`),
  then deploy the template to a sandbox account.
