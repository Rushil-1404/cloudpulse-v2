# 5-minute capstone demo script

1. **Problem (30 s).** AWS gives raw metrics. Nobody tells you why *your* EC2 → S3 transfers are slow or flaky.
2. **Architecture (60 s).** Walk the diagram in the README: agent → CloudWatch + API → analytics/alerts → dashboard.
3. **Live traffic (60 s).** On the EC2 instance run `cloudpulse loadgen <bucket> --count 20`. Watch the KPI widgets
   update (30 s refresh).
4. **Inject a failure (60 s).** `cloudpulse upload ./file.csv no-such-bucket-12345 x` (the process flushes its background sinks before exiting). It records a non-retryable failure,
   raises a `transfer_failed` alert (email via SNS) and lowers the success rate.
5. **Reliability story (60 s).** Switch the dashboard to the seeded 7-day data (`cloudpulse seed`): the score panel,
   recurring `SlowDown` failures on large uploads around 02:00 UTC, the recent upload degradation warning, and the
   recommendations (multipart tuning, prefix spreading, VPC endpoint).
6. **Reports + engineering (30 s).** Download the daily report; mention tests, least-privilege IAM, TLS, idempotent
   ingest and the disk spool.

Likely questions: *Why both CloudWatch and a DB?* (event-level analytics) · *How do you know an anomaly is real?*
(robust modified z-score per direction × size bucket) · *What if the API is down?* (spool and replay).
