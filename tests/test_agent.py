import json
import os
import tempfile
import unittest

from cloudpulse.agent.sinks import ApiSink, build_metric_data
from cloudpulse.agent.transfer import TransferMonitor
from helpers import rec


class ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    """Fails the first N calls with `error`, then succeeds and reports progress."""

    def __init__(self, failures=0, error="RequestTimeout", size=5_000_000):
        self.failures, self.error, self.size, self.calls = failures, error, size, 0

    def _go(self, cb):
        self.calls += 1
        if self.calls <= self.failures:
            raise ClientError(self.error)
        cb(1024)
        cb(self.size)

    def upload_file(self, path, bucket, key, Callback=None, **kw):
        self._go(Callback)

    def download_file(self, bucket, key, path, Callback=None, **kw):
        self._go(Callback)

    def head_object(self, Bucket, Key):
        return {"ContentLength": self.size}


class NoSampler:
    def start(self): pass
    def stop(self, d): return {"cpu_percent": 10.0, "memory_percent": 20.0, "network_mbps": 1.0}


class Clock:
    def __init__(self): self.t = 100.0
    def __call__(self): return self.t


class Collect:
    def __init__(self): self.records = []
    def emit(self, r): self.records.append(r)


def monitor(s3, sink, **kw):
    clock, sleeps = Clock(), []

    def sleep(s):
        sleeps.append(s)
        clock.t += s

    m = TransferMonitor(s3, [sink], instance_id="i-9", sleep=sleep, clock=clock, monotonic=clock,
                        jitter=lambda: 0.0, sampler_factory=NoSampler, **kw)
    return m, sleeps


class AgentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.write(b"x" * 2048)
        self.tmp.close()
        self.addCleanup(os.unlink, self.tmp.name)

    def test_successful_upload(self):
        sink = Collect()
        m, _ = monitor(FakeS3(), sink)
        r = m.upload(self.tmp.name, "bkt", "k")
        self.assertEqual((r.status, r.retries, r.file_size_bytes, r.direction), ("success", 0, 2048, "upload"))
        self.assertIsNotNone(r.latency_ms)
        self.assertEqual(r.instance_id, "i-9")
        self.assertEqual(sink.records, [r])

    def test_retries_then_succeeds_with_backoff(self):
        m, sleeps = monitor(FakeS3(failures=2), Collect(), backoff_base_s=1.0)
        r = m.download("bkt", "k", "/tmp/x")
        self.assertEqual((r.status, r.retries, r.file_size_bytes), ("success", 2, 5_000_000))
        self.assertEqual(sleeps, [0.5, 1.0])  # equal-jitter with jitter()=0 -> half of 1s, 2s
        self.assertEqual(r.duration_s, 1.5)

    def test_exhausts_retries(self):
        m, _ = monitor(FakeS3(failures=99, error="SlowDown"), Collect(), max_retries=3)
        r = m.download("bkt", "k", "/tmp/x")
        self.assertEqual((r.status, r.retries, r.error_code), ("failed", 3, "SlowDown"))
        self.assertIsNone(r.latency_ms)

    def test_non_retryable_fails_fast(self):
        s3 = FakeS3(failures=99, error="AccessDenied")
        m, sleeps = monitor(s3, Collect())
        r = m.download("bkt", "k", "/tmp/x")
        self.assertEqual((r.status, r.retries, s3.calls, sleeps), ("failed", 0, 1, []))

    def test_missing_file_is_a_recorded_failure(self):
        m, _ = monitor(FakeS3(), Collect())
        r = m.upload("/definitely/missing", "bkt", "k")
        self.assertEqual((r.status, r.error_code), ("failed", "FileNotFoundError"))

    def test_sink_failure_never_breaks_transfer(self):
        class Boom:
            def emit(self, r): raise RuntimeError("down")

        m = TransferMonitor(FakeS3(), [Boom()], sampler_factory=NoSampler)
        self.assertEqual(m.upload(self.tmp.name, "b", "k").status, "success")


class SinkTests(unittest.TestCase):
    class Resp:
        def __init__(self, ok): self.status_code = 200 if ok else 503

    class Session:
        def __init__(self): self.up, self.calls = True, []
        def post(self, url, json=None, headers=None, timeout=None):
            self.calls.append((url, json, headers))
            return SinkTests.Resp(self.up)

    def test_spool_and_replay(self):
        spool = os.path.join(tempfile.mkdtemp(), "spool.jsonl")
        sess = self.Session()
        sink = ApiSink("http://api/", "key", spool, session=sess)
        sess.up = False
        sink.emit(rec(1))
        sink.emit(rec(2))
        with open(spool) as fh:
            self.assertEqual(len(fh.read().splitlines()), 2)  # spooled, nothing lost
        sess.up = True
        sink.emit(rec(3))
        self.assertFalse(os.path.exists(spool))  # flushed
        sent = sess.calls[-1][1]["transfers"]
        self.assertEqual([t["transfer_id"] for t in sent], ["t1", "t2", "t3"])
        self.assertEqual(sess.calls[-1][2], {"X-API-Key": "key"})
        json.dumps(sent)  # payload is JSON-serialisable

    def test_cloudwatch_datapoints(self):
        ok = {d["MetricName"]: d for d in build_metric_data(rec(cpu_percent=5.0, network_mbps=2.0))}
        self.assertEqual(ok["TransferSuccess"]["Value"], 1.0)
        self.assertEqual(ok["ThroughputMBps"]["Unit"], "Megabytes/Second")
        self.assertIn("CpuUtilization", ok)
        bad = {d["MetricName"] for d in build_metric_data(rec(status="failed", code="X"))}
        self.assertIn("TransferFailure", bad)
        self.assertNotIn("ThroughputMBps", bad)


class LoadgenTests(unittest.TestCase):
    def test_generates_upload_and_download_pairs(self):
        from cloudpulse.agent.loadgen import run_loadgen
        m, _ = monitor(FakeS3(), Collect())
        recs = run_loadgen(m, "bkt", count=3, min_mb=0.01, max_mb=0.02, interval_s=0, seed=1, sleep=lambda s: None)
        self.assertEqual([r.direction for r in recs], ["upload", "download"] * 3)
        self.assertTrue(all(r.ok for r in recs))


if __name__ == "__main__":
    unittest.main()
