import unittest

from cloudpulse import analytics as an
from cloudpulse.alerts import AlertManager, AlertThresholds, evaluate_transfer
from cloudpulse.config import parse_api_keys
from cloudpulse.models import MIB
from cloudpulse.storage import SQLiteStore
from helpers import rec


class ModelTests(unittest.TestCase):
    def test_throughput_and_duration(self):
        r = rec(size=100 * MIB, duration=4)
        self.assertAlmostEqual(r.throughput_mbps, 25.0)
        self.assertEqual(r.duration_s, 4)

    def test_failed_has_no_throughput(self):
        self.assertIsNone(rec(status="failed", code="X").throughput_mbps)

    def test_validate(self):
        r = rec()
        r.completed_at = r.started_at - 1
        with self.assertRaises(ValueError):
            r.validate()
        with self.assertRaises(ValueError):
            rec(direction="sideways").validate()

    def test_api_keys_parse(self):
        self.assertEqual(parse_api_keys("a:admin, b:viewer"), {"a": "admin", "b": "viewer"})
        with self.assertRaises(ValueError):
            parse_api_keys("a:root")


class AnalyticsTests(unittest.TestCase):
    def test_percentile_and_range(self):
        self.assertEqual(an.percentile([1, 2, 3, 4], 50), 2.5)
        self.assertIsNone(an.percentile([], 50))
        self.assertEqual(an.parse_range("24h"), 86400)
        self.assertEqual(an.parse_range("30m"), 1800)
        with self.assertRaises(ValueError):
            an.parse_range("abc")

    def test_summary(self):
        recs = [rec(0, duration=1), rec(1, duration=2), rec(2, status="failed", code="SlowDown", retries=3)]
        s = an.summarize(recs)
        self.assertEqual((s["total_transfers"], s["successful"], s["failed"]), (3, 2, 1))
        self.assertAlmostEqual(s["success_rate_pct"], 66.67)
        self.assertAlmostEqual(s["avg_throughput_mbps"], 7.5)  # (10 + 5) / 2
        self.assertEqual(s["total_retries"], 3)
        self.assertEqual(an.summarize([])["success_rate_pct"], None)

    def test_reliability_perfect_and_bad(self):
        good = [rec(i, duration=1.0 + (i % 2) * 0.01) for i in range(20)]
        self.assertGreaterEqual(an.reliability_score(good)["score"], 95)
        bad = [rec(i, status="failed", code="X", retries=3) for i in range(10)]
        r = an.reliability_score(bad)
        self.assertLess(r["score"], 40)
        self.assertEqual(r["grade"], "F")
        self.assertIsNone(an.reliability_score([])["score"])

    def test_failure_patterns_recurring(self):
        fails = [rec(i, status="failed", code="SlowDown", size=200 * MIB, start=1_000_000 + i) for i in range(4)]
        p = an.failure_patterns(fails + [rec(99, status="failed", code="Other")])
        self.assertEqual(p["by_error_code"]["SlowDown"], 4)
        self.assertTrue(any("SlowDown on large uploads" in x["pattern"] for x in p["recurring"]))
        self.assertFalse(any("Other" in x["pattern"] for x in p["recurring"]))

    def test_anomaly_detection(self):
        normal = [rec(i, duration=1.0 + (i % 5) * 0.02, start=1e6 + i) for i in range(30)]
        slow = rec(100, duration=20.0, start=1e6 + 100)
        found = an.detect_anomalies(normal + [slow])
        self.assertEqual([a["transfer_id"] for a in found if a["metric"] == "throughput_mbps"], ["t100"])
        self.assertEqual(an.detect_anomalies(normal[:5]), [])  # too few samples

    def test_degradation(self):
        now = 2_000_000.0
        base = [rec(i, start=now - 7200 - i * 60, duration=1.0 + (i % 3) * 0.01) for i in range(30)]
        recent = [rec(100 + i, start=now - 600 - i * 60, duration=3.0) for i in range(6)]
        d = an.detect_degradation(base + recent, now)
        self.assertTrue(any(x["metric"] == "throughput_mbps" and x["change_pct"] < -50 for x in d))
        self.assertEqual(an.detect_degradation(base, now), [])

    def test_recommendations(self):
        recs = [rec(i, status="failed", code="SlowDown") for i in range(5)] + [rec(9)]
        titles = [r["title"] for r in an.recommendations(recs)]
        self.assertIn("S3 request throttling detected", titles)
        self.assertEqual(an.recommendations([rec(i) for i in range(5)])[0]["severity"], "info")

    def test_timeseries_buckets(self):
        pts = an.timeseries([rec(0, start=0), rec(1, start=30), rec(2, start=90)], 60)
        self.assertEqual([p["count"] for p in pts], [2, 1])


class StorageAndAlertTests(unittest.TestCase):
    def setUp(self):
        self.store = SQLiteStore(":memory:")

    def test_idempotent_insert_and_filters(self):
        a, b = rec(1, start=100), rec(2, start=200, direction="download")
        self.assertEqual(len(self.store.insert_transfers([a, b])), 2)
        self.assertEqual(len(self.store.insert_transfers([a])), 0)  # duplicate ignored
        self.assertEqual(self.store.count_transfers(), 2)
        self.assertEqual([r.transfer_id for r in self.store.query_transfers(direction="download")], ["t2"])
        self.assertEqual([r.transfer_id for r in self.store.query_transfers(since=150)], ["t2"])
        self.assertEqual(self.store.query_transfers(descending=True, limit=1)[0].transfer_id, "t2")

    def test_alert_rules(self):
        th = AlertThresholds(min_throughput_mbps=5, max_latency_ms=1000, max_retries=3)
        self.assertEqual({a.rule for a in evaluate_transfer(rec(status="failed", code="X"), th)}, {"transfer_failed"})
        slow = rec(size=50 * MIB, duration=20, latency=2000, retries=3)
        self.assertEqual({a.rule for a in evaluate_transfer(slow, th)},
                         {"low_throughput", "high_latency", "excessive_retries"})
        tiny_slow = rec(size=1000, duration=5)  # tiny files are not judged on throughput
        self.assertEqual(evaluate_transfer(tiny_slow, th), [])
        self.assertEqual(evaluate_transfer(rec(), th), [])

    def test_cooldown_dedupes_and_persists(self):
        sent = []

        class N:
            def notify(self, a):
                sent.append(a)

        mgr = AlertManager(self.store, notifiers=[N()], cooldown_s=300, max_notify_age_s=None)
        f = lambda i, t: rec(i, status="failed", code="X", start=t)  # noqa: E731
        self.assertEqual(len(mgr.process([f(1, 1000)])), 1)
        self.assertEqual(len(mgr.process([f(2, 1030)])), 0)   # inside 60 s failure cool-down
        self.assertEqual(len(mgr.process([f(3, 1100)])), 1)   # after cool-down
        self.assertEqual(len(sent), 2)
        alerts = self.store.list_alerts()
        self.assertEqual(len(alerts), 2)
        self.assertTrue(self.store.ack_alert(alerts[0].id))
        self.assertEqual(len(self.store.list_alerts(open_only=True)), 1)
        self.assertFalse(self.store.ack_alert(9999))

    def test_broken_notifier_does_not_break_ingest(self):
        class Boom:
            def notify(self, a):
                raise RuntimeError("sns down")

        out = AlertManager(self.store, notifiers=[Boom()], max_notify_age_s=None).process([rec(status="failed", code="X")])
        self.assertEqual(len(out), 1)


if __name__ == "__main__":
    unittest.main()
