import time
import unittest

from cloudpulse import analytics as an
from cloudpulse.alerts import AlertManager, NullNotifier
from cloudpulse.reports import build_report, render_markdown, save_report
from cloudpulse.simulator import generate
from cloudpulse.storage import SQLiteStore
import tempfile
from pathlib import Path

NOW = 1_800_000_000.0


class SimulatedSystemTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recs = generate(days=7, per_day=300, seed=7, now=NOW)
        cls.store = SQLiteStore(":memory:")
        new = cls.store.insert_transfers(cls.recs)
        AlertManager(cls.store, notifiers=[NullNotifier()], max_notify_age_s=None).process(new)

    def test_deterministic(self):
        again = generate(days=1, per_day=50, seed=7, now=NOW)
        self.assertEqual([r.transfer_id for r in again], [r.transfer_id for r in generate(1, 50, 7, NOW)])
        self.assertEqual(len(self.recs), 2100)
        for r in self.recs:
            r.validate()

    def test_planted_patterns_are_found(self):
        s = an.summarize(self.recs)
        self.assertTrue(94 < s["success_rate_pct"] < 100, s["success_rate_pct"])
        patterns = an.failure_patterns(self.recs)
        self.assertTrue(any("SlowDown" in p["pattern"] for p in patterns["recurring"]), patterns["recurring"][:5])
        deg = an.detect_degradation(self.recs, NOW)
        self.assertTrue(any(d["direction"] == "upload" and d["metric"] == "throughput_mbps" for d in deg), deg)

    def test_report_daily_and_weekly(self):
        for kind in ("daily", "weekly"):
            rep = build_report(self.store, kind, now=NOW)
            md = render_markdown(rep)
            self.assertIn("## Performance Summary", md)
            self.assertIn("## Reliability Summary", md)
            self.assertIn("## Recommendations", md)
            self.assertIsNotNone(rep["reliability"]["score"])
        with tempfile.TemporaryDirectory() as d:
            j, m = save_report(build_report(self.store, "daily", now=NOW), d)
            self.assertTrue(Path(j).exists() and Path(m).exists())
        with self.assertRaises(ValueError):
            build_report(self.store, "monthly")

    def test_empty_report_does_not_crash(self):
        md = render_markdown(build_report(SQLiteStore(":memory:"), "daily", now=NOW))
        self.assertIn("n/a", md)


class PerformanceTests(unittest.TestCase):
    def test_30_days_of_1000_per_day_analyses_well_under_budget(self):
        """NFR: dashboard response < 3 s. Analytics over 30 days x 1000/day must fit comfortably."""
        store = SQLiteStore(":memory:")
        store.insert_transfers(generate(days=30, per_day=1000, seed=1, now=time.time()))
        t0 = time.perf_counter()
        recs = store.query_transfers(since=time.time() - 30 * 86400)
        an.summarize(recs), an.timeseries(recs, 3600), an.reliability_score(recs), an.failure_patterns(recs)
        an.detect_anomalies(recs), an.recommendations(recs)
        elapsed = time.perf_counter() - t0
        self.assertLess(elapsed, 3.0, f"took {elapsed:.2f}s")


if __name__ == "__main__":
    unittest.main()
