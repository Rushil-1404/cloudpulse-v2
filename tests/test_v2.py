"""Regression tests for every defect fixed in V2 (see CHANGELOG.md)."""
import contextlib
import io
import json
import os
import tempfile
import time
import unittest

from cloudpulse import analytics as an
from cloudpulse.agent.aws import RetryCounter
from cloudpulse.agent.resources import ResourceSampler, cpu_percent_between
from cloudpulse.agent.sinks import ApiSink, BackgroundSink
from cloudpulse.agent.transfer import TransferMonitor
from cloudpulse.alerts import AlertManager, AsyncNotifier, NullNotifier
from cloudpulse.auth import AuthError, Authenticator, hash_key
from cloudpulse.config import Settings, parse_api_keys
from cloudpulse.models import MIB
from cloudpulse.service import CloudPulseService, ServiceError, TTLCache
from cloudpulse.storage import SQLiteStore
from helpers import rec
from test_agent import Clock, FakeS3, NoSampler

NOW = 2_000_000_000.0


def make_service(**settings_kw):
    store = SQLiteStore(":memory:")
    settings = Settings(api_keys={"k": "admin"}, cache_ttl_s=0, retention_days=0, **settings_kw)
    mgr = AlertManager(store, notifiers=[NullNotifier()], clock=lambda: NOW)
    return CloudPulseService(store, settings, mgr, clock=lambda: NOW), store


def tr(i, **kw):  # a transfer dict ending shortly before NOW
    kw.setdefault("start", NOW - 100)
    return rec(i, **kw).to_dict()


class AuthTests(unittest.TestCase):
    def test_plain_hashed_and_disabled(self):
        a = Authenticator({"plain": "viewer", hash_key("hashed"): "admin"})
        self.assertEqual(a.authorize("plain", "read"), "viewer")
        self.assertEqual(a.authorize("hashed", "admin"), "admin")
        self.assertEqual(Authenticator({}, disabled=True).authorize(None, "admin"), "admin")

    def test_errors(self):
        a = Authenticator({"v": "viewer", "g": "agent"})
        for key, perm, status in ((None, "read", 401), ("bad", "read", 401), ("v", "ingest", 403),
                                  ("g", "read", 403), ("v", "admin", 403)):
            with self.assertRaises(AuthError) as cm:
                a.authorize(key, perm)
            self.assertEqual(cm.exception.status, status, (key, perm))

    def test_hashed_key_config_validation(self):
        self.assertEqual(parse_api_keys(f"{hash_key('x')}:viewer"), {hash_key("x"): "viewer"})
        with self.assertRaises(ValueError):
            parse_api_keys("sha256$tooshort:viewer")


class ServiceTests(unittest.TestCase):
    def test_ingest_duplicates_and_partial_rejection(self):
        svc, _ = make_service()
        self.assertEqual(svc.ingest([tr(1)])["inserted"], 1)
        self.assertEqual(svc.ingest([tr(1)])["duplicates"], 1)
        out = svc.ingest([tr(2), tr(3, start=NOW + 86400)])  # one valid, one "from the future"
        self.assertEqual((out["inserted"], len(out["rejected"])), (1, 1))
        self.assertIn("future", out["rejected"][0]["reason"])

    def test_bad_values_rejected(self):
        svc, store = make_service()
        nan = tr(1)
        nan["started_at"] = float("nan")
        huge = tr(2)
        huge["file_size_bytes"] = 10 ** 18
        slow = tr(3, duration=30 * 86400, start=NOW - 40 * 86400)
        for bad in (nan, huge, slow):
            with self.assertRaises(ServiceError) as cm:
                svc.ingest([bad])
            self.assertEqual(cm.exception.status, 422)
        self.assertEqual(store.count_transfers(), 0)

    def test_range_and_bucket_limits(self):
        svc, _ = make_service()
        for bad in ("400d", "abc", "0h"):
            with self.assertRaises(ServiceError):
                svc.summary(bad, None)
        with self.assertRaises(ServiceError):
            svc.timeseries("30d", None, 10)  # 259,200 buckets would be a DoS vector

    def test_degradation_is_detected_even_for_a_short_range(self):
        """V1 only looked at records inside the requested range, so a 1 h view never had a baseline."""
        svc, _ = make_service()
        base = [tr(i, start=NOW - 5 * 3600 + i * 300, duration=1.0 + (i % 3) * 0.01) for i in range(30)]
        recent = [tr(100 + i, start=NOW - 2400 + i * 60, duration=3.0) for i in range(6)]
        svc.ingest(base + recent)
        deg = svc.reliability("1h", None)["degradation"]
        self.assertTrue(any(d["metric"] == "throughput_mbps" for d in deg), deg)

    def test_cache_is_bounded(self):
        c = TTLCache(ttl_s=100, maxsize=3)
        for i in range(10):
            c.get_or_compute(i, lambda: i)
        self.assertLessEqual(len(c), 3)

    def test_health_checks_the_database(self):
        svc, store = make_service()
        self.assertEqual(svc.health()["status"], "ok")
        store.ping = lambda: (_ for _ in ()).throw(RuntimeError("disk gone"))
        with self.assertRaises(ServiceError) as cm:
            svc.health()
        self.assertEqual(cm.exception.status, 503)

    def test_retention_purge_runs_once_an_hour(self):
        svc, store = make_service()
        svc.settings = Settings(api_keys={"k": "admin"}, retention_days=30)
        store.insert_transfers([rec(1, start=NOW - 40 * 86400), rec(2, start=NOW - 86400)])
        self.assertEqual(svc.maybe_purge()["transfers"], 1)
        self.assertEqual(store.count_transfers(), 1)
        self.assertIsNone(svc.maybe_purge())

    def test_ack_unknown_alert(self):
        svc, _ = make_service()
        with self.assertRaises(ServiceError) as cm:
            svc.ack(999)
        self.assertEqual(cm.exception.status, 404)


class AlertFixTests(unittest.TestCase):
    def test_late_arrivals_are_stored_but_not_notified(self):
        sent, store = [], SQLiteStore(":memory:")

        class N:
            def notify(self, a): sent.append(a)

        mgr = AlertManager(store, notifiers=[N()], max_notify_age_s=3600, clock=lambda: 100_000)
        mgr.process([rec(1, status="failed", code="X", start=1000)])           # replayed after an outage
        mgr.process([rec(2, status="failed", code="X", start=99_990, instance="i-2")])  # fresh
        self.assertEqual(len(sent), 1)
        stored = store.list_alerts()
        self.assertEqual(len(stored), 2)
        self.assertTrue(any(a.message.startswith("[late arrival]") for a in stored))

    def test_cooldown_is_symmetric_for_out_of_order_arrival(self):
        store = SQLiteStore(":memory:")
        mgr = AlertManager(store, notifiers=[], max_notify_age_s=None)
        self.assertEqual(len(mgr.process([rec(1, status="failed", code="X", start=2000)])), 1)
        self.assertEqual(len(mgr.process([rec(2, status="failed", code="X", start=1980)])), 0)  # older, inside window

    def test_async_notifier_does_not_block_and_flushes(self):
        got = []

        class Slow:
            def notify(self, a):
                time.sleep(0.1)
                got.append(a)

        n = AsyncNotifier(Slow(), workers=1)
        t0 = time.perf_counter()
        for i in range(3):
            n.notify(i)
        self.assertLess(time.perf_counter() - t0, 0.05)
        n.close()
        self.assertEqual(got, [0, 1, 2])


class ScoreAndStorageTests(unittest.TestCase):
    def test_ten_percent_failures_cannot_score_a_b(self):
        recs = [rec(i, start=1e6 + i) for i in range(90)] + [rec(100 + i, status="failed", code="X") for i in range(10)]
        r = an.reliability_score(recs)
        self.assertIn(r["grade"], ("D", "F"), r)

    def test_one_percent_failures_is_a_b(self):
        recs = [rec(i, start=1e6 + i) for i in range(99)] + [rec(999, status="failed", code="X")]
        self.assertTrue(90 <= an.reliability_score(recs)["score"] < 95)

    def test_purge_and_delete_simulated(self):
        s = SQLiteStore(":memory:")
        s.insert_transfers([rec(1, start=100), rec(2, start=10_000)])
        self.assertEqual(s.purge_older_than(5000), {"transfers": 1, "alerts": 0})
        r = rec(3)
        r.transfer_id = "sim-abc"
        s.insert_transfers([r])
        self.assertEqual(s.delete_simulated()["transfers"], 1)
        self.assertEqual(s.count_transfers(), 1)

    def test_reseeding_replaces_demo_data_instead_of_going_stale(self):
        from cloudpulse.cli import main
        with tempfile.TemporaryDirectory() as d:
            os.environ["CLOUDPULSE_DB_PATH"] = os.path.join(d, "t.db")
            self.addCleanup(os.environ.pop, "CLOUDPULSE_DB_PATH", None)
            outs = []
            for _ in range(2):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    main(["seed", "--days", "2", "--per-day", "50"])
                outs.append(buf.getvalue())
            self.assertIn("Seeded 100 transfers", outs[0])
            self.assertIn("Seeded 100 transfers", outs[1])  # V1: "Seeded 0" (ids already existed -> stale window)


class SinkFixTests(unittest.TestCase):
    class Resp:
        def __init__(self, code): self.status_code = code

    class Session:
        def __init__(self, rule): self.rule, self.received = rule, []
        def post(self, url, json=None, headers=None, timeout=None):
            items = json["transfers"]
            code = self.rule(items)
            if code == 200:
                self.received += [i["transfer_id"] for i in items]
            return SinkFixTests.Resp(code)

    def sink(self, rule):
        d = tempfile.mkdtemp()
        sess = self.Session(rule)
        return ApiSink("http://api", "k", os.path.join(d, "spool.jsonl"), session=sess, batch_size=10), sess

    def test_poison_record_does_not_block_the_queue(self):
        state = {"down": True}

        def rule(items):
            if state["down"]:
                return 503
            return 422 if any(i["transfer_id"] == "bad" for i in items) else 200

        sink, sess = self.sink(rule)
        for i in ("t1", "bad", "t3"):
            r = rec(0)
            r.transfer_id = i
            sink.emit(r)  # API down: all spooled
        state["down"] = False
        last = rec(0)
        last.transfer_id = "t4"
        sink.emit(last)
        self.assertEqual(sess.received, ["t1", "t3", "t4"])      # good ones delivered
        self.assertFalse(os.path.exists(sink.spool_path))         # spool drained
        with open(sink.dead_letter_path) as fh:
            self.assertEqual([json.loads(l)["transfer_id"] for l in fh], ["bad"])

    def test_auth_failure_keeps_records_for_later(self):
        sink, sess = self.sink(lambda items: 401)
        sink.emit(rec(1))
        with open(sink.spool_path) as fh:
            self.assertEqual(len(fh.read().splitlines()), 1)
        self.assertFalse(os.path.exists(sink.dead_letter_path))

    def test_corrupt_spool_line_is_skipped(self):
        sink, sess = self.sink(lambda items: 200)
        with open(sink.spool_path, "w") as fh:
            fh.write('{"transfer_id": "half-writt\n')
        sink.emit(rec(1))
        self.assertEqual(sess.received, ["t1"])

    def test_background_sink_never_blocks_the_caller(self):
        got = []

        class Slow:
            def emit(self, r):
                time.sleep(0.1)
                got.append(r.transfer_id)

        bg = BackgroundSink(Slow())
        t0 = time.perf_counter()
        for i in range(4):
            bg.emit(rec(i))
        self.assertLess(time.perf_counter() - t0, 0.05)
        bg.close()
        self.assertEqual(got, ["t0", "t1", "t2", "t3"])  # ordered, fully flushed


class AgentFixTests(unittest.TestCase):
    def test_cpu_percent_between_snapshots(self):
        self.assertEqual(cpu_percent_between((100, 50), (200, 100)), 50.0)
        self.assertIsNone(cpu_percent_between((100, 50), (100, 50)))
        self.assertEqual(cpu_percent_between((0, 0), (10, 0)), 100.0)

    def test_real_sampler_smoke(self):
        s = ResourceSampler()
        s.start()
        sum(i * i for i in range(300_000))
        out = s.stop(0.1)
        if out["cpu_percent"] is not None:
            self.assertTrue(0 <= out["cpu_percent"] <= 100)
        self.assertEqual(set(out), {"cpu_percent", "memory_percent", "network_mbps"})

    def test_retry_counter_counts_attempts_beyond_the_first(self):
        class Events:
            def __init__(self): self.handlers = {}
            def register(self, name, fn): self.handlers[name] = fn

        class Client:
            class meta: events = Events()

        c = RetryCounter().attach(Client)
        h = Client.meta.events.handlers
        h["before-call"]()
        for _ in range(3):
            h["request-created"]()
        self.assertEqual(c.value(), 2)

    def test_hidden_botocore_retries_appear_in_the_record(self):
        class Events:
            def register(self, *a): pass

        class C:
            class meta: events = Events()

        counter = RetryCounter().attach(C)

        class HookedS3(FakeS3):
            def _go(self, cb):
                counter._on_call()
                for _ in range(3):  # botocore retried twice internally, then succeeded
                    counter._on_attempt()
                super()._go(cb)

        clock = Clock()
        m = TransferMonitor(HookedS3(), [], clock=clock, monotonic=clock, sampler_factory=NoSampler,
                            retry_counter=counter)
        self.assertEqual(m.download("b", "k", "/tmp/x").retries, 2)


if __name__ == "__main__":
    unittest.main()
