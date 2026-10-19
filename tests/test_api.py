import time
import unittest

try:
    from fastapi.testclient import TestClient
except ImportError:  # fastapi/httpx not installed: skipped, runs in CI via requirements-dev.txt
    TestClient = None

from cloudpulse.config import Settings
from cloudpulse.storage import SQLiteStore


def payload(i, **kw):
    now = time.time()
    d = dict(transfer_id=f"api-{i}", direction="upload", bucket="b", key="k", file_size_bytes=10 * 1024 * 1024,
             started_at=now - 5, completed_at=now - 4, status="success", latency_ms=40.0, retries=0)
    d.update(kw)
    return d


@unittest.skipIf(TestClient is None, "fastapi not installed")
class ApiTests(unittest.TestCase):
    def setUp(self):
        from cloudpulse.api import create_app
        s = Settings(api_keys={"adm": "admin", "view": "viewer", "agt": "agent"}, cache_ttl_s=0)
        self.c = TestClient(create_app(s, SQLiteStore(":memory:")))
        self.h = lambda k: {"X-API-Key": k}

    def test_health_is_public_and_leaks_nothing(self):
        body = self.c.get("/health").json()
        self.assertEqual(body["status"], "ok")
        self.assertNotIn("transfers_stored", body)

    def test_future_timestamps_are_rejected(self):
        far = time.time() + 86400
        r = self.c.post("/api/v1/transfers", json=payload(1, started_at=far, completed_at=far + 1), headers=self.h("agt"))
        self.assertEqual(r.status_code, 422)

    def test_hashed_key_works(self):
        from cloudpulse.api import create_app
        from cloudpulse.auth import hash_key
        c = TestClient(create_app(Settings(api_keys={hash_key("s3cret"): "viewer"}), SQLiteStore(":memory:")))
        self.assertEqual(c.get("/api/v1/whoami", headers={"X-API-Key": "s3cret"}).json()["role"], "viewer")
        self.assertEqual(c.get("/api/v1/whoami", headers={"X-API-Key": "nope"}).status_code, 401)

    def test_auth_and_rbac(self):
        self.assertEqual(self.c.get("/api/v1/metrics/summary").status_code, 401)
        self.assertEqual(self.c.get("/api/v1/metrics/summary", headers=self.h("nope")).status_code, 401)
        self.assertEqual(self.c.get("/api/v1/metrics/summary", headers=self.h("view")).status_code, 200)
        self.assertEqual(self.c.post("/api/v1/transfers", json=payload(1), headers=self.h("view")).status_code, 403)
        self.assertEqual(self.c.get("/api/v1/metrics/summary", headers=self.h("agt")).status_code, 403)
        self.assertEqual(self.c.post("/api/v1/transfers", json=payload(1), headers=self.h("agt")).status_code, 201)

    def test_ingest_is_idempotent_and_raises_alerts(self):
        r = self.c.post("/api/v1/transfers/batch", headers=self.h("agt"), json={"transfers": [
            payload(1), payload(2, status="failed", latency_ms=None, error_code="SlowDown")]})
        self.assertEqual(r.json(), {"received": 2, "inserted": 2, "duplicates": 0, "rejected": [], "alerts_raised": 1})
        again = self.c.post("/api/v1/transfers", headers=self.h("agt"), json=payload(1)).json()
        self.assertEqual(again["duplicates"], 1)
        alerts = self.c.get("/api/v1/alerts", headers=self.h("view")).json()["items"]
        self.assertEqual(alerts[0]["rule"], "transfer_failed")
        self.assertEqual(self.c.post(f"/api/v1/alerts/{alerts[0]['id']}/ack", headers=self.h("view")).status_code, 403)
        self.assertEqual(self.c.post(f"/api/v1/alerts/{alerts[0]['id']}/ack", headers=self.h("adm")).status_code, 200)

    def test_validation(self):
        bad = payload(1, completed_at=0)
        self.assertEqual(self.c.post("/api/v1/transfers", json=bad, headers=self.h("agt")).status_code, 422)
        self.assertEqual(self.c.get("/api/v1/metrics/summary?range=banana", headers=self.h("view")).status_code, 422)

    def test_analytics_endpoints(self):
        self.c.post("/api/v1/transfers/batch", headers=self.h("agt"),
                    json={"transfers": [payload(i) for i in range(12)]})
        v = self.h("view")
        s = self.c.get("/api/v1/metrics/summary?range=1h", headers=v).json()["summary"]
        self.assertEqual(s["total_transfers"], 12)
        self.assertEqual(self.c.get("/api/v1/metrics/timeseries?range=1h", headers=v).json()["points"][0]["count"], 12)
        self.assertEqual(self.c.get("/api/v1/transfers?limit=5", headers=v).json()["total"], 12)
        self.assertIsNotNone(self.c.get("/api/v1/reliability", headers=v).json()["reliability"]["score"])
        self.assertIn("Performance Summary", self.c.get("/api/v1/reports/daily?format=markdown", headers=v).text)
        self.assertEqual(self.c.get("/api/v1/whoami", headers=v).json()["role"], "viewer")

    def test_refuses_to_start_without_keys(self):
        from cloudpulse.api import create_app
        with self.assertRaises(RuntimeError):
            create_app(Settings(api_keys={}), SQLiteStore(":memory:"))


if __name__ == "__main__":
    unittest.main()
