"""SQLite persistence (zero-setup default). Swap for PostgreSQL/DynamoDB behind the same interface."""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import fields

from .models import Alert, TransferRecord

_COLS = [f.name for f in fields(TransferRecord)]

SCHEMA = """
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    direction TEXT NOT NULL,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    started_at REAL NOT NULL,
    completed_at REAL NOT NULL,
    status TEXT NOT NULL,
    latency_ms REAL,
    retries INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    instance_id TEXT NOT NULL DEFAULT 'local',
    cpu_percent REAL,
    memory_percent REAL,
    network_mbps REAL
);
CREATE INDEX IF NOT EXISTS idx_transfers_started ON transfers(started_at);
CREATE INDEX IF NOT EXISTS idx_transfers_dir_started ON transfers(direction, started_at);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at REAL NOT NULL,
    rule TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    transfer_id TEXT,
    dedupe_key TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_dedupe ON alerts(dedupe_key, created_at);
"""


class SQLiteStore:
    def __init__(self, path: str = "cloudpulse.db"):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> None:
        """Raises if the database is unusable (used by /health)."""
        with self._lock:
            self._conn.execute("SELECT 1").fetchone()

    # ---- retention -------------------------------------------------------
    def purge_older_than(self, cutoff: float) -> dict:
        """Delete transfers started, and alerts raised, before `cutoff` (epoch seconds)."""
        with self._lock, self._conn:
            t = self._conn.execute("DELETE FROM transfers WHERE started_at < ?", (cutoff,)).rowcount
            a = self._conn.execute("DELETE FROM alerts WHERE created_at < ?", (cutoff,)).rowcount
        return {"transfers": t, "alerts": a}

    def delete_simulated(self) -> dict:
        """Remove demo data (ids starting with 'sim-') so re-seeding always yields a fresh time window."""
        with self._lock, self._conn:
            a = self._conn.execute("DELETE FROM alerts WHERE transfer_id LIKE 'sim-%'").rowcount
            t = self._conn.execute("DELETE FROM transfers WHERE transfer_id LIKE 'sim-%'").rowcount
        return {"transfers": t, "alerts": a}

    # ---- transfers -------------------------------------------------------
    def insert_transfers(self, records: list[TransferRecord]) -> list[TransferRecord]:
        """Idempotent insert. Returns only the records that were new (duplicates are ignored)."""
        sql = f"INSERT OR IGNORE INTO transfers ({','.join(_COLS)}) VALUES ({','.join('?' * len(_COLS))})"
        inserted: list[TransferRecord] = []
        with self._lock, self._conn:
            for r in records:
                cur = self._conn.execute(sql, [getattr(r, c) for c in _COLS])
                if cur.rowcount == 1:
                    inserted.append(r)
        return inserted

    def query_transfers(
        self,
        since: float | None = None,
        until: float | None = None,
        direction: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        descending: bool = False,
    ) -> list[TransferRecord]:
        where, args = self._where(since, until, direction, status)
        sql = f"SELECT * FROM transfers {where} ORDER BY started_at {'DESC' if descending else 'ASC'}"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            args += [int(limit), int(offset)]
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [TransferRecord(**{c: row[c] for c in _COLS}) for row in rows]

    def count_transfers(self, since=None, until=None, direction=None, status=None) -> int:
        where, args = self._where(since, until, direction, status)
        with self._lock:
            return self._conn.execute(f"SELECT COUNT(*) FROM transfers {where}", args).fetchone()[0]

    @staticmethod
    def _where(since, until, direction, status) -> tuple[str, list]:
        clauses, args = [], []
        if since is not None:
            clauses.append("started_at >= ?")
            args.append(since)
        if until is not None:
            clauses.append("started_at <= ?")
            args.append(until)
        if direction:
            clauses.append("direction = ?")
            args.append(direction)
        if status:
            clauses.append("status = ?")
            args.append(status)
        return ("WHERE " + " AND ".join(clauses)) if clauses else "", args

    # ---- alerts ----------------------------------------------------------
    def add_alert(self, a: Alert) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO alerts (created_at, rule, severity, message, transfer_id, dedupe_key, acknowledged)"
                " VALUES (?,?,?,?,?,?,?)",
                (a.created_at, a.rule, a.severity, a.message, a.transfer_id, a.dedupe_key, int(a.acknowledged)),
            )
            a.id = cur.lastrowid
            return a.id

    def list_alerts(self, since: float | None = None, open_only: bool = False, limit: int = 200) -> list[Alert]:
        clauses, args = [], []
        if since is not None:
            clauses.append("created_at >= ?")
            args.append(since)
        if open_only:
            clauses.append("acknowledged = 0")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM alerts {where} ORDER BY created_at DESC, id DESC LIMIT ?", args + [limit]
            ).fetchall()
        return [
            Alert(
                id=r["id"], created_at=r["created_at"], rule=r["rule"], severity=r["severity"],
                message=r["message"], transfer_id=r["transfer_id"], dedupe_key=r["dedupe_key"],
                acknowledged=bool(r["acknowledged"]),
            )
            for r in rows
        ]

    def ack_alert(self, alert_id: int) -> bool:
        with self._lock, self._conn:
            return self._conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,)).rowcount == 1

    def last_alert_at(self, dedupe_key: str) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(created_at) FROM alerts WHERE dedupe_key=?", (dedupe_key,)
            ).fetchone()
        return row[0]
