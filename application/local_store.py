import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger("local_store")


def _connect(database_path: str) -> sqlite3.Connection:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def init_local_store(database_path: str) -> None:
    with _connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS offline_events (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'sent', 'failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                next_attempt_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_offline_events_ready
                ON offline_events(status, next_attempt_at, created_at);
            """
        )


def enqueue_event(database_path: str, event: dict[str, Any]) -> str:
    event_id = uuid.uuid4().hex
    now = time.time()
    payload = {**event, "event_id": event_id}
    with _connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO offline_events
                (id, payload, status, created_at, updated_at, next_attempt_at)
            VALUES (?, ?, 'pending', ?, ?, ?)
            """,
            (event_id, json.dumps(payload), now, now, now),
        )
    return event_id


def claim_batch(database_path: str, batch_size: int, processing_timeout: float) -> list[dict[str, Any]]:
    now = time.time()
    stale_before = now - processing_timeout
    with _connect(database_path) as connection:
        connection.execute(
            """
            UPDATE offline_events
            SET status = 'pending', updated_at = ?
            WHERE status = 'processing' AND updated_at < ?
            """,
            (now, stale_before),
        )
        rows = connection.execute(
            """
            SELECT id, payload
            FROM offline_events
            WHERE status IN ('pending', 'failed') AND next_attempt_at <= ?
            ORDER BY created_at
            LIMIT ?
            """,
            (now, batch_size),
        ).fetchall()
        if not rows:
            return []

        ids = [row["id"] for row in rows]
        connection.executemany(
            """
            UPDATE offline_events
            SET status = 'processing', attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND status IN ('pending', 'failed')
            """,
            [(now, event_id) for event_id in ids],
        )

    return [json.loads(row["payload"]) for row in rows]


def mark_sent(database_path: str, event_ids: list[str]) -> None:
    if not event_ids:
        return
    now = time.time()
    with _connect(database_path) as connection:
        connection.executemany(
            """
            UPDATE offline_events
            SET status = 'sent', updated_at = ?
            WHERE id = ? AND status = 'processing'
            """,
            [(now, event_id) for event_id in event_ids],
        )


def mark_failed(database_path: str, event_ids: list[str], retry_delay: float) -> None:
    if not event_ids:
        return
    now = time.time()
    with _connect(database_path) as connection:
        connection.executemany(
            """
            UPDATE offline_events
            SET status = 'failed', updated_at = ?, next_attempt_at = ?
            WHERE id = ? AND status = 'processing'
            """,
            [(now, now + retry_delay, event_id) for event_id in event_ids],
        )


def get_queue_status(database_path: str, recent_limit: int = 20) -> dict[str, Any]:
    init_local_store(database_path)
    with _connect(database_path) as connection:
        counts = {
            row["status"]: row["total"]
            for row in connection.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM offline_events
                GROUP BY status
                """
            ).fetchall()
        }
        recent = [
            {
                "event_id": row["id"],
                "status": row["status"],
                "attempts": row["attempts"],
                "created_at": row["created_at"],
                "student_id": json.loads(row["payload"]).get("student_id"),
                "bus_id": json.loads(row["payload"]).get("bus_id"),
            }
            for row in connection.execute(
                """
                SELECT id, payload, status, attempts, created_at
                FROM offline_events
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()
        ]

    return {
        "counts": {status: counts.get(status, 0) for status in ("pending", "processing", "sent", "failed")},
        "recent": recent,
    }
