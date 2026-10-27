"""SQLite connection + schema. SQLite (WAL mode) is the single source of truth."""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    type             TEXT NOT NULL,
    payload          TEXT NOT NULL DEFAULT '{}',
    pool             TEXT NOT NULL DEFAULT 'default',
    priority         INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    timeout_s        REAL NOT NULL DEFAULT 60,
    run_after        REAL NOT NULL DEFAULT 0,
    worker_id        TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    progress         REAL NOT NULL DEFAULT 0,
    checkpoint       TEXT,
    result           TEXT,
    error            TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    started_at       REAL,
    finished_at      REAL
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(pool, status, run_after);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    ts          REAL NOT NULL,
    from_status TEXT,
    to_status   TEXT,
    attempt     INTEGER,
    worker_id   TEXT,
    message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, id);
"""


def connect(path: str) -> sqlite3.Connection:
    # isolation_level=None: we issue BEGIN IMMEDIATE ourselves so every write
    # transaction takes the write lock up front (no lock-upgrade deadlocks).
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init(path: str) -> None:
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
