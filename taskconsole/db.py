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
    finished_at      REAL,
    -- Phase 2: leasing, fencing, idempotent submission
    fence            INTEGER NOT NULL DEFAULT 0,
    lease_expires_at REAL,
    idempotency_key  TEXT UNIQUE,
    request_hash     TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(pool, status, run_after);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_finished ON tasks(finished_at);
-- Lease recovery runs on every claim. A lease is set only while CLAIMED/RUNNING, so this
-- partial index holds just the in-flight tasks and the scan stays O(in-flight), not O(history).
CREATE INDEX IF NOT EXISTS idx_tasks_lease ON tasks(lease_expires_at) WHERE lease_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    ts          REAL NOT NULL,
    from_status TEXT,
    to_status   TEXT,
    attempt     INTEGER,
    worker_id   TEXT,
    fence       INTEGER,
    kind        TEXT NOT NULL DEFAULT 'transition',  -- transition | info | rejected
    message     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, id);
CREATE INDEX IF NOT EXISTS idx_events_kind ON task_events(kind, id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON task_events(ts);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    pool            TEXT NOT NULL,
    pid             INTEGER,
    status          TEXT NOT NULL,          -- starting | idle | busy | draining | stopped | dead
    started_at      REAL NOT NULL,
    last_heartbeat  REAL,
    current_task_id TEXT,
    tasks_succeeded INTEGER NOT NULL DEFAULT 0,
    tasks_failed    INTEGER NOT NULL DEFAULT 0,
    exit_code       INTEGER,
    stopped_at      REAL
);

-- Our side-effect ledger: step `task_id:step` done => never redo it.
CREATE TABLE IF NOT EXISTS effects (
    key        TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    fence      INTEGER NOT NULL,
    result     TEXT,
    created_at REAL NOT NULL
);

-- Fake external provider (payments / email) that honours an Idempotency-Key, like Stripe.
CREATE TABLE IF NOT EXISTS external_effects (
    idempotency_key TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    detail          TEXT NOT NULL,
    created_at      REAL NOT NULL,
    duplicate_calls INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
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
