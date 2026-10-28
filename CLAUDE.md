# Async task console

A durable async task queue with a web console: submit / view / cancel tasks,
multiple worker pools, crash recovery, metrics, and one-click demos of every
key feature.

## Goal
> "Showcase technical excellence on the backend or API design. We will review
> the code together afterward and we want to see your technical craft at its
> best in whichever part you choose to prioritize."

Prioritize depth over breadth: correctness under concurrency and crashes, a
clean and consistent API, and tests that prove the guarantees. The code will be
reviewed, so every design choice should be deliberate and explainable.

## Stack
- Python 3.13, FastAPI + uvicorn, SQLite (WAL) as the single source of truth.
- Workers are separate OS processes (so a worker can really be `kill -9`'d).
- Web console: one vanilla HTML/JS page (`taskconsole/static/index.html`) polling the API.
- Tests: pytest (`python -m pytest -q`). Run the app: `python -m taskconsole`.

## State machine
```
CREATED ─► PENDING ─► CLAIMED ─► RUNNING ─► SUCCEEDED
              ▲          │          ├─────► FAILED      (non-retryable error)
              │          │          ├─────► DEAD        (attempts exhausted / poison pill)
          RETRY_WAIT ◄───┴──────────┘                   (retryable error / lease expired)
   any non-terminal ─► CANCELLED
```
- Every status change goes through the transition table in `taskconsole/states.py`.
- Every transition is appended to `task_events` (audit log + attempt history).

## Plan

### Phase 1: basic flow (one worker)
- [x] Schema (`tasks`, `task_events`), transition table, store API
- [x] Submit / list / get / cancel REST API
- [x] Single worker loop; demo task types (sleep, count_primes, flaky, fail_permanent)
- [x] Retry: retryable vs non-retryable errors, RETRY_WAIT + `run_after`, max_attempts -> DEAD
- [x] Cancel: immediate for queued tasks, `cancel_requested` flag for running ones
- [x] Cooperative execution timeout
- [x] Basic console page; unit tests for the state machine

### Phase 2: scaling and error handling
1. [x] Worker pools + supervisor (restarts dead worker processes); atomic claiming
   (single `UPDATE … WHERE id=(SELECT …) RETURNING`)
2. [x] Retry with exponential backoff + full jitter (capped)
3. [x] Leasing: lease renewed by heartbeat; expired leases recovered in the claim path
   (no separate reaper); crashes count as attempts (poison pill -> DEAD)
4. [x] Fencing tokens: `fence` incremented on every claim; every worker write is
   `WHERE id=? AND fence=?`; stale (zombie) writes are rejected and counted;
   a worker that loses its lease stops
5. [x] Idempotency: submission key (unique; same key returns the existing task,
   different body -> 409) + side-effect ledger keyed `task_id:step`, also passed
   to the (fake) payment/email provider as its idempotency key
6. [x] Heartbeats: `workers` table, alive/stale/dead; hard timeout enforced by the
   worker exiting (the supervisor restarts it); graceful drain on SIGTERM
   (finish the task or release the lease)

### Phase 3: metrics, console, demos
- [x] Metrics: counts by status, queue depth per pool, counters (retries, lease
  recoveries, fenced rejections, dedup hits, worker restarts), wait/run p50/p95,
  throughput time series; `/metrics` in Prometheus text format
- [x] Console: metric tiles + throughput chart, pools/workers panel (scale +/-,
  Kill, Pause/Resume, Drain), submit form, task table with live lease/retry
  countdowns, detail drawer with event timeline, side-effects ledger
- [x] One-click demo scenarios: happy path, retry+backoff, crash -> lease recovery,
  poison pill -> DEAD, hang -> timeout, zombie worker -> fencing, idempotent
  submit, charge+crash -> no double charge, cancel running, multi-pool burst
- [x] Fault-injection tests (fake clock), README with demo walkthrough

### Out of scope
Authentication, distributed brokers (Redis/RabbitMQ), task DAGs, cron scheduling,
WebSocket push, multiple machines.

## Conventions
- DB access and retries follow `GUIDELINES.md` (rule IDs C*, W*, R*); `tests/test_guidelines.py`
  enforces the checkable ones. Read it before touching `store.py`, `db.py` or any retry path.
- Signal handlers only set flags; never touch the DB from one (it may interrupt a transaction).
- Worker heartbeats must keep running during any grace period (timeout, drain), or the lease expires.
- One clock: the store's `clock` callable (injectable in tests); all processes run on one host.
- Worker-side writes must always carry the fencing token (from Phase 2 on).
- Commit at the end of each phase; update the checkboxes above.
