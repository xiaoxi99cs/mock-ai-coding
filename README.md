# Async task console

A durable async task queue with a web console. You can submit, watch and cancel tasks
running on several pools of worker processes, kill or freeze those workers, and see the
queue recover. Every key failure mode (crashes, hangs, zombie workers, duplicate
submissions, side effects run twice) has a one-click demo.

SQLite is the only infrastructure. Workers are real OS processes, so `kill -9` really
kills one, and the recovery you see in the console is the same code path a production
crash would take.

## Quick start

```bash
pip install -r requirements.txt        # Python 3.13
python -m taskconsole                  # --db tasks.db --host 127.0.0.1 --port 8000
```

Open <http://127.0.0.1:8000>. By default the supervisor starts 3 workers in the `default`
pool and 2 in the `cpu` pool, with a 5 s lease and a 1 s heartbeat. Click a demo
button, or submit a task from the form.

## Architecture

```
            browser (static/index.html, polls ~1s)
                         │ HTTP
 ┌───────────────────────▼────────────────────────┐
 │ API process: FastAPI + uvicorn                  │
 │   REST API · metrics · demo runner · Supervisor │──spawn / SIGKILL / SIGSTOP / SIGCONT / SIGTERM──┐
 └───────────────────────┬────────────────────────┘                                                  │
                         │                              ┌──────────────┐ ┌──────────────┐ ┌──────────▼───┐
                         ▼                              │ worker proc  │ │ worker proc  │ │ worker proc  │
              ┌─────────────────────┐ ◄─────────────────│ default-1    │ │ default-2    │ │ cpu-3  ...   │
              │ SQLite (WAL) tasks.db│                   └──────────────┘ └──────────────┘ └──────────────┘
              │ single source of truth│
              └─────────────────────┘
```

- **One store, one clock.** Every read and write goes through `TaskStore`
  (`taskconsole/store.py`). Each write transaction starts with `BEGIN IMMEDIATE`, and
  timestamps come from the store's injectable `clock`. All processes run on one host.
- **Workers** (`taskconsole/worker.py`) each run the handler in a thread. The main
  thread is a heartbeat loop that renews the lease, passes cancel requests on to the
  handler, enforces the hard timeout and handles draining.
- **Supervisor** (`taskconsole/supervisor.py`) keeps each pool at its desired size.
  It replaces dead workers, drains surplus ones and exposes the kill, pause, resume and
  drain controls.

### State machine

```
CREATED ─► PENDING ─► CLAIMED ─► RUNNING ─► SUCCEEDED
              ▲          │          ├─────► FAILED      (non-retryable error)
              │          │          ├─────► DEAD        (attempts exhausted / poison pill)
          RETRY_WAIT ◄───┴──────────┘                   (retryable error / lease expired)
   any non-terminal ─► CANCELLED
```

Every status change goes through the transition table in `taskconsole/states.py` and is
appended to `task_events`. That table is the audit log, the attempt history and the
timeline you see in the console. CLAIMED/RUNNING → PENDING is a graceful release by a
draining worker, and it does not count as an attempt.

### Reliability in brief

- **Atomic claiming.** A claim is one statement:
  `UPDATE tasks SET … WHERE id = (SELECT the best ready task … LIMIT 1) RETURNING *`.
  Picking the task and taking it cannot be separated, so two workers can never claim the
  same task. The claim increments `attempts` and the fencing token `fence`, and sets
  `lease_expires_at`. A partial index over the claimable tasks (`idx_tasks_ready`) keeps
  them in claim order (priority, then `run_after`, then age), so a claim reads one row
  instead of sorting the backlog.
- **Retries.** Retryable errors go to RETRY_WAIT with `run_after = now + backoff`.
  Backoff is exponential with full jitter: `uniform(0, min(cap, base·2^(attempt-1)))`,
  with a base of 1 s and a cap of 30 s. Non-retryable errors go to FAILED. When attempts
  run out, the task goes to DEAD.
- **Leases.** A running task's lease is renewed by its worker's heartbeat. Expired leases
  are recovered inside the claim path and on every supervisor tick. A crash counts as an
  attempt, so a poison pill ends up DEAD instead of killing workers forever.
- **Fencing.** Every worker write (start, heartbeat, progress, complete, fail, cancel,
  release, effect) is applied only if the task is still CLAIMED or RUNNING with the
  writer's `fence`. A stale write from a zombie is rejected, logged as a `rejected` event
  and counted in `fenced_writes_rejected`. A worker that loses its lease stops.
- **Idempotency.**
  - Submissions accept an `idempotency_key`. Sending the same key with the same body
    returns the existing task. Sending it with a different body returns 409.
  - Side effects such as charges and emails are recorded in a ledger keyed
    `task_id:step`. A completed step is skipped on retry.
  - The same key is also passed to the fake payment/email provider as its Idempotency-Key.
    This covers a crash between the provider call and the ledger write.
- **Timeouts and draining.**
  - Handlers check `timeout_s` themselves (cooperative timeout).
  - If a handler ignores it, the worker records the failure and exits, and the supervisor
    restarts it (hard timeout).
  - On SIGTERM a worker stops claiming. It gives its current task a grace period to finish,
    then hands the task back.
  - Heartbeats keep running through every grace period.

### Task types

| type | what it does |
|---|---|
| `sleep` | sleeps `seconds`, reporting progress 10 times |
| `count_primes` | counts primes below `n` in `chunks` and saves a checkpoint after each one, so a retry resumes where the last attempt stopped (pool `cpu`) |
| `flaky` | fails with a transient error on its first `fail_times` attempts |
| `fail_permanent` | always fails with a non-retryable error |
| `crash` | kills its worker (kill -9 style) at `crash_at` on the attempts listed in `crash_on_attempts` |
| `poison` | kills its worker on every attempt |
| `hang` | ignores its context and never returns (exercises the hard timeout) |
| `charge_and_email` | charges a card, then emails a receipt, both as idempotent side effects. `crash`: `after_charge` or `before_record` |

Each type declares a pydantic payload model (`GET /api/task-types` publishes its JSON
Schema). Unknown fields are rejected, so a typo fails instead of silently using a default.
The API checks the payload at submit time and returns **422** without enqueuing anything.
The worker checks it again before running, and a payload that fails there (for example,
one that bypassed the API) is a non-retryable FAILED on attempt 1. Retrying bad input
only burns attempts.

## Design decisions and trade-offs

Each choice below names what it buys, what it costs, and what I would change at a larger
scale.

**SQLite is both the queue and the source of truth.** The task row, its audit event and
the counters commit in one transaction. There is no broker to keep in sync with a
database, so there is no dual-write problem, and WAL gives crash consistency for free.
*Cost:* one writer at a time. Every claim, heartbeat, progress update and completion
queues for the same write lock, so throughput is bounded by the commit rate, and
everything runs on one host. *At scale:* Postgres, claiming with
`SELECT … FOR UPDATE SKIP LOCKED` and waking workers with `LISTEN/NOTIFY`. The
`TaskStore` interface would not change.

**At-least-once execution, effectively-once side effects.** A worker that stalls past its
lease (SIGSTOP, a long GC pause, a slow lock wait) loses its task while still running it,
so a handler can run twice. Preventing that would need coordination with whatever the
handler talks to, so the system makes the second run harmless instead:
- fencing turns the stale run's database writes into rejected no-ops;
- side effects go through the `task_id:step` ledger and the provider's idempotency key.

*Cost:* a handler must route external effects through `ctx.effect()`. An effect it
performs directly is not protected.

**The lease length trades recovery time against false alarms.** With a 5 s lease and a
1 s heartbeat, a crashed worker's task is recovered within about 5 s (plus backoff).
*Cost:* a healthy worker stalled for more than 5 s loses its task and the work is redone.
The connection's 30 s `busy_timeout` is longer than the lease, so under heavy write
contention a heartbeat stuck waiting for the lock can cause this. The supervisor learns
of a process death immediately but still waits for the lease to expire, which keeps a
single, well-tested recovery path. *Next step:* recover a known-dead worker's tasks at
once (still fenced), and keep the heartbeat's lock wait well under the lease.

**Recovery runs in the claim path, not in a separate reaper.** An expired task is
recovered in the same transaction that could claim it, so there is no window where it is
neither recovered nor claimable, and no extra process can die. The supervisor tick also
runs recovery so the console shows it while the queue is idle. *Cost:* every claim scans
the in-flight tasks while holding the write lock. A partial index on `lease_expires_at`
keeps that scan O(in-flight), not O(history), and a test checks the query plan.

**Crashes count as attempts.** A poison pill that kills every worker ends up DEAD instead
of looping forever. *Cost:* a task whose worker an operator kills also loses an attempt.
A graceful drain hands the task back without consuming one, so only ungraceful deaths
count.

**A worker is a process; its handler runs in a thread.** A separate process can really be
`kill -9`'d, and exiting the process is the only reliable way to stop stuck Python code
(threads cannot be killed). The handler runs in a thread so that the main thread keeps
heartbeating even while the handler is busy; the GIL is released between bytecodes often
enough for that. *Cost:* one task at a time per process, so concurrency equals the number
of processes, each with its own interpreter's memory.

**Cancellation and timeouts are cooperative, with a hard backstop.** A handler notices
them at `ctx.sleep()` / `ctx.progress()`. If it ignores them, the worker records the
failure at `timeout_s + 1 s` and exits, and the supervisor replaces it. *Cost:* a cancel
takes effect after up to one heartbeat plus the time to the handler's next check.

**Two clocks, on purpose.** Everything persisted or compared across processes (leases,
`run_after`, timestamps) uses the store's clock: wall time, injectable, so the fault tests
drive it with a fake clock. *Cost:* if NTP steps the system clock, every lease shifts with
it. A handler's timeout is measured inside one process with `time.monotonic()`, which is
immune to clock steps but is not driven by the fake clock; those tests use short real
durations instead.

**One API process.** The supervisor lives in the FastAPI lifespan, so running
`uvicorn --workers N` would start N supervisors and N times the workers. Worker ids come
from a persisted counter, so a restarted app never reuses an id, and at startup any
worker rows still marked alive from the previous run are marked dead (their processes
exit on their own once their parent is gone).

**Known limits, deliberately out of scope:**
- **Strict priority with no aging:** a steady stream of high-priority tasks starves
  low-priority ones.
- **Polling:** idle workers poll every 0.5 s, so pickup latency is up to 0.5 s.
- **No retention:** `tasks`, `task_events` and `effects` grow forever. The fix is a TTL
  purge of terminal tasks that keeps idempotency keys for their window.
- **Idempotency keys:** they are a body field rather than the conventional
  `Idempotency-Key` header, and they never expire.
- **No re-drive:** there is no endpoint to re-queue a DEAD or FAILED task.

## API reference

All timestamps are store-clock epoch seconds.

Every error has one shape, so clients branch on `code`, never on the message text:

```json
{"error": {"code": "task_not_found", "message": "task abc not found"}}
```

Codes: `validation_error` (422, with field-level `details`), `unknown_task_type` and
`unknown_pool` (422, with the valid values in `details`), `invalid_payload` (422, with
field-level `details`), `invalid_cursor` (400),
`task_not_found`, `worker_not_found`, `pool_not_found`, `demo_not_found`, `not_found` (404),
`method_not_allowed` (405), `idempotency_conflict`, `task_already_finished` (409).

### Tasks

| method & path | description |
|---|---|
| `GET /api/task-types` | `{type: {description, example, pool, payload_schema}}` (`payload_schema` is JSON Schema) |
| `POST /api/tasks` | body `{type, payload?, pool?, priority?, max_attempts? (1-20), timeout_s?, idempotency_key?}`. Returns **201** with the task, **200** with `deduplicated: true` for a repeated key, **409** `idempotency_conflict` for a key re-used with a different body, **422** for an unknown type or pool, or a payload that fails the type's schema |
| `GET /api/tasks?status=&pool=&limit=&cursor=` | newest first; returns `{items, next_cursor}`. Pass `next_cursor` back as `cursor` for the next page (null on the last page). Keyset pagination on `(created_at, id)`, so tasks inserted while paging never shift a page. An unknown `status` or `pool` is a **422**, not an empty list; `limit` is 1-500 |
| `GET /api/tasks/{id}` | the task plus `events` (its full timeline; `kind` is `transition`, `info` or `rejected`, and each event carries `attempt`, `worker_id` and `fence`) |
| `POST /api/tasks/{id}/cancel` | Idempotent. Queued: CANCELLED at once, **200**. Running: sets `cancel_requested`, **202**; the worker stops it at its next heartbeat. Repeating either is a no-op with the same status. **409** `task_already_finished` if it already SUCCEEDED, FAILED or went DEAD |

A task row has: `id, type, pool, status, attempts, max_attempts, progress, error, result,
lease_expires_at, run_after, fence, worker_id, cancel_requested, created_at, started_at,
finished_at, timeout_s, idempotency_key` (plus `payload`, `checkpoint`, `priority`).

### Workers and pools

| method & path | description |
|---|---|
| `GET /api/workers` | each worker has `state` (`idle`, `busy`, `draining`, `paused`, `stale`, `stopped` or `dead`), `heartbeat_age`, `paused`, `draining`, `current_task_id`, `pid`, `tasks_succeeded` and `tasks_failed`. A worker is `stale` when its heartbeat is older than 3 × heartbeat |
| `POST /api/workers/{id}/{kill,pause,resume,drain}` | **202**: the signal is sent and the effect shows up in `GET /api/workers` once the process reacts. Any other action is a **422** |
| `POST /api/workers/{id}/kill` | SIGKILL. The supervisor records a crash and starts a replacement |
| `POST /api/workers/{id}/pause` / `resume` | SIGSTOP / SIGCONT, which creates a zombie |
| `POST /api/workers/{id}/drain` | SIGTERM: a graceful exit |
| `GET /api/pools` | `{pool: desired size}` |
| `PUT /api/pools/{pool}` | body `{"size": n}` (0-16). The supervisor scales the pool up or down; workers removed when scaling down are drained |
| `GET /api/effects` | the fake provider's ledger: `idempotency_key, kind, detail, created_at, duplicate_calls` |

### Metrics

`GET /api/metrics` returns:

```jsonc
{
  "now": 1767225600.0,                       // server clock, for countdowns
  "status_counts": {"PENDING": 3, "RUNNING": 2, ...},   // every status
  "queue_depth": {"default": {"ready": 2, "delayed": 1, "running": 2}, "cpu": {...}},
  "counters": {"retries": 4, "lease_expirations": 1, "fenced_writes_rejected": 2,
               "idempotent_submits_deduplicated": 1, "worker_restarts": 1,
               "provider_duplicates_blocked": 1, "effects_skipped_already_done": 1, ...},
  "latency": {"wait": {"p50": 0.4, "p95": 1.9, "n": 30},   // first claim - created_at
              "run":  {"p50": 2.1, "p95": 4.0, "n": 25}},  // SUCCEEDED: finished - started
  "throughput": {"bucket_s": 5, "series": [{"t": 1767225300, "succeeded": 3, "failed": 0,
                                            "dead": 0, "cancelled": 0}, ...]},  // last 60 buckets
  "workers": {"alive": 5, "stale": 0, "dead": 1}
}
```

- `ready`: PENDING or RETRY_WAIT tasks with `run_after <= now`.
- `delayed`: PENDING or RETRY_WAIT tasks still waiting for `run_after`.
- `running`: CLAIMED or RUNNING tasks.
- Latency covers tasks that finished in the last 300 s.

`GET /metrics` serves the same data in Prometheus text format (`text/plain; version=0.0.4`).
The metrics are `taskconsole_tasks{status}`, `taskconsole_queue_depth{pool,state}`,
`taskconsole_<counter>_total`, `taskconsole_task_wait_seconds{quantile="0.5"|"0.95"}`,
`taskconsole_task_run_seconds{quantile}` and `taskconsole_workers{state}`.

### Demos

| method & path | description |
|---|---|
| `GET /api/demos` | `[{name, title, description, watch}]` (`watch` says what to look at in the console) |
| `POST /api/demos/{name}` | runs the scenario in the background. Returns **202** `{run_id, name, status: "running", log: [], task_ids: []}`, or **404** for an unknown name |
| `GET /api/demos/runs` | the last 20 runs, newest first: `{run_id, name, status (running, done or error), started_at, finished_at, log: [{t, msg}], task_ids}` |

## Demo walkthrough

Each button in the demo panel runs one scenario against the live workers. The run log
explains each step as it happens. Click a task id in the log to open its timeline. Every
scenario has a bounded wait: if the expected outcome never arrives, the run ends with
status `error` and the log says why.

| scenario | what it does | what to watch |
|---|---|---|
| **happy_path** | Runs one `sleep` task and one `count_primes` task. Each succeeds on its first attempt. | Task table: PENDING → CLAIMED → RUNNING → SUCCEEDED with live progress. The drawer shows one attempt. The wait/run p50/p95 tiles and the throughput chart pick up both tasks. |
| **retry_backoff** | A `flaky` task fails twice with a transient error, then succeeds on attempt 3. A `fail_permanent` task goes straight to FAILED. | The flaky row alternates RUNNING → RETRY_WAIT with a "retry in Xs" countdown. The jitter window doubles each attempt. The **retries** tile goes up by 2. The other task is FAILED after 1 attempt. |
| **crash_recovery** | Starts a `sleep` task and kills the worker running it with `kill -9`. | Workers panel: the worker turns **dead** and a replacement appears (**worker_restarts** +1). Task: the lease countdown runs out, the timeline shows "recovered: lease expired" → RETRY_WAIT, then attempt 2 on another worker → SUCCEEDED. |
| **poison_pill** | A `poison` task kills its worker on every attempt (`max_attempts=3`). | Three workers die and are restarted. The timeline shows three lease-expiry recoveries, the last one "attempts exhausted (3/3)" → **DEAD**. |
| **hang_timeout** | A `hang` task runs with `timeout_s=2` and `max_attempts=2`. The handler never returns. | "hard timeout after 2s" → RETRY_WAIT, then DEAD after the second timeout. Two workers exit and are replaced (**worker_restarts** +2). |
| **zombie_fencing** | Freezes (SIGSTOP) the worker running a task. When its lease expires another worker reclaims the task with fence +1. Then the frozen worker is resumed. | The paused worker shows as **paused/stale**. The timeline shows a new claim with a higher fence, then red **REJECTED stale …** entries from the old worker. **fenced_writes_rejected** goes up. The task succeeds once, on the new worker. |
| **idempotent_submit** | Submits a task with a key, then the same request again (the existing task is returned), then the same key with a different body (409). | Only one task exists for the key. Its timeline has a "duplicate submit" info entry. The **dedup hits** tile goes up by 1. The run log shows the conflict. |
| **no_double_charge** | Runs two `charge_and_email` tasks whose worker dies on attempt 1. With `after_charge` it dies after the charge is recorded in our ledger. With `before_record` it dies after the provider charged but before our ledger saw it. | Side-effects ledger: exactly one charge and one email per task. The `before_record` charge shows **duplicate_calls = 1** (blocked by the provider's idempotency key). Timelines: "effect 'charge' already done … skipped". |
| **cancel_running** | Starts a 10 s `sleep` task and cancels it mid-run. | A "cancel requested" info entry appears while the task is RUNNING. RUNNING → CANCELLED follows within about one heartbeat, and the worker goes back to idle. |
| **multi_pool_burst** | Submits 20 `sleep` tasks to `default` and 8 `count_primes` tasks to `cpu`. | Queue-depth tiles for each pool fill up and drain independently. The throughput chart spikes and every worker is busy. Try pool +/- while the burst runs. The final log line shows tasks/s and tasks per worker. |

You can also inject faults by hand. Use **Kill**, **Pause/Resume** and **Drain** on any
busy worker, or submit `crash` / `hang` tasks from the form.

## Running tests

```bash
python -m pytest -q
```

- `tests/test_phase1.py`: the state machine, retries, cancel, timeouts and the REST API.
- `tests/test_api.py`: the API contract: the error envelope, strict parameters and payloads, cursor
  pagination (ties, inserts between pages, malformed cursors), cancel status codes, and
  that the claim and the lease-recovery scan use their partial indexes.
- `tests/test_phase2.py`:
  - atomic claiming under concurrency (threads and separate processes), jittered backoff, leases,
    fencing and idempotency
  - worker ids that stay unique across supervisor restarts
  - an integration test with real worker processes under the supervisor
- `tests/test_metrics.py`, `tests/test_demos.py`: the metrics endpoints and the demo
  scenarios (some run end to end with real workers).
- `tests/test_faults.py`: deterministic fault injection on a fake clock, with no
  processes. It covers:
  - a crash at every checkpoint of `count_primes`, then a resume
  - backoff growth and the cap across successive lease expiries
  - late completions racing lease recovery
  - cancel combined with lease expiry
  - draining mid-task
  - every kind of zombie write being rejected
  - provider dedup when the ledger write is lost
  - the worker's own timeout and lease-lost grace periods
  - an exhaustive check of the transition table
