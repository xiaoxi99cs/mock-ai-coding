# Engineering guidelines: API, DB access and retries

Rules every PR must follow. Each rule has an ID (cite it in reviews), a one-line
reason, and how it is checked. **Enforced** rules have a test in
`tests/test_guidelines.py` that fails CI when they are broken; **Review** rules are
checked by the reviewer using the PR template checklist.

## A: API

**A1. Validate every input at the edge, with bounds.** *Enforced.*
Every request-model `int`/`float` has a lower and an upper bound, every `str` has a
`max_length`, and every `dict` has a size validator. Query parameters are bounded too.
*Why:* bad input must get a 422. Unbounded input becomes a 500 (an int too big for
SQLite's 64-bit integer) or a slow query.

**A2. Every list endpoint is paginated: `limit` in `1..max`, `offset >= 0`.** *Enforced.*
Reject out-of-range values with a 422 instead of clamping them silently.
*Why:* one request must not read the whole table. SQLite reads `LIMIT -1` as "no
limit", so the lower bound matters as much as the upper.

**A3. Use the right status codes, and a domain error is never a 500.** *Enforced.*
201 created · 200 OK or dedup hit · 404 not found · 409 conflict or illegal state · 422 bad input.
*Why:* clients branch on the code, and a 500 should mean "our bug".

**A4. Every POST that creates something accepts an idempotency key.** *Enforced.*
Same key and same body returns the original (200); same key with a different body returns 409.
*Why:* a client that retries after a timeout must not create a duplicate.

**A5. Repeating a state-changing action is safe.** *Enforced.*
A repeat of a request that already took effect returns the current state (200) and
writes nothing. Example: cancelling an already-cancelled task. A request that is
impossible, such as cancelling a finished task, is still a 409.
*Why:* clients retry on network errors and can't tell whether the first call landed.

**A6. Handlers are thin: validate, call one store method, map exceptions to codes.** *Enforced (W1) + Review.*
No SQL and no business logic in `api.py`.
*Why:* one place for the logic, and it can be tested without HTTP.

**A7. Errors always have the same shape: `{"detail": ...}`, with no stack traces or SQL.** *Enforced.*
*Why:* clients parse one format, and internal details stay internal.

**A8. Change responses only by adding.** *Review.*
Never rename or remove a response field, or change its type, without a version bump.
*Why:* the console and other clients rely on the field names.

## C: Connections

**C1. Open connections only through `db.connect()`.** *Enforced.*
Never call `sqlite3.connect` anywhere else.
*Why:* one place sets the timeout, WAL mode and transaction mode. A connection
opened elsewhere silently skips them.

**C2. Every connection sets a busy timeout, WAL mode and manual transactions.** *Enforced.*
`busy_timeout > 0`, `journal_mode=WAL`, `isolation_level=None` (we issue `BEGIN` ourselves).
*Why:* without a busy timeout a concurrent writer fails right away with
"database is locked". WAL lets readers run alongside the writer.

**C3. One connection per thread; never share a connection across threads or processes.** *Review.*
Use `TaskStore.conn()` (thread-local), and open a new store after `fork`/`spawn`.
*Why:* a connection's transaction state belongs to one thread. Sharing it mixes
two transactions into one.

**C4. Never touch the DB from a signal handler.** *Review.*
Set a flag and let the main loop act on it.
*Why:* the handler can run in the middle of a transaction on the same connection.

## W: Writes

**W1. All write SQL (`INSERT`/`UPDATE`/`DELETE`/`REPLACE`) lives in `store.py`.** *Enforced.*
The API, workers, metrics and demos call store methods. They may run read-only
`SELECT`s through `store.conn()`.
*Why:* the invariants below can only be checked if there is one place to check them.

**W2. Every write runs inside `with self.tx()` (`BEGIN IMMEDIATE … COMMIT`).** *Enforced.*
No autocommit writes, and no `BEGIN DEFERRED` for read-modify-write.
*Why:* `IMMEDIATE` takes the write lock up front, so two read-then-write
transactions cannot deadlock trying to upgrade their locks. Grouping writes in one
transaction keeps the task row, its event and its counters consistent.

**W3. Keep transactions short: no sleeps, network calls or handler code inside `tx()`.** *Review.*
*Why:* SQLite has a single writer. Every millisecond inside `tx()` blocks every
other worker.

**W4. Status changes go through `_transition()` (or the atomic claim), never a raw `SET status=`.** *Enforced.*
*Why:* `_transition()` checks the state machine, adds a compare-and-set
(`AND status=<seen>`) and writes the audit event. A raw update skips all three.

**W5. Worker-side writes carry the fencing token.** *Enforced.*
Every `TaskStore` method that takes a `fence` must route its writes through
`_fenced()` (`WHERE id=? AND fence=?`).
*Why:* a paused or zombie worker whose lease expired must not overwrite the new
owner's work.

**W6. Conditional updates must check `rowcount` / `RETURNING`.** *Review.*
*Why:* an `UPDATE … WHERE` that matched 0 rows is a lost race, not a success.

**W7. Always use bound parameters (`?`); never format values into SQL.** *Review.*
Building SQL text is allowed only from trusted identifiers, such as column names
taken from code, not from user input.
*Why:* SQL injection, and correct quoting for free.

## R: Retries

**R1. Every retry is bounded by a maximum attempt count.** *Enforced (API) + Review.*
Once attempts run out the task goes to `DEAD`; it is never retried forever. The API caps `max_attempts`.
*Why:* a poison message must not loop forever and use up workers.

**R2. Backoff is exponential with a cap and full jitter.** *Enforced.*
`delay = uniform(0, min(cap, base * 2^(attempt-1)))`, computed so that it cannot
overflow for any attempt number.
*Why:* without a cap, delays grow without limit. Without jitter, every task that
failed together retries at the same moment and they all fail again together
(thundering herd).

**R3. Retry only retryable errors.** *Review.*
Raise `NonRetryableError` for errors a retry cannot fix (bad input, auth, 4xx).
Retry timeouts, crashes and 5xx.
*Why:* retrying a permanent error only wastes attempts and delays the failure.

**R4. Retries must be safe to repeat.** *Review.*
A step with an external side effect goes through `perform_effect()` (ledger +
provider idempotency key `task_id:step`).
*Why:* a crash after the side effect but before the commit replays the step.
Without these keys the payment is charged twice.

**R5. Don't add your own retry loop around DB "database is locked" errors.** *Review.*
The connection's `busy_timeout` already waits for the lock. If it still fails, raise.
*Why:* stacked retry layers multiply the number of attempts and hide contention.

**R6. Sleeps between retries use the store's clock and `run_after`, not `time.sleep` in a loop.** *Review.*
*Why:* a scheduled retry survives a crash and a fake clock can test it; a sleeping
thread does neither.
