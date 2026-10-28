## Summary

<!-- What changed and why. -->

## Guidelines checklist ([GUIDELINES.md](../GUIDELINES.md))

Enforced rules (A1–A7, C1, C2, W1, W2, W4, W5, R1, R2) are checked by `tests/test_guidelines.py`.
For the rules a test can't check, tick each item or explain why it doesn't apply:

- [ ] **A6** Handlers stay thin: no SQL or business logic in `api.py`
- [ ] **A8** Response changes only add: no renamed, removed or retyped fields
- [ ] **C3** No connection is shared across threads or processes (`store.conn()` is thread-local)
- [ ] **C4** Signal handlers only set flags; no DB access from them
- [ ] **W3** No sleeps, network calls or handler code inside `with self.tx()`
- [ ] **W6** Every conditional `UPDATE … WHERE` checks `rowcount` / `RETURNING`
- [ ] **W7** Values are bound with `?`; SQL text is built only from trusted identifiers
- [ ] **R1** Any new retry path has a maximum attempt count and ends in a terminal state
- [ ] **R3** New errors are classified: `NonRetryableError` for ones a retry can't fix
- [ ] **R4** New external side effects go through `perform_effect()` (ledger + idempotency key)
- [ ] **R5** No hand-rolled retry loop around "database is locked"
- [ ] **R6** Retries are scheduled with `run_after` on the store clock, not `time.sleep` loops

## Tests

- [ ] `python -m pytest -q` passes
