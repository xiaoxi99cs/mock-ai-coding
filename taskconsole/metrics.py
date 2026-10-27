"""Metrics: status counts, queue depth, counters, latency percentiles, throughput; Prometheus export.

Everything is computed from SQLite on request (the console polls every second), with a
handful of aggregate queries. `snapshot()` is a pure function of (store, pools, heartbeat_s,
now), so it can be unit-tested on a fake clock.
"""

import math
import re

from fastapi.responses import PlainTextResponse

from .states import ACTIVE, CLAIMABLE, TERMINAL, Status

KEY_COUNTERS = ("retries", "lease_expirations", "fenced_writes_rejected", "idempotent_submits_deduplicated",
                "worker_restarts", "provider_duplicates_blocked", "effects_skipped_already_done")
LATENCY_WINDOW_S = 300.0
BUCKET_S = 5
N_BUCKETS = 60
THROUGHPUT_KEYS = {s: str(s).lower() for s in (Status.SUCCEEDED, Status.FAILED, Status.DEAD, Status.CANCELLED)}


def percentile(values: list[float], q: float) -> float | None:
    """Linear-interpolated percentile (q in [0, 1]); None for no data."""
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def _summary(values):
    return {"p50": percentile(values, 0.5), "p95": percentile(values, 0.95), "n": len(values)}


def snapshot(store, pools, heartbeat_s: float = 1.0, now: float | None = None) -> dict:
    """The /api/metrics document. `pools` is an iterable of pool names that always appear."""
    c = store.conn()
    now = store.clock() if now is None else now

    status_counts = {str(s): 0 for s in Status}
    for r in c.execute("SELECT status, COUNT(*) n FROM tasks GROUP BY status"):
        status_counts[r["status"]] = r["n"]

    queue_depth = {p: {"ready": 0, "delayed": 0, "running": 0} for p in pools}
    rows = c.execute(
        "SELECT pool,"
        " SUM(status IN (?,?) AND run_after <= ?) ready,"
        " SUM(status IN (?,?) AND run_after > ?) delayed,"
        " SUM(status IN (?,?)) running"
        " FROM tasks WHERE status IN (?,?,?,?) GROUP BY pool",
        (*CLAIMABLE, now, *CLAIMABLE, now, *ACTIVE, *CLAIMABLE, *ACTIVE),
    )
    for r in rows:
        queue_depth[r["pool"]] = {"ready": r["ready"], "delayed": r["delayed"], "running": r["running"]}

    counters = dict.fromkeys(KEY_COUNTERS, 0) | store.counters()

    since = now - LATENCY_WINDOW_S
    wait, run = [], []
    rows = c.execute(
        "SELECT t.status, t.created_at, t.started_at, t.finished_at,"
        " (SELECT MIN(e.ts) FROM task_events e WHERE e.task_id = t.id AND e.kind = 'transition'"
        "  AND e.to_status = ?) first_claim"
        " FROM tasks t WHERE t.status IN (?,?,?,?) AND t.finished_at >= ?",
        (Status.CLAIMED, *TERMINAL, since),
    )
    for r in rows:
        if r["first_claim"] is not None:
            wait.append(r["first_claim"] - r["created_at"])
        if r["status"] == Status.SUCCEEDED and r["started_at"] is not None:
            run.append(r["finished_at"] - r["started_at"])

    last = math.floor(now / BUCKET_S) * BUCKET_S
    first = last - (N_BUCKETS - 1) * BUCKET_S
    series = [{"t": first + i * BUCKET_S, **dict.fromkeys(THROUGHPUT_KEYS.values(), 0)} for i in range(N_BUCKETS)]
    rows = c.execute(
        "SELECT CAST((ts - ?) / ? AS INTEGER) b, to_status, COUNT(*) n FROM task_events"
        " WHERE kind = 'transition' AND to_status IN (?,?,?,?) AND ts >= ? AND ts < ?"
        " GROUP BY b, to_status",
        (first, BUCKET_S, *TERMINAL, first, last + BUCKET_S),
    )
    for r in rows:
        series[r["b"]][THROUGHPUT_KEYS[Status(r["to_status"])]] += r["n"]

    workers = {"alive": 0, "stale": 0, "dead": 0}
    for w in store.list_workers():
        if w["status"] == "dead":
            workers["dead"] += 1
        elif w["status"] != "stopped":
            age = None if w["last_heartbeat"] is None else now - w["last_heartbeat"]
            workers["stale" if age is not None and age > 3 * heartbeat_s else "alive"] += 1

    return {
        "now": now,
        "status_counts": status_counts,
        "queue_depth": queue_depth,
        "counters": counters,
        "latency": {"wait": _summary(wait), "run": _summary(run)},
        "throughput": {"bucket_s": BUCKET_S, "series": series},
        "workers": workers,
    }


def _label(v) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _num(v) -> str:
    return str(v) if isinstance(v, int) else repr(float(v))


def prometheus(m: dict) -> str:
    """Render a snapshot() in the Prometheus text exposition format (0.0.4)."""
    out = []

    def metric(name, typ, help_, samples):
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} {typ}")
        for labels, value in samples:
            lbl = ",".join(f'{k}="{_label(v)}"' for k, v in labels.items())
            out.append(f"{name}{{{lbl}}} {_num(value)}" if lbl else f"{name} {_num(value)}")

    metric("taskconsole_tasks", "gauge", "Tasks by status.",
           [({"status": s}, n) for s, n in m["status_counts"].items()])
    metric("taskconsole_queue_depth", "gauge", "Tasks per pool: ready, delayed (backoff) or running.",
           [({"pool": p, "state": k}, n) for p, d in m["queue_depth"].items() for k, n in d.items()])
    for name, value in sorted(m["counters"].items()):
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        metric(f"taskconsole_{safe}_total", "counter", f"Counter {name}.", [({}, value)])
    for kind, help_ in (("wait", "Queue wait (created -> first claim)"), ("run", "Run time of succeeded tasks")):
        lat = m["latency"][kind]
        samples = [({"quantile": q}, lat[k]) for q, k in (("0.5", "p50"), ("0.95", "p95")) if lat[k] is not None]
        metric(f"taskconsole_task_{kind}_seconds", "gauge",
               f"{help_}, over tasks finished in the last {LATENCY_WINDOW_S:g}s.", samples)
    metric("taskconsole_workers", "gauge", "Worker processes by heartbeat state.",
           [({"state": s}, n) for s, n in m["workers"].items()])
    return "\n".join(out) + "\n"


def register(app, store, sup, heartbeat_s=1.0):
    @app.get("/api/metrics")
    def metrics_json():
        return snapshot(store, list(sup.desired), heartbeat_s)

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics_prometheus():
        return PlainTextResponse(prometheus(snapshot(store, list(sup.desired), heartbeat_s)),
                                 media_type="text/plain; version=0.0.4")
