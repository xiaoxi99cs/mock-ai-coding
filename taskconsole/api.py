"""REST API + static web console."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import demos, handlers, metrics
from .states import IllegalTransition
from .store import IdempotencyConflict, TaskNotFound, TaskStore
from .supervisor import Supervisor

STATIC = Path(__file__).parent / "static"
DEFAULT_POOLS = {"default": 3, "cpu": 2}


class SubmitRequest(BaseModel):
    type: str
    payload: dict = Field(default_factory=dict)
    pool: str | None = None
    priority: int = 0
    max_attempts: int = Field(3, ge=1, le=20)
    timeout_s: float = Field(60, gt=0, le=3600)
    idempotency_key: str | None = Field(None, max_length=200)


class PoolSize(BaseModel):
    size: int = Field(ge=0, le=16)


def create_app(db_path: str = "tasks.db", start_workers: bool = True, pools: dict | None = None,
               lease_s: float = 5.0, heartbeat_s: float = 1.0) -> FastAPI:
    store = TaskStore(db_path)
    sup = Supervisor(store, db_path, pools or DEFAULT_POOLS, lease_s=lease_s, heartbeat_s=heartbeat_s)

    @asynccontextmanager
    async def lifespan(app):
        if start_workers:
            sup.start()
        yield
        if start_workers:
            sup.stop()

    app = FastAPI(title="Async task console", lifespan=lifespan)
    app.state.store = store
    app.state.supervisor = sup

    def not_found(what):
        return HTTPException(404, f"{what} not found")

    # -- tasks ------------------------------------------------------------

    @app.get("/api/task-types")
    def task_types():
        return {name: {k: v for k, v in spec.items() if k != "fn"} for name, spec in handlers.REGISTRY.items()}

    @app.post("/api/tasks", status_code=201)
    def submit(req: SubmitRequest, response: Response):
        spec = handlers.REGISTRY.get(req.type)
        if spec is None:
            raise HTTPException(422, f"unknown task type {req.type!r}")
        pool = req.pool or spec["pool"]
        if pool not in sup.desired:
            raise HTTPException(422, f"unknown pool {pool!r}")
        try:
            task, created = store.submit(req.type, req.payload, pool=pool, priority=req.priority,
                                         max_attempts=req.max_attempts, timeout_s=req.timeout_s,
                                         idempotency_key=req.idempotency_key)
        except IdempotencyConflict as e:
            raise HTTPException(409, str(e))
        if not created:
            response.status_code = 200
        return {**task, "deduplicated": not created}

    @app.get("/api/tasks")
    def list_tasks(status: str | None = None, pool: str | None = None, limit: int = 100, offset: int = 0):
        return store.list_tasks(status=status, pool=pool, limit=min(limit, 500), offset=offset)

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str):
        try:
            return {**store.get(task_id), "events": store.events(task_id)}
        except TaskNotFound:
            raise not_found(f"task {task_id}")

    @app.post("/api/tasks/{task_id}/cancel")
    def cancel(task_id: str):
        try:
            return store.cancel(task_id)
        except TaskNotFound:
            raise not_found(f"task {task_id}")
        except IllegalTransition as e:
            raise HTTPException(409, str(e))

    # -- workers & pools ---------------------------------------------------

    @app.get("/api/workers")
    def workers():
        now = store.clock()
        live = sup.snapshot()
        out = []
        for w in store.list_workers():
            ctl = live.get(w["id"], {})
            age = None if w["last_heartbeat"] is None else now - w["last_heartbeat"]
            state = w["status"]
            if ctl.get("paused"):
                state = "paused"
            elif state not in ("stopped", "dead") and age is not None and age > 3 * heartbeat_s:
                state = "stale"
            out.append({**w, "state": state, "heartbeat_age": age, **ctl})
        return out

    @app.post("/api/workers/{worker_id}/{action}")
    def worker_action(worker_id: str, action: str):
        fn = {"kill": sup.kill, "pause": sup.pause, "resume": sup.resume, "drain": sup.drain}.get(action)
        if fn is None:
            raise HTTPException(422, f"unknown action {action!r}")
        try:
            fn(worker_id)
        except KeyError:
            raise not_found(f"worker {worker_id}")
        return {"ok": True}

    @app.get("/api/pools")
    def pools_():
        return sup.desired

    @app.put("/api/pools/{pool}")
    def set_pool(pool: str, body: PoolSize):
        if pool not in sup.desired:
            raise not_found(f"pool {pool}")
        sup.set_pool_size(pool, body.size)
        return sup.desired

    @app.get("/api/effects")
    def effects():
        return store.external_effects()

    metrics.register(app, store, sup, heartbeat_s=heartbeat_s)
    demos.register(app, store, sup)

    @app.get("/")
    def index():
        return FileResponse(STATIC / "index.html")

    return app
