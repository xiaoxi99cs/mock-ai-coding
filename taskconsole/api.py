"""REST API + static web console."""

import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import handlers
from .states import IllegalTransition
from .store import TaskNotFound, TaskStore
from .worker import Worker

STATIC = Path(__file__).parent / "static"


class SubmitRequest(BaseModel):
    type: str
    payload: dict = Field(default_factory=dict)
    pool: str | None = None
    priority: int = 0
    max_attempts: int = Field(3, ge=1, le=20)
    timeout_s: float = Field(60, gt=0, le=3600)


def create_app(db_path: str = "tasks.db", start_workers: bool = True) -> FastAPI:
    store = TaskStore(db_path)
    stop = threading.Event()

    @asynccontextmanager
    async def lifespan(app):
        threads = []
        if start_workers:
            for pool in ("default", "cpu"):
                w = Worker(store, pool=pool)
                t = threading.Thread(target=w.run_forever, args=(stop,), daemon=True, name=w.id)
                t.start()
                threads.append(t)
        yield
        stop.set()
        for t in threads:
            t.join(timeout=5)

    app = FastAPI(title="Async task console", lifespan=lifespan)
    app.state.store = store

    def not_found(task_id):
        return HTTPException(404, f"task {task_id} not found")

    @app.get("/api/task-types")
    def task_types():
        return {name: {k: v for k, v in spec.items() if k != "fn"} for name, spec in handlers.REGISTRY.items()}

    @app.post("/api/tasks", status_code=201)
    def submit(req: SubmitRequest):
        spec = handlers.REGISTRY.get(req.type)
        if spec is None:
            raise HTTPException(422, f"unknown task type {req.type!r}")
        return store.submit(req.type, req.payload, pool=req.pool or spec["pool"], priority=req.priority,
                            max_attempts=req.max_attempts, timeout_s=req.timeout_s)

    @app.get("/api/tasks")
    def list_tasks(status: str | None = None, pool: str | None = None, limit: int = 100, offset: int = 0):
        return store.list_tasks(status=status, pool=pool, limit=min(limit, 500), offset=offset)

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str):
        try:
            return {**store.get(task_id), "events": store.events(task_id)}
        except TaskNotFound:
            raise not_found(task_id)

    @app.post("/api/tasks/{task_id}/cancel")
    def cancel(task_id: str):
        try:
            return store.cancel(task_id)
        except TaskNotFound:
            raise not_found(task_id)
        except IllegalTransition as e:
            raise HTTPException(409, str(e))

    @app.get("/")
    def index():
        return FileResponse(STATIC / "index.html")

    return app
