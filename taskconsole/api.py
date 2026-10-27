"""REST API + static web console.

Errors
------
Every error response has one shape, so clients branch on a stable code, never on prose:

    {"error": {"code": "task_not_found", "message": "task abc not found", "details": ...}}

Domain exceptions raised by the store are mapped to (status, code) in one table
(`DOMAIN_ERRORS`) instead of try/except in every route.
"""

import base64
import binascii
import json
import time
from contextlib import asynccontextmanager
from enum import StrEnum
from pathlib import Path

from fastapi import FastAPI, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import handlers
from .states import IllegalTransition, Status
from .store import IdempotencyConflict, TaskFinished, TaskNotFound, TaskStore
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


class WorkerAction(StrEnum):
    KILL = "kill"
    PAUSE = "pause"
    RESUME = "resume"
    DRAIN = "drain"


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details=None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


# Most specific first: TaskFinished is an IllegalTransition.
DOMAIN_ERRORS: list[tuple[type[Exception], int, str]] = [
    (TaskNotFound, 404, "task_not_found"),
    (IdempotencyConflict, 409, "idempotency_conflict"),
    (TaskFinished, 409, "task_already_finished"),
    (IllegalTransition, 409, "illegal_transition"),
]
HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def _error(status: int, code: str, message: str, details=None) -> JSONResponse:
    body = {"code": code, "message": message}
    if details is not None:
        body["details"] = details
    return JSONResponse({"error": body}, status_code=status)


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    def api_error(request: Request, e: ApiError):
        return _error(e.status, e.code, e.message, e.details)

    @app.exception_handler(RequestValidationError)
    def validation_error(request: Request, e: RequestValidationError):
        return _error(422, "validation_error", "request validation failed", jsonable_encoder(e.errors()))

    @app.exception_handler(StarletteHTTPException)
    def http_error(request: Request, e: StarletteHTTPException):
        return _error(e.status_code, HTTP_CODES.get(e.status_code, "http_error"), str(e.detail))

    for exc, status, code in DOMAIN_ERRORS:
        def handler(request: Request, e: Exception, status=status, code=code):
            message = f"task {e} not found" if isinstance(e, TaskNotFound) else str(e)
            return _error(status, code, message)
        app.add_exception_handler(exc, handler)


# -- cursors ------------------------------------------------------------------
# Opaque to clients; internally the (created_at, id) keyset of the last row served.

def encode_cursor(task: dict) -> str:
    raw = json.dumps([task["created_at"], task["id"]]).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[float, str]:
    try:
        created_at, task_id = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        if isinstance(created_at, (int, float)) and isinstance(task_id, str):
            return float(created_at), task_id
    except (binascii.Error, ValueError, TypeError):
        pass
    raise ApiError(400, "invalid_cursor", "cursor is malformed; pass back next_cursor exactly as received")


def create_app(db_path: str = "tasks.db", start_workers: bool = True, pools: dict | None = None,
               lease_s: float = 5.0, heartbeat_s: float = 1.0, clock=time.time) -> FastAPI:
    store = TaskStore(db_path, clock=clock)
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
    _install_error_handlers(app)

    def require_pool(pool: str) -> str:
        if pool not in sup.desired:
            raise ApiError(422, "unknown_pool", f"unknown pool {pool!r}", {"pools": sorted(sup.desired)})
        return pool

    # -- tasks ------------------------------------------------------------

    @app.get("/api/task-types")
    def task_types():
        return {name: {k: v for k, v in spec.items() if k != "fn"} for name, spec in handlers.REGISTRY.items()}

    @app.post("/api/tasks", status_code=201)
    def submit(req: SubmitRequest, response: Response):
        """201 with a new task; 200 with the original task for a repeated idempotency key."""
        spec = handlers.REGISTRY.get(req.type)
        if spec is None:
            raise ApiError(422, "unknown_task_type", f"unknown task type {req.type!r}",
                           {"types": sorted(handlers.REGISTRY)})
        task, created = store.submit(req.type, req.payload, pool=require_pool(req.pool or spec["pool"]),
                                     priority=req.priority, max_attempts=req.max_attempts,
                                     timeout_s=req.timeout_s, idempotency_key=req.idempotency_key)
        if not created:
            response.status_code = 200
        return {**task, "deduplicated": not created}

    @app.get("/api/tasks")
    def list_tasks(status: Status | None = None, pool: str | None = None,
                   limit: int = Query(100, ge=1, le=500), cursor: str | None = None):
        """Newest first. Follow `next_cursor` for the next page; it is null on the last page."""
        if pool is not None:
            require_pool(pool)
        after = decode_cursor(cursor) if cursor else None
        rows = store.list_tasks(status=status, pool=pool, limit=limit + 1, after=after)
        page = rows[:limit]
        return {"items": page, "next_cursor": encode_cursor(page[-1]) if len(rows) > limit else None}

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str):
        return {**store.get(task_id), "events": store.events(task_id)}

    @app.post("/api/tasks/{task_id}/cancel")
    def cancel(task_id: str, response: Response):
        """Idempotent. 200 once the task is CANCELLED; 202 if it is running and the worker
        has been asked to stop (it sees the flag on its next heartbeat)."""
        task = store.cancel(task_id)
        response.status_code = 200 if task["status"] == Status.CANCELLED else 202
        return task

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

    @app.post("/api/workers/{worker_id}/{action}", status_code=202)
    def worker_action(worker_id: str, action: WorkerAction):
        """202: the signal is sent; the effect shows up in GET /api/workers once the process reacts."""
        fn = {WorkerAction.KILL: sup.kill, WorkerAction.PAUSE: sup.pause,
              WorkerAction.RESUME: sup.resume, WorkerAction.DRAIN: sup.drain}[action]
        try:
            fn(worker_id)
        except KeyError:
            raise ApiError(404, "worker_not_found", f"worker {worker_id} not found")
        return {"worker_id": worker_id, "action": action}

    @app.get("/api/pools")
    def pools_():
        return sup.desired

    @app.put("/api/pools/{pool}")
    def set_pool(pool: str, body: PoolSize):
        if pool not in sup.desired:
            raise ApiError(404, "pool_not_found", f"pool {pool} not found")
        sup.set_pool_size(pool, body.size)
        return sup.desired

    @app.get("/api/effects")
    def effects():
        return store.external_effects()

    @app.get("/")
    def index():
        return FileResponse(STATIC / "index.html")

    return app
