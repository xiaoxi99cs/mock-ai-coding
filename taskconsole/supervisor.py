"""Supervisor: keeps each pool at its desired number of worker processes.

Runs as a background thread inside the API server. Each worker is a separate OS process
(`python -m taskconsole.worker`), so the console can really `kill -9` one.

Every `tick_s` the supervisor:
  1. notices worker processes that have exited, and records why;
  2. starts or drains processes so each pool has `desired[pool]` workers;
  3. recovers expired leases.

Note the supervisor does NOT rescue the tasks of a dead worker itself. That happens
through the lease: the dead worker stops renewing it, it expires, and lease recovery
(`store._recover_expired`) retries the task. So the design stays correct even if the
supervisor is down.

* A worker that exits unexpectedly (crash, kill -9, hard timeout) is marked dead and replaced.
* Scaling a pool down drains surplus workers (SIGTERM, graceful).
* Kill / pause / resume / drain controls exist so the console can inject faults.
* Each tick also recovers expired leases, so the UI shows recovery even when no worker is claiming.
"""

import itertools
import logging
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("taskconsole.supervisor")
ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Proc:
    """One worker process started by this supervisor."""
    id: str
    pool: str
    popen: subprocess.Popen
    paused: bool = False
    draining: bool = False
    meta: dict = field(default_factory=dict)


class Supervisor:
    def __init__(self, store, db_path, pools: dict[str, int], lease_s=5.0, heartbeat_s=1.0,
                 drain_grace_s=10.0, tick_s=0.5):
        self.store = store
        self.db_path = str(Path(db_path).resolve())
        self.desired = dict(pools)  # pool name -> how many worker processes we want
        self.lease_s = lease_s
        self.heartbeat_s = heartbeat_s
        self.drain_grace_s = drain_grace_s
        self.tick_s = tick_s
        self.procs: dict[str, Proc] = {}
        self._seq = itertools.count(1)
        self._lock = threading.RLock()  # the tick thread and API requests both touch `procs`
        self._stop = threading.Event()
        self._thread = None
        self.on_tick = []  # callbacks (e.g. metrics sampler)

    # -- lifecycle --------------------------------------------------------

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True, name="supervisor")
        self._thread.start()

    def stop(self, timeout=8.0):
        """Shut down: SIGTERM every worker (graceful drain), SIGKILL any that don't exit in time."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        with self._lock:
            for p in self.procs.values():
                self._signal(p, signal.SIGCONT)  # a paused (SIGSTOPped) process can't handle SIGTERM
                self._signal(p, signal.SIGTERM)
            for p in self.procs.values():
                try:
                    p.popen.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    p.popen.kill()
                self.store.worker_exited(p.id, "stopped", p.popen.returncode)
            self.procs.clear()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("supervisor tick failed")
            self._stop.wait(self.tick_s)

    def tick(self):
        with self._lock:
            # 1. Reap exited processes. Exit code 0 after a drain we asked for = clean stop;
            #    anything else (kill -9, crash, hard timeout exit 70/71) = dead, replace it.
            for p in list(self.procs.values()):
                rc = p.popen.poll()  # None = still running
                if rc is None:
                    continue
                del self.procs[p.id]
                if p.draining and rc == 0:
                    log.info("worker %s drained and exited", p.id)
                    self.store.worker_exited(p.id, "stopped", rc)
                else:
                    log.warning("worker %s died (exit %s); restarting", p.id, rc)
                    self.store.worker_exited(p.id, "dead", rc)
                    self.store.incr("worker_restarts")
            # 2. Scale each pool to its desired size: spawn the missing workers, drain the extra.
            #    Removing a dead process in step 1 makes the pool short, so it is replaced here.
            for pool, want in self.desired.items():
                live = [p for p in self.procs.values() if p.pool == pool and not p.draining]
                for _ in range(want - len(live)):
                    self._spawn(pool)
                for p in live[want:] if len(live) > want else []:
                    self.drain(p.id)
        # 3. Recover expired leases even when no worker is claiming (claim() also does this).
        self.store.recover_expired()
        for cb in self.on_tick:
            cb()

    def _spawn(self, pool):
        wid = f"{pool}-{next(self._seq)}"
        cmd = [sys.executable, "-m", "taskconsole.worker", "--db", self.db_path, "--pool", pool, "--id", wid,
               "--lease", str(self.lease_s), "--heartbeat", str(self.heartbeat_s),
               "--drain-grace", str(self.drain_grace_s)]
        popen = subprocess.Popen(cmd, cwd=ROOT)
        self.store.register_worker(wid, pool, popen.pid, "starting")
        self.procs[wid] = Proc(wid, pool, popen)
        log.info("spawned %s (pid %s)", wid, popen.pid)
        return wid

    # -- controls ---------------------------------------------------------

    def _signal(self, p: Proc, sig):
        try:
            p.popen.send_signal(sig)
        except ProcessLookupError:
            pass

    def _get(self, wid) -> Proc:
        p = self.procs.get(wid)
        if p is None:
            raise KeyError(wid)
        return p

    def kill(self, wid):
        """kill -9: no cleanup at all. Its task is recovered when the lease expires."""
        with self._lock:
            p = self._get(wid)
            self._signal(p, signal.SIGCONT)
            self._signal(p, signal.SIGKILL)

    def pause(self, wid):
        """SIGSTOP: the process is frozen, heartbeats stop, its lease expires -> a zombie in waiting."""
        with self._lock:
            p = self._get(wid)
            self._signal(p, signal.SIGSTOP)
            p.paused = True

    def resume(self, wid):
        with self._lock:
            p = self._get(wid)
            self._signal(p, signal.SIGCONT)
            p.paused = False

    def drain(self, wid):
        """SIGTERM: the worker finishes (or hands back) its task, then exits with 0."""
        with self._lock:
            p = self._get(wid)
            p.draining = True
            self._signal(p, signal.SIGCONT)
            self._signal(p, signal.SIGTERM)

    def set_pool_size(self, pool, size):
        with self._lock:
            self.desired[pool] = max(0, int(size))

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return {p.id: {"paused": p.paused, "draining": p.draining, "pid": p.popen.pid}
                    for p in self.procs.values()}
