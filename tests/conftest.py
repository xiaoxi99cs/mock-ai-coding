import pytest

from taskconsole.store import TaskStore


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


@pytest.fixture
def clock():
    return FakeClock()


class Store(TaskStore):
    def submit_task(self, *a, **kw):
        return self.submit(*a, **kw)[0]


@pytest.fixture
def store(tmp_path, clock):
    # rng=1.0: full jitter at its upper bound, so backoff is deterministic in tests
    return Store(str(tmp_path / "t.db"), clock=clock, rng=lambda: 1.0)
