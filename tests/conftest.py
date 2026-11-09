import socket
from contextlib import contextmanager

import pytest

from worker import Settings, create_server, mark_serving


def free_port() -> int:
    """Return a TCP port that nothing is listening on right now."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextmanager
def running_worker(backend_factory=None, **settings_kwargs):
    """Start a real gRPC worker in this process on a free port; yield its address."""
    defaults = dict(port=0, bind_host="127.0.0.1", translator="mock", retry_backoff=0.0)
    defaults.update(settings_kwargs)
    server, health_servicer, port = create_server(Settings(**defaults), backend_factory)
    server.start()
    mark_serving(health_servicer)
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None).wait()


@pytest.fixture
def worker_factory():
    return running_worker
