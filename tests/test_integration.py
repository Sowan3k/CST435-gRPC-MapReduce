import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import grpc

import coordinator
import healthcheck
import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc
from conftest import free_port, running_worker

ROOT = Path(__file__).resolve().parent.parent

PARAGRAPHS = [f"Paragraph number {i} has two lines.\nSecond line {i}." for i in range(1, 7)]


def expected_output(paragraphs):
    return "\n\n".join(
        "\n".join(f"[ms] {line}" for line in p.split("\n")) for p in paragraphs
    ) + "\n"


def write_input(tmp_path, paragraphs=PARAGRAPHS):
    path = tmp_path / "input.txt"
    path.write_text("\n\n".join(paragraphs) + "\n", encoding="utf-8")
    return str(path)


def test_end_to_end_order_and_structure(tmp_path):
    with running_worker() as a, running_worker() as b, running_worker() as c:
        result = coordinator.run_translation(write_input(tmp_path), [a, b, c], wait_timeout=5)
    assert not result.failed
    assert result.n_chunks == 3
    assert result.output_text == expected_output(PARAGRAPHS)
    assert sum(result.served_by.values()) == 3
    assert set(result.served_by) == {a, b, c}


def test_chunks_and_concurrency_are_independent(tmp_path):
    with running_worker() as a, running_worker() as b:
        result = coordinator.run_translation(
            write_input(tmp_path), [a, b], chunks=6, concurrency=1, wait_timeout=5
        )
    assert result.n_chunks == 6
    assert sum(result.served_by.values()) == 6
    assert result.output_text == expected_output(PARAGRAPHS)


def test_failover_when_one_worker_is_down(tmp_path):
    dead = f"127.0.0.1:{free_port()}"
    with running_worker() as live:
        result = coordinator.run_translation(
            write_input(tmp_path), [dead, live], chunks=4, wait_timeout=0.5, rpc_timeout=5
        )
    assert not result.failed
    assert result.unreachable == [dead]
    assert result.served_by == {live: 4}
    assert result.failovers >= 1
    assert result.output_text == expected_output(PARAGRAPHS)


def test_coordinator_can_start_before_the_worker(tmp_path):
    port = free_port()
    addr = f"127.0.0.1:{port}"
    started = threading.Event()

    def start_late():
        time.sleep(1.0)
        with running_worker(port=port) as _:
            started.set()
            time.sleep(3.0)

    t = threading.Thread(target=start_late)
    t.start()
    try:
        result = coordinator.run_translation(
            write_input(tmp_path), [addr], wait_timeout=10, rpc_timeout=5
        )
    finally:
        t.join()
    assert started.is_set()
    assert not result.failed
    assert result.unreachable == []
    assert result.wait_elapsed >= 0.5
    assert result.output_text == expected_output(PARAGRAPHS)


def test_all_workers_down_reports_failed_chunks(tmp_path):
    dead = [f"127.0.0.1:{free_port()}", f"127.0.0.1:{free_port()}"]
    result = coordinator.run_translation(
        write_input(tmp_path), dead, chunks=2, wait_timeout=0.3, rpc_timeout=2
    )
    assert set(result.failed) == {"chunk-1", "chunk-2"}


class FailingBackend:
    def translate(self, text):
        raise RuntimeError("backend exploded")


def test_error_marker_is_reported_and_exit_code_is_nonzero(tmp_path, capsys):
    out = tmp_path / "out.txt"
    with running_worker(backend_factory=FailingBackend, retry_max=2) as addr:
        code = coordinator.main([
            "--input", write_input(tmp_path), "--output", str(out),
            "--workers", addr, "--chunks", "2", "--wait-timeout", "5",
        ])
    assert code == 1
    assert not out.exists()
    err = capsys.readouterr().err
    assert "chunk-1" in err and "chunk-2" in err and "backend exploded" in err


def test_main_success_writes_output_and_exits_zero(tmp_path):
    out = tmp_path / "out.txt"
    with running_worker() as addr:
        code = coordinator.main([
            "--input", write_input(tmp_path), "--output", str(out),
            "--workers", addr, "--wait-timeout", "5",
        ])
    assert code == 0
    assert out.read_text(encoding="utf-8") == expected_output(PARAGRAPHS)


def test_empty_input_exits_nonzero(tmp_path):
    empty = tmp_path / "empty.txt"
    empty.write_text("", encoding="utf-8")
    with running_worker() as addr:
        code = coordinator.main(["--input", str(empty), "--output", str(tmp_path / "o.txt"),
                                 "--workers", addr])
    assert code == 1


def test_requests_run_in_parallel_inside_one_worker():
    """Two concurrent Map calls must be inside the backend at the same time.

    If requests were serialised, the barrier would time out and the worker
    would return an error marker.
    """
    barrier = threading.Barrier(2, timeout=5)
    instances = []

    class BarrierBackend:
        def __init__(self):
            instances.append(self)

        def translate(self, text):
            barrier.wait()
            return text

    with running_worker(backend_factory=BarrierBackend, max_workers=2) as addr:
        with grpc.insecure_channel(addr) as ch:
            stub = pb2_grpc.WorkerStub(ch)
            futs = [stub.Map.future(pb2.MapRequest(chunk_id=f"c{i}", text="hi"), timeout=10)
                    for i in range(2)]
            replies = [f.result() for f in futs]
    assert [r.counts[0].word for r in replies] == ["hi", "hi"]
    # One backend instance per server thread, not one shared instance.
    assert len(instances) == 2


def test_health_check_reports_serving_and_not_serving():
    assert healthcheck.check(f"127.0.0.1:{free_port()}", timeout=0.5) is False
    with running_worker() as addr:
        assert healthcheck.check(addr) is True
        assert healthcheck.check(addr, service="mapreduce.Worker") is True


def test_worker_exits_cleanly_on_sigterm():
    port = free_port()
    env = {"PATH": "", "PORT": str(port), "BIND_HOST": "127.0.0.1", "TRANSLATOR": "mock"}
    proc = subprocess.Popen([sys.executable, "worker.py"], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.monotonic() + 20
        while not healthcheck.check(f"127.0.0.1:{port}", timeout=0.5):
            assert proc.poll() is None, proc.stdout.read()
            assert time.monotonic() < deadline, "worker never became healthy"
            time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=15) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
