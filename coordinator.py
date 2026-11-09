# coordinator.py - parallel fan-out/fan-in with failover

import argparse
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import grpc

import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc

ERROR_MARKER = "[ERROR"


def split_text(text: str, n_chunks: int) -> List[Tuple[str, str]]:
    """Split text into at most n_chunks groups of consecutive paragraphs.

    Paragraphs are separated by blank lines. Line breaks inside a paragraph are
    kept. Groups differ in size by at most one paragraph and keep input order.
    Returns (chunk_id, chunk_text) pairs; empty input gives an empty list, and
    fewer paragraphs than n_chunks gives fewer chunks.
    """
    paragraphs: List[str] = []
    current: List[str] = []
    for line in text.splitlines():
        if line.strip():
            current.append(line)
        elif current:
            paragraphs.append("\n".join(current))
            current = []
    if current:
        paragraphs.append("\n".join(current))

    n_chunks = max(1, min(n_chunks, len(paragraphs)))
    if not paragraphs:
        return []
    base, extra = divmod(len(paragraphs), n_chunks)
    chunks = []
    start = 0
    for i in range(n_chunks):
        size = base + (1 if i < extra else 0)
        chunks.append((f"chunk-{i + 1}", "\n\n".join(paragraphs[start:start + size])))
        start += size
    return chunks


@dataclass
class RunResult:
    output_text: str = ""
    elapsed: float = 0.0
    wait_elapsed: float = 0.0
    n_chunks: int = 0
    served_by: Counter = field(default_factory=Counter)  # worker address -> chunks
    failovers: int = 0  # chunk attempts that failed on one worker and moved to the next
    failed: Dict[str, str] = field(default_factory=dict)  # chunk_id -> reason
    unreachable: List[str] = field(default_factory=list)  # workers never ready at start


def wait_for_workers(channels, addrs, timeout: float) -> List[str]:
    """Wait up to `timeout` seconds for every channel to be ready.

    Returns the addresses that were not ready in time. This lets the
    coordinator start before the workers do.
    """
    deadline = time.monotonic() + timeout
    # Start connecting to every worker first, so one dead worker does not
    # delay the connection attempts to the others.
    ready = [grpc.channel_ready_future(ch) for ch in channels]
    unreachable = []
    for fut, addr in zip(ready, addrs):
        try:
            fut.result(timeout=max(0.0, deadline - time.monotonic()))
        except grpc.FutureTimeoutError:
            unreachable.append(addr)
            fut.cancel()
    return unreachable


def run_translation(
    input_path: str,
    worker_addrs: List[str],
    chunks: Optional[int] = None,
    concurrency: Optional[int] = None,
    rpc_timeout: float = 120.0,
    wait_timeout: float = 30.0,
) -> RunResult:
    """Translate a file across workers.

    chunks: number of chunks to split into (default: number of workers).
    concurrency: maximum RPCs in flight at once (default: number of chunks).
    """
    if not worker_addrs:
        raise RuntimeError("No workers configured.")

    text = Path(input_path).read_text(encoding="utf-8")
    chunks = chunks or len(worker_addrs)
    parts = split_text(text, chunks)
    result = RunResult(n_chunks=len(parts))
    if not parts:
        return result
    concurrency = max(1, concurrency or len(parts))

    # Reuse channels/stubs (thread-safe for multiple RPCs)
    channels = [grpc.insecure_channel(addr) for addr in worker_addrs]
    stubs = [pb2_grpc.WorkerStub(ch) for ch in channels]
    try:
        t_wait = time.perf_counter()
        result.unreachable = wait_for_workers(channels, worker_addrs, wait_timeout)
        result.wait_elapsed = time.perf_counter() - t_wait

        def translate(i: int):
            """Try the preferred worker, then each other worker in turn."""
            cid, ctext = parts[i]
            req = pb2.MapRequest(chunk_id=cid, text=ctext)
            errors = []
            for k in range(len(stubs)):
                w = (i + k) % len(stubs)
                try:
                    resp = stubs[w].Map(req, timeout=rpc_timeout)
                except grpc.RpcError as e:
                    errors.append(f"{worker_addrs[w]}: {e.code().name}")
                    continue
                return w, k, "\n".join(wc.word for wc in resp.counts), errors
            return None, len(stubs), None, errors

        # Fan out: at most `concurrency` blocking RPCs in flight.
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            outcomes = list(pool.map(translate, range(len(parts))))
        result.elapsed = time.perf_counter() - t0

        # Fan in: results come back in chunk order.
        translated = []
        for (cid, _), (w, failed_attempts, body, errors) in zip(parts, outcomes):
            result.failovers += failed_attempts if w is not None else 0
            if w is None:
                result.failed[cid] = "all workers failed (" + "; ".join(errors) + ")"
                translated.append("")
                continue
            result.served_by[worker_addrs[w]] += 1
            if body.startswith(ERROR_MARKER):
                result.failed[cid] = f"worker {worker_addrs[w]} returned {body}"
            translated.append(body)
    finally:
        for ch in channels:
            ch.close()

    result.output_text = "\n\n".join(translated) + "\n"
    return result


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/input.txt")
    p.add_argument("--output", default="data/output_malay.txt")
    p.add_argument("--chunks", type=int, default=None,
                   help="Number of chunks to split the input into (default: number of workers)")
    p.add_argument("--concurrency", type=int, default=None,
                   help="Maximum RPCs in flight at once (default: number of chunks)")
    p.add_argument(
        "--workers",
        default="localhost:50051",
        help="Comma-separated worker addresses, e.g. localhost:50051,localhost:50052",
    )
    p.add_argument("--rpc-timeout", type=float, default=120.0,
                   help="Seconds allowed for one Map call")
    p.add_argument("--wait-timeout", type=float, default=30.0,
                   help="Seconds to wait for workers to become ready at start")
    args = p.parse_args(argv)

    worker_addrs = [w.strip() for w in args.workers.split(",") if w.strip()]
    print(f"Starting distributed translation with {len(worker_addrs)} worker(s): {worker_addrs}")

    result = run_translation(
        args.input, worker_addrs,
        chunks=args.chunks, concurrency=args.concurrency,
        rpc_timeout=args.rpc_timeout, wait_timeout=args.wait_timeout,
    )

    if result.n_chunks == 0:
        print(f"No text found in {args.input}; nothing to translate.", file=sys.stderr)
        return 1
    for addr in result.unreachable:
        print(f"Warning: worker {addr} was not ready after {args.wait_timeout:g} s", file=sys.stderr)

    print(f"Chunks: {result.n_chunks} | Concurrency: {args.concurrency or result.n_chunks}")
    print("Chunks per worker:")
    for addr in worker_addrs:
        print(f"  {addr}: {result.served_by.get(addr, 0)}")
    if result.failovers:
        print(f"Failed attempts retried on another worker: {result.failovers}")
    print(f"Waited for workers: {result.wait_elapsed:.2f} s")
    print(f"Total time: {result.elapsed:.2f} s")

    if result.failed:
        print(f"\nFAILED: {len(result.failed)} of {result.n_chunks} chunk(s) failed:", file=sys.stderr)
        for cid, reason in result.failed.items():
            print(f"  {cid}: {reason}", file=sys.stderr)
        print(f"Output not written to {args.output}.", file=sys.stderr)
        return 1

    Path(args.output).write_text(result.output_text, encoding="utf-8")
    print(f"Translation complete. Output saved to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
