# coordinator.py — parallel fan-out/fan-in version

import time
import math
import grpc
import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc
from pathlib import Path
from typing import List, Tuple

def split_text(text: str, n_chunks: int) -> List[Tuple[str, str]]:
    """Split text roughly evenly by paragraphs."""
    paragraphs = [p for p in text.split("\n") if p.strip()]
    n_chunks = max(1, n_chunks)
    chunk_size = max(1, math.ceil(len(paragraphs) / n_chunks))
    chunks = []
    for i in range(n_chunks):
        start = i * chunk_size
        end = min(len(paragraphs), (i + 1) * chunk_size)
        if start < len(paragraphs):
            chunks.append((f"chunk-{i+1}", "\n".join(paragraphs[start:end])))
    if not chunks and text.strip():
        chunks = [("chunk-1", text)]
    return chunks

def run_translation(input_path: str, concurrency: int, worker_addrs: list[str]):
    text = Path(input_path).read_text(encoding="utf-8")
    chunks = split_text(text, concurrency)

    # Reuse channels/stubs (thread-safe for multiple RPCs)
    channels = [grpc.insecure_channel(addr) for addr in worker_addrs]
    stubs = [pb2_grpc.WorkerStub(ch) for ch in channels]
    if not stubs:
        raise RuntimeError("No workers configured.")

    # Fan out: launch all RPCs concurrently using async futures
    t0 = time.perf_counter()
    futures = []  # (index, future)
    for i, (cid, ctext) in enumerate(chunks):
        stub = stubs[i % len(stubs)]
        req = pb2.MapRequest(chunk_id=cid, text=ctext)
        fut = stub.Map.future(req, timeout=120)  # non-blocking call
        futures.append((i, fut))

    # Fan in: collect all results (preserve original chunk order)
    translated_results = [None] * len(chunks)
    for i, fut in futures:
        resp = fut.result()  # wait for this one to finish
        translated_lines = [wc.word for wc in resp.counts]
        translated_results[i] = "\n".join(translated_lines)

    elapsed = time.perf_counter() - t0
    output_text = "\n\n".join(translated_results)
    return elapsed, output_text

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/input.txt")
    p.add_argument("--output", default="data/output_malay.txt")
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument(
        "--workers",
        default="localhost:50051",
        help="Comma-separated worker addresses, e.g. localhost:50051,localhost:50052"
    )
    args = p.parse_args()

    worker_addrs = [w.strip() for w in args.workers.split(",") if w.strip()]
    print(f"Starting distributed translation with {len(worker_addrs)} worker(s): {worker_addrs}")

    elapsed, malay_text = run_translation(args.input, args.concurrency, worker_addrs)
    Path(args.output).write_text(malay_text, encoding="utf-8")

    print("\n✅ Translation complete!")
    print(f"Workers: {len(worker_addrs)} | Concurrency: {args.concurrency}")
    print(f"Output saved to {args.output}")
    print(f"Total time: {elapsed:.2f} s")
