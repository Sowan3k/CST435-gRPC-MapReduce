# CST435 gRPC MapReduce: Distributed English to Malay Translation

A coordinator splits an English text into chunks, sends the chunks to several gRPC worker services in parallel, and joins the translated Malay chunks back in their original order. Built for CST435 Parallel and Cloud Computing at Universiti Sains Malaysia, November 2025.

The project follows the MapReduce shape. The map step runs on the workers: each worker translates one chunk. The reduce step runs on the coordinator: it collects the results and concatenates them in chunk order. The service contract keeps the classic word-count message types from the assignment template, so each `MapReply` carries the translated chunk as a single `WordCount` entry with `count = 1`.

## How it works

**Fan-out.** `coordinator.py` reads `data/input.txt` and splits it into paragraphs, which are separated by blank lines. It groups consecutive paragraphs into `--chunks` chunks (default: one per worker) whose sizes differ by at most one paragraph. It opens one gRPC channel per worker, waits for the channels to become ready, then sends the chunks with up to `--concurrency` calls in flight at once. Chunk `i` goes to worker `i` modulo the number of workers first.

**Failover.** If a call fails on one worker, for example because the worker is down, the coordinator sends the same chunk to the next worker, and so on until every worker has been tried. A chunk fails only if all of them fail.

**Fan-in.** The coordinator collects results in chunk order and joins them with a blank line, so the output keeps the input's paragraph structure. Runs of several blank lines in the input become one blank line.

**Errors.** If a worker cannot translate a chunk after its own retries, it returns a reply that starts with `[ERROR`. The coordinator treats that as a failed chunk. At the end it lists every failed chunk and exits with code 1, and it does not write the output file. A successful run exits with code 0 and prints how many chunks each worker served and the total time.

**Worker.** `worker.py` runs a gRPC server with a thread pool. Each server thread has its own translator object, so concurrent requests do not wait on each other. A worker translates its chunk line by line, which keeps line breaks and blank lines intact whatever the backend does with newlines. A failed translation retries with linear backoff (`RETRY_BACKOFF` times the attempt number) before the worker returns the `[ERROR ...]` marker.

**Health checks and shutdown.** Each worker serves the standard gRPC health service and reports `SERVING` once the server has started. `healthcheck.py` queries it and exits 0 or 1. Docker Compose runs that script as the healthcheck for each worker, and the coordinator container starts only when all three are healthy. On SIGTERM or SIGINT, the worker reports `NOT_SERVING`, lets in-flight calls finish for up to five seconds, and exits with code 0.

`mapreduce.proto` defines the `Worker` service. `mapreduce_pb2.py` and `mapreduce_pb2_grpc.py` are the generated Python stubs and are committed, so no code generation step is needed.

## Stack

- Python 3.12
- gRPC (`grpcio` 1.76, `grpcio-health-checking` 1.76) and Protocol Buffers 6.33
- `googletrans` 4.0.0-rc1, an unofficial Google Translate client that needs internet access
- Docker and Docker Compose
- pytest

## Settings

Worker environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `50051` | Port the worker listens on |
| `BIND_HOST` | `0.0.0.0` | Address the worker binds to. Use `127.0.0.1` to accept local connections only |
| `TRANSLATOR` | `google` | Translation backend: `google` (googletrans, needs internet) or `mock` (offline, returns `[ms] ` plus the text) |
| `MOCK_DELAY_SECONDS` | `0` | Seconds the mock backend sleeps on each translation call, to simulate work |
| `SRC_LANG` | `en` | Source language |
| `DST_LANG` | `ms` | Target language. The mock backend uses it as its prefix, `[ms] ` by default |
| `MAX_WORKERS` | `4` | Threads in the worker's gRPC server |
| `RETRY_MAX` | `3` | Translation attempts per line |
| `RETRY_BACKOFF` | `0.5` | Seconds of backoff, multiplied by the attempt number |

Coordinator options:

| Option | Default | Purpose |
|---|---|---|
| `--input` | `data/input.txt` | Input text file |
| `--output` | `data/output_malay.txt` | Output file, written only if every chunk succeeds |
| `--workers` | `localhost:50051` | Comma-separated worker addresses |
| `--chunks` | number of workers | Number of chunks to split the input into |
| `--concurrency` | number of chunks | Maximum calls in flight at once |
| `--rpc-timeout` | `120` | Seconds allowed for one `Map` call |
| `--wait-timeout` | `30` | Seconds to wait for workers to become ready at start. A worker still down after this fails fast, and its chunks go to the next worker |

For a sequential baseline, run with `--chunks 3 --concurrency 1`. For a parallel run, leave `--concurrency` at its default.

## Running locally

Install the dependencies:

```bash
pip install -r requirements.txt
```

Start three workers, each in its own terminal. Add `TRANSLATOR=mock` to run without internet.

```bash
PORT=50051 python worker.py
PORT=50052 python worker.py
PORT=50053 python worker.py
```

Run the coordinator:

```bash
python coordinator.py --workers localhost:50051,localhost:50052,localhost:50053
```

The coordinator may start before the workers. It waits up to `--wait-timeout` seconds for them. On Windows, set the port with `set PORT=50051` before `python worker.py`.

## Running with Docker Compose

`docker-compose.yml` builds one image and starts three workers on ports 50051 to 50053, plus a coordinator container. The coordinator waits until all workers are healthy, translates `data/input.txt`, writes `data/output_malay.txt` to the host through a bind mount, and exits.

With the real translator, which needs internet access:

```bash
docker compose up --build
```

Offline, with the mock backend:

```bash
TRANSLATOR=mock docker compose up --build
```

The workers keep running after the coordinator finishes. To run once and stop everything, use the one-shot form and then clean up:

```bash
TRANSLATOR=mock docker compose up --build --abort-on-container-exit --exit-code-from coordinator
docker compose down
```

The containers run as UID 1000 so the coordinator can write to `./data`. If your host user has a different UID on Linux, pass it in:

```bash
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) TRANSLATOR=mock docker compose up --build
```

## Running tests

```bash
pip install -r requirements-dev.txt
pytest
```

The tests need no internet. They cover text splitting, an end-to-end run against real in-process gRPC workers that use the mock backend on free ports, failover when a worker is down, `[ERROR` detection and exit codes, parallel handling inside one worker, the health service, and clean exit on SIGTERM. GitHub Actions runs the tests and a mock-backed Compose run on every push and pull request.

## Design notes

**Why a mock backend.** The googletrans backend needs internet access and depends on a service that can throttle or block clients. The mock backend returns predictable output, so the project can run on any machine and in CI, and the tests can check exact output and ordering. `MOCK_DELAY_SECONDS` adds fixed work per call, which makes the effect of `--chunks` and `--concurrency` visible without depending on network speed.

**The googletrans trade-off.** `googletrans` is an unofficial client that calls the public Google Translate web endpoint. It needs no API key, which suits a coursework demo. It is not supported by Google, it can break or be rate limited without notice, and it is not suitable for production use. A production version would use the official Cloud Translation API behind the same worker interface. The backend is one small class in `worker.py`, so replacing it does not touch the coordinator or the proto.

**Per-line translation.** The worker translates each non-blank line separately instead of sending a whole chunk in one request. That costs more requests but keeps the paragraph structure independent of how a backend treats newlines.

**Failed chunks.** The coordinator does not retry a chunk that a worker reports as `[ERROR`, because that worker has already retried it. It also writes no output file when any chunk fails, so a partial result is never mistaken for a complete one.

## Project structure

```
mapreduce.proto          gRPC service and message definitions
mapreduce_pb2.py         Generated message classes
mapreduce_pb2_grpc.py    Generated client and server stubs
worker.py                Translator worker (map), health service
coordinator.py           Splitter, fan-out, failover and fan-in (reduce)
healthcheck.py           Health check client used by Docker Compose
data/input.txt           Sample English input
tests/                   pytest unit and integration tests
Dockerfile               Image for workers and coordinator
docker-compose.yml       Three workers and a coordinator
requirements.txt         Runtime dependencies
requirements-dev.txt     Runtime and test dependencies
.github/workflows/       CI
```

`data/output_malay.txt` is generated by the coordinator and is not committed.

## Academic context

CST435 Parallel and Cloud Computing, School of Computer Sciences, Universiti Sains Malaysia.
