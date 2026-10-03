# CST435 gRPC MapReduce: Distributed English to Malay Translation

A coordinator splits an English text into chunks, sends the chunks to several gRPC worker services in parallel, and joins the translated Malay chunks back in their original order. Built for CST435 Parallel and Cloud Computing at Universiti Sains Malaysia, November 2025.

The project follows the MapReduce shape. The map step runs on the workers: each worker translates one chunk. The reduce step runs on the coordinator: it collects the results and concatenates them in chunk order. The service contract keeps the classic word-count message types from the assignment template, so each `MapReply` carries the translated chunk as a single `WordCount` entry with `count = 1`.

## How it works

`coordinator.py` reads `data/input.txt` and splits it by paragraph into as many chunks as the `--concurrency` value. It opens one gRPC channel per worker and assigns chunks round-robin. Every `Map` call goes out at once as a non-blocking future, which is the fan-out. The coordinator then waits on each future in order, which is the fan-in, writes the joined text to `data/output_malay.txt`, and prints the total time.

`worker.py` runs a gRPC server with a thread pool. Each `Map` call translates its chunk with `googletrans` and returns the result. A failed translation retries with linear backoff before the worker returns an `[ERROR ...]` marker instead of failing the whole job. A lock guards the shared translator instance, because the client is not guaranteed thread-safe.

`mapreduce.proto` defines the `Worker` service. `mapreduce_pb2.py` and `mapreduce_pb2_grpc.py` are the generated Python stubs and are committed, so no code generation step is needed to run the project.

## Stack

- Python 3.12
- gRPC (`grpcio` 1.76) and Protocol Buffers
- `googletrans` 4.0.0-rc1, an unofficial Google Translate client that needs internet access
- Docker and Docker Compose

## Running locally

Install the dependencies:

```bash
pip install -r requirements.txt
```

Start three workers, each in its own terminal:

```bash
PORT=50051 python worker.py
PORT=50052 python worker.py
PORT=50053 python worker.py
```

Run the coordinator with three chunks in parallel:

```bash
python coordinator.py --input data/input.txt --output data/output_malay.txt \
  --concurrency 3 --workers localhost:50051,localhost:50052,localhost:50053
```

On Windows, set the port with `set PORT=50051` before `python worker.py`.

To compare against a sequential run, use `--concurrency 1 --workers localhost:50051`. The coordinator prints the total time for each run.

## Worker settings

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `50051` | Port the worker listens on |
| `SRC_LANG` | `en` | Source language |
| `DST_LANG` | `ms` | Target language |
| `MAX_WORKERS` | `4` | Threads in the worker's gRPC server |
| `RETRY_MAX` | `3` | Translation attempts per chunk |
| `RETRY_BACKOFF` | `0.5` | Seconds of backoff, multiplied by the attempt number |

## Docker Compose

`docker-compose.yml` builds one image and starts three workers on ports 50051 to 50053, plus a coordinator container that sends work to all three.

```bash
docker compose up --build
```

Known issue: `worker.py` binds to `127.0.0.1`, which is only reachable from inside its own container. The coordinator container and the host cannot reach the workers until the bind address in `serve()` changes to `0.0.0.0`. The local setup above works as written.

## Project structure

```
mapreduce.proto          gRPC service and message definitions
mapreduce_pb2.py         Generated message classes
mapreduce_pb2_grpc.py    Generated client and server stubs
worker.py                Translator worker (map)
coordinator.py           Splitter, fan-out and fan-in (reduce)
data/input.txt           Sample English input
data/output_malay.txt    Sample Malay output
Dockerfile               Worker image
docker-compose.yml       Three workers and a coordinator
requirements.txt         Python dependencies
```

## Academic context

CST435 Parallel and Cloud Computing, School of Computer Sciences, Universiti Sains Malaysia.
