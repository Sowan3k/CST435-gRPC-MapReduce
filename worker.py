# worker.py  ->  English to Malay translator microservice (gRPC)
import os
import signal
import threading
import time
from concurrent import futures
from dataclasses import dataclass

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc

SHUTDOWN_GRACE_SECONDS = 5.0


@dataclass(frozen=True)
class Settings:
    """Worker configuration. Built from environment variables by from_env()."""

    port: int = 50051
    bind_host: str = "0.0.0.0"
    src_lang: str = "en"
    dst_lang: str = "ms"
    max_workers: int = 4
    retry_max: int = 3
    retry_backoff: float = 0.5  # seconds, multiplied by the attempt number
    translator: str = "google"  # "google" or "mock"
    mock_delay: float = 0.0  # seconds slept per translation call by the mock

    @classmethod
    def from_env(cls, env=None) -> "Settings":
        env = os.environ if env is None else env
        return cls(
            port=int(env.get("PORT", "50051")),
            bind_host=env.get("BIND_HOST", "0.0.0.0"),
            src_lang=env.get("SRC_LANG", "en"),
            dst_lang=env.get("DST_LANG", "ms"),
            max_workers=int(env.get("MAX_WORKERS", "4")),
            retry_max=int(env.get("RETRY_MAX", "3")),
            retry_backoff=float(env.get("RETRY_BACKOFF", "0.5")),
            translator=env.get("TRANSLATOR", "google").strip().lower(),
            mock_delay=float(env.get("MOCK_DELAY_SECONDS", "0")),
        )


# ---- Translator backends ----
# A backend is any object with translate(text) -> str.

class GoogleBackend:
    """Unofficial Google Translate client (googletrans). Needs internet access."""

    def __init__(self, src: str, dst: str):
        # Imported here so the mock backend works without googletrans installed.
        from googletrans import Translator

        self._translator = Translator()
        self._src = src
        self._dst = dst

    def translate(self, text: str) -> str:
        return self._translator.translate(text, src=self._src, dest=self._dst).text


class MockBackend:
    """Offline and deterministic: returns "[<dst>] " + text, after an optional sleep."""

    def __init__(self, dst: str, delay: float = 0.0):
        self._prefix = f"[{dst}] "
        self._delay = delay

    def translate(self, text: str) -> str:
        if self._delay > 0:
            time.sleep(self._delay)
        return self._prefix + text


def make_backend_factory(settings: Settings):
    """Return a zero-argument callable that builds one backend instance."""
    if settings.translator == "google":
        return lambda: GoogleBackend(settings.src_lang, settings.dst_lang)
    if settings.translator == "mock":
        return lambda: MockBackend(settings.dst_lang, settings.mock_delay)
    raise ValueError(
        f"Unknown TRANSLATOR {settings.translator!r}; expected 'google' or 'mock'"
    )


class TranslatorService(pb2_grpc.WorkerServicer):
    """Implements the gRPC Worker service for translation."""

    def __init__(self, settings: Settings, backend_factory=None):
        self._settings = settings
        self._backend_factory = backend_factory or make_backend_factory(settings)
        # One backend per server thread, so concurrent requests do not share a
        # client and no lock is needed.
        self._local = threading.local()

    def _backend(self):
        backend = getattr(self._local, "backend", None)
        if backend is None:
            backend = self._backend_factory()
            self._local.backend = backend
        return backend

    def _translate_line(self, backend, line: str) -> str:
        last_err = None
        for attempt in range(1, self._settings.retry_max + 1):
            try:
                return backend.translate(line)
            except Exception as e:
                last_err = e
                if attempt < self._settings.retry_max:
                    # Linear backoff: RETRY_BACKOFF * attempt seconds.
                    time.sleep(self._settings.retry_backoff * attempt)
        raise last_err

    def _translate_chunk(self, text: str) -> str:
        """Translate line by line so newlines and blank lines survive any backend."""
        backend = self._backend()
        try:
            out = [
                self._translate_line(backend, line) if line.strip() else line
                for line in text.split("\n")
            ]
        except Exception as e:
            # Return an error marker; the coordinator detects it and reports the chunk.
            return f"[ERROR translating: {e}]"
        return "\n".join(out)

    def Map(self, request, context):
        """Receives English text and returns the Malay translation."""
        start = time.perf_counter()

        # Propagate client cancellations promptly
        if not context.is_active():
            context.abort(grpc.StatusCode.CANCELLED, "Client cancelled")

        translated_text = self._translate_chunk(request.text)

        elapsed = time.perf_counter() - start
        status = "ok" if not translated_text.startswith("[ERROR") else "err"
        print(f"[worker] {status} chunk={request.chunk_id} in {elapsed:.3f}s", flush=True)

        # Return translated text packed into one WordCount message (per given proto)
        return pb2.MapReply(
            chunk_id=request.chunk_id,
            counts=[pb2.WordCount(word=translated_text, count=1)],
        )


def create_server(settings: Settings, backend_factory=None, bind_host=None):
    """Build a gRPC server with the Worker and Health services.

    Returns (server, health_servicer, bound_port). The server is not started.
    Use settings.port = 0 to let the OS pick a free port.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=settings.max_workers))
    pb2_grpc.add_WorkerServicer_to_server(
        TranslatorService(settings, backend_factory), server
    )

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    host = bind_host or settings.bind_host
    bind_target = f"{host}:{settings.port}"
    bound_port = server.add_insecure_port(bind_target)
    if bound_port == 0:
        raise RuntimeError(f"Failed to bind to {bind_target}. Is the port in use?")
    return server, health_servicer, bound_port


def mark_serving(health_servicer):
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    health_servicer.set(
        pb2.DESCRIPTOR.services_by_name["Worker"].full_name,
        health_pb2.HealthCheckResponse.SERVING,
    )


def serve():
    """Run the worker until SIGTERM or SIGINT, then stop gracefully."""
    settings = Settings.from_env()
    server, health_servicer, bound_port = create_server(settings)

    stop_event = threading.Event()

    def request_stop(signum, _frame):
        print(f"[worker] Received {signal.Signals(signum).name}, shutting down", flush=True)
        stop_event.set()

    # Docker stops containers with SIGTERM, not KeyboardInterrupt.
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    server.start()
    mark_serving(health_servicer)
    print(
        f"[worker] Translator listening on {settings.bind_host}:{bound_port} "
        f"(backend={settings.translator}, src={settings.src_lang}, "
        f"dst={settings.dst_lang}, threads={settings.max_workers})",
        flush=True,
    )

    stop_event.wait()

    # Tell health checkers to stop sending traffic, then drain in-flight RPCs.
    health_servicer.enter_graceful_shutdown()
    server.stop(grace=SHUTDOWN_GRACE_SECONDS).wait()
    print("[worker] Stopped", flush=True)


if __name__ == "__main__":
    serve()
