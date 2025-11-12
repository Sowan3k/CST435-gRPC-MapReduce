# worker.py  →  English → Malay translator microservice (gRPC)
import os
import time
import threading
from concurrent import futures

import grpc
from googletrans import Translator  # pin: googletrans==4.0.0rc1
import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc

# ---- Config via ENV ----
PORT = os.environ.get("PORT", "50051")
SRC_LANG = os.environ.get("SRC_LANG", "en")
DST_LANG = os.environ.get("DST_LANG", "ms")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
RETRY_MAX = int(os.environ.get("RETRY_MAX", "3"))
RETRY_BACKOFF = float(os.environ.get("RETRY_BACKOFF", "0.5"))  # seconds

class TranslatorService(pb2_grpc.WorkerServicer):
    """Implements the gRPC Worker service for translation."""

    def __init__(self):
        # Single Translator instance; protect with a lock for thread safety
        self._translator = Translator()
        self._lock = threading.Lock()

    def _translate_with_retries(self, text: str) -> str:
        last_err = None
        for attempt in range(1, RETRY_MAX + 1):
            try:
                # Protect translator session (not guaranteed thread-safe)
                with self._lock:
                    res = self._translator.translate(text, src=SRC_LANG, dest=DST_LANG)
                return res.text
            except Exception as e:
                last_err = e
                # Simple exponential backoff
                time.sleep(RETRY_BACKOFF * attempt)
        # After retries, return an error marker to the coordinator
        return f"[ERROR translating: {last_err}]"

    def Map(self, request, context):
        """Receives English text and returns Malay translation."""
        start = time.perf_counter()

        # Propagate client cancellations promptly
        if not context.is_active():
            context.abort(grpc.StatusCode.CANCELLED, "Client cancelled")

        translated_text = self._translate_with_retries(request.text)

        elapsed = time.perf_counter() - start
        status = "ok" if not translated_text.startswith("[ERROR") else "err"
        print(f"[worker:{PORT}] {status} chunk={request.chunk_id} in {elapsed:.3f}s")

        # Return translated text packed into WordCount messages (per given proto)
        return pb2.MapReply(
            chunk_id=request.chunk_id,
            counts=[pb2.WordCount(word=translated_text, count=1)]
        )

def serve():
    """Starts the gRPC server for this worker."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))

    # If available, you can enable health/reflection:
    # from grpc_health.v1 import health, health_pb2_grpc
    # from grpc_reflection.v1alpha import reflection
    # health_servicer = health.HealthServicer()
    # health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    # SERVICE_NAMES = (
    #     pb2.DESCRIPTOR.services_by_name['Worker'].full_name,
    #     reflection.SERVICE_NAME,
    #     health.SERVICE_NAME,
    # )
    # reflection.enable_server_reflection(SERVICE_NAMES, server)

    pb2_grpc.add_WorkerServicer_to_server(TranslatorService(), server)

    # Bind to requested port; fail fast if unavailable (better for coordinator)
    bind_target = f"127.0.0.1:{PORT}"
    bound_port = server.add_insecure_port(bind_target)
    if bound_port == 0:
        raise RuntimeError(f"Failed to bind to {bind_target}. Is the port in use?")

    print(f"[worker] Translator listening on {bind_target} "
          f"(src={SRC_LANG} → dst={DST_LANG}, threads={MAX_WORKERS})")

    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[worker] Shutting down...")
        server.stop(grace=True)

if __name__ == "__main__":
    serve()
