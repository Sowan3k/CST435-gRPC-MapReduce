# Image for the gRPC translator worker and the coordinator
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first so this layer is cached between code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code: generated stubs, worker, coordinator, health check script.
# data/ is not baked in; Compose mounts it into the coordinator.
COPY mapreduce_pb2.py mapreduce_pb2_grpc.py worker.py coordinator.py healthcheck.py ./

# Run as a non-root user. UID 1000 matches the usual first Linux user, so the
# coordinator can write to the mounted ./data directory.
RUN useradd --create-home --uid 1000 app
USER app

# Default port (can be overridden via env)
ENV PORT=50051
EXPOSE 50051

# Run the worker by default
CMD ["python", "worker.py"]
