# Worker container for gRPC translator
FROM python:3.12-slim

# Setup work dir
WORKDIR /app

# System deps (optional but good practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency list and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code (proto + generated files + worker + coordinator + data)
COPY mapreduce.proto mapreduce.proto
COPY mapreduce_pb2.py mapreduce_pb2.py
COPY mapreduce_pb2_grpc.py mapreduce_pb2_grpc.py
COPY worker.py worker.py
COPY coordinator.py coordinator.py
COPY data ./data

# Default port (can be overridden via env)
ENV PORT=50051
EXPOSE 50051

# Run the worker by default
CMD ["python", "worker.py"]
