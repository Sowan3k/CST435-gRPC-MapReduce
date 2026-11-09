# healthcheck.py - exit 0 if the local worker reports SERVING, else exit 1.
# Used by the Docker Compose healthcheck. Uses the standard gRPC health protocol.
import argparse
import os
import sys

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc


def check(addr: str, service: str = "", timeout: float = 3.0) -> bool:
    with grpc.insecure_channel(addr) as channel:
        stub = health_pb2_grpc.HealthStub(channel)
        try:
            resp = stub.Check(health_pb2.HealthCheckRequest(service=service), timeout=timeout)
        except grpc.RpcError:
            return False
    return resp.status == health_pb2.HealthCheckResponse.SERVING


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="gRPC health check for a worker")
    p.add_argument("--addr", default=f"127.0.0.1:{os.environ.get('PORT', '50051')}")
    p.add_argument("--service", default="", help="Service name; empty checks the whole server")
    p.add_argument("--timeout", type=float, default=3.0)
    args = p.parse_args(argv)
    return 0 if check(args.addr, args.service, args.timeout) else 1


if __name__ == "__main__":
    sys.exit(main())
