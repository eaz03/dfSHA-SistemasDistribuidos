"""
Mide las operaciones de metadatos (las que solo tocan el registro del superpeer): latencia y
operaciones por segundo de mkdir, create, stat, ls y rm. Las que modifican el registro incluyen el
fsync del log de la persistencia; las consultas no.

Se ejecuta en el contenedor client:
  docker compose exec client python -m benchmarks.bench_metadata
"""

import statistics
import time

from dfsha.client.auth_client import credentials_from_environment
from dfsha.client.client import DFSClient

OPERATIONS = 300
ROOT = "/bench-metadata"


def run(label, function, count):
    latencies = []
    start = time.perf_counter()
    for number in range(count):
        operation_start = time.perf_counter()
        function(number)
        latencies.append(time.perf_counter() - operation_start)
    total = time.perf_counter() - start
    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    print(f"| {label} | {count / total:.0f} | {statistics.median(latencies) * 1000:.1f} | {p95 * 1000:.1f} |",
          flush=True)


def main():
    client = DFSClient(["sp1:7000"], *credentials_from_environment())
    client.mkdir(ROOT)
    print(f"### Operaciones de metadatos ({OPERATIONS} de cada una, un cliente)\n")
    print("| Operación | ops/s | mediana (ms) | p95 (ms) |")
    print("|---|---|---|---|")
    run("mkdir (modifica: fsync)", lambda n: client.mkdir(f"{ROOT}/d{n}"), OPERATIONS)
    run("create (modifica: fsync)", lambda n: client.create(f"{ROOT}/d{n}/f"), OPERATIONS)
    run("stat (consulta)", lambda n: client.stat(f"{ROOT}/d{n}/f"), OPERATIONS)
    run("ls (consulta)", lambda n: client.ls(f"{ROOT}/d{n}"), OPERATIONS)
    run("rm (modifica: fsync)", lambda n: client.rm(f"{ROOT}/d{n}/f"), OPERATIONS)
    for number in range(OPERATIONS):
        client.rmdir(f"{ROOT}/d{number}")
    client.rmdir(ROOT)


if __name__ == "__main__":
    main()
