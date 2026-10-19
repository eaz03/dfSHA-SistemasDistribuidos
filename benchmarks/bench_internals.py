"""
Mide el costo de piezas internas, aisladas del resto del sistema:
  - cifrado en reposo: escribir y leer bloques de 1 MiB con y sin AES-256-GCM;
  - TLS: transferir bloques de 1 MiB por gRPC con y sin mTLS (un servidor de prueba en este proceso);
  - persistencia del registro: duración y tamaño de un snapshot según la cantidad de archivos.

Se ejecuta en el contenedor sp1 (no necesita los demás servicios):
  docker compose run --rm sp1 python -m benchmarks.bench_internals
"""

import os
import tempfile
import time
import uuid
from concurrent import futures

import grpc

from dfsha.common.tls import secure_channel, server_credentials
from dfsha.peer.disk_block_storage import DiskBlockStorage
from dfsha.peer.encrypted_block_storage import EncryptedBlockStorage
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.superpeer.dict_registry import DictRegistry
from dfsha.superpeer.persistent_registry import PersistentRegistry

MEBIBYTE = 1024 * 1024
BLOCKS = 100


def storage_throughput(storage):
    """(MiB/s de escritura, MiB/s de lectura) de BLOCKS bloques de 1 MiB."""
    data = os.urandom(MEBIBYTE)
    block_ids = [uuid.uuid4().hex for _ in range(BLOCKS)]
    start = time.perf_counter()
    for block_id in block_ids:
        storage.write_block(block_id, data)
    write_seconds = time.perf_counter() - start
    start = time.perf_counter()
    for block_id in block_ids:
        storage.read_block(block_id)
    read_seconds = time.perf_counter() - start
    return BLOCKS / write_seconds, BLOCKS / read_seconds


class MemoryPeer(rpc.PeerServicer):
    """Peer mínimo que entrega siempre el mismo bloque de 1 MiB: aísla el costo del canal."""
    data = os.urandom(MEBIBYTE)

    def ReadBlock(self, request, context):
        return pb.BlockData(data=self.data)


def channel_throughput(secure):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    rpc.add_PeerServicer_to_server(MemoryPeer(), server)
    if secure:
        port = server.add_secure_port("localhost:0", server_credentials("certs/sp1"))
        channel = secure_channel(f"localhost:{port}", "certs/sp1")
    else:
        port = server.add_insecure_port("localhost:0")
        channel = grpc.insecure_channel(f"localhost:{port}")
    server.start()
    stub = rpc.PeerStub(channel)
    stub.ReadBlock(pb.BlockRequest(block_id="0" * 32), timeout=10)           # handshake fuera de la medición
    start = time.perf_counter()
    for _ in range(BLOCKS):
        stub.ReadBlock(pb.BlockRequest(block_id="0" * 32), timeout=10)
    seconds = time.perf_counter() - start
    channel.close()
    server.stop(0)
    return BLOCKS / seconds


def snapshot_cost(file_count):
    inner = DictRegistry()
    directories = max(1, file_count // 100)
    for number in range(directories):
        inner.make_directory("u", f"/d{number}", 1.0)
    for number in range(file_count):
        inner.create_file("u", f"/d{number % directories}/f{number}", 1.0)
    registry = PersistentRegistry(inner, tempfile.mkdtemp())
    start = time.perf_counter()
    registry._take_snapshot()
    snapshot_ms = (time.perf_counter() - start) * 1000
    size_mb = os.path.getsize(registry._snapshot_path) / 1e6
    start = time.perf_counter()
    registry.make_directory("u", "/otro", 1.0)
    operation_ms = (time.perf_counter() - start) * 1000
    return snapshot_ms, size_mb, operation_ms


def main():
    directory = tempfile.mkdtemp()
    plain = DiskBlockStorage(os.path.join(directory, "plain"))
    encrypted = EncryptedBlockStorage(DiskBlockStorage(os.path.join(directory, "encrypted")), os.urandom(32))
    print(f"### Cifrado en reposo ({BLOCKS} bloques de 1 MiB, con fsync)\n")
    print("| Almacenamiento | escritura (MiB/s) | lectura (MiB/s) |")
    print("|---|---|---|")
    for label, storage in [("disco, sin cifrar", plain), ("disco + AES-256-GCM", encrypted)]:
        write_rate, read_rate = storage_throughput(storage)
        print(f"| {label} | {write_rate:.0f} | {read_rate:.0f} |", flush=True)

    print(f"\n### Canal gRPC ({BLOCKS} bloques de 1 MiB, un cliente, sin disco)\n")
    print("| Canal | MiB/s |")
    print("|---|---|")
    for label, secure in [("sin TLS", False), ("mTLS", True)]:
        print(f"| {label} | {channel_throughput(secure):.0f} |", flush=True)

    print("\n### Persistencia del registro\n")
    print("| Archivos | snapshot (ms) | tamaño (MB) | una modificación con fsync (ms) |")
    print("|---|---|---|---|")
    for file_count in [1_000, 10_000, 50_000]:
        snapshot_ms, size_mb, operation_ms = snapshot_cost(file_count)
        print(f"| {file_count:,} | {snapshot_ms:.0f} | {size_mb:.2f} | {operation_ms:.1f} |", flush=True)


if __name__ == "__main__":
    main()
