"""
Prueba de los RPC de bloques del superpeer (AllocateBlocks, CommitFile, GetBlockLocations),
haciendo a mano lo que después hará el cliente: pedir el plan, subir cada bloque a sus peers,
hacer commit y volver a leer el archivo con las ubicaciones que entrega el superpeer.

Se ejecuta dentro de un contenedor, desde WSL, con los peers levantados:
  docker compose exec client python -m tests.test_superpeer_blocks
Al terminar borra los archivos y los bloques que creó.
"""

import argparse
import errno
import hashlib
import os
import sys
import uuid

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.client.auth_client import authenticated_superpeer_stub, credentials_from_environment
from dfsha.common.tls import secure_channel
from dfsha.common.errors import errno_from_rpc_error

MEBIBYTE = 1024 * 1024
RPC_TIMEOUT_SECONDS = 5

results = []    # True o False por cada caso


def report(case_name, passed, detail):
    if passed:
        status = "PASS"
    else:
        status = "FAIL"
    print(f"[{status}] {case_name}: {detail}")
    results.append(passed)


def errno_name_of(rpc_call):
    """Ejecuta rpc_call() y devuelve el nombre del errno con que falló, o 'ok' si no falló."""
    try:
        rpc_call()
        return "ok"
    except grpc.RpcError as rpc_error:
        return errno.errorcode.get(errno_from_rpc_error(rpc_error), "?")


class PeerConnections:
    """
    Un canal por dirección de peer, creado la primera vez que se usa.
    Con el certificado de sp1: la limpieza usa DeleteBlock, que un peer solo acepta de un superpeer.
    """

    def __init__(self, tls_dir):
        self._stubs = {}
        self._tls_dir = tls_dir

    def stub(self, address):
        if address not in self._stubs:
            self._stubs[address] = rpc.PeerStub(secure_channel(address, self._tls_dir))
        return self._stubs[address]


def upload(superpeer, peers, path, data, replicas_to_write=None):
    """
    Sube data como el contenido de path: AllocateBlocks, WriteBlock a cada réplica, CommitFile.
    replicas_to_write: si se da, escribe solo esa cantidad de réplicas por bloque (simula peers caídos).
    Devuelve el plan (BlockList) que entregó el superpeer.
    """
    plan = superpeer.AllocateBlocks(pb.AllocateRequest(path=path, size=len(data)), timeout=RPC_TIMEOUT_SECONDS)
    committed_blocks = []
    offset = 0
    for planned_block in plan.blocks:
        block_data = data[offset:offset + planned_block.size]
        offset += planned_block.size
        replicas = list(planned_block.replicas)
        if replicas_to_write is not None:
            replicas = replicas[:replicas_to_write]
        for replica in replicas:
            write_request = pb.WriteBlockRequest(block_id=planned_block.block_id, data=block_data,
                                                 access_token=planned_block.access_token)
            peers.stub(replica.address).WriteBlock(write_request, timeout=RPC_TIMEOUT_SECONDS)
        committed_block = pb.BlockLocation(block_id=planned_block.block_id, size=planned_block.size, replicas=replicas)
        committed_blocks.append(committed_block)
    commit_request = pb.CommitRequest(path=path, size=len(data), blocks=committed_blocks)
    superpeer.CommitFile(commit_request, timeout=RPC_TIMEOUT_SECONDS)
    return plan


def download(superpeer, peers, path):
    """Lee path completo: GetBlockLocations y ReadBlock de cada bloque desde su primera réplica."""
    locations = superpeer.GetBlockLocations(pb.PathRequest(path=path), timeout=RPC_TIMEOUT_SECONDS)
    pieces = []
    for block_location in locations.blocks:
        first_replica = block_location.replicas[0]
        read_request = pb.BlockRequest(block_id=block_location.block_id, access_token=block_location.access_token)
        pieces.append(peers.stub(first_replica.address).ReadBlock(read_request, timeout=RPC_TIMEOUT_SECONDS).data)
    return b"".join(pieces), locations


def short_hash(data):
    return hashlib.sha256(data).hexdigest()[:12]


def main():
    parser = argparse.ArgumentParser(description="Prueba de AllocateBlocks/CommitFile/GetBlockLocations")
    parser.add_argument("--superpeer", default="sp1:7000")
    parser.add_argument("--peer-tls-dir", default="certs/sp1")
    arguments = parser.parse_args()

    superpeer = authenticated_superpeer_stub(arguments.superpeer, *credentials_from_environment())
    peers = PeerConnections(arguments.peer_tls_dir)
    test_directory = "/test-blocks-" + uuid.uuid4().hex[:6]
    superpeer.MakeDir(pb.PathRequest(path=test_directory), timeout=RPC_TIMEOUT_SECONDS)
    big_path = test_directory + "/big.bin"
    subset_path = test_directory + "/subset.bin"
    empty_path = test_directory + "/empty.bin"

    # 1. El plan: 2.5 MiB -> 3 bloques [1, 1, 0.5] MiB, cada uno con 2 réplicas en peers distintos
    big_data = os.urandom(int(2.5 * MEBIBYTE))
    superpeer.CreateFile(pb.PathRequest(path=big_path), timeout=RPC_TIMEOUT_SECONDS)
    plan = upload(superpeer, peers, big_path, big_data)
    block_sizes = []
    replica_ids = []
    for planned_block in plan.blocks:
        block_sizes.append(planned_block.size)
        peer_ids_of_block = []
        for replica in planned_block.replicas:
            peer_ids_of_block.append(replica.peer_id)
        replica_ids.append(peer_ids_of_block)
    sizes_ok = block_sizes == [MEBIBYTE, MEBIBYTE, MEBIBYTE // 2] and plan.block_size == MEBIBYTE
    replicas_ok = True
    for peer_ids_of_block in replica_ids:
        if len(peer_ids_of_block) != 2 or len(set(peer_ids_of_block)) != 2:
            replicas_ok = False
    report("1. AllocateBlocks plan", sizes_ok and replicas_ok, f"sizes {block_sizes}, replicas {replica_ids}")

    # 2. Después del commit, Stat muestra el tamaño real
    size_after_commit = superpeer.Stat(pb.PathRequest(path=big_path), timeout=RPC_TIMEOUT_SECONDS).size
    report("2. CommitFile + Stat", size_after_commit == len(big_data), f"size {size_after_commit}")

    # 3. GetBlockLocations + ReadBlock devuelve exactamente los bytes subidos
    downloaded, locations = download(superpeer, peers, big_path)
    report("3. GetBlockLocations + ReadBlock", downloaded == big_data,
           f"uploaded {short_hash(big_data)}, downloaded {short_hash(downloaded)}, {len(locations.blocks)} blocks")

    # 4. Cada réplica tiene una dirección para conectarse
    addresses_ok = True
    for block_location in locations.blocks:
        for replica in block_location.replicas:
            if replica.address == "":
                addresses_ok = False
    first_block_replicas = []
    for replica in locations.blocks[0].replicas:
        first_block_replicas.append(f"{replica.peer_id}@{replica.address}")
    report("4. replicas carry addresses", addresses_ok, f"block 0 -> {first_block_replicas}")

    # 5. Commit con solo una de las dos réplicas (como si un peer se cayera durante la subida)
    subset_data = os.urandom(1000)
    superpeer.CreateFile(pb.PathRequest(path=subset_path), timeout=RPC_TIMEOUT_SECONDS)
    upload(superpeer, peers, subset_path, subset_data, replicas_to_write=1)
    subset_downloaded, subset_locations = download(superpeer, peers, subset_path)
    report("5. commit with 1 of 2 replicas", subset_downloaded == subset_data and len(subset_locations.blocks[0].replicas) == 1,
           f"stored replicas: {len(subset_locations.blocks[0].replicas)}")

    # 6. Archivo vacío: plan vacío, commit vacío, lectura vacía
    superpeer.CreateFile(pb.PathRequest(path=empty_path), timeout=RPC_TIMEOUT_SECONDS)
    upload(superpeer, peers, empty_path, b"")
    empty_downloaded, empty_locations = download(superpeer, peers, empty_path)
    report("6. empty file", empty_downloaded == b"" and len(empty_locations.blocks) == 0, "0 blocks")

    # 7. Errores: cada uno con su errno
    invented_block = pb.BlockLocation(block_id=uuid.uuid4().hex, size=10,
                                      replicas=[pb.Replica(peer_id="peer1", address="peer1:7100")])
    error_cases = [
        ("locations of a directory", lambda: superpeer.GetBlockLocations(pb.PathRequest(path=test_directory)), "EISDIR"),
        ("locations of a missing file", lambda: superpeer.GetBlockLocations(pb.PathRequest(path=test_directory + "/nope")), "ENOENT"),
        ("allocate with negative size", lambda: superpeer.AllocateBlocks(pb.AllocateRequest(path=big_path, size=-1)), "EINVAL"),
        ("commit an invented block", lambda: superpeer.CommitFile(pb.CommitRequest(path=big_path, size=10, blocks=[invented_block])), "EINVAL"),
    ]
    for case_name, rpc_call, expected_errno in error_cases:
        obtained_errno = errno_name_of(rpc_call)
        report(f"7. {case_name}", obtained_errno == expected_errno, obtained_errno)

    # 8. Un commit fallido no cambia el archivo
    downloaded_again, _ = download(superpeer, peers, big_path)
    report("8. failed commit leaves the file intact", downloaded_again == big_data, short_hash(downloaded_again))

    # Limpieza: bloques (todavía no hay limpiador automático) y archivos
    for path in [big_path, subset_path]:
        cleanup_locations = superpeer.GetBlockLocations(pb.PathRequest(path=path), timeout=RPC_TIMEOUT_SECONDS)
        for block_location in cleanup_locations.blocks:
            for replica in block_location.replicas:
                peers.stub(replica.address).DeleteBlock(pb.BlockRequest(block_id=block_location.block_id),
                                                        timeout=RPC_TIMEOUT_SECONDS)
    for path in [big_path, subset_path, empty_path]:
        superpeer.Delete(pb.PathRequest(path=path), timeout=RPC_TIMEOUT_SECONDS)
    superpeer.RemoveDir(pb.PathRequest(path=test_directory), timeout=RPC_TIMEOUT_SECONDS)

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
