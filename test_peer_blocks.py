"""
Prueba de la API de bloques de los peers (WriteBlock, ReadBlock, DeleteBlock), hablando directo
con cada peer: sin FUSE y sin pasar por el registro del superpeer.

Se ejecuta dentro de un contenedor (necesita grpc y el código generado), desde WSL:
  docker compose exec sp1 python test_peer_blocks.py
Opciones: --peers peer1:7100,peer2:7100,peer3:7100   --superpeer localhost:7000
Al terminar borra los bloques que escribió.
"""

import argparse
import errno
import hashlib
import os
import sys
import time
import uuid

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from errors import errno_from_rpc_error

BLOCK_SIZE_BYTES = 1024 * 1024
RPC_TIMEOUT_SECONDS = 5
HEARTBEAT_WAIT_SECONDS = 4          # un poco más que el intervalo de heartbeat (3 s)

results = []    # True o False por cada caso


def report(case_name, passed, detail):
    if passed:
        status = "PASS"
    else:
        status = "FAIL"
    print(f"[{status}] {case_name}: {detail}")
    results.append(passed)


def new_block_id():
    """Un ID con la misma forma que los que genera el registro (uuid4().hex)."""
    return uuid.uuid4().hex


def short_hash(data):
    return hashlib.sha256(data).hexdigest()[:12]


def call_and_get_error(rpc_call):
    """Ejecuta rpc_call(). Devuelve (None, None) si funcionó, o (código gRPC, nombre del errno) si falló."""
    try:
        rpc_call()
        return None, None
    except grpc.RpcError as rpc_error:
        error_number = errno_from_rpc_error(rpc_error)
        return rpc_error.code().name, errno.errorcode.get(error_number, str(error_number))


def free_bytes_of(superpeer_stub, peer_id):
    """Espacio libre de peer_id según el último heartbeat que recibió el superpeer."""
    peer_list = superpeer_stub.ListPeers(pb.Empty(), timeout=RPC_TIMEOUT_SECONDS)
    for peer in peer_list.peers:
        if peer.peer_id == peer_id:
            return peer.free_bytes
    return None


def main():
    parser = argparse.ArgumentParser(description="Prueba de WriteBlock/ReadBlock/DeleteBlock en los peers")
    parser.add_argument("--peers", default="peer1:7100,peer2:7100,peer3:7100",
                        help="direcciones de los peers, separadas por comas")
    parser.add_argument("--superpeer", default="localhost:7000")
    arguments = parser.parse_args()

    peer_addresses = arguments.peers.split(",")
    peer_stubs = {}
    for peer_address in peer_addresses:
        peer_stubs[peer_address] = rpc.PeerStub(grpc.insecure_channel(peer_address))
    first_address = peer_addresses[0]
    first_peer = peer_stubs[first_address]
    blocks_to_clean_up = []        # (dirección, block_id) escritos durante la prueba

    # 1. Escribir y leer un bloque completo de 1 MiB
    block_id = new_block_id()
    data = os.urandom(BLOCK_SIZE_BYTES)
    first_peer.WriteBlock(pb.WriteBlockRequest(block_id=block_id, data=data), timeout=RPC_TIMEOUT_SECONDS)
    blocks_to_clean_up.append((first_address, block_id))
    read_data = first_peer.ReadBlock(pb.BlockRequest(block_id=block_id), timeout=RPC_TIMEOUT_SECONDS).data
    report("1. write + read 1 MiB", read_data == data,
           f"{first_address}: wrote {short_hash(data)}, read {short_hash(read_data)}")

    # 2. Reintentar la misma escritura no es error y deja los mismos bytes
    first_peer.WriteBlock(pb.WriteBlockRequest(block_id=block_id, data=data), timeout=RPC_TIMEOUT_SECONDS)
    read_again = first_peer.ReadBlock(pb.BlockRequest(block_id=block_id), timeout=RPC_TIMEOUT_SECONDS).data
    report("2. retried write is safe", read_again == data, "same bytes after writing the block twice")

    # 3. Un bloque que el peer no tiene
    missing_block_id = new_block_id()
    grpc_code, errno_name = call_and_get_error(
        lambda: first_peer.ReadBlock(pb.BlockRequest(block_id=missing_block_id), timeout=RPC_TIMEOUT_SECONDS))
    report("3. read a missing block", errno_name == "ENOENT", f"{grpc_code} / {errno_name}")

    # 4. Un block_id que intenta salirse de la carpeta de datos
    grpc_code, errno_name = call_and_get_error(
        lambda: first_peer.ReadBlock(pb.BlockRequest(block_id="../../etc/passwd"), timeout=RPC_TIMEOUT_SECONDS))
    report("4. path traversal in block_id", errno_name == "EINVAL", f"{grpc_code} / {errno_name}")

    # 5. Borrar: después ya no se puede leer, y borrar otra vez no es error
    first_peer.DeleteBlock(pb.BlockRequest(block_id=block_id), timeout=RPC_TIMEOUT_SECONDS)
    grpc_code, errno_name = call_and_get_error(
        lambda: first_peer.ReadBlock(pb.BlockRequest(block_id=block_id), timeout=RPC_TIMEOUT_SECONDS))
    second_delete_code, second_delete_errno = call_and_get_error(
        lambda: first_peer.DeleteBlock(pb.BlockRequest(block_id=block_id), timeout=RPC_TIMEOUT_SECONDS))
    report("5. delete, then read and delete again", errno_name == "ENOENT" and second_delete_code is None,
           f"read after delete: {errno_name}, second delete: {'ok' if second_delete_code is None else second_delete_code}")

    # 6. El mismo bloque en todos los peers (como réplicas) se lee igual desde cada uno
    replica_block_id = new_block_id()
    replica_data = os.urandom(BLOCK_SIZE_BYTES // 2)
    all_replicas_match = True
    for peer_address in peer_addresses:
        peer_stubs[peer_address].WriteBlock(pb.WriteBlockRequest(block_id=replica_block_id, data=replica_data),
                                            timeout=RPC_TIMEOUT_SECONDS)
        blocks_to_clean_up.append((peer_address, replica_block_id))
    for peer_address in peer_addresses:
        replica_read = peer_stubs[peer_address].ReadBlock(pb.BlockRequest(block_id=replica_block_id),
                                                          timeout=RPC_TIMEOUT_SECONDS).data
        if replica_read != replica_data:
            all_replicas_match = False
    report("6. same block on every peer", all_replicas_match, f"{len(peer_addresses)} replicas of {short_hash(replica_data)}")

    # 7. El espacio libre que informa el heartbeat baja al guardar bloques
    superpeer_stub = rpc.SuperpeerStub(grpc.insecure_channel(arguments.superpeer))
    first_peer_id = first_address.split(":")[0]           # en Compose, el nombre es el peer_id
    free_before = free_bytes_of(superpeer_stub, first_peer_id)
    for block_number in range(3):
        extra_block_id = new_block_id()
        first_peer.WriteBlock(pb.WriteBlockRequest(block_id=extra_block_id, data=os.urandom(BLOCK_SIZE_BYTES)),
                              timeout=RPC_TIMEOUT_SECONDS)
        blocks_to_clean_up.append((first_address, extra_block_id))
    time.sleep(HEARTBEAT_WAIT_SECONDS)
    free_after = free_bytes_of(superpeer_stub, first_peer_id)
    if free_before is None or free_after is None:
        report("7. heartbeat reports less free space", False, f"{first_peer_id} not found in ListPeers")
    else:
        dropped_megabytes = (free_before - free_after) / BLOCK_SIZE_BYTES
        report("7. heartbeat reports less free space", dropped_megabytes >= 3,
               f"{first_peer_id} free space dropped {dropped_megabytes:.1f} MiB after writing 3 blocks")

    # 8. Límite de gRPC: un mensaje de más de 4 MB se rechaza antes de llegar al peer
    grpc_code, errno_name = call_and_get_error(
        lambda: first_peer.WriteBlock(pb.WriteBlockRequest(block_id=new_block_id(), data=os.urandom(5 * BLOCK_SIZE_BYTES)),
                                      timeout=RPC_TIMEOUT_SECONDS))
    report("8. 5 MiB message is rejected by gRPC", grpc_code == "RESOURCE_EXHAUSTED", f"{grpc_code} (errno {errno_name})")

    # Limpieza
    for peer_address, cleanup_block_id in blocks_to_clean_up:
        peer_stubs[peer_address].DeleteBlock(pb.BlockRequest(block_id=cleanup_block_id), timeout=RPC_TIMEOUT_SECONDS)

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
