"""
Herramienta de línea de comandos para DFSha, sin FUSE: usa client.py para hablar por gRPC
con el superpeer y con los peers.

TEMPORAL: sirve para tener archivos con contenido mientras el mount no escribe.
Se borra cuando el mount escriba (ver PLAN.md).

Uso, desde WSL (dentro de los contenedores, la carpeta del proyecto se ve en /app):
  docker compose exec sp1 python dfs_cli.py put /app/README.md /readme.md     subir un archivo local
  docker compose exec sp1 python dfs_cli.py get /readme.md /tmp/readme.md     bajar un archivo
  docker compose exec sp1 python dfs_cli.py blocks /readme.md                 ver sus bloques y réplicas
Opción: --superpeer sp1:7000 (varias direcciones separadas por comas).
"""

import argparse
import os
import sys

import grpc

from client import DFSClient
from errors import FileSystemError, errno_from_rpc_error

MEBIBYTE = 1024 * 1024
EXPECTED_REPLICAS = 2      # el factor de replicación del registro: avisar si un bloque quedó con menos


def command_put(client, local_path, remote_path):
    """Sube un archivo local a DFSha y avisa si algún bloque quedó con menos réplicas de las esperadas."""
    committed_blocks = client.upload_from_file(remote_path, local_path)
    file_size = os.path.getsize(local_path)
    print(f"{local_path} -> {remote_path}: {file_size} bytes in {len(committed_blocks)} blocks")

    under_replicated_count = 0
    for block_location in committed_blocks:
        if len(block_location.replicas) < EXPECTED_REPLICAS:
            under_replicated_count += 1
    if under_replicated_count > 0:
        print(f"warning: {under_replicated_count} block(s) stored with fewer than {EXPECTED_REPLICAS} replicas")


def command_get(client, remote_path, local_path):
    """Baja un archivo de DFSha a un archivo local."""
    written_bytes = client.download_to_file(remote_path, local_path)
    print(f"{remote_path} -> {local_path}: {written_bytes} bytes")


def command_blocks(client, remote_path):
    """Muestra los bloques del archivo y en qué peers está cada uno, marcando los peers caídos."""
    file_attributes = client.stat(remote_path)
    block_list = client.get_block_locations(remote_path)

    peer_is_alive = {}
    for peer in client.list_peers():
        peer_is_alive[peer.peer_id] = peer.alive

    block_size_mebibytes = block_list.block_size / MEBIBYTE
    print(f"{remote_path}: {file_attributes.size} bytes, {len(block_list.blocks)} blocks of {block_size_mebibytes:g} MiB")
    for block_number, block_location in enumerate(block_list.blocks):
        replica_descriptions = []
        for replica in block_location.replicas:
            if peer_is_alive.get(replica.peer_id, False):
                replica_descriptions.append(replica.peer_id)
            else:
                replica_descriptions.append(f"{replica.peer_id} (down)")
        print(f"  {block_number:>3}  {block_location.block_id[:8]}..  {block_location.size:>9}  "
              f"{', '.join(replica_descriptions)}")


def main():
    parser = argparse.ArgumentParser(description="DFSha sin FUSE: put / get / blocks")
    parser.add_argument("--superpeer", default="sp1:7000", help="dirección del superpeer (varias, separadas por comas)")
    subcommands = parser.add_subparsers(dest="command", required=True)

    put_parser = subcommands.add_parser("put", help="subir un archivo local")
    put_parser.add_argument("local_path")
    put_parser.add_argument("remote_path")

    get_parser = subcommands.add_parser("get", help="bajar un archivo")
    get_parser.add_argument("remote_path")
    get_parser.add_argument("local_path")

    blocks_parser = subcommands.add_parser("blocks", help="ver los bloques de un archivo y sus réplicas")
    blocks_parser.add_argument("remote_path")

    arguments = parser.parse_args()

    try:
        client = DFSClient(arguments.superpeer.split(","))
        if arguments.command == "put":
            command_put(client, arguments.local_path, arguments.remote_path)
        elif arguments.command == "get":
            command_get(client, arguments.remote_path, arguments.local_path)
        elif arguments.command == "blocks":
            command_blocks(client, arguments.remote_path)
    except grpc.RpcError as rpc_error:
        error_number = errno_from_rpc_error(rpc_error)
        print(f"error: {os.strerror(error_number)} ({rpc_error.details()})", file=sys.stderr)
        sys.exit(1)
    except FileSystemError as file_system_error:
        print(f"error: {file_system_error}", file=sys.stderr)
        sys.exit(1)
    except (ConnectionError, OSError) as local_error:
        print(f"error: {local_error}", file=sys.stderr)      # superpeer inalcanzable o archivo local inválido
        sys.exit(1)


if __name__ == "__main__":
    main()
