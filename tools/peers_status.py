"""
Muestra la tabla de peers del superpeer (RPC ListPeers).

Uso, desde WSL en la carpeta del proyecto:
  docker compose exec client python -m tools.peers_status           tabla legible
  docker compose exec client python -m tools.peers_status --json    JSON, para scripts (lo usa test_peers.py)
ListPeers pide token: usa las credenciales del montaje (DFSHA_USER, DFSHA_PASSWORD, DFSHA_TLS_DIR).
"""

import argparse
import json

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.client.auth_client import authenticated_superpeer_stub, credentials_from_environment, superpeers_from_environment

BYTES_PER_MB = 1024 * 1024


def fetch_peers(superpeer_addresses):
    """
    Pide ListPeers al primer superpeer de la lista que responda y devuelve la lista de pb.PeerStatus
    (cada superpeer tiene su propia tabla, pero los peers se registran en todos: son iguales).
    """
    username, password, tls_dir = credentials_from_environment()
    last_error = None
    for superpeer_address in superpeer_addresses:
        try:
            superpeer_stub = authenticated_superpeer_stub(superpeer_address, username, password, tls_dir)
            return superpeer_stub.ListPeers(pb.Empty(), timeout=3).peers
        except grpc.RpcError as rpc_error:
            last_error = rpc_error                     # caído: se prueba el siguiente
    raise last_error


def print_table(peers):
    if len(peers) == 0:
        print("no peers registered")
        return
    print(f"{'PEER':<10} {'ADDRESS':<22} {'ALIVE':<6} {'FREE MB':>8} {'CAPACITY MB':>12} {'LAST HEARTBEAT':>15}")
    for peer in peers:
        if peer.alive:
            alive_text = "yes"
        else:
            alive_text = "NO"
        free_megabytes = peer.free_bytes // BYTES_PER_MB
        capacity_megabytes = peer.capacity_bytes // BYTES_PER_MB
        last_heartbeat_text = f"{peer.seconds_since_heartbeat:.1f} s ago"
        print(f"{peer.peer_id:<10} {peer.address:<22} {alive_text:<6} {free_megabytes:>8} "
              f"{capacity_megabytes:>12} {last_heartbeat_text:>15}")


def print_json(peers):
    peer_dictionaries = []
    for peer in peers:
        peer_dictionary = {
            "peer_id": peer.peer_id,
            "address": peer.address,
            "capacity_bytes": peer.capacity_bytes,
            "free_bytes": peer.free_bytes,
            "seconds_since_heartbeat": peer.seconds_since_heartbeat,
            "alive": peer.alive,
        }
        peer_dictionaries.append(peer_dictionary)
    print(json.dumps(peer_dictionaries))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tabla de peers del superpeer")
    parser.add_argument("--superpeer", default=",".join(superpeers_from_environment()),
                        help="direcciones de los superpeers, separadas por comas: se usa el primero que responda")
    parser.add_argument("--json", action="store_true", help="salida en JSON")
    arguments = parser.parse_args()

    peers = fetch_peers(arguments.superpeer.split(","))
    if arguments.json:
        print_json(peers)
    else:
        print_table(peers)
