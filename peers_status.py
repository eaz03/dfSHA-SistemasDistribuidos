"""
Muestra la tabla de peers del superpeer (RPC ListPeers).

Uso, desde WSL en la carpeta del proyecto:
  docker compose exec sp1 python peers_status.py           tabla legible
  docker compose exec sp1 python peers_status.py --json    JSON, para scripts (lo usa test_peers.py)
"""

import argparse
import json

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc

BYTES_PER_MB = 1024 * 1024


def fetch_peers(superpeer_address):
    """Pide ListPeers al superpeer y devuelve la lista de pb.PeerStatus."""
    with grpc.insecure_channel(superpeer_address) as channel:
        superpeer_stub = rpc.SuperpeerStub(channel)
        peer_list = superpeer_stub.ListPeers(pb.Empty(), timeout=3)
    return peer_list.peers


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
    parser.add_argument("--superpeer", default="localhost:7000",
                        help="dirección del superpeer (por defecto el de este mismo contenedor)")
    parser.add_argument("--json", action="store_true", help="salida en JSON")
    arguments = parser.parse_args()

    peers = fetch_peers(arguments.superpeer)
    if arguments.json:
        print_json(peers)
    else:
        print_table(peers)
