"""
Muestra la federación de superpeers (RPC ListSuperpeers): quién está vivo y listo, a qué usuarios
atiende cada uno, y hasta dónde va el log de cada usuario en cada superpeer.

Uso, desde WSL en la carpeta del proyecto:
  docker compose exec client python -m tools.superpeers_status           tabla legible
  docker compose exec client python -m tools.superpeers_status --json    JSON, para scripts (lo usa test_superpeers.py)
Le pregunta a cada superpeer de la lista (DFSHA_SUPERPEERS, o sp1:7000,sp2:7000); los caídos se omiten.
ListSuperpeers pide token: usa las credenciales del montaje (DFSHA_USER, DFSHA_PASSWORD, DFSHA_TLS_DIR).
"""

import argparse
import json

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.client.auth_client import authenticated_superpeer_stub, credentials_from_environment, superpeers_from_environment


def fetch_views(superpeer_addresses):
    """La pb.SuperpeerView de cada superpeer que responde, en el orden de la lista."""
    username, password, tls_dir = credentials_from_environment()
    views = []
    for superpeer_address in superpeer_addresses:
        try:
            superpeer_stub = authenticated_superpeer_stub(superpeer_address, username, password, tls_dir)
            views.append(superpeer_stub.ListSuperpeers(pb.Empty(), timeout=3))
        except grpc.RpcError:
            continue                                   # caído o inalcanzable: se omite
    return views


def view_to_dictionary(view):
    superpeers = []
    for status in view.superpeers:
        superpeers.append({"superpeer_id": status.superpeer_id, "address": status.address, "alive": status.alive,
                           "ready": status.ready, "served_users": list(status.served_users)})
    positions = {}
    for position in view.positions:
        positions[position.user] = {"epoch": position.epoch, "epoch_owner": position.epoch_owner, "seq": position.seq}
    return {"superpeer_id": view.superpeer_id, "superpeers": superpeers, "positions": positions}


def print_views(views):
    if len(views) == 0:
        print("no superpeer answering")
        return
    for view in views:
        print(f"according to {view.superpeer_id}:")
        print(f"  {'SUPERPEER':<10} {'ADDRESS':<18} {'ALIVE':<6} {'READY':<6} SERVES")
        for status in view.superpeers:
            alive_text = "yes"
            if not status.alive:
                alive_text = "NO"
            ready_text = "yes"
            if not status.ready:
                ready_text = "no"
            print(f"  {status.superpeer_id:<10} {status.address:<18} {alive_text:<6} {ready_text:<6} "
                  f"{', '.join(status.served_users)}")
        for position in view.positions:
            epoch_owner_text = position.epoch_owner
            if epoch_owner_text == "":
                epoch_owner_text = "-"                 # época 0: datos de antes de la federación
            print(f"  log of {position.user}: epoch {position.epoch} ({epoch_owner_text}), seq {position.seq}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estado de la federación de superpeers")
    parser.add_argument("--superpeers", default=",".join(superpeers_from_environment()),
                        help="direcciones de los superpeers a consultar, separadas por comas")
    parser.add_argument("--json", action="store_true", help="salida en JSON")
    arguments = parser.parse_args()

    views = fetch_views(arguments.superpeers.split(","))
    if arguments.json:
        dictionaries = []
        for view in views:
            dictionaries.append(view_to_dictionary(view))
        print(json.dumps(dictionaries))
    else:
        print_views(views)
