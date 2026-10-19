"""
Administración de usuarios de DFSha desde la terminal.

    docker compose run --rm sp1 python -m tools.manage_users --file /data/users.json add sofi
    docker compose run --rm sp1 python -m tools.manage_users --file /data/users.json add luis --home sp2
    docker compose run --rm sp1 python -m tools.manage_users --file /data/users.json list
    docker compose run --rm sp1 python -m tools.manage_users --file /data/users.json remove sofi

--home dice qué superpeer atiende normalmente al usuario (sin --home, el primero de la lista).
Basta con crearlo en un superpeer: los superpeers se pasan los usuarios entre sí (federación).

La contraseña se pide con getpass (no se ve al escribirla) y nunca se recibe como argumento:
un argumento queda en el historial de la terminal y en la lista de procesos (ps).
"""

import argparse
import getpass
import sys

from dfsha.superpeer.json_user_store import JsonUserStore

DEFAULT_USERS_FILE = "/data/users.json"


def ask_new_password():
    """Pide la contraseña dos veces, para no crear un usuario con un error de tipeo."""
    password = getpass.getpass("Password: ")
    if getpass.getpass("Repeat password: ") != password:
        raise ValueError("passwords do not match")
    return password


def main():
    parser = argparse.ArgumentParser(description="Administración de usuarios de DFSha")
    parser.add_argument("--file", default=DEFAULT_USERS_FILE, help="archivo de usuarios")
    subcommands = parser.add_subparsers(dest="command", required=True)
    add_parser = subcommands.add_parser("add", help="crear un usuario")
    add_parser.add_argument("username")
    add_parser.add_argument("--home", default=None, help="id del superpeer que lo atiende normalmente, p. ej. sp2")
    subcommands.add_parser("remove", help="borrar un usuario").add_argument("username")
    subcommands.add_parser("list", help="listar los usuarios")
    arguments = parser.parse_args()

    store = JsonUserStore(arguments.file)
    try:
        if arguments.command == "add":
            store.add_user(arguments.username, ask_new_password(), home=arguments.home)
            print(f"user '{arguments.username}' created")
        elif arguments.command == "remove":
            store.remove_user(arguments.username)
            print(f"user '{arguments.username}' removed")
        elif arguments.command == "list":
            for username in store.list_users():
                print(username)
    except ValueError as error:
        sys.exit(f"error: {error}")     # mensaje claro y código de salida distinto de 0


if __name__ == "__main__":
    main()
