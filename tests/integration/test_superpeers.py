"""
Prueba de la federación de superpeers sobre los contenedores de Docker Compose (sp1 y sp2):
los dos atienden usuarios a la vez, sus copias de cada log coinciden, y el sistema sigue
funcionando cuando cae cualquiera de los dos, con el montaje FUSE en uso.

Se ejecuta fuera de los contenedores, desde la raíz del repositorio, con todo levantado:
  python3 -m tests.integration.test_superpeers
Solo usa la biblioteca estándar. El usuario del montaje es el del .env (su home es sp1, el primero de
la lista); la prueba crea además un usuario temporal con home sp2 y lo borra al terminar.
Tarda alrededor de un minuto y medio. Al terminar deja todos los contenedores corriendo.

Casos:
  1. Los dos superpeers atienden a la vez: cada usuario en el de su home.
  2. Las copias coinciden: la posición del log de cada usuario es la misma en sp1 y sp2.
  3. Failover con el montaje en uso: se detiene sp1 y el montaje sigue leyendo y escribiendo con sp2.
  4. Regreso de sp1: se pone al día, recupera a sus usuarios y tiene lo escrito durante su ausencia.
  5. Caída del respaldo: con sp2 detenido, sp1 atiende también a los usuarios de sp2; al volver, sp2
     se pone al día y recupera a los suyos.
"""

import json
import os
import secrets
import subprocess
import sys
import time

MOUNT = "/mnt/dfsha"
TEST_DIRECTORY = f"{MOUNT}/test-superpeers-{int(time.time())}"
WAIT_SECONDS = 40

results = []    # True o False por cada caso


def report(case_name, passed, detail):
    results.append(passed)
    print(f"[{'PASS' if passed else 'FAIL'}] {case_name}: {detail}", flush=True)


def compose(*arguments, stdin=None, check=True):
    environment = dict(os.environ, MSYS_NO_PATHCONV="1")       # Git Bash en Windows: no tocar las rutas
    # La entrada va como bytes: en modo texto, Windows convierte cada salto de línea en CR LF, y una
    # contraseña enviada así quedaría guardada con un retorno de carro (CR) invisible al final
    result = subprocess.run(["docker", "compose"] + list(arguments),
                            input=None if stdin is None else stdin.encode("utf-8"),
                            capture_output=True, env=environment)
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    if check and result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace").strip() or stdout)
    return stdout


def in_client(shell_command, environment=()):
    """Ejecuta un comando de shell en el contenedor client (el que tiene el montaje)."""
    variables = []
    for variable in ("GRPC_VERBOSITY=NONE",) + tuple(environment):
        variables += ["-e", variable]
    return compose("exec", "-T", *variables, "client", "sh", "-c", shell_command)


def views():
    """{id del superpeer que responde: su vista}, según ListSuperpeers (los caídos no aparecen)."""
    output = compose("exec", "-T", "-e", "GRPC_VERBOSITY=NONE", "client", "python", "-m", "tools.superpeers_status",
                     "--json", check=False)
    try:
        return {view["superpeer_id"]: view for view in json.loads(output)}
    except ValueError:
        return {}


def served_by(view, user):
    """El superpeer que atiende a user según view, o None."""
    for superpeer in view["superpeers"]:
        if user in superpeer["served_users"]:
            return superpeer["superpeer_id"]
    return None


def positions_match(users):
    current = views()
    if set(current) != {"sp1", "sp2"}:
        return False
    return all(current["sp1"]["positions"].get(user) == current["sp2"]["positions"].get(user) for user in users)


def wait_until(condition, timeout_seconds=WAIT_SECONDS):
    """Segundos que tardó condition() en ser verdadera, o None si no ocurrió a tiempo."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        try:
            if condition():
                return time.monotonic() - start
        except RuntimeError:
            pass
        time.sleep(0.5)
    return None


def main():
    mount_user = in_client("printenv DFSHA_USER")
    other_user = "test-sp2-" + secrets.token_hex(3)
    other_password = secrets.token_urlsafe(12)
    compose("exec", "-T", "sp1", "python", "-m", "tools.manage_users", "--file", "/data/users.json", "add",
            other_user, "--home", "sp2", stdin=f"{other_password}\n{other_password}\n")
    other_env = (f"DFSHA_USER={other_user}", f"DFSHA_PASSWORD={other_password}")
    try:
        run_cases(mount_user, other_user, other_env)
    finally:
        compose("start", "sp1", "sp2", check=False)
        wait_until(lambda: set(views()) == {"sp1", "sp2"})
        in_client(f"rm -rf {TEST_DIRECTORY}")
        compose("exec", "-T", "sp1", "python", "-m", "tools.manage_users", "--file", "/data/users.json",
                "remove", other_user, check=False)

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


def run_cases(mount_user, other_user, other_env):
    users = [mount_user, other_user]

    # 1. Cada usuario en el superpeer de su home (el nuevo, cuando sp2 ya recibió su cuenta)
    in_client(f"mkdir -p {TEST_DIRECTORY} && head -c 3000000 /dev/urandom > /tmp/a.bin && cp /tmp/a.bin {TEST_DIRECTORY}/a.bin")
    # Los superpeers se pasan los usuarios por sus heartbeats: hasta que sp2 conozca la cuenta nueva,
    # un login que llegue a sp2 la rechaza. Se espera a que sp2 la atienda (y se mide cuánto tarda)
    propagation = wait_until(lambda: served_by(views().get("sp2", {"superpeers": []}), other_user) == "sp2")
    print(f"(the new user reached sp2 in {propagation if propagation is None else round(propagation, 1)} s)", flush=True)
    in_client("echo hola > /tmp/o.txt && python -m dfsha.client.dfs_cli put /tmp/o.txt /o.txt", other_env)
    view = views()["sp1"]
    owners = (served_by(view, mount_user), served_by(view, other_user))
    report("1. both superpeers serve users at the same time", owners == ("sp1", "sp2"),
           f"{mount_user} -> {owners[0]}, {other_user} -> {owners[1]}")

    # 2. Las dos copias de cada log van en la misma posición
    match = wait_until(lambda: positions_match(users), 10) is not None
    positions = {user: views()["sp1"]["positions"].get(user) for user in users}
    report("2. both copies of each log are at the same position", match, f"{positions}")

    # 3. Failover: sp1 se detiene con el montaje en uso
    compose("stop", "sp1")
    stopped_at = time.monotonic()
    read_ok = wait_until(lambda: in_client(f"cmp /tmp/a.bin {TEST_DIRECTORY}/a.bin && echo same") == "same")
    failover_seconds = time.monotonic() - stopped_at
    in_client(f"head -c 2000000 /dev/urandom > /tmp/b.bin && cp /tmp/b.bin {TEST_DIRECTORY}/b.bin")
    write_ok = in_client(f"cmp /tmp/b.bin {TEST_DIRECTORY}/b.bin && echo same") == "same"
    serving = served_by(views().get("sp2", {"superpeers": []}), mount_user)
    report("3. failover with the mount in use", read_ok is not None and write_ok and serving == "sp2",
           f"read again {failover_seconds:.1f} s after stopping sp1, write ok: {write_ok}, {mount_user} now served by {serving}")

    # 4. sp1 vuelve: se pone al día y recupera a su usuario
    compose("start", "sp1")
    back = wait_until(lambda: served_by(views().get("sp1", {"superpeers": []}), mount_user) == "sp1"
                      and positions_match(users))
    sees_new_file = in_client(f"cmp /tmp/b.bin {TEST_DIRECTORY}/b.bin && echo same") == "same"
    report("4. sp1 comes back, catches up and serves its users again", back is not None and sees_new_file,
           f"caught up in {back if back is None else round(back, 1)} s, file written while it was down: {sees_new_file}")

    # 5. Cae el respaldo (sp2): sp1 atiende también al usuario de sp2; al volver, sp2 se pone al día
    compose("stop", "sp2")
    in_client(f"echo durante > {TEST_DIRECTORY}/c.txt")
    other_ok = wait_until(lambda: in_client("python -m dfsha.client.dfs_cli get /o.txt /tmp/o2.txt && cat /tmp/o2.txt",
                                            other_env).endswith("hola"))
    taken_over = served_by(views().get("sp1", {"superpeers": []}), other_user)
    in_client("echo nuevo > /tmp/o3.txt && python -m dfsha.client.dfs_cli put /tmp/o3.txt /o3.txt", other_env)
    compose("start", "sp2")
    caught_up = wait_until(lambda: served_by(views().get("sp2", {"superpeers": []}), other_user) == "sp2"
                           and positions_match(users))
    report("5. backup down: the other superpeer covers, then sp2 catches up",
           other_ok is not None and taken_over == "sp1" and caught_up is not None,
           f"{other_user} served by {taken_over} while sp2 was down; sp2 caught up and took it back: {caught_up is not None}")


if __name__ == "__main__":
    main()
