"""
Prueba de alta disponibilidad y consistencia ante fallas, sobre los contenedores de Docker Compose:
reinicio del superpeer (persistencia), lectura con un peer caído (réplica alternativa),
re-replicación y borrado de bloques obsoletos.

Se ejecuta fuera de los contenedores (en WSL o en la terminal del host), desde la raíz del repositorio:
  python3 -m tests.integration.test_failures
Solo usa la biblioteca estándar: controla los contenedores con 'docker compose'. Levanta el superpeer
con los tiempos cortos de compose.fast.yml y al terminar lo deja otra vez con los normales.
Usa las credenciales del contenedor client (las del .env). Tarda unos 2 minutos.
"""

import os
import subprocess
import sys
import time

COMPOSE_FILES = ["-f", "docker-compose.yml", "-f", "tests/integration/compose.fast.yml"]
PEERS = ["peer1", "peer2", "peer3"]
REMOTE_PATH = "/test-failures.bin"
DEAD_AFTER_SECONDS = 9          # 3 heartbeats perdidos (peer_tracker)
REPLICATION_DELAY_SECONDS = 10  # compose.fast.yml
DELETION_GRACE_SECONDS = 3      # compose.fast.yml

results = []    # True o False por cada caso


def report(case_name, passed, detail):
    results.append(passed)
    print(f"[{'PASS' if passed else 'FAIL'}] {case_name}: {detail}")


def compose(*arguments, files=COMPOSE_FILES, check=True):
    """Ejecuta 'docker compose <arguments>' y devuelve su salida."""
    command = ["docker", "compose"] + files + list(arguments)
    environment = dict(os.environ, MSYS_NO_PATHCONV="1")      # Git Bash en Windows: no tocar las rutas
    result = subprocess.run(command, capture_output=True, text=True, env=environment)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed: {result.stderr.strip()}")
    return result.stdout


def in_client(python_code):
    """Ejecuta python_code en el contenedor client y devuelve lo que imprime."""
    return compose("exec", "-T", "-e", "GRPC_VERBOSITY=NONE", "client", "python", "-c", python_code).strip()


CLIENT = ("from dfsha.client.client import DFSClient\n"
          "from dfsha.client.auth_client import credentials_from_environment\n"
          "client = DFSClient(['sp1:7000'], *credentials_from_environment())\n")


def blocks_of(path):
    """[(block_id, [peer_id, ...]), ...] del archivo, según el superpeer."""
    output = in_client(CLIENT + f"for b in client.get_block_locations({path!r}).blocks:\n"
                                "    print(b.block_id, ','.join(r.peer_id for r in b.replicas))")
    blocks = []
    for line in output.splitlines():
        # Justo después de reiniciar el superpeer su tabla de peers está vacía y los bloques
        # llegan sin réplicas (no sabe todavía la dirección de ningún peer)
        parts = line.split()
        blocks.append((parts[0], parts[1].split(",") if len(parts) > 1 else []))
    return blocks


def read_matches_original():
    """True si el archivo remoto se baja completo e idéntico al que se subió."""
    output = in_client(CLIENT + f"client.download_to_file({REMOTE_PATH!r}, '/tmp/test-failures.copy')\n"
                                "print(open('/tmp/test-failures.copy','rb').read() == open('/tmp/test-failures.orig','rb').read())")
    return output.endswith("True")


def has_block(peer, block_id):
    return subprocess.run(["docker", "compose", "exec", "-T", peer, "test", "-f", f"/data/{block_id}"],
                          capture_output=True).returncode == 0


def wait_until(condition, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if condition():
                return True
        except RuntimeError:
            pass                                               # el servicio todavía está arrancando
        time.sleep(1)
    return False


def main():
    print("starting the system with short timings...")
    compose("up", "-d", "--force-recreate")
    wait_until(lambda: compose("logs", "sp1").count("peer registered") >= 3, 40)
    in_client(CLIENT + "import os\nopen('/tmp/test-failures.orig','wb').write(os.urandom(2621440))\n"
                       f"client.upload_from_file({REMOTE_PATH!r}, '/tmp/test-failures.orig')")
    try:
        run_cases()
    finally:
        print("restoring the normal superpeer and all peers...")
        compose("start", *PEERS)
        compose("up", "-d", "--force-recreate", "sp1", files=[])
        try:
            in_client(CLIENT + f"client.rm({REMOTE_PATH!r})")
        except RuntimeError:
            pass

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


def run_cases():
    # 1. Persistencia: reiniciar el superpeer no pierde el archivo ni sus bloques
    before = blocks_of(REMOTE_PATH)
    compose("restart", "sp1")
    recovered = wait_until(lambda: blocks_of(REMOTE_PATH) == before, 30)
    report("1. superpeer restart keeps files and blocks", recovered and read_matches_original(),
           f"{len(before)} blocks, same locations after restart: {recovered}")

    # 2. Un peer caído: la lectura usa la otra réplica
    first_block, holders = before[0]
    down, survivor = holders[0], holders[1]
    compose("stop", down)
    report("2. read with a replica down", read_matches_original(), f"{down} stopped, file read from the other replicas")

    # 3. Re-replicación: el bloque vuelve a tener 2 réplicas vivas, y sobrevive a perder la otra original
    replicated = wait_until(lambda: len(set(blocks_of(REMOTE_PATH)[0][1]) - {down}) >= 2,
                            DEAD_AFTER_SECONDS + REPLICATION_DELAY_SECONDS + 30)
    new_holders = blocks_of(REMOTE_PATH)[0][1]
    compose("stop", survivor)
    survives = read_matches_original()
    report("3. re-replication avoids data loss", replicated and survives,
           f"block {first_block[:8]} now in {new_holders}; with {down} and {survivor} both down, file intact: {survives}")
    compose("start", down, survivor)
    wait_until(lambda: compose("logs", "--since", "20s", "sp1").count("peer registered") >= 2, 30)

    # 4. Sobrescribir: los bloques de la versión anterior desaparecen de sus peers tras la gracia
    old_blocks = blocks_of(REMOTE_PATH)
    in_client(CLIENT + f"client.upload_from_file({REMOTE_PATH!r}, '/tmp/test-failures.orig')")
    old_id, old_holders = old_blocks[0]
    still_there_right_after = any(has_block(peer, old_id) for peer in old_holders)
    gone = wait_until(lambda: not any(has_block(peer, old_id) for peer in old_holders), DELETION_GRACE_SECONDS + 15)
    report("4. obsolete blocks are deleted after the grace period", still_there_right_after and gone,
           f"old block {old_id[:8]}: present right after the overwrite: {still_there_right_after}, deleted later: {gone}")


if __name__ == "__main__":
    main()
