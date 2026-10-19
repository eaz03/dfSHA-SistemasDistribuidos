"""
Prueba del paso 1 (conexión peer <-> superpeer) sobre los contenedores de Docker Compose.

Se ejecuta en WSL (no dentro de un contenedor), desde cualquier carpeta:
  python3 test_peers.py
Solo usa la biblioteca estándar de Python: controla los contenedores con 'docker compose'
y consulta la tabla de peers ejecutando peers_status.py dentro del contenedor client
(que tiene las credenciales y el certificado de cliente, porque ListPeers pide token).
Tarda alrededor de un minuto. Al terminar deja sp1 y peer1 corriendo.

Casos:
  1. peer1 se registra en el superpeer.
  2. 'docker compose stop peer1' termina rápido: el peer atiende SIGTERM.
  3. Sin heartbeats, el superpeer marca a peer1 como muerto (unos 9 s).
  4. peer1 vuelve a arrancar y revive en la tabla.
  5. sp1 se reinicia (su tabla queda vacía) y peer1 se vuelve a registrar solo.
"""

import json
import os
import subprocess
import sys
import time

SUPERPEER_SERVICE = "sp1"
PEER_SERVICE = "peer1"
PEER_ID = "peer1"
WAIT_TIMEOUT_SECONDS = 25
MAX_STOP_SECONDS = 5           # con el manejador de SIGTERM tarda ~2 s; sin él, Docker espera 10 s
POLL_INTERVAL_SECONDS = 1
REREGISTER_LOG_TEXT = "registering again"     # lo imprime peer.py cuando recibe known=False


# ---------- Ayudantes ----------

def compose(*arguments):
    """Ejecuta 'docker compose <arguments>' y devuelve su salida. Si el comando falla, lanza RuntimeError."""
    command = ["docker", "compose"]
    command.extend(arguments)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed: {result.stderr.strip()}")
    return result.stdout


def peer_state(peer_id):
    """
    Estado de peer_id según el superpeer:
      'alive' / 'dead'  -> está en la tabla, vivo o muerto
      'missing'         -> el superpeer responde pero no lo conoce
      'unreachable'     -> no se pudo consultar al superpeer (p. ej. se está reiniciando)
    """
    try:
        output = compose("exec", "-T", "client", "python", "peers_status.py", "--json")
    except RuntimeError:
        return "unreachable"
    peers = json.loads(output)
    for peer in peers:
        if peer["peer_id"] == peer_id:
            if peer["alive"]:
                return "alive"
            return "dead"
    return "missing"


def wait_for_state(peer_id, expected_state, timeout_seconds):
    """Consulta el estado hasta que sea expected_state. Devuelve los segundos que tardó, o None si se agotó el tiempo."""
    started = time.monotonic()
    while time.monotonic() - started < timeout_seconds:
        if peer_state(peer_id) == expected_state:
            return time.monotonic() - started
        time.sleep(POLL_INTERVAL_SECONDS)
    return None


def count_in_logs(service, text):
    """Cuántas veces aparece text en los logs de service."""
    logs = compose("logs", "--no-color", service)
    return logs.count(text)


results = []    # True o False por cada caso


def report(case_name, passed, detail):
    if passed:
        status = "PASS"
    else:
        status = "FAIL"
    print(f"[{status}] {case_name}: {detail}")
    results.append(passed)


def report_wait(case_name, seconds, success_text, failure_text):
    """Reporta un caso que consiste en esperar un estado: seconds es lo que devolvió wait_for_state."""
    if seconds is None:
        report(case_name, False, failure_text)
    else:
        report(case_name, True, f"{success_text} {seconds:.1f} s")


# ---------- Casos ----------

def main():
    # docker compose busca docker-compose.yml en la carpeta actual: ir a la del proyecto.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("starting sp1 and peer1...")
    compose("up", "-d", SUPERPEER_SERVICE, PEER_SERVICE)

    # 1. Registro
    seconds = wait_for_state(PEER_ID, "alive", WAIT_TIMEOUT_SECONDS)
    report_wait("1. peer1 registers", seconds,
                success_text="alive after",
                failure_text=f"not alive after {WAIT_TIMEOUT_SECONDS} s")

    # 2. Apagado ordenado
    stop_started = time.monotonic()
    compose("stop", PEER_SERVICE)
    stop_seconds = time.monotonic() - stop_started
    report("2. peer1 handles SIGTERM", stop_seconds < MAX_STOP_SECONDS,
           f"docker compose stop took {stop_seconds:.1f} s (limit {MAX_STOP_SECONDS} s)")

    # 3. Detección de la caída
    seconds = wait_for_state(PEER_ID, "dead", WAIT_TIMEOUT_SECONDS)
    report_wait("3. superpeer detects peer1 is down", seconds,
                success_text="marked dead after the stop, in",
                failure_text=f"still not dead after {WAIT_TIMEOUT_SECONDS} s")

    # 4. Regreso del peer
    compose("start", PEER_SERVICE)
    seconds = wait_for_state(PEER_ID, "alive", WAIT_TIMEOUT_SECONDS)
    report_wait("4. peer1 comes back", seconds,
                success_text="alive again after",
                failure_text=f"not alive after {WAIT_TIMEOUT_SECONDS} s")

    # 5. Reinicio del superpeer: tabla vacía, el peer debe registrarse de nuevo por sí solo
    reregistrations_before = count_in_logs(PEER_SERVICE, REREGISTER_LOG_TEXT)
    restart_started = time.monotonic()
    compose("restart", SUPERPEER_SERVICE)
    seconds = wait_for_state(PEER_ID, "alive", WAIT_TIMEOUT_SECONDS)
    total_seconds = time.monotonic() - restart_started
    reregistrations_after = count_in_logs(PEER_SERVICE, REREGISTER_LOG_TEXT)
    peer_reregistered = reregistrations_after > reregistrations_before
    peer_alive_again = seconds is not None
    report("5. peer1 re-registers after sp1 restarts", peer_alive_again and peer_reregistered,
           f"alive in the new table: {peer_alive_again} ({total_seconds:.1f} s after the restart began), "
           f"'{REREGISTER_LOG_TEXT}' in peer1 logs: {peer_reregistered}")

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
