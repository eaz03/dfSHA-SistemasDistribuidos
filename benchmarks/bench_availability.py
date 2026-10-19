"""
Mide la alta disponibilidad con los tiempos reales del sistema (los de docker-compose.yml):
  - cuánto tarda el superpeer en detectar que un peer se cayó;
  - cuánto tarda en re-replicar los bloques de ese peer (retardo de 60 s incluido);
  - cuánto tarda el superpeer en volver a atender después de un reinicio, y si el archivo sigue igual.

Se ejecuta fuera de los contenedores, desde la raíz del repositorio (tarda unos 3 minutos):
  python3 -m benchmarks.bench_availability
Usa las credenciales del contenedor client (las del .env). Al terminar deja todo levantado.
"""

import os
import subprocess
import time

REMOTE_PATH = "/bench-availability.bin"
CLIENT = ("from dfsha.client.client import DFSClient\n"
          "from dfsha.client.auth_client import credentials_from_environment\n"
          "client = DFSClient(['sp1:7000'], *credentials_from_environment())\n")


def compose(*arguments):
    environment = dict(os.environ, MSYS_NO_PATHCONV="1")
    result = subprocess.run(["docker", "compose"] + list(arguments), capture_output=True, text=True, env=environment)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def in_client(python_code):
    return compose("exec", "-T", "-e", "GRPC_VERBOSITY=NONE", "client", "python", "-c", python_code).strip()


def wait_for(condition, timeout_seconds):
    """Segundos hasta que condition() es verdadera (consultando cada 0,5 s), o None si no ocurrió."""
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        try:
            if condition():
                return time.monotonic() - start
        except RuntimeError:
            pass
        time.sleep(0.5)
    return None


def peer_alive(peer_id):
    output = in_client(CLIENT + "for p in client.list_peers():\n    print(p.peer_id, p.alive)")
    return f"{peer_id} True" in output


def replicas_per_block():
    output = in_client(CLIENT + f"for b in client.get_block_locations({REMOTE_PATH!r}).blocks:\n"
                                "    print(','.join(r.peer_id for r in b.replicas))")
    return [line.split(",") if line else [] for line in output.splitlines()]


def main():
    print("preparing: a 5 MiB file with 2 replicas per block...", flush=True)
    in_client(CLIENT + "import os\nopen('/tmp/avail.bin','wb').write(os.urandom(5*1024*1024))\n"
                       f"client.upload_from_file({REMOTE_PATH!r}, '/tmp/avail.bin')")
    victim = replicas_per_block()[0][0]
    rows = []

    stopped_at = time.monotonic()
    compose("stop", victim)
    detected = wait_for(lambda: not peer_alive(victim), 60)
    rows.append(("Detección de un peer caído", f"{detected:.1f} s", "muerto 9 s después de su último heartbeat, que pudo llegar hasta 3 s antes de la caída"))

    def fully_replicated():
        return all(len([peer for peer in replicas if peer != victim]) >= 2 for replicas in replicas_per_block())
    replicated = wait_for(fully_replicated, 240)
    total = time.monotonic() - stopped_at
    rows.append(("Re-replicación completa de sus bloques", f"{total:.1f} s desde la caída",
                 "9 s para darlo por muerto + 60 s de retardo + pasadas cada 10 s"))
    compose("start", victim)

    original_check = "print(open('/tmp/avail.bin','rb').read() == open('/tmp/avail.copy','rb').read())"
    compose("restart", "sp1")
    restarted_at = time.monotonic()
    serving = wait_for(lambda: in_client(CLIENT + "client.stat('/') and print('ok')") == "ok", 60)
    readable = wait_for(lambda: in_client(CLIENT + f"client.download_to_file({REMOTE_PATH!r}, '/tmp/avail.copy')\n"
                                                   + original_check) == "True", 60)
    rows.append(("Superpeer atendiendo tras un reinicio", f"{serving:.1f} s",
                 "arranque + reconstrucción desde snapshot y log"))
    rows.append(("Archivo legible e idéntico tras el reinicio", f"{time.monotonic() - restarted_at:.1f} s",
                 "espera a que los peers se vuelvan a registrar"))

    in_client(CLIENT + f"client.rm({REMOTE_PATH!r})")
    print("\n### Alta disponibilidad (tiempos reales de docker-compose.yml)\n")
    print("| Medición | Resultado | Explicación |")
    print("|---|---|---|")
    for label, value, explanation in rows:
        print(f"| {label} | {value} | {explanation} |")
    if replicated is None or readable is None:
        print("\n(algún evento no ocurrió dentro del tiempo límite)")


if __name__ == "__main__":
    main()
