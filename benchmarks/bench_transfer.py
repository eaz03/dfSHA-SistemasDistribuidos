"""
Mide la transferencia de archivos: tiempo y throughput de put y get según el tamaño del archivo, y
el efecto de transferir varios bloques en paralelo.

Se ejecuta en el contenedor client (credenciales del .env), con todos los servicios levantados:
  docker compose exec client python -m benchmarks.bench_transfer
Imprime tablas en Markdown. Borra los archivos que crea.
"""

import os
import statistics
import time

from dfsha.client import client as client_module
from dfsha.client.auth_client import credentials_from_environment
from dfsha.client.client import DFSClient

MEBIBYTE = 1024 * 1024
SIZES_MIB = [1, 10, 50, 100]
PARALLELISM_LEVELS = [1, 2, 4, 8]
PARALLELISM_SIZE_MIB = 50
REPETITIONS = 5


def make_local_file(size_mib):
    path = f"/tmp/bench-{size_mib}MiB.bin"
    if not os.path.exists(path) or os.path.getsize(path) != size_mib * MEBIBYTE:
        with open(path, "wb") as local_file:
            for _ in range(size_mib):
                local_file.write(os.urandom(MEBIBYTE))
    return path


def timed(function):
    start = time.perf_counter()
    function()
    return time.perf_counter() - start


def measure(client, size_mib):
    """
    REPETITIONS subidas y bajadas de un archivo de size_mib MiB: (tiempos de put, tiempos de get).
    Cada repetición usa un archivo remoto nuevo: sobrescribir el mismo dejaría bloques obsoletos que el
    limpiador borra más tarde, en medio de otras mediciones.
    """
    local_path = make_local_file(size_mib)
    put_seconds, get_seconds, remote_paths = [], [], []
    for repetition in range(REPETITIONS):
        remote_path = f"/bench-{size_mib}MiB-{repetition}.bin"
        remote_paths.append(remote_path)
        put_seconds.append(timed(lambda: client.upload_from_file(remote_path, local_path)))
        get_seconds.append(timed(lambda: client.download_to_file(remote_path, local_path + ".copy")))
    with open(local_path, "rb") as original, open(local_path + ".copy", "rb") as copy:
        assert original.read() == copy.read(), "el archivo bajado no es igual al subido"
    for remote_path in remote_paths:
        client.rm(remote_path)
    return put_seconds, get_seconds


def row(label, size_mib, put_seconds, get_seconds):
    """Mediana (con mínimo y máximo) en segundos, y throughput a partir de la mediana."""
    put_s, get_s = statistics.median(put_seconds), statistics.median(get_seconds)
    return (f"| {label} | {put_s:.2f} ({min(put_seconds):.2f}–{max(put_seconds):.2f}) | {size_mib / put_s:.1f} | "
            f"{get_s:.2f} ({min(get_seconds):.2f}–{max(get_seconds):.2f}) | {size_mib / get_s:.1f} |")


def main():
    client = DFSClient(["sp1:7000"], *credentials_from_environment())
    print(f"### Transferencia según el tamaño (k = 2 réplicas, {client_module.PARALLEL_BLOCK_TRANSFERS} bloques "
          f"en paralelo, mediana de {REPETITIONS})\n")
    print("| Tamaño | put: s (mín–máx) | put (MiB/s) | get: s (mín–máx) | get (MiB/s) |")
    print("|---|---|---|---|---|")
    for size_mib in SIZES_MIB:
        print(row(f"{size_mib} MiB", size_mib, *measure(client, size_mib)), flush=True)

    print(f"\n### Efecto del paralelismo ({PARALLELISM_SIZE_MIB} MiB, mediana de {REPETITIONS})\n")
    print("| Bloques en paralelo | put: s (mín–máx) | put (MiB/s) | get: s (mín–máx) | get (MiB/s) |")
    print("|---|---|---|---|---|")
    original_level = client_module.PARALLEL_BLOCK_TRANSFERS
    try:
        for level in PARALLELISM_LEVELS:
            client_module.PARALLEL_BLOCK_TRANSFERS = level      # solo durante la medición
            print(row(str(level), PARALLELISM_SIZE_MIB, *measure(client, PARALLELISM_SIZE_MIB)), flush=True)
    finally:
        client_module.PARALLEL_BLOCK_TRANSFERS = original_level


if __name__ == "__main__":
    main()
