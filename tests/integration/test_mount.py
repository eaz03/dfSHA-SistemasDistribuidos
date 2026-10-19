"""
Prueba del montaje FUSE (hito 7): lectura y escritura con comandos comunes de Linux sobre /mnt/dfsha.

Se ejecuta en WSL (no dentro de un contenedor), desde cualquier carpeta, con el usuario del .env ya creado:
  python3 -m tests.integration.test_mount
Solo usa la biblioteca estándar de Python. Los comandos corren dentro del contenedor client (el que tiene
el montaje) con 'docker compose exec'; las fallas se simulan deteniendo peers con 'docker compose stop'.
Trabaja en una carpeta propia de DFSha (/test-mount-<hora>), que borra al terminar.
Tarda alrededor de un minuto. Al terminar deja todos los contenedores corriendo.

Casos:
  Lectura (el archivo se sube con dfs_cli, sin FUSE: así no depende de la escritura del montaje)
    1. cat entrega exactamente los bytes del archivo (sha256)
    2. head y tail entregan el principio y el final
    3. dd lee 12 bytes que cruzan el borde entre dos bloques
  Escritura
    4. cp hacia el montaje y de vuelta, 10 MiB (sha256 igual; también al bajarlo con dfs_cli)
    5. cp sobre un archivo existente, con uno más corto
    6. > reemplaza el contenido, >> agrega al final
    7. truncate achica y agranda (rellena con ceros)
    8. sed -i, que guarda como un editor (archivo temporal + rename encima)
  Archivo abierto
    9. stat muestra el tamaño de lo escrito antes de cerrar
    10. mv con el archivo abierto: lo escrito después llega al path nuevo
    11. rm con el archivo abierto: no reaparece al cerrarlo
  Fallas
    12. con un peer detenido, se lee y se escribe igual
    13. sin peers, el cp falla al cerrar y la versión anterior queda intacta
"""

import json
import os
import subprocess
import sys
import time

MOUNT_DIRECTORY = "/mnt/dfsha"                     # donde el contenedor client monta DFSha
LOCAL_DIRECTORY = "/tmp/test_mount"                # archivos originales, en el disco del contenedor (fuera de DFSha)
CLIENT_SERVICE = "client"
PEER_SERVICES = ["peer1", "peer2", "peer3"]        # en el compose, cada servicio se registra con su mismo nombre como ID
MEBIBYTE = 1024 * 1024                             # el tamaño de bloque
MEDIUM_FILE_BYTES = 2 * MEBIBYTE + MEBIBYTE // 2   # 2,5 MiB: 3 bloques, el último a medias
BIG_FILE_BYTES = 10 * MEBIBYTE
COMMAND_TIMEOUT_SECONDS = 120                      # si el montaje se cuelga, el comando falla en vez de bloquear la prueba
WAIT_TIMEOUT_SECONDS = 30
POLL_INTERVAL_SECONDS = 1


# ---------- Ayudantes ----------

def compose(*arguments):
    """Ejecuta 'docker compose <arguments>' y devuelve su salida. Si el comando falla, lanza RuntimeError."""
    command = ["docker", "compose"]
    command.extend(arguments)
    result = subprocess.run(command, capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SECONDS)
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed: {result.stderr.strip()}")
    return result.stdout


def run_in_client(shell_command):
    """
    Ejecuta shell_command con sh dentro del contenedor client.
    Devuelve (código de salida, stdout, stderr) sin lanzar error: algunos casos esperan que el comando falle.
    """
    command = ["docker", "compose", "exec", "-T", CLIENT_SERVICE, "sh", "-c", shell_command]
    result = subprocess.run(command, capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SECONDS)
    return result.returncode, result.stdout, result.stderr


def output_in_client(shell_command):
    """Como run_in_client, pero devuelve solo stdout (sin espacios al final). Si el comando falla, lanza RuntimeError."""
    exit_code, output, error_output = run_in_client(shell_command)
    if exit_code != 0:
        raise RuntimeError(f"'{shell_command}' failed ({exit_code}): {error_output.strip()}")
    return output.strip()


def python_in_client(script, *arguments):
    """
    Ejecuta un script de Python dentro del contenedor client (el script entra por stdin: python - <arguments>)
    y devuelve lo que imprimió, interpretado como JSON. Sirve para lo que no se puede hacer con un comando,
    como dejar un archivo abierto mientras se lo renombra.
    """
    command = ["docker", "compose", "exec", "-T", CLIENT_SERVICE, "python", "-"]
    command.extend(arguments)
    result = subprocess.run(command, input=script, capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SECONDS)
    if result.returncode != 0:
        raise RuntimeError(f"python script failed ({result.returncode}): {result.stderr.strip()}")
    return json.loads(result.stdout)


# sha256_in_client y hex_in_client no usan tuberías (cmd | otro): sh devuelve el código de salida del
# último comando, y un error al leer (p. ej. "No data available") quedaría oculto como un resultado vacío

def sha256_in_client(path):
    """sha256 del archivo path, calculado dentro del contenedor client."""
    output = output_in_client(f"sha256sum '{path}'")      # "<sha256>  <path>"
    return output.split()[0]


def hex_in_client(path):
    """Contenido de path en hexadecimal: compara los bytes exactos, incluidos ceros y saltos de línea."""
    output = output_in_client(f"od -An -tx1 -v '{path}'")  # "76 65 72 ..." en varias líneas
    return "".join(output.split())


def file_is_readable(path):
    """True si path se puede leer completo (todos sus bloques tienen al menos una réplica que responde)."""
    exit_code, output, error_output = run_in_client(f"cat '{path}' > /dev/null")
    return exit_code == 0


def mount_is_ready():
    """True si el contenedor client responde y tiene DFSha montado en MOUNT_DIRECTORY."""
    exit_code, output, error_output = run_in_client(f"grep -q ' {MOUNT_DIRECTORY} ' /proc/mounts")
    return exit_code == 0


def alive_peer_ids():
    """IDs de los peers que el superpeer considera vivos (tools.peers_status). Vacío si no responde."""
    exit_code, output, error_output = run_in_client("python -m tools.peers_status --json")
    if exit_code != 0:
        return []
    alive_ids = []
    for peer in json.loads(output):
        if peer["alive"]:
            alive_ids.append(peer["peer_id"])
    return alive_ids


def all_peers_alive():
    alive_ids = alive_peer_ids()
    for peer_service in PEER_SERVICES:
        if peer_service not in alive_ids:
            return False
    return True


def wait_until(condition):
    """Consulta condition() hasta que sea True. Devuelve True si lo fue antes de WAIT_TIMEOUT_SECONDS."""
    started = time.monotonic()
    while time.monotonic() - started < WAIT_TIMEOUT_SECONDS:
        if condition():
            return True
        time.sleep(POLL_INTERVAL_SECONDS)
    return False


results = []    # True o False por cada caso


def report(case_name, passed, detail):
    if passed:
        status = "PASS"
    else:
        status = "FAIL"
    print(f"[{status}] {case_name}: {detail}")
    results.append(passed)


def report_same(case_name, expected, actual, what):
    """Reporta un caso que consiste en comparar un valor esperado con el obtenido."""
    if expected == actual:
        report(case_name, True, f"{what} as expected")
    else:
        report(case_name, False, f"{what} differs: expected {expected!r}, got {actual!r}")


# ---------- Scripts que corren dentro de client (archivos abiertos) ----------

# 9. Tamaño mientras el archivo sigue abierto. La pausa deja vencer el caché de atributos del kernel (1 s):
#    así el tamaño lo responde el getattr del montaje, que debe usar la copia local sin subir
SIZE_WHILE_OPEN_SCRIPT = """
import json
import os
import sys
import time

path = sys.argv[1]
file_descriptor = os.open(path, os.O_WRONLY | os.O_CREAT)
os.write(file_descriptor, b"x" * 300000)
time.sleep(1.5)
size_while_open = os.stat(path).st_size
os.close(file_descriptor)
size_after_close = os.stat(path).st_size
print(json.dumps({"while_open": size_while_open, "after_close": size_after_close}))
"""

# 10. Renombrar con el archivo abierto y seguir escribiendo
RENAME_WHILE_OPEN_SCRIPT = """
import json
import os
import sys

old_path = sys.argv[1]
new_path = sys.argv[2]
file_descriptor = os.open(old_path, os.O_WRONLY | os.O_CREAT)
os.write(file_descriptor, b"antes")
os.rename(old_path, new_path)
os.write(file_descriptor, b" y despues")
os.close(file_descriptor)
with open(new_path, "rb") as new_file:
    content = new_file.read().decode()
print(json.dumps({"content": content, "old_path_exists": os.path.exists(old_path)}))
"""

# 11. Borrar con el archivo abierto y con cambios sin subir
REMOVE_WHILE_OPEN_SCRIPT = """
import json
import os
import sys

path = sys.argv[1]
with open(path, "wb") as saved_file:
    saved_file.write(b"version guardada")
file_descriptor = os.open(path, os.O_WRONLY)
os.write(file_descriptor, b"cambio que no debe quedar")
os.remove(path)
os.close(file_descriptor)
hidden_names = []
for name in os.listdir(os.path.dirname(path)):
    if name.startswith(".fuse_hidden"):
        hidden_names.append(name)
print(json.dumps({"exists": os.path.exists(path), "hidden_names": hidden_names}))
"""


# ---------- Casos ----------

def check_reading(remote_directory, test_directory):
    medium_local = f"{LOCAL_DIRECTORY}/medium.bin"
    medium_in_mount = f"{test_directory}/medium.bin"
    output_in_client(f"python -m dfsha.client.dfs_cli put {medium_local} {remote_directory}/medium.bin")

    # 1. cat completo
    expected_sha = sha256_in_client(medium_local)
    actual_sha = output_in_client(f"cat '{medium_in_mount}' | sha256sum | cut -d ' ' -f 1")
    report_same("1. cat 2.5 MiB", expected_sha, actual_sha, "sha256")

    # 2. head y tail
    head_command = "head -c 100 '{}' | sha256sum"
    tail_command = "tail -c 100 '{}' | sha256sum"
    head_matches = output_in_client(head_command.format(medium_local)) == output_in_client(head_command.format(medium_in_mount))
    tail_matches = output_in_client(tail_command.format(medium_local)) == output_in_client(tail_command.format(medium_in_mount))
    report("2. head and tail", head_matches and tail_matches,
           f"first 100 bytes match: {head_matches}, last 100 bytes match: {tail_matches}")

    # 3. dd de 12 bytes desde 6 antes del final del bloque 0: 6 bytes del bloque 0 y 6 del bloque 1
    dd_command = "dd if='{}' bs=4096 skip=" + str(MEBIBYTE - 6) + " count=12 iflag=skip_bytes,count_bytes 2>/dev/null | od -An -tx1"
    expected_bytes = output_in_client(dd_command.format(medium_local))
    actual_bytes = output_in_client(dd_command.format(medium_in_mount))
    report_same("3. dd across a block boundary", expected_bytes, actual_bytes, "12 bytes")


def check_writing(remote_directory, test_directory):
    big_local = f"{LOCAL_DIRECTORY}/big.bin"
    big_in_mount = f"{test_directory}/big.bin"

    # 4. cp de ida y vuelta; además se baja con dfs_cli, sin FUSE: así se comprueba lo que quedó en los peers
    expected_sha = sha256_in_client(big_local)
    output_in_client(f"cp {big_local} '{big_in_mount}'")
    output_in_client(f"cp '{big_in_mount}' {LOCAL_DIRECTORY}/big.back")
    output_in_client(f"python -m dfsha.client.dfs_cli get {remote_directory}/big.bin {LOCAL_DIRECTORY}/big.cli")
    back_sha = sha256_in_client(f"{LOCAL_DIRECTORY}/big.back")
    cli_sha = sha256_in_client(f"{LOCAL_DIRECTORY}/big.cli")
    report("4. cp 10 MiB in and out", back_sha == expected_sha and cli_sha == expected_sha,
           f"copied back: {back_sha == expected_sha}, downloaded with dfs_cli: {cli_sha == expected_sha}")

    # 5. cp sobre el archivo existente, con uno más corto: no deben quedar restos del anterior
    output_in_client(f"cp {LOCAL_DIRECTORY}/medium.bin '{big_in_mount}'")
    size_after = int(output_in_client(f"stat -c %s '{big_in_mount}'"))
    same_content = sha256_in_client(big_in_mount) == sha256_in_client(f"{LOCAL_DIRECTORY}/medium.bin")
    report("5. cp over an existing file", size_after == MEDIUM_FILE_BYTES and same_content,
           f"size {size_after} (expected {MEDIUM_FILE_BYTES}), same content: {same_content}")

    # 6. > y >>
    text_path = f"{test_directory}/text.txt"
    output_in_client(f"printf 'uno\\n' > '{text_path}' && printf 'dos\\n' >> '{text_path}'")
    after_append = hex_in_client(text_path)
    output_in_client(f"printf 'tres\\n' > '{text_path}'")
    after_replace = hex_in_client(text_path)
    report("6. > and >>", after_append == b"uno\ndos\n".hex() and after_replace == b"tres\n".hex(),
           f"after >>: {bytes.fromhex(after_append)!r}, after >: {bytes.fromhex(after_replace)!r}")

    # 7. truncate a 3 bytes y después a 6 (los 3 nuevos son ceros)
    output_in_client(f"truncate -s 3 '{text_path}'")
    after_shrink = hex_in_client(text_path)
    output_in_client(f"truncate -s 6 '{text_path}'")
    after_grow = hex_in_client(text_path)
    report("7. truncate", after_shrink == b"tre".hex() and after_grow == b"tre\x00\x00\x00".hex(),
           f"after -s 3: {bytes.fromhex(after_shrink)!r}, after -s 6: {bytes.fromhex(after_grow)!r}")

    # 8. sed -i escribe un archivo temporal en la misma carpeta y lo renombra encima del original
    edited_path = f"{test_directory}/edited.txt"
    output_in_client(f"printf 'uno\\ndos\\ntres\\n' > '{edited_path}' && sed -i 's/dos/DOS/' '{edited_path}'")
    edited_content = hex_in_client(edited_path)
    leftover_names = []
    for name in output_in_client(f"ls -A '{test_directory}'").split():
        if name.startswith("sed"):
            leftover_names.append(name)
    report("8. sed -i (editor-style save)", edited_content == b"uno\nDOS\ntres\n".hex() and len(leftover_names) == 0,
           f"content: {bytes.fromhex(edited_content)!r}, leftover temporary files: {leftover_names}")


def check_open_files(test_directory):
    # 9. Tamaño mientras se escribe
    sizes = python_in_client(SIZE_WHILE_OPEN_SCRIPT, f"{test_directory}/growing.bin")
    report("9. size while still open", sizes["while_open"] == 300000 and sizes["after_close"] == 300000,
           f"while open: {sizes['while_open']}, after close: {sizes['after_close']} (expected 300000)")

    # 10. mv con el archivo abierto
    renamed = python_in_client(RENAME_WHILE_OPEN_SCRIPT, f"{test_directory}/before.txt", f"{test_directory}/after.txt")
    report("10. mv while open", renamed["content"] == "antes y despues" and not renamed["old_path_exists"],
           f"content at the new path: {renamed['content']!r}, old path still exists: {renamed['old_path_exists']}")

    # 11. rm con el archivo abierto
    removed = python_in_client(REMOVE_WHILE_OPEN_SCRIPT, f"{test_directory}/removed.txt")
    report("11. rm while open", not removed["exists"] and len(removed["hidden_names"]) == 0,
           f"exists after close: {removed['exists']}, leftover .fuse_hidden files: {removed['hidden_names']}")


def check_failures(test_directory):
    medium_local = f"{LOCAL_DIRECTORY}/medium.bin"
    big_local = f"{LOCAL_DIRECTORY}/big.bin"

    # 12. Un peer detenido: cada bloque tiene otra réplica, y al escribir la réplica caída se omite
    compose("stop", "peer2")
    read_sha = output_in_client(f"cat '{test_directory}/medium.bin' | sha256sum | cut -d ' ' -f 1")
    read_ok = read_sha == sha256_in_client(medium_local)
    write_path = f"{test_directory}/written_with_peer_down.bin"
    exit_code, output, error_output = run_in_client(f"cp {big_local} '{write_path}'")
    write_ok = exit_code == 0 and sha256_in_client(write_path) == sha256_in_client(big_local)
    compose("start", "peer2")
    report("12. one peer stopped", read_ok and write_ok,
           f"read correct: {read_ok}, write correct: {write_ok} {error_output.strip()}")

    # 13. Sin peers: la subida falla al cerrar (cp lo informa) y queda la versión anterior completa
    if not wait_until(all_peers_alive):
        report("13. no peers", False, "peer2 did not come back, cannot prepare the case")
        return
    kept_path = f"{test_directory}/kept.txt"
    output_in_client(f"printf 'version anterior\\n' > '{kept_path}'")
    compose("stop", *PEER_SERVICES)
    exit_code, output, error_output = run_in_client(f"cp {medium_local} '{kept_path}'")
    compose("start", *PEER_SERVICES)
    # Los peers tardan unos segundos en volver a atender: hasta entonces, leer da "No data available"
    def kept_file_is_readable():
        return file_is_readable(kept_path)
    peers_back = wait_until(kept_file_is_readable)
    kept_content = hex_in_client(kept_path)
    copy_failed = exit_code != 0
    previous_version_kept = kept_content == b"version anterior\n".hex()
    report("13. no peers", copy_failed and previous_version_kept and peers_back,
           f"cp failed: {copy_failed} ({error_output.strip()}), previous version kept: {previous_version_kept} "
           f"({bytes.fromhex(kept_content)!r})")


def main():
    # docker compose busca docker-compose.yml desde la carpeta actual hacia arriba: ir a la del proyecto.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("starting all containers...")
    compose("up", "-d")
    if not wait_until(mount_is_ready):
        sys.exit(f"DFSha is not mounted at {MOUNT_DIRECTORY} in '{CLIENT_SERVICE}': see 'docker compose logs {CLIENT_SERVICE}'")
    if not wait_until(all_peers_alive):
        sys.exit(f"not all peers are alive ({alive_peer_ids()}): see 'docker compose exec client python -m tools.peers_status'")

    remote_directory = f"/test-mount-{int(time.time())}"          # el path dentro de DFSha
    test_directory = MOUNT_DIRECTORY + remote_directory             # el mismo, visto a través del montaje
    print(f"working in {test_directory}")
    output_in_client(f"rm -rf {LOCAL_DIRECTORY} && mkdir -p {LOCAL_DIRECTORY}"
                     f" && head -c {MEDIUM_FILE_BYTES} /dev/urandom > {LOCAL_DIRECTORY}/medium.bin"
                     f" && head -c {BIG_FILE_BYTES} /dev/urandom > {LOCAL_DIRECTORY}/big.bin")
    output_in_client(f"mkdir '{test_directory}'")

    # Un comando que falla de forma inesperada (p. ej. cp con "Input/output error") corta su grupo,
    # que cuenta como fallido, pero los demás grupos se prueban igual
    groups = [
        ("reading", check_reading, (remote_directory, test_directory)),
        ("writing", check_writing, (remote_directory, test_directory)),
        ("open files", check_open_files, (test_directory,)),
        ("failures", check_failures, (test_directory,)),
    ]
    try:
        for group_name, check_group, group_arguments in groups:
            try:
                check_group(*group_arguments)
            except (RuntimeError, ValueError, subprocess.TimeoutExpired) as error:   # ValueError: un script no imprimió JSON
                report(f"{group_name} (stopped early)", False, str(error))
    finally:
        compose("start", *PEER_SERVICES)                                 # por si un caso se interrumpió con peers detenidos
        wait_until(all_peers_alive)
        run_in_client(f"rm -rf '{test_directory}' {LOCAL_DIRECTORY}")

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
