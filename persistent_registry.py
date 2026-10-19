"""
Registro persistente: envuelve otro Registry (normalmente DictRegistry) y guarda en disco cada
modificación, para que el superpeer no pierda el espacio de nombres al reiniciarse.
Es la misma idea que el edit log + fsimage de HDFS:

  - Log de operaciones (registry.log): una línea JSON por cada modificación que salió bien,
        {"seq": 42, "op": "make_directory", "user": "sofi", "path": "/tesis", "now": 1760900000.5}
    Solo se anotan las que cambian el espacio de nombres; los planes de allocate_blocks no, porque
    son estado temporal (si el superpeer se reinicia a mitad de una subida, el cliente pide otro plan).
  - Snapshot (registry.snapshot.json): una foto completa del registro y el seq de la última
    operación que incluye. Se toma cada snapshot_every operaciones y al arrancar; después el log
    se vacía, para que no crezca sin límite y el arranque no tenga que repetir demasiado.

Al arrancar: se carga el snapshot y se repiten las operaciones del log posteriores a él. Repetir da
el mismo resultado porque las operaciones son deterministas: la hora llega como 'now' y los bloques
llegan completos, nada se calcula de nuevo.

Cada línea del log es también la unidad que se le podría enviar a otro superpeer para que aplique
el mismo cambio (sincronización entre superpeers).
"""

import contextlib
import json
import os
import sys
import threading
import uuid

from dict_registry import block_from_dict, block_to_dict
from registry import Registry

LOG_FILE_NAME = "registry.log"
SNAPSHOT_FILE_NAME = "registry.snapshot.json"
DEFAULT_SNAPSHOT_EVERY = 1000
EXIT_CODE_LOG_FAILURE = 70       # código de salida si no se puede escribir el log (ver _append)


class CorruptLogError(Exception):
    """El log tiene una línea dañada que no es la última: no se puede reconstruir el registro con seguridad."""


class PersistentRegistry(Registry):
    """Registry que anota cada modificación en un log en disco antes de responder."""

    def __init__(self, inner_registry, directory, snapshot_every=DEFAULT_SNAPSHOT_EVERY):
        self._inner = inner_registry
        self._log_path = os.path.join(directory, LOG_FILE_NAME)
        self._snapshot_path = os.path.join(directory, SNAPSHOT_FILE_NAME)
        self._snapshot_every = snapshot_every
        # Aplicar en el registro y anotar en el log van bajo este mismo lock: así el orden del log
        # es exactamente el orden en que se aplicaron, y repetirlo da el mismo resultado
        self._lock = threading.Lock()
        self._last_seq = 0                    # número de la última operación aplicada
        self._operations_since_snapshot = 0
        self._log_file = None

        os.makedirs(directory, exist_ok=True)
        self._recover()
        self._take_snapshot()                 # se arranca con todo en el snapshot y el log vacío

    # ---------- Consultas: no cambian nada, no se anotan ----------

    def stat(self, user, path):
        return self._inner.stat(user, path)

    def list_directory(self, user, path):
        return self._inner.list_directory(user, path)

    def get_block_locations(self, user, path):
        return self._inner.get_block_locations(user, path)

    # Los planes son estado temporal: tampoco se anotan
    def allocate_blocks(self, user, path, file_size):
        return self._inner.allocate_blocks(user, path, file_size)

    def expire_pending_blocks(self):
        return self._inner.expire_pending_blocks()

    def export_state(self):
        return self._inner.export_state()

    def import_state(self, state):
        raise NotImplementedError("PersistentRegistry builds its state from its own snapshot and log")

    # ---------- Modificaciones: se aplican y, si salen bien, se anotan ----------
    # Si la operación lanza FileSystemError (p. ej. EEXIST), no se llega a _append: no queda rastro.

    def make_directory(self, user, path, now):
        with self._lock:
            self._inner.make_directory(user, path, now)
            self._append({"op": "make_directory", "user": user, "path": path, "now": now})

    def remove_directory(self, user, path, now):
        with self._lock:
            self._inner.remove_directory(user, path, now)
            self._append({"op": "remove_directory", "user": user, "path": path, "now": now})

    def create_file(self, user, path, now):
        with self._lock:
            self._inner.create_file(user, path, now)
            self._append({"op": "create_file", "user": user, "path": path, "now": now})

    def delete_file(self, user, path, now):
        with self._lock:
            obsolete_blocks = self._inner.delete_file(user, path, now)
            self._append({"op": "delete_file", "user": user, "path": path, "now": now})
            return obsolete_blocks

    def rename(self, user, source_path, destination_path, now):
        with self._lock:
            obsolete_blocks = self._inner.rename(user, source_path, destination_path, now)
            self._append({"op": "rename", "user": user, "source_path": source_path,
                          "destination_path": destination_path, "now": now})
            return obsolete_blocks

    def commit_file(self, user, path, size, blocks, now):
        blocks = list(blocks)
        with self._lock:
            obsolete_blocks = self._inner.commit_file(user, path, size, blocks, now)
            # Se anotan los bloques completos: al repetirlo no hay que recalcular nada
            self._append({"op": "commit_file", "user": user, "path": path, "size": size,
                          "blocks": [block_to_dict(block) for block in blocks], "now": now})
            return obsolete_blocks

    def replay_commit_file(self, user, path, size, blocks, now):
        blocks = list(blocks)
        with self._lock:
            obsolete_blocks = self._inner.replay_commit_file(user, path, size, blocks, now)
            self._append({"op": "commit_file", "user": user, "path": path, "size": size,
                          "blocks": [block_to_dict(block) for block in blocks], "now": now})
            return obsolete_blocks

    # ---------- Log (quien llama debe tener self._lock) ----------

    def _append(self, record):
        """
        Anota record en el log y espera a que esté en disco (fsync): cuando el cliente recibe "ok",
        la operación ya sobrevive a una caída.
        Si el log no se puede escribir (disco lleno, disco dañado), el cambio ya está aplicado en
        memoria pero no en disco: seguir funcionando haría que memoria y disco diverjan. Por eso el
        superpeer se detiene (fail-stop); al reiniciar se reconstruye desde el log, sin esa operación,
        que el cliente nunca vio confirmada.
        """
        record = {"seq": self._last_seq + 1, **record}
        try:
            self._log_file.write(json.dumps(record) + "\n")
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
        except OSError as error:
            print(f"FATAL: cannot write registry log {self._log_path}: {error}", file=sys.stderr, flush=True)
            os._exit(EXIT_CODE_LOG_FAILURE)    # _exit y no exit: termina el proceso entero, no solo este hilo
        self._last_seq = record["seq"]
        self._operations_since_snapshot += 1
        if self._operations_since_snapshot >= self._snapshot_every:
            self._take_snapshot()

    def _take_snapshot(self):
        """
        Guarda la foto completa (de forma atómica) y después vacía el log.
        Si el proceso se cae entre las dos cosas, el log tendrá operaciones que ya están en la
        foto: _recover las salta porque su seq no es mayor que el de la foto.
        """
        snapshot = {"seq": self._last_seq, "state": self._inner.export_state()}
        _write_atomically(self._snapshot_path, json.dumps(snapshot).encode("utf-8"))
        if self._log_file is not None:
            self._log_file.close()
        self._log_file = open(self._log_path, "w", encoding="utf-8")     # "w" lo deja vacío
        self._log_file.flush()
        os.fsync(self._log_file.fileno())
        self._operations_since_snapshot = 0

    # ---------- Recuperación al arrancar ----------

    def _recover(self):
        """Carga el snapshot (si hay) y repite las operaciones del log posteriores a él."""
        if os.path.exists(self._snapshot_path):
            with open(self._snapshot_path, encoding="utf-8") as snapshot_file:
                snapshot = json.load(snapshot_file)
            self._inner.import_state(snapshot["state"])
            self._last_seq = snapshot["seq"]

        if not os.path.exists(self._log_path):
            return
        with open(self._log_path, encoding="utf-8") as log_file:
            lines = log_file.readlines()
        for line_number, line in enumerate(lines, start=1):
            is_last_line = (line_number == len(lines))
            try:
                if not line.endswith("\n"):
                    raise ValueError("incomplete line")
                record = json.loads(line)
            except ValueError:
                if is_last_line:
                    # El proceso se cayó mientras escribía esta línea: esa operación nunca se le
                    # confirmó al cliente, así que se descarta
                    break
                raise CorruptLogError(f"{self._log_path}: damaged line {line_number}") from None

            if record["seq"] <= self._last_seq:
                continue                      # ya está en el snapshot
            if record["seq"] != self._last_seq + 1:
                raise CorruptLogError(f"{self._log_path}: missing operations before seq {record['seq']}")
            self._replay(record)
            self._last_seq = record["seq"]

    def _replay(self, record):
        """Repite una operación del log sobre el registro de adentro (sin volver a anotarla)."""
        operation = record["op"]
        if operation == "make_directory":
            self._inner.make_directory(record["user"], record["path"], record["now"])
        elif operation == "remove_directory":
            self._inner.remove_directory(record["user"], record["path"], record["now"])
        elif operation == "create_file":
            self._inner.create_file(record["user"], record["path"], record["now"])
        elif operation == "delete_file":
            self._inner.delete_file(record["user"], record["path"], record["now"])
        elif operation == "rename":
            self._inner.rename(record["user"], record["source_path"], record["destination_path"], record["now"])
        elif operation == "commit_file":
            blocks = [block_from_dict(block) for block in record["blocks"]]
            self._inner.replay_commit_file(record["user"], record["path"], record["size"], blocks, record["now"])
        else:
            raise CorruptLogError(f"{self._log_path}: unknown operation {operation!r} (seq {record['seq']})")


def _write_atomically(path, content):
    """Escribe path completo o nada: temporal + fsync + os.replace (como los bloques y los usuarios)."""
    temporary_path = f"{path}.tmp-{uuid.uuid4().hex}"
    try:
        with open(temporary_path, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(temporary_path)
