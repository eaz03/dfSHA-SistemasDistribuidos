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

Cada línea del log es también la unidad que se envía a los demás superpeers para que apliquen el
mismo cambio (federación, ver federation.py). Para eso cada línea lleva además su posición en el log
de su usuario: {"epoch": 3, "epoch_owner": "sp1", "user_seq": 57}. Los usuarios son independientes
(cada uno tiene un solo superpeer dueño), así que el orden que importa es el de cada usuario:
  - user_seq: la operación número 57 de ese usuario.
  - epoch, epoch_owner: la "época" en que se hizo. Cada vez que un superpeer empieza a atender a un
    usuario abre una época nueva (start_epoch); así una operación de un dueño anterior que llega tarde
    se reconoce como vieja y se rechaza.
"""

import contextlib
import json
import os
import sys
import threading
import uuid
from dataclasses import dataclass

from dfsha.superpeer.dict_registry import block_from_dict, block_to_dict
from dfsha.superpeer.registry import Registry

LOG_FILE_NAME = "registry.log"
SNAPSHOT_FILE_NAME = "registry.snapshot.json"
DEFAULT_SNAPSHOT_EVERY = 1000
EXIT_CODE_LOG_FAILURE = 70       # código de salida si no se puede escribir el log (ver _append)


class CorruptLogError(Exception):
    """El log tiene una línea dañada que no es la última: no se puede reconstruir el registro con seguridad."""


@dataclass(frozen=True, order=True)
class UserPosition:
    """
    Hasta dónde va el log de un usuario. order=True compara campo por campo, en este orden:
    primero la época (y quién la abrió) y después seq. Así "más adelante" significa: una época más
    nueva gana siempre (es la del dueño actual), y dentro de la misma época, más operaciones.
    """
    epoch: int
    epoch_owner: str
    seq: int

    def epoch_key(self):
        """(época, quién la abrió): identifica a un único dueño en un único período."""
        return (self.epoch, self.epoch_owner)

    def to_dict(self):
        return {"epoch": self.epoch, "epoch_owner": self.epoch_owner, "seq": self.seq}

    @staticmethod
    def from_dict(data):
        return UserPosition(epoch=data["epoch"], epoch_owner=data["epoch_owner"], seq=data["seq"])

    @staticmethod
    def from_record(record):
        """La posición que trae una línea del log."""
        return UserPosition(epoch=record["epoch"], epoch_owner=record["epoch_owner"], seq=record["user_seq"])


ZERO_POSITION = UserPosition(epoch=0, epoch_owner="", seq=0)     # un usuario sin operaciones


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
        self._positions = {}                  # usuario -> UserPosition de su última operación
        self._record_listener = None          # función(record) que recibe cada operación local nueva (federación)

        os.makedirs(directory, exist_ok=True)
        self._recover()
        self._take_snapshot()                 # se arranca con todo en el snapshot y el log vacío

    # ---------- Federación: posiciones y operaciones de otros superpeers ----------

    def set_record_listener(self, listener):
        """
        listener(record) se llama con cada operación hecha en este superpeer, ya anotada en el log
        (no con las que llegan de otro superpeer). La usa ReplicatedRegistry para enviarlas a los demás.
        Se llama con el lock tomado: debe volver enseguida (sin llamadas por red).
        """
        self._record_listener = listener

    def position(self, user):
        """La UserPosition de la última operación de user (ZERO_POSITION si no tiene ninguna)."""
        with self._lock:
            return self._positions.get(user, ZERO_POSITION)

    def positions(self):
        """Copia de {usuario: UserPosition} de todos los usuarios con operaciones."""
        with self._lock:
            return dict(self._positions)

    def start_epoch(self, user, epoch, epoch_owner):
        """
        Anota que epoch_owner empieza a atender a user con la época epoch. Es una operación más del
        log de user (no cambia el espacio de nombres) y lleva su base: la posición en que empieza.
        Un superpeer que la recibe solo la acepta si está exactamente en esa base, es decir, si tiene
        la misma historia que el dueño nuevo.
        """
        with self._lock:
            base = self._positions.get(user, ZERO_POSITION)
            self._append({"op": "start_epoch", "user": user, "epoch": epoch, "epoch_owner": epoch_owner,
                          "user_seq": base.seq + 1, "base": base.to_dict()})

    def apply_record(self, record):
        """
        Aplica una operación que llegó del dueño de su usuario (otro superpeer) y la anota en el log
        con la misma posición que trae. Quien llama ya comprobó que es la siguiente (ReplicatedRegistry).
        Lanza FileSystemError si no se puede aplicar (la copia de ese usuario está desalineada).
        """
        local_record = dict(record)
        local_record.pop("seq", None)         # el seq global es el del log del otro superpeer: aquí se asigna uno nuevo
        with self._lock:
            self._replay(local_record)
            self._append(local_record, notify=False)

    def export_user(self, user):
        """{"position": ..., "namespace": ...}: lo necesario para que otro superpeer reemplace su copia de user."""
        with self._lock:
            position = self._positions.get(user, ZERO_POSITION)
            namespace = self._inner.export_user_state(user)
        return {"position": position.to_dict(), "namespace": namespace}

    def install_user(self, user, exported_user):
        """
        Reemplaza la copia de user por exported_user (lo devuelto por export_user en otro superpeer)
        y adopta su posición. Queda en el log como una operación "install_user" con el estado completo.
        """
        position = UserPosition.from_dict(exported_user["position"])
        with self._lock:
            self._inner.import_user_state(user, exported_user["namespace"])
            self._append({"op": "install_user", "user": user, "namespace": exported_user["namespace"],
                          "epoch": position.epoch, "epoch_owner": position.epoch_owner, "user_seq": position.seq},
                         notify=False)

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

    def under_replicated_blocks(self, available_peer_ids):
        return self._inner.under_replicated_blocks(available_peer_ids)

    def export_state(self):
        return self._inner.export_state()

    def import_state(self, state):
        raise NotImplementedError("PersistentRegistry builds its state from its own snapshot and log")

    def export_user_state(self, user):
        return self._inner.export_user_state(user)

    def import_user_state(self, user, state):
        raise NotImplementedError("use install_user: it also records the position")

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

    def add_block_replica(self, user, path, block_id, peer_id):
        # Se anota: si no, después de un reinicio el registro olvidaría la copia y la volvería a hacer
        with self._lock:
            self._inner.add_block_replica(user, path, block_id, peer_id)
            self._append({"op": "add_block_replica", "user": user, "path": path,
                          "block_id": block_id, "peer_id": peer_id})

    def replay_commit_file(self, user, path, size, blocks, now):
        blocks = list(blocks)
        with self._lock:
            obsolete_blocks = self._inner.replay_commit_file(user, path, size, blocks, now)
            self._append({"op": "commit_file", "user": user, "path": path, "size": size,
                          "blocks": [block_to_dict(block) for block in blocks], "now": now})
            return obsolete_blocks

    # ---------- Log (quien llama debe tener self._lock) ----------

    def _append(self, record, notify=True):
        """
        Anota record en el log y espera a que esté en disco (fsync): cuando el cliente recibe "ok",
        la operación ya sobrevive a una caída.
        Si record no trae posición (una operación hecha aquí), le toca la siguiente de su usuario en
        la época actual. Las que llegan de otro superpeer (apply_record, install_user) ya la traen.
        Con notify, record se le pasa a la federación para que lo envíe a los demás superpeers.
        Si el log no se puede escribir (disco lleno, disco dañado), el cambio ya está aplicado en
        memoria pero no en disco: seguir funcionando haría que memoria y disco diverjan. Por eso el
        superpeer se detiene (fail-stop); al reiniciar se reconstruye desde el log, sin esa operación,
        que el cliente nunca vio confirmada.
        """
        user = record["user"]
        if "user_seq" not in record:
            current = self._positions.get(user, ZERO_POSITION)
            record = {**record, "epoch": current.epoch, "epoch_owner": current.epoch_owner,
                      "user_seq": current.seq + 1}
        record = {"seq": self._last_seq + 1, **record}
        try:
            self._log_file.write(json.dumps(record) + "\n")
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
        except OSError as error:
            print(f"FATAL: cannot write registry log {self._log_path}: {error}", file=sys.stderr, flush=True)
            os._exit(EXIT_CODE_LOG_FAILURE)    # _exit y no exit: termina el proceso entero, no solo este hilo
        self._last_seq = record["seq"]
        self._positions[user] = UserPosition.from_record(record)
        if notify and self._record_listener is not None:
            self._record_listener(record)
        self._operations_since_snapshot += 1
        if self._operations_since_snapshot >= self._snapshot_every:
            self._take_snapshot()

    def _take_snapshot(self):
        """
        Guarda la foto completa (de forma atómica) y después vacía el log.
        Si el proceso se cae entre las dos cosas, el log tendrá operaciones que ya están en la
        foto: _recover las salta porque su seq no es mayor que el de la foto.
        """
        exported_positions = {}
        for user, position in self._positions.items():
            exported_positions[user] = position.to_dict()
        snapshot = {"seq": self._last_seq, "state": self._inner.export_state(), "positions": exported_positions}
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
            exported_positions = snapshot.get("positions")
            if exported_positions is None:
                # Snapshot de antes de la federación: cada usuario con datos cuenta como si tuviera una
                # operación, para que un superpeer nuevo (sin nada) vea que está atrasado y lo copie
                for user in snapshot["state"]["namespaces"]:
                    self._positions[user] = UserPosition(epoch=0, epoch_owner="", seq=1)
            else:
                for user, exported_position in exported_positions.items():
                    self._positions[user] = UserPosition.from_dict(exported_position)

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
            user = record["user"]
            if "user_seq" in record:
                self._positions[user] = UserPosition.from_record(record)
            else:
                # Línea de antes de la federación (sin posición): cuenta como la siguiente de su usuario
                previous = self._positions.get(user, ZERO_POSITION)
                self._positions[user] = UserPosition(previous.epoch, previous.epoch_owner, previous.seq + 1)

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
        elif operation == "add_block_replica":
            self._inner.add_block_replica(record["user"], record["path"], record["block_id"], record["peer_id"])
        elif operation == "commit_file":
            blocks = [block_from_dict(block) for block in record["blocks"]]
            self._inner.replay_commit_file(record["user"], record["path"], record["size"], blocks, record["now"])
        elif operation == "start_epoch":
            pass                              # no cambia el espacio de nombres: solo la posición (la pone quien llama)
        elif operation == "install_user":
            self._inner.import_user_state(record["user"], record["namespace"])
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
