"""
Registro replicado: envuelve al PersistentRegistry y le agrega lo que hace falta con varios superpeers.

Capas (cada una envuelve a la anterior y el superpeer solo ve la última):
    DictRegistry        el espacio de nombres en memoria
    PersistentRegistry  cada modificación al log en disco, con su posición (época, dueño, seq)
    ReplicatedRegistry  solo el dueño modifica, y cada modificación llega a los demás superpeers

Toda modificación de un usuario sigue el mismo patrón (ver _write):
  1. Con el lock de ese usuario: sus operaciones van de a una, en orden, aunque lleguen a la vez.
  2. Si este superpeer no lo atiende ahora, NotServingError: el cliente se cambia de superpeer.
  3. Si es la primera operación desde que lo atiende, abre una época nueva (start_epoch).
  4. Aplica la operación (PersistentRegistry la anota en el log).
  5. La envía a los demás superpeers y espera sus respuestas (replicación sincrónica): cuando el
     cliente recibe "ok", la operación ya está en los otros superpeers vivos.
  6. Si alguno responde que la época es vieja, otro superpeer tomó al usuario: NotServingError.
Las consultas (stat, ls, ubicaciones) no cambian nada y pasan directo.
Los usuarios son independientes: el lock es por usuario, así que dos usuarios no se esperan entre sí.
"""

import errno
import threading

from dfsha.common.errors import FileSystemError
from dfsha.superpeer.persistent_registry import UserPosition
from dfsha.superpeer.registry import Registry


class NotServingError(FileSystemError):
    """
    Este superpeer no atiende a ese usuario ahora (el dueño es otro, o se está poniendo al día).
    Es EAGAIN ("Resource temporarily unavailable"), que viaja como UNAVAILABLE: el cliente lo
    trata como un superpeer caído y se cambia al dueño.
    """

    def __init__(self, user):
        super().__init__(errno.EAGAIN, f"user {user} is served by another superpeer now")


class ReplicatedRegistry(Registry):

    def __init__(self, persistent_registry, federation):
        self._inner = persistent_registry
        self._federation = federation
        self._user_locks = {}                    # usuario -> Lock: sus modificaciones van de a una
        self._user_locks_guard = threading.Lock()
        self._outbox = {}                        # usuario -> líneas del log todavía sin enviar a los demás
        self._outbox_lock = threading.Lock()
        self._epoch_opened = {}                  # usuario -> época que abrió este superpeer al empezar a atenderlo
        self._inner.set_record_listener(self._remember_record)

    # ---------- Helpers ----------

    def _user_lock(self, user):
        with self._user_locks_guard:
            if user not in self._user_locks:
                self._user_locks[user] = threading.Lock()
            return self._user_locks[user]

    def _remember_record(self, record):
        """PersistentRegistry la llama con cada operación local recién anotada: queda para enviarla (paso 5)."""
        with self._outbox_lock:
            if record["user"] not in self._outbox:
                self._outbox[record["user"]] = []
            self._outbox[record["user"]].append(record)

    def _write(self, user, operation, *arguments):
        """
        El patrón de toda modificación (ver el comentario del módulo). operation es el método del
        PersistentRegistry que la aplica; devuelve lo que devuelva operation.
        """
        with self._user_lock(user):
            if not self._federation.serves(user):
                raise NotServingError(user)
            self._open_epoch_if_needed(user)
            try:
                result = operation(*arguments)     # si lanza FileSystemError (p. ej. EEXIST), no se anotó nada
            finally:
                # Se envía aunque la operación haya fallado: la apertura de la época sí quedó anotada
                someone_has_newer_epoch = self._send_pending_records(user)
            if someone_has_newer_epoch:
                # Otro superpeer tomó al usuario mientras este creía tenerlo: esta operación no vale
                # (se reemplazará al ponerse al día con el otro) y el cliente debe reintentar en el dueño
                raise NotServingError(user)
            return result

    def _open_epoch_if_needed(self, user):
        """
        Abre una época nueva si este superpeer recién empieza a atender a user: la primera vez desde
        que arrancó, o si la última operación de user la hizo otro superpeer. La época nueva es mayor
        que cualquiera conocida, así una operación del dueño anterior que llegue tarde se reconoce como vieja.
        """
        position = self._inner.position(user)
        my_id = self._federation.my_id
        if position.epoch_owner == my_id and self._epoch_opened.get(user) == position.epoch:
            return                                  # sigue en la época que abrió: nadie más la cambió
        highest_epoch = max(position.epoch, self._federation.highest_known_epoch(user))
        new_epoch = highest_epoch + 1
        self._inner.start_epoch(user, new_epoch, my_id)
        self._epoch_opened[user] = new_epoch
        print(f"federation: serving user {user} (epoch {new_epoch})", flush=True)

    def _send_pending_records(self, user):
        """
        Envía a los demás superpeers las operaciones de user que quedaron anotadas, en orden.
        Devuelve True si alguno respondió que este superpeer tiene una época vieja.
        """
        with self._outbox_lock:
            records = self._outbox.pop(user, [])
        someone_has_newer_epoch = False
        for record in records:
            for superpeer_id, reply in self._federation.send_record(record):
                if reply.stale_epoch:
                    someone_has_newer_epoch = True
        return someone_has_newer_epoch

    # ---------- Lo que llega de otros superpeers ----------

    def apply_replicated(self, record):
        """
        Una operación que envió el dueño de su usuario (AppendLog). Devuelve (resultado, posición
        de este superpeer después), con resultado:
          "applied"    era la siguiente: aplicada y anotada.
          "duplicate"  ya la tenía (un reintento).
          "stale"      viene de una época más vieja que la que conoce este superpeer: se rechaza.
          "behind"     no encaja (falta alguna anterior): no se aplica, y este superpeer se pondrá al
                       día con el estado completo (FetchUser) en su próxima vuelta de la federación.
        """
        user = record["user"]
        incoming = UserPosition.from_record(record)
        with self._user_lock(user):
            mine = self._inner.position(user)
            if incoming.epoch_key() < mine.epoch_key():
                return "stale", mine
            if incoming.epoch_key() == mine.epoch_key():
                if incoming.seq <= mine.seq:
                    return "duplicate", mine
                if incoming.seq != mine.seq + 1:
                    return "behind", mine
            else:
                # Una época más nueva solo se acepta desde su apertura, y si empieza justo donde va
                # este superpeer: así se sabe que el dueño nuevo tiene la misma historia hasta ahí
                if record["op"] != "start_epoch":
                    return "behind", mine
                if UserPosition.from_dict(record["base"]) != mine:
                    return "behind", mine
            try:
                self._inner.apply_record(record)
            except FileSystemError:
                return "behind", mine               # no encaja con la copia de este superpeer: se copiará entera
            return "applied", self._inner.position(user)

    def install_from(self, user, exported_user):
        """
        Reemplaza la copia de user por la de otro superpeer que va más adelante (FetchUser).
        No hace nada si mientras tanto esta copia ya llegó igual o más lejos. Devuelve True si la reemplazó.
        """
        incoming = UserPosition.from_dict(exported_user["position"])
        with self._user_lock(user):
            if incoming <= self._inner.position(user):
                return False
            self._inner.install_user(user, exported_user)
            self._epoch_opened.pop(user, None)      # si lo vuelve a atender, abrirá una época nueva
            return True

    def export_user(self, user):
        """El estado completo de user para otro superpeer (FetchUser). Sin el lock de usuario: ver federation.py."""
        return self._inner.export_user(user)

    def position(self, user):
        return self._inner.position(user)

    def positions(self):
        return self._inner.positions()

    # ---------- Consultas: directo al registro ----------

    def stat(self, user, path):
        return self._inner.stat(user, path)

    def list_directory(self, user, path):
        return self._inner.list_directory(user, path)

    def get_block_locations(self, user, path):
        return self._inner.get_block_locations(user, path)

    # ---------- Modificaciones: solo el dueño, y se replican ----------

    def make_directory(self, user, path, now):
        return self._write(user, self._inner.make_directory, user, path, now)

    def remove_directory(self, user, path, now):
        return self._write(user, self._inner.remove_directory, user, path, now)

    def create_file(self, user, path, now):
        return self._write(user, self._inner.create_file, user, path, now)

    def delete_file(self, user, path, now):
        return self._write(user, self._inner.delete_file, user, path, now)

    def rename(self, user, source_path, destination_path, now):
        return self._write(user, self._inner.rename, user, source_path, destination_path, now)

    def commit_file(self, user, path, size, blocks, now):
        return self._write(user, self._inner.commit_file, user, path, size, blocks, now)

    def add_block_replica(self, user, path, block_id, peer_id):
        return self._write(user, self._inner.add_block_replica, user, path, block_id, peer_id)

    def allocate_blocks(self, user, path, file_size):
        # El plan no se anota ni se replica (es temporal), pero solo el dueño puede darlo:
        # el commit que lo sigue también tiene que hacerse en el dueño
        if not self._federation.serves(user):
            raise NotServingError(user)
        return self._inner.allocate_blocks(user, path, file_size)

    # ---------- Tareas de fondo del superpeer ----------

    def under_replicated_blocks(self, available_peer_ids):
        # Solo los de los usuarios que atiende este superpeer: de los demás se encarga su dueño
        served_blocks = []
        for user, path, block in self._inner.under_replicated_blocks(available_peer_ids):
            if self._federation.serves(user):
                served_blocks.append((user, path, block))
        return served_blocks

    def expire_pending_blocks(self):
        return self._inner.expire_pending_blocks()

    # ---------- Persistencia: la maneja PersistentRegistry ----------

    def replay_commit_file(self, user, path, size, blocks, now):
        raise NotImplementedError("replays happen inside PersistentRegistry")

    def export_state(self):
        return self._inner.export_state()

    def import_state(self, state):
        raise NotImplementedError("PersistentRegistry builds its state from its own snapshot and log")

    def export_user_state(self, user):
        return self._inner.export_user_state(user)

    def import_user_state(self, user, state):
        raise NotImplementedError("use install_from: it also records the position")
