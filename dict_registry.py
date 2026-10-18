"""
Implementación del registro con un diccionario plano por usuario (la estructura original):

    {"demo": {"/":           {"is_dir": True,  "size": 0, "mtime": ...},
              "/docs":       {"is_dir": True,  "size": 0, "mtime": ...},
              "/docs/a.txt": {"is_dir": False, "size": 0, "mtime": ..., "blocks": [BlockInfo, ...]}}}

Los archivos además guardan "blocks": sus bloques en orden (el bloque i tiene los bytes
desde i * block_size), así que el byte N de un archivo está en blocks[N // block_size].

Ventaja: stat es una sola búsqueda en el diccionario.
Costo: list_directory, remove_directory y rename recorren TODAS las entradas del usuario.
Si eso se vuelve un problema, se escribe otra implementación de Registry (p. ej. un árbol)
y se cambia una sola línea en superpeer.py.
"""

import errno
import posixpath
import threading
import time
import uuid

from errors import FileSystemError
from registry import BlockInfo, EntryInfo, Registry

DEFAULT_BLOCK_SIZE = 1024 * 1024     # 1 MiB: en desarrollo, para que hasta un archivo chico se parta en bloques
DEFAULT_REPLICATION_FACTOR = 2       # cada bloque en 2 peers distintos: aguanta la caída de uno


def fixed_test_peers():
    """
    Peers inventados, mientras no exista la tabla real de peers vivos (Paso 1).
    Cuando exista, superpeer.py le pasa a DictRegistry una función que consulte esa tabla.
    """
    return ["peer1:7001", "peer2:7002"]


class DictRegistry(Registry):
    """Registro en memoria: {usuario: {path absoluto: entrada}}."""

    def __init__(self, live_peers=fixed_test_peers, block_size=DEFAULT_BLOCK_SIZE,
                 replication_factor=DEFAULT_REPLICATION_FACTOR):
        self._lock = threading.Lock()    # gRPC atiende varias llamadas a la vez, en hilos distintos
        self._namespaces = {}            # usuario -> {path absoluto: {"is_dir", "size", "mtime", "blocks"}}

        # Una función y no una lista: los peers aparecen y se caen, así que se pregunta en cada asignación
        self._live_peers = live_peers                    # () -> lista de direcciones "host:puerto"
        self._block_size = block_size
        self._replication_factor = replication_factor
        self._next_peer = 0              # cursor del round robin: dónde empieza el próximo bloque
        self._pending_blocks = {}        # block_id -> (usuario, path, BlockInfo): asignados, sin commit todavía

    # ---------- Helpers internos (quien llama debe tener self._lock) ----------

    def _namespace(self, user):
        """
        Devuelve el diccionario {path: entrada} del usuario.
        Si el usuario todavía no tiene uno, se lo crea con solo el directorio raíz "/".
        """
        if user not in self._namespaces:
            root_entry = {"is_dir": True, "size": 0, "mtime": time.time()}
            self._namespaces[user] = {"/": root_entry}
        return self._namespaces[user]

    def _child_names(self, user_namespace, directory_path):
        """Nombres de los hijos directos de un directorio, p. ej. "/docs" -> ["notes.txt", "report.pdf"]."""
        child_names = []
        for entry_path in user_namespace:
            if entry_path == "/":
                continue                                  # la raíz no es hija de nadie
            parent_path = posixpath.dirname(entry_path)   # "/docs/a.txt" -> "/docs"
            if parent_path == directory_path:
                child_names.append(posixpath.basename(entry_path))   # -> "a.txt"
        return child_names

    def _get_parent_directory(self, user_namespace, path):
        """
        Devuelve la entrada del directorio que contendría a path.
        Lanza FileSystemError(ENOENT) si ese directorio no existe,
        o FileSystemError(ENOTDIR) si es un archivo.
        """
        parent_path = posixpath.dirname(path)             # "/docs/new" -> "/docs"
        parent_entry = user_namespace.get(parent_path)
        if parent_entry is None:
            raise FileSystemError(errno.ENOENT, parent_path)
        if not parent_entry["is_dir"]:
            raise FileSystemError(errno.ENOTDIR, parent_path)
        return parent_entry

    def _mark_parent_modified(self, user_namespace, path, now):
        """
        Actualiza la fecha de modificación del directorio que contiene a path.
        Convención POSIX: agregar o quitar un hijo modifica al directorio (ls -l lo muestra).
        """
        parent_path = posixpath.dirname(path)
        user_namespace[parent_path]["mtime"] = now

    def _get_file_entry(self, user_namespace, path):
        """
        Devuelve la entrada del archivo path.
        Lanza FileSystemError(ENOENT) si no existe, o FileSystemError(EISDIR) si es un directorio.
        """
        file_entry = user_namespace.get(path)
        if file_entry is None:
            raise FileSystemError(errno.ENOENT, path)
        if file_entry["is_dir"]:
            raise FileSystemError(errno.EISDIR, path)
        return file_entry

    def _choose_peers(self, peer_addresses):
        """
        Elige replication_factor peers distintos para un bloque, por turnos (round robin),
        como quien reparte cartas: toma los siguientes peers desde el cursor y deja el cursor
        donde terminó, para que el próximo bloque empiece en otros peers.
        Con peers [A, B, C, D] y factor 2: bloque 0 -> (A, B), bloque 1 -> (C, D), bloque 2 -> (A, B)...
        Quien llama garantiza que hay al menos replication_factor peers, así que nunca se repite uno.
        """
        chosen_peers = []
        for offset in range(self._replication_factor):
            position = (self._next_peer + offset) % len(peer_addresses)   # % da la vuelta al final de la lista
            chosen_peers.append(peer_addresses[position])
        self._next_peer = (self._next_peer + self._replication_factor) % len(peer_addresses)
        return tuple(chosen_peers)

    # ---------- Consultas ----------
    # Cada método público toma el lock: así es atómico aunque gRPC lo llame desde varios hilos.
    # Si se lanza un FileSystemError adentro del 'with', el lock se libera igual.

    def stat(self, user, path):
        with self._lock:
            entry = self._namespace(user).get(path)
            if entry is None:
                raise FileSystemError(errno.ENOENT, path)
            return EntryInfo(is_dir=entry["is_dir"], size=entry["size"], mtime=entry["mtime"])

    def list_directory(self, user, path):
        with self._lock:
            user_namespace = self._namespace(user)
            directory_entry = user_namespace.get(path)

            if directory_entry is None:
                raise FileSystemError(errno.ENOENT, path)
            if not directory_entry["is_dir"]:
                raise FileSystemError(errno.ENOTDIR, path)

            return sorted(self._child_names(user_namespace, path))

    # ---------- Modificaciones ----------

    def make_directory(self, user, path, now):
        with self._lock:
            user_namespace = self._namespace(user)

            # Validaciones POSIX: el padre existe y es directorio, y el nombre está libre
            self._get_parent_directory(user_namespace, path)
            if path in user_namespace:
                raise FileSystemError(errno.EEXIST, path)

            user_namespace[path] = {"is_dir": True, "size": 0, "mtime": now}
            self._mark_parent_modified(user_namespace, path, now)

    def remove_directory(self, user, path, now):
        with self._lock:
            user_namespace = self._namespace(user)
            directory_entry = user_namespace.get(path)

            # Validaciones POSIX: no es la raíz, existe, es directorio y está vacío
            if path == "/":
                raise FileSystemError(errno.EBUSY, path)            # la raíz nunca se borra
            if directory_entry is None:
                raise FileSystemError(errno.ENOENT, path)
            if not directory_entry["is_dir"]:
                raise FileSystemError(errno.ENOTDIR, path)
            child_names = self._child_names(user_namespace, path)
            if len(child_names) > 0:
                raise FileSystemError(errno.ENOTEMPTY, path)        # rm -r borra el contenido antes

            del user_namespace[path]
            self._mark_parent_modified(user_namespace, path, now)

    def create_file(self, user, path, now):
        with self._lock:
            user_namespace = self._namespace(user)

            # Validaciones POSIX: el padre existe y es directorio, y el nombre está libre
            self._get_parent_directory(user_namespace, path)
            if path in user_namespace:
                raise FileSystemError(errno.EEXIST, path)

            user_namespace[path] = {"is_dir": False, "size": 0, "mtime": now, "blocks": []}
            self._mark_parent_modified(user_namespace, path, now)

    def delete_file(self, user, path, now):
        with self._lock:
            user_namespace = self._namespace(user)
            file_entry = user_namespace.get(path)

            # Validaciones POSIX: existe y no es directorio (los directorios se borran con rmdir)
            if file_entry is None:
                raise FileSystemError(errno.ENOENT, path)
            if file_entry["is_dir"]:
                raise FileSystemError(errno.EISDIR, path)

            del user_namespace[path]
            self._mark_parent_modified(user_namespace, path, now)
            # Tarea 4: aquí también habrá que liberar los bloques del archivo en los peers

    def rename(self, user, source_path, destination_path, now):
        with self._lock:
            user_namespace = self._namespace(user)
            source_entry = user_namespace.get(source_path)
            destination_entry = user_namespace.get(destination_path)

            # 1. Validaciones POSIX
            if source_path == "/" or destination_path == "/":
                raise FileSystemError(errno.EBUSY, "/")             # la raíz no se mueve ni se reemplaza
            if source_entry is None:
                raise FileSystemError(errno.ENOENT, source_path)
            self._get_parent_directory(user_namespace, destination_path)
            if destination_path.startswith(source_path + "/"):
                # mover un directorio dentro de sí mismo
                raise FileSystemError(errno.EINVAL, destination_path)
            if source_path == destination_path:
                return                                              # mismo nombre: no hay nada que hacer

            # 2. Si el destino ya existe, se reemplaza, pero solo si es del mismo tipo
            #    (y, si es directorio, solo si está vacío)
            if destination_entry is not None:
                if source_entry["is_dir"] and not destination_entry["is_dir"]:
                    raise FileSystemError(errno.ENOTDIR, destination_path)
                if not source_entry["is_dir"] and destination_entry["is_dir"]:
                    raise FileSystemError(errno.EISDIR, destination_path)
                if destination_entry["is_dir"]:
                    destination_children = self._child_names(user_namespace, destination_path)
                    if len(destination_children) > 0:
                        raise FileSystemError(errno.ENOTEMPTY, destination_path)
                del user_namespace[destination_path]

            # 3. Reunir las entradas a mover: la fuente y, si es directorio, todo lo que está debajo.
            #    Primero se reúnen y después se mueven, porque Python no permite cambiar las
            #    llaves de un diccionario mientras se recorre.
            paths_to_move = []
            for entry_path in user_namespace:
                is_the_source_itself = (entry_path == source_path)
                is_inside_the_source = entry_path.startswith(source_path + "/")
                if is_the_source_itself or is_inside_the_source:
                    paths_to_move.append(entry_path)

            # 4. Mover: se reemplaza el prefijo viejo por el nuevo
            #    "/docs" -> "/archive/docs" convierte "/docs/a.txt" en "/archive/docs/a.txt"
            for old_path in paths_to_move:
                path_below_source = old_path[len(source_path):]          # "" o "/a.txt"
                new_path = destination_path + path_below_source
                user_namespace[new_path] = user_namespace.pop(old_path)  # pop: saca la entrada vieja

            # 5. Ambos directorios (el de origen y el de destino) cambiaron de contenido
            self._mark_parent_modified(user_namespace, source_path, now)
            self._mark_parent_modified(user_namespace, destination_path, now)

    # ---------- Bloques ----------

    def allocate_blocks(self, user, path, file_size):
        with self._lock:
            user_namespace = self._namespace(user)

            # 1. Validaciones: es un archivo existente, el tamaño tiene sentido y hay peers suficientes
            self._get_file_entry(user_namespace, path)
            if file_size < 0:
                raise FileSystemError(errno.EINVAL, path)
            peer_addresses = list(self._live_peers())    # una sola foto de los peers para todo el archivo
            if file_size > 0 and len(peer_addresses) < self._replication_factor:
                raise FileSystemError(errno.ENOSPC, path)    # no se escribe con menos réplicas de las acordadas

            # 2. Partir file_size en bloques de block_size; el último se queda con lo que sobra
            #    2.5 MiB con bloques de 1 MiB -> [1 MiB, 1 MiB, 0.5 MiB]
            blocks = []
            remaining_bytes = file_size
            while remaining_bytes > 0:
                this_block_size = min(self._block_size, remaining_bytes)
                block = BlockInfo(block_id=uuid.uuid4().hex, size=this_block_size,
                                  peers=self._choose_peers(peer_addresses))
                blocks.append(block)
                remaining_bytes -= this_block_size

            # 3. Recordar a quién se le asignó cada bloque, para validarlo en commit_file
            for block in blocks:
                self._pending_blocks[block.block_id] = (user, path, block)
            return blocks

    def commit_file(self, user, path, size, blocks, now):
        with self._lock:
            user_namespace = self._namespace(user)
            blocks = list(blocks)        # se recorre varias veces: si llega un iterador, se agotaría

            # 1. Validaciones: es un archivo existente y el tamaño tiene sentido
            file_entry = self._get_file_entry(user_namespace, path)
            if size < 0:
                raise FileSystemError(errno.EINVAL, path)

            # 2. Cada bloque es exactamente uno de los asignados a este usuario y path, sin repetir.
            #    Fase 8: _pending_blocks vive en memoria y no sobrevive a un reinicio, así que el
            #    replay desde un log no podrá pasar por esta validación (el log solo guardará
            #    commits que ya la pasaron).
            seen_block_ids = set()
            for block in blocks:
                if self._pending_blocks.get(block.block_id) != (user, path, block):
                    raise FileSystemError(errno.EINVAL, path)
                if block.block_id in seen_block_ids:
                    raise FileSystemError(errno.EINVAL, path)
                seen_block_ids.add(block.block_id)

            # 3. Los tamaños encajan: todos menos el último miden block_size (para que el byte N
            #    siga estando en blocks[N // block_size]) y entre todos suman size
            for block in blocks[:-1]:
                if block.size != self._block_size:
                    raise FileSystemError(errno.EINVAL, path)
            if sum(block.size for block in blocks) != size:
                raise FileSystemError(errno.EINVAL, path)

            # 4. Cambiar de versión de una vez: como estamos dentro del lock, nadie ve una mezcla.
            #    El mtime del directorio no cambia: escribir un archivo no agrega ni quita hijos.
            obsolete_blocks = file_entry["blocks"]
            file_entry["blocks"] = blocks
            file_entry["size"] = size
            file_entry["mtime"] = now
            for block in blocks:
                del self._pending_blocks[block.block_id]     # ya no están pendientes: son del archivo
            return obsolete_blocks

    def get_block_locations(self, user, path):
        with self._lock:
            file_entry = self._get_file_entry(self._namespace(user), path)
            return list(file_entry["blocks"])    # copia: quien la recibe no puede cambiar el registro sin el lock
