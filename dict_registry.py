"""
Implementación del registro con un diccionario plano por usuario (la estructura original):

    {"demo": {"/":           {"is_dir": True,  "size": 0, "mtime": ...},
              "/docs":       {"is_dir": True,  "size": 0, "mtime": ...},
              "/docs/a.txt": {"is_dir": False, "size": 0, "mtime": ...}}}

Ventaja: stat es una sola búsqueda en el diccionario.
Costo: list_directory, remove_directory y rename recorren TODAS las entradas del usuario.
Si eso se vuelve un problema, se escribe otra implementación de Registry (p. ej. un árbol)
y se cambia una sola línea en superpeer.py.
"""

import errno
import posixpath
import threading
import time

from errors import FileSystemError
from registry import EntryInfo, Registry


class DictRegistry(Registry):
    """Registro en memoria: {usuario: {path absoluto: entrada}}."""

    def __init__(self):
        self._lock = threading.Lock()    # gRPC atiende varias llamadas a la vez, en hilos distintos
        self._namespaces = {}            # usuario -> {path absoluto: {"is_dir", "size", "mtime"}}

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

            user_namespace[path] = {"is_dir": False, "size": 0, "mtime": now}
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
