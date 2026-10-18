import argparse
import errno
import os
import posixpath
import threading
from concurrent import futures
import time

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc

DEMO_USER = "demo"

def clean_path(path):
    """Normaliza la ruta y asegura que comience con '/'"""
    return posixpath.normpath("/" + path.lstrip("/"))

# Traducción errno -> código gRPC, para clientes que no leen la metadata (scripts, pruebas).
# El mount no la necesita: recibe el errno exacto en la metadata.
GRPC_CODE = {
    errno.ENOENT: grpc.StatusCode.NOT_FOUND,             # No such file or directory
    errno.EEXIST: grpc.StatusCode.ALREADY_EXISTS,        # File exists
    errno.EACCES: grpc.StatusCode.PERMISSION_DENIED,     # Permission denied
    errno.ENOTEMPTY: grpc.StatusCode.FAILED_PRECONDITION # Directory not empty
}

def fail(context, error_number, path):
    """
    Termina el RPC actual con un error de sistema de archivos.
    error_number: un errno de Linux, p. ej. errno.ENOENT ("No such file or directory").
    Nunca retorna: context.abort lanza una excepción que termina el handler.
    """
    # 1. El errno exacto viaja en la metadata final, para que el mount se lo entregue tal cual al kernel
    context.set_trailing_metadata((("errno", str(error_number)),))

    # 2. Código gRPC equivalente, para clientes que no leen la metadata (scripts, pruebas)
    grpc_status_code = GRPC_CODE.get(error_number, grpc.StatusCode.INVALID_ARGUMENT)

    # 3. Mensaje legible, p. ej. "/docs: No such file or directory"
    readable_message = f"{path}: {os.strerror(error_number)}"

    context.abort(grpc_status_code, readable_message)

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.Lock()
        self.namespaces = {}  # namespace: user -> {path -> entry} (placeholder registry)
        self.tokens = {}  # token -> user (placeholder authentication)

    # ---------- Helpers (internos: no son RPC, quien llama debe tener self.lock) ----------

    def _namespace(self, user):
        """
        Devuelve el registro del usuario: un diccionario {path absoluto: metadatos}.
        Si el usuario todavía no tiene registro, se lo crea con solo el directorio raíz "/".
        Quien llama debe tener self.lock.
        """
        if user not in self.namespaces:
            root_entry = {"is_dir": True, "size": 0, "mtime": time.time()}
            self.namespaces[user] = {"/": root_entry}
        return self.namespaces[user]

    def _add(self, user, path, is_dir, size=0):
        """
        Agrega (o reemplaza) una entrada en el registro del usuario.
        No valida nada: quien llama debe comprobar antes que el padre exista y el nombre esté libre.
        Quien llama debe tener self.lock.
        """
        user_namespace = self._namespace(user)
        normalized_path = clean_path(path)
        new_entry = {"is_dir": is_dir, "size": size, "mtime": time.time()}
        user_namespace[normalized_path] = new_entry

    def _child_names(self, user_namespace, directory_path):
        """
        Nombres de los hijos directos de un directorio, p. ej. "/docs" -> ["notes.txt", "report.pdf"].
        Quien llama debe tener self.lock.
        """
        child_names = []
        for entry_path in user_namespace:
            if entry_path == "/":
                continue                                  # la raíz no es hija de nadie
            parent_path = posixpath.dirname(entry_path)   # "/docs/a.txt" -> "/docs"
            if parent_path == directory_path:
                child_names.append(posixpath.basename(entry_path))   # -> "a.txt"
        return child_names

    def _get_parent_directory_or_fail(self, user_namespace, path, context):
        """
        Devuelve la entrada del directorio que contendría a path.
        Termina el RPC con ENOENT si ese directorio no existe, o ENOTDIR si es un archivo.
        Quien llama debe tener self.lock.
        """
        parent_path = posixpath.dirname(path)             # "/docs/new" -> "/docs"
        parent_entry = user_namespace.get(parent_path)
        if parent_entry is None:
            fail(context, errno.ENOENT, parent_path)
        if not parent_entry["is_dir"]:
            fail(context, errno.ENOTDIR, parent_path)
        return parent_entry

    def _mark_parent_modified(self, user_namespace, path):
        """
        Actualiza la fecha de modificación del directorio que contiene a path.
        Convención POSIX: agregar o quitar un hijo modifica al directorio (ls -l lo muestra).
        Quien llama debe tener self.lock.
        """
        parent_path = posixpath.dirname(path)
        user_namespace[parent_path]["mtime"] = time.time()

    def seed_demo(self):
        """Entradas de ejemplo para tener algo que listar al arrancar con --demo."""
        with self.lock:
            self._add(DEMO_USER, "/docs", is_dir=True)
            self._add(DEMO_USER, "/docs/report.pdf", is_dir=False, size=90_000)
            self._add(DEMO_USER, "/docs/notes.txt", is_dir=False, size=120)
            self._add(DEMO_USER, "/photos", is_dir=True)
            self._add(DEMO_USER, "/readme.md", is_dir=False, size=42)

    # ---------- RPC: consultas (no modifican el registro) ----------

    def Stat(self, request, context):
        """RPC: metadatos de un path. Lo usa getattr en el mount."""
        requested_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)     # CAMBIAR A USUARIO REAL
            entry = user_namespace.get(requested_path)       # None si no existe

        if entry is None:
            fail(context, errno.ENOENT, requested_path)
        return pb.Attr(is_dir=entry["is_dir"], size=entry["size"], mtime=entry["mtime"])

    def ListDir(self, request, context):
        """RPC: nombres de los hijos directos de un directorio. Lo usa readdir en el mount."""
        directory_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)     # CAMBIAR A USUARIO REAL
            directory_entry = user_namespace.get(directory_path)
            child_names = self._child_names(user_namespace, directory_path)

        if directory_entry is None:
            fail(context, errno.ENOENT, directory_path)
        if not directory_entry["is_dir"]:
            fail(context, errno.ENOTDIR, directory_path)
        return pb.DirListing(names=sorted(child_names))

    # ---------- RPC: modificaciones del registro ----------
    # Todas validan y modifican DENTRO del lock: así dos clientes no pueden, por ejemplo,
    # crear el mismo nombre a la vez. fail() dentro del lock es seguro: lanza una excepción
    # y el "with" libera el lock automáticamente.

    def MakeDir(self, request, context):
        """RPC: crea un directorio vacío. Lo usa mkdir en el mount."""
        new_directory_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)          # CAMBIAR A USUARIO REAL

            # Validaciones POSIX: el padre existe y es directorio, y el nombre está libre
            self._get_parent_directory_or_fail(user_namespace, new_directory_path, context)
            if new_directory_path in user_namespace:
                fail(context, errno.EEXIST, new_directory_path)

            self._add(DEMO_USER, new_directory_path, is_dir=True)
            self._mark_parent_modified(user_namespace, new_directory_path)

        return pb.Empty()

    def RemoveDir(self, request, context):
        """RPC: borra un directorio vacío. Lo usa rmdir en el mount."""
        directory_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)          # CAMBIAR A USUARIO REAL
            directory_entry = user_namespace.get(directory_path)

            # Validaciones POSIX: no es la raíz, existe, es directorio y está vacío
            if directory_path == "/":
                fail(context, errno.EBUSY, directory_path)       # la raíz nunca se borra
            if directory_entry is None:
                fail(context, errno.ENOENT, directory_path)
            if not directory_entry["is_dir"]:
                fail(context, errno.ENOTDIR, directory_path)
            child_names = self._child_names(user_namespace, directory_path)
            if len(child_names) > 0:
                fail(context, errno.ENOTEMPTY, directory_path)   # rm -r borra el contenido antes

            del user_namespace[directory_path]
            self._mark_parent_modified(user_namespace, directory_path)

        return pb.Empty()

    def Delete(self, request, context):
        """RPC: borra un archivo. Lo usa unlink (comando rm) en el mount."""
        file_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)          # CAMBIAR A USUARIO REAL
            file_entry = user_namespace.get(file_path)

            # Validaciones POSIX: existe y no es directorio (los directorios se borran con rmdir)
            if file_entry is None:
                fail(context, errno.ENOENT, file_path)
            if file_entry["is_dir"]:
                fail(context, errno.EISDIR, file_path)

            del user_namespace[file_path]
            self._mark_parent_modified(user_namespace, file_path)
            # Tarea 4: aquí también se liberarán los bloques del archivo en los peers

        return pb.Empty()

    def CreateFile(self, request, context):
        """RPC: crea un archivo vacío. Lo usa create (touch, primer paso de cp) en el mount."""
        new_file_path = clean_path(request.path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)          # CAMBIAR A USUARIO REAL

            # Validaciones POSIX: el padre existe y es directorio, y el nombre está libre
            self._get_parent_directory_or_fail(user_namespace, new_file_path, context)
            if new_file_path in user_namespace:
                fail(context, errno.EEXIST, new_file_path)

            self._add(DEMO_USER, new_file_path, is_dir=False, size=0)
            self._mark_parent_modified(user_namespace, new_file_path)

        return pb.Empty()

    def Rename(self, request, context):
        """
        RPC: mueve o renombra un archivo o directorio. Lo usa rename (comando mv) en el mount.
        Si se mueve un directorio, se mueve todo lo que hay debajo de él.
        """
        source_path = clean_path(request.source_path)
        destination_path = clean_path(request.destination_path)

        with self.lock:
            user_namespace = self._namespace(DEMO_USER)          # CAMBIAR A USUARIO REAL
            source_entry = user_namespace.get(source_path)
            destination_entry = user_namespace.get(destination_path)

            # 1. Validaciones POSIX
            if source_path == "/" or destination_path == "/":
                fail(context, errno.EBUSY, "/")                  # la raíz no se mueve ni se reemplaza
            if source_entry is None:
                fail(context, errno.ENOENT, source_path)
            self._get_parent_directory_or_fail(user_namespace, destination_path, context)
            if destination_path.startswith(source_path + "/"):
                fail(context, errno.EINVAL, destination_path)    # mover un directorio dentro de sí mismo
            if source_path == destination_path:
                return pb.Empty()                                # mismo nombre: no hay nada que hacer

            # 2. Si el destino ya existe, se reemplaza, pero solo si es del mismo tipo
            #    (y, si es directorio, solo si está vacío)
            if destination_entry is not None:
                if source_entry["is_dir"] and not destination_entry["is_dir"]:
                    fail(context, errno.ENOTDIR, destination_path)
                if not source_entry["is_dir"] and destination_entry["is_dir"]:
                    fail(context, errno.EISDIR, destination_path)
                if destination_entry["is_dir"]:
                    destination_children = self._child_names(user_namespace, destination_path)
                    if len(destination_children) > 0:
                        fail(context, errno.ENOTEMPTY, destination_path)
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
            self._mark_parent_modified(user_namespace, source_path)
            self._mark_parent_modified(user_namespace, destination_path)

        return pb.Empty()


def serve(node_id, port, demo):
    servicer = Superpeer(node_id)
    if demo:
        servicer.seed_demo()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    rpc.add_SuperpeerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"superpeer {node_id} listening on :{port}" + (" (demo data)" if demo else ""))
    server.wait_for_termination()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--id", default="sp1")
    p.add_argument("--port", type=int, default=7000)
    p.add_argument("--demo", action="store_true")
    a = p.parse_args()
    serve(a.id, a.port, a.demo)
