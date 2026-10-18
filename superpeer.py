import argparse
import posixpath
from concurrent import futures
import time

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from errors import translate_file_system_errors_to_grpc
from dict_registry import DictRegistry      # la ÚNICA línea que elige la implementación del registro

DEMO_USER = "demo"

def clean_path(path):
    """Normaliza la ruta y asegura que comience con '/'"""
    return posixpath.normpath("/" + path.lstrip("/"))

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id, registry):
        self.node_id = node_id
        self.registry = registry   # cualquier implementación de registry.Registry
        self.tokens = {}           # token -> user (placeholder authentication)

    # ---------- RPC ----------
    # Patrón de todos los handlers:
    #   1. Limpiar los paths que llegan por la red (clean_path).
    #   2. Llamar al registro dentro de translate_file_system_errors_to_grpc, que convierte
    #      cualquier FileSystemError en un error gRPC con su errno.
    #   3. Responder.
    # El superpeer no sabe cómo está guardado el registro, ni si necesita locks:
    # eso lo resuelve la implementación de Registry.

    def Stat(self, request, context):
        """RPC: metadatos de un path. Lo usa getattr en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            entry = self.registry.stat(DEMO_USER, path)          # CAMBIAR A USUARIO REAL
        return pb.Attr(is_dir=entry.is_dir, size=entry.size, mtime=entry.mtime)

    def ListDir(self, request, context):
        """RPC: nombres de los hijos directos de un directorio. Lo usa readdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            child_names = self.registry.list_directory(DEMO_USER, path)
        return pb.DirListing(names=child_names)

    def MakeDir(self, request, context):
        """RPC: crea un directorio vacío. Lo usa mkdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.make_directory(DEMO_USER, path, now=time.time())
        return pb.Empty()

    def RemoveDir(self, request, context):
        """RPC: borra un directorio vacío. Lo usa rmdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.remove_directory(DEMO_USER, path, now=time.time())
        return pb.Empty()

    def CreateFile(self, request, context):
        """RPC: crea un archivo vacío. Lo usa create (touch, primer paso de cp) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.create_file(DEMO_USER, path, now=time.time())
        return pb.Empty()

    def Delete(self, request, context):
        """RPC: borra un archivo. Lo usa unlink (comando rm) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.delete_file(DEMO_USER, path, now=time.time())
        return pb.Empty()

    def Rename(self, request, context):
        """RPC: mueve o renombra un archivo o directorio. Lo usa rename (comando mv) en el mount."""
        source_path = clean_path(request.source_path)
        destination_path = clean_path(request.destination_path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.rename(DEMO_USER, source_path, destination_path, now=time.time())
        return pb.Empty()


def seed_demo_entries(registry):
    """
    Entradas de ejemplo para arrancar con --demo, creadas con las mismas operaciones
    que usan los clientes. Los archivos quedan vacíos (tamaño 0): no tienen contenido real.
    """
    now = time.time()
    registry.make_directory(DEMO_USER, "/docs", now)
    registry.create_file(DEMO_USER, "/docs/report.pdf", now)
    registry.create_file(DEMO_USER, "/docs/notes.txt", now)
    registry.make_directory(DEMO_USER, "/photos", now)
    registry.create_file(DEMO_USER, "/readme.md", now)


def serve(node_id, port, demo):
    registry = DictRegistry()      # para cambiar de registro, se cambia esta línea (y el import)
    if demo:
        seed_demo_entries(registry)
    servicer = Superpeer(node_id, registry)
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
