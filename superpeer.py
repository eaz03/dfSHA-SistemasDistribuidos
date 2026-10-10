import argparse
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

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.Lock()
        self.namespaces = {}  # namespace: user -> {path -> entry} (placeholder registry)
        self.tokens = {}  # user -> token (placeholder authentication)

    # Helpers
    def _namespace(self, user):
        """Obtiene o crea el namespace para un usuario"""
        return self.namespaces.setdefault(user, {"/": {"is_dir": True, "size": 0, "mtime": time.time()}})

    def _add(self, user, path, entry):
        return self._namespace(user)[clean_path(path)] = {"is_dir": is_dir, "size": size, "mtime": time.time()}

    def seed_demo(self):
        """Sample entries so ls has something to show, until mkdir/create exist."""
        with self.lock:
            self._add(DEMO_USER, "/docs", is_dir=True)
            self._add(DEMO_USER, "/docs/report.pdf", is_dir=False, size=90_000)
            self._add(DEMO_USER, "/docs/notes.txt", is_dir=False, size=120)
            self._add(DEMO_USER, "/photos", is_dir=True)
            self._add(DEMO_USER, "/readme.md", is_dir=False, size=42)

    # RPC methods

    def Stat(self, request, context):
        path = clean_path(request.path)
        with self.lock:
            # CAMBIAR A USUARIO REAL
            entry = self._namespace(DEMO_USER).get(path)
        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"{path}: no such file or directory")
        return pb.Attr(is_dir=entry["is_dir"], size=entry["size"], mtime=entry["mtime"])

    def ListDir(self, request, context):
        path = clean_path(request.path)
        with self.lock:
            ns = self._namespace(DEMO_USER)
            entry = ns.get(path)
            names = sorted(posixpath.basename(p) for p in ns
                           if p != "/" and posixpath.dirname(p) == path)
        if entry is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"{path}: no such file or directory")
        if not entry["is_dir"]:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"{path}: not a directory")
        return pb.DirListing(names=names)

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