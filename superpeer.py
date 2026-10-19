import argparse
import posixpath
from concurrent import futures
import time

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from errors import translate_file_system_errors_to_grpc
from dict_registry import DictRegistry      # la ÚNICA línea que elige la implementación del registro
from peer_tracker import PeerTracker
from registry import BlockInfo

DEMO_USER = "demo"
HEARTBEAT_INTERVAL_SECONDS = 3.0    # cada cuánto envían heartbeat los peers; muertos tras 3 perdidos (9 s)
PING_TIMEOUT_SECONDS = 2.0          # cuánto espera RegisterPeer la respuesta del peer
BLOCK_SIZE_BYTES = 1024 * 1024      # 1 MiB por bloque: el registro parte los archivos con este tamaño,
                                    # y un peer recibe bloques nuevos solo si le cabe al menos uno

def clean_path(path):
    """Normaliza la ruta y asegura que comience con '/'"""
    return posixpath.normpath("/" + path.lstrip("/"))

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id, registry, peer_tracker):
        self.node_id = node_id
        self.registry = registry           # cualquier implementación de registry.Registry
        self.peer_tracker = peer_tracker   # tabla de peers: quién está, dónde, si sigue vivo
        self.tokens = {}                   # token -> user (placeholder authentication)

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

    # ---------- RPC: membresía de peers ----------
    # No son operaciones de archivos: no usan el registro ni FileSystemError.
    # Los errores se envían directamente con context.abort, que termina el RPC (lanza una excepción).

    def RegisterPeer(self, request, context):
        """
        RPC: un peer se presenta al arrancar, o de nuevo si un heartbeat recibió known=False. Lo llama peer.py.
        Antes de anotarlo comprueba que su dirección responde: con una dirección equivocada,
        los clientes no podrían leer ni escribir sus bloques.
        """
        peer_id = request.peer_id
        address = request.address
        capacity_bytes = request.capacity_bytes

        if peer_id == "" or address == "":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "peer_id and address are required")
        if capacity_bytes <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"peer {peer_id}: capacity_bytes must be positive")
        if not peer_answers_ping(address):
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"peer {peer_id}: no answer to Ping at {address}")

        self.peer_tracker.register(peer_id, address, capacity_bytes)
        print(f"peer registered: {peer_id} at {address}, capacity {capacity_bytes} bytes")
        heartbeat_interval_seconds = self.peer_tracker.heartbeat_interval_seconds
        return pb.RegisterPeerReply(heartbeat_interval_seconds=heartbeat_interval_seconds)

    def Heartbeat(self, request, context):
        """
        RPC: un peer avisa que sigue vivo y cuánto espacio libre tiene. Lo llama peer.py cada pocos segundos.
        known=False si el peer no está en la tabla (p. ej. el superpeer se reinició): debe registrarse de nuevo.
        """
        peer_is_known = self.peer_tracker.heartbeat(request.peer_id, request.free_bytes)
        return pb.HeartbeatReply(known=peer_is_known)

    def ListPeers(self, request, context):
        """RPC: estado de todos los peers conocidos, vivos y muertos. Para pruebas y la demo."""
        peer_statuses = []
        for peer in self.peer_tracker.all_peers():
            peer_status = pb.PeerStatus(
                peer_id=peer.peer_id,
                address=peer.address,
                capacity_bytes=peer.capacity_bytes,
                free_bytes=peer.free_bytes,
                seconds_since_heartbeat=peer.seconds_since_heartbeat,
                alive=peer.alive,
            )
            peer_statuses.append(peer_status)
        return pb.PeerList(peers=peer_statuses)

    # ---------- RPC: bloques ----------
    # El superpeer nunca toca los bytes: entrega IDs de bloques y dónde están (o dónde escribirlos),
    # y el cliente habla directo con los peers.
    # El registro guarda peer_id; aquí se traduce cada uno a su dirección con la tabla de peers.

    def GetBlockLocations(self, request, context):
        """
        RPC: qué bloques forman un archivo, en orden, y en qué peers buscar cada uno.
        Lo usa open en el mount, para leer. En cada bloque van primero los peers vivos.
        """
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            blocks = self.registry.get_block_locations(DEMO_USER, path)
        return self._block_list_reply(blocks)

    def AllocateBlocks(self, request, context):
        """
        RPC: el plan para subir una versión nueva de un archivo de request.size bytes:
        bloques nuevos y en qué peers escribir cada uno. El archivo no cambia hasta CommitFile.
        """
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            blocks = self.registry.allocate_blocks(DEMO_USER, path, request.size)
        return self._block_list_reply(blocks)

    def CommitFile(self, request, context):
        """
        RPC: el cliente ya subió los bloques del plan; la versión nueva del archivo se vuelve visible.
        En cada bloque, las réplicas son los peers donde el cliente SÍ logró escribirlo.
        """
        path = clean_path(request.path)
        blocks = []
        for block_location in request.blocks:
            written_peer_ids = []
            for replica in block_location.replicas:
                written_peer_ids.append(replica.peer_id)
            block = BlockInfo(block_id=block_location.block_id, size=block_location.size,
                              peer_ids=tuple(written_peer_ids))
            blocks.append(block)

        with translate_file_system_errors_to_grpc(context):
            obsolete_blocks = self.registry.commit_file(DEMO_USER, path, request.size, blocks, now=time.time())
        # Siguiente subtarea: entregar obsolete_blocks (los de la versión anterior) al limpiador,
        # que los borrará de sus peers con DeleteBlock. Por ahora quedan huérfanos en los peers.
        return pb.Empty()

    # ---------- Ayudantes de bloques ----------

    def _block_list_reply(self, blocks):
        """Convierte una lista de BlockInfo del registro en la respuesta pb.BlockList."""
        block_locations = []
        for block in blocks:
            replicas = self._replicas_alive_first(block.peer_ids)
            block_location = pb.BlockLocation(block_id=block.block_id, size=block.size, replicas=replicas)
            block_locations.append(block_location)
        return pb.BlockList(blocks=block_locations, block_size=BLOCK_SIZE_BYTES)

    def _replicas_alive_first(self, peer_ids):
        """
        Traduce cada peer_id a su dirección con la tabla de peers, en dos grupos:
        primero los vivos y al final los muertos (pudieron volver sin que llegue aún su heartbeat;
        el cliente los prueba solo si fallan los vivos). Se omiten los que la tabla no conoce.
        """
        alive_replicas = []
        dead_replicas = []
        for peer_id in peer_ids:
            peer = self.peer_tracker.peer_snapshot(peer_id)
            if peer is None:
                continue
            replica = pb.Replica(peer_id=peer_id, address=peer.address)
            if peer.alive:
                alive_replicas.append(replica)
            else:
                dead_replicas.append(replica)
        return alive_replicas + dead_replicas


def peer_answers_ping(address):
    """
    True si en address hay un servidor Peer que responde Ping antes de PING_TIMEOUT_SECONDS.
    Abre una conexión solo para esta comprobación; el 'with' la cierra al salir.
    """
    with grpc.insecure_channel(address) as channel:
        peer_stub = rpc.PeerStub(channel)
        try:
            peer_stub.Ping(pb.Empty(), timeout=PING_TIMEOUT_SECONDS)
            return True
        except grpc.RpcError:
            return False


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
    # La tabla de peers va primero: el registro la consulta para elegir dónde poner los bloques
    peer_tracker = PeerTracker(heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS)

    def peers_available_for_blocks():
        """peer_id de los peers vivos donde cabe al menos un bloque más. El registro la llama en cada allocate_blocks."""
        return peer_tracker.alive_peer_ids_with_free_bytes(BLOCK_SIZE_BYTES)

    registry = DictRegistry(live_peers=peers_available_for_blocks,    # para cambiar de registro, se cambia esta línea (y el import)
                            block_size=BLOCK_SIZE_BYTES)
    if demo:
        seed_demo_entries(registry)
    servicer = Superpeer(node_id, registry, peer_tracker)
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
