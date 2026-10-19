import argparse
import posixpath
from concurrent import futures
import time

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.common.auth_interceptor import AuthenticationInterceptor, RoleInterceptor, current_user
from dfsha.common.auth_tokens import TokenIssuer, TokenVerifier, load_or_create_signing_key
from dfsha.common.errors import FileSystemError, translate_file_system_errors_to_grpc
from dfsha.superpeer.dict_registry import DictRegistry      # la ÚNICA línea que elige la implementación del registro
from dfsha.superpeer.json_user_store import JsonUserStore
from dfsha.superpeer.peer_tracker import PeerTracker
from dfsha.superpeer.persistent_registry import DEFAULT_SNAPSHOT_EVERY, PersistentRegistry
from dfsha.superpeer.registry import BlockInfo
from dfsha.common.tls import secure_channel, server_credentials

DEMO_USER = "demo"                  # dueño de las entradas de ejemplo de --demo (crear su cuenta con manage_users.py)
# RPCs que no piden token: Login (es el que lo entrega) y los de los peers, que no son usuarios.
# Todos los demás, incluidos los que se agreguen, lo exigen.
PUBLIC_METHODS = {
    "/dfsha.Superpeer/Login",
    "/dfsha.Superpeer/RegisterPeer",
    "/dfsha.Superpeer/Heartbeat",
}
# Los de los peers se protegen con su certificado (mTLS): solo un nodo con rol "peer" puede llamarlos,
# así nadie más puede registrar un peer falso para recibir bloques ajenos
NODE_METHOD_ROLES = {
    "/dfsha.Superpeer/RegisterPeer": "peer",
    "/dfsha.Superpeer/Heartbeat": "peer",
}
HEARTBEAT_INTERVAL_SECONDS = 3.0    # cada cuánto envían heartbeat los peers; muertos tras 3 perdidos (9 s)
PING_TIMEOUT_SECONDS = 2.0          # cuánto espera RegisterPeer la respuesta del peer
BLOCK_SIZE_BYTES = 1024 * 1024      # 1 MiB por bloque: el registro parte los archivos con este tamaño,
                                    # y un peer recibe bloques nuevos solo si le cabe al menos uno

def clean_path(path):
    """Normaliza la ruta y asegura que comience con '/'"""
    return posixpath.normpath("/" + path.lstrip("/"))

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id, registry, peer_tracker, user_store, token_issuer, tls_dir):
        self.node_id = node_id
        self.tls_dir = tls_dir             # certificado de este superpeer, para hablar con los peers
        self.registry = registry           # cualquier implementación de registry.Registry
        self.peer_tracker = peer_tracker   # tabla de peers: quién está, dónde, si sigue vivo
        self.user_store = user_store       # cualquier implementación de user_store.UserStore
        self.token_issuer = token_issuer   # emite el token de sesión después de un Login correcto

    # ---------- RPC ----------
    # Patrón de todos los handlers:
    #   1. Limpiar los paths que llegan por la red (clean_path).
    #   2. Llamar al registro dentro de translate_file_system_errors_to_grpc, que convierte
    #      cualquier FileSystemError en un error gRPC con su errno.
    #   3. Responder.
    # El superpeer no sabe cómo está guardado el registro, ni si necesita locks:
    # eso lo resuelve la implementación de Registry.
    # El usuario sale de current_user(): el interceptor ya verificó su token antes de llegar aquí.

    def Login(self, request, context):
        """
        RPC: usuario y contraseña -> token de sesión. Lo usa el cliente al conectarse y cuando su token vence.
        Si falla responde lo mismo sin importar si el usuario no existe o la contraseña es incorrecta,
        para no revelar qué usuarios existen.
        """
        if not self.user_store.authenticate(request.username, request.password):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid username or password")
        token = self.token_issuer.issue(request.username)
        return pb.LoginReply(token=token, expires_in_seconds=self.token_issuer.ttl_seconds)

    def Stat(self, request, context):
        """RPC: metadatos de un path. Lo usa getattr en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            entry = self.registry.stat(current_user(), path)
        return pb.Attr(is_dir=entry.is_dir, size=entry.size, mtime=entry.mtime)

    def ListDir(self, request, context):
        """RPC: nombres de los hijos directos de un directorio. Lo usa readdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            child_names = self.registry.list_directory(current_user(), path)
        return pb.DirListing(names=child_names)

    def MakeDir(self, request, context):
        """RPC: crea un directorio vacío. Lo usa mkdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.make_directory(current_user(), path, now=time.time())
        return pb.Empty()

    def RemoveDir(self, request, context):
        """RPC: borra un directorio vacío. Lo usa rmdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.remove_directory(current_user(), path, now=time.time())
        return pb.Empty()

    def CreateFile(self, request, context):
        """RPC: crea un archivo vacío. Lo usa create (touch, primer paso de cp) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.create_file(current_user(), path, now=time.time())
        return pb.Empty()

    def Delete(self, request, context):
        """RPC: borra un archivo. Lo usa unlink (comando rm) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.delete_file(current_user(), path, now=time.time())
        return pb.Empty()

    def Rename(self, request, context):
        """RPC: mueve o renombra un archivo o directorio. Lo usa rename (comando mv) en el mount."""
        source_path = clean_path(request.source_path)
        destination_path = clean_path(request.destination_path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.rename(current_user(), source_path, destination_path, now=time.time())
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
        if not peer_answers_ping(address, self.tls_dir):
            context.abort(grpc.StatusCode.UNAVAILABLE,
                          f"peer {peer_id}: no answer to Ping at {address}")

        self.peer_tracker.register(peer_id, address, capacity_bytes)
        print(f"peer registered: {peer_id} at {address}, capacity {capacity_bytes} bytes")
        heartbeat_interval_seconds = self.peer_tracker.heartbeat_interval_seconds
        # Con la clave pública el peer verifica los permisos de bloque que firma este superpeer.
        # Le llega auténtica porque el peer habla con el superpeer por mTLS y verifica su certificado
        return pb.RegisterPeerReply(heartbeat_interval_seconds=heartbeat_interval_seconds,
                                    token_public_key=self.token_issuer.public_key_pem())

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
            blocks = self.registry.get_block_locations(current_user(), path)
        return self._block_list_reply(blocks, "read")

    def AllocateBlocks(self, request, context):
        """
        RPC: el plan para subir una versión nueva de un archivo de request.size bytes:
        bloques nuevos y en qué peers escribir cada uno. El archivo no cambia hasta CommitFile.
        """
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            blocks = self.registry.allocate_blocks(current_user(), path, request.size)
        return self._block_list_reply(blocks, "write")

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
            obsolete_blocks = self.registry.commit_file(current_user(), path, request.size, blocks, now=time.time())
        # Siguiente subtarea: entregar obsolete_blocks (los de la versión anterior) al limpiador,
        # que los borrará de sus peers con DeleteBlock. Por ahora quedan huérfanos en los peers.
        return pb.Empty()

    # ---------- Ayudantes de bloques ----------

    def _block_list_reply(self, blocks, operation):
        """
        Convierte una lista de BlockInfo del registro en la respuesta pb.BlockList.
        Cada bloque lleva un permiso firmado para operation ("read" o "write") a nombre del usuario:
        sin él, los peers no dejan leer ni escribir ese bloque (el peer no sabe de quién es cada bloque).
        """
        block_locations = []
        for block in blocks:
            replicas = self._replicas_alive_first(block.peer_ids)
            access_token = self.token_issuer.issue_block_token(current_user(), block.block_id, operation)
            block_location = pb.BlockLocation(block_id=block.block_id, size=block.size, replicas=replicas,
                                              access_token=access_token)
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


def peer_answers_ping(address, tls_dir):
    """
    True si en address hay un servidor Peer que responde Ping antes de PING_TIMEOUT_SECONDS.
    Abre una conexión solo para esta comprobación; el 'with' la cierra al salir.
    Con mTLS además comprueba que en esa dirección haya un nodo con certificado de la CA para ese nombre.
    """
    with secure_channel(address, tls_dir) as channel:
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
    Como el registro es persistente, al reiniciar ya existen: en ese caso no se crean de nuevo.
    """
    try:
        registry.stat(DEMO_USER, "/docs")
        return                                     # ya se crearon en un arranque anterior
    except FileSystemError:
        pass
    now = time.time()
    registry.make_directory(DEMO_USER, "/docs", now)
    registry.create_file(DEMO_USER, "/docs/report.pdf", now)
    registry.create_file(DEMO_USER, "/docs/notes.txt", now)
    registry.make_directory(DEMO_USER, "/photos", now)
    registry.create_file(DEMO_USER, "/readme.md", now)


def serve(node_id, port, demo, users_file, token_key_file, token_ttl_seconds, tls_dir, registry_dir, snapshot_every):
    # La tabla de peers va primero: el registro la consulta para elegir dónde poner los bloques
    peer_tracker = PeerTracker(heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS)

    def peers_available_for_blocks():
        """peer_id de los peers vivos donde cabe al menos un bloque más. El registro la llama en cada allocate_blocks."""
        return peer_tracker.alive_peer_ids_with_free_bytes(BLOCK_SIZE_BYTES)

    # El registro en memoria, envuelto por la persistencia: cada modificación queda en un log en
    # registry_dir y al arrancar se reconstruye desde ahí (para cambiar de registro, se cambia esta línea)
    registry = PersistentRegistry(DictRegistry(live_peers=peers_available_for_blocks, block_size=BLOCK_SIZE_BYTES),
                                  registry_dir, snapshot_every)
    if demo:
        seed_demo_entries(registry)

    # Autenticación: la clave privada firma los tokens y la pública los verifica.
    # La clave se crea la primera vez y vive en un volumen aparte (/keys), nunca en el repositorio
    signing_key = load_or_create_signing_key(token_key_file)
    token_issuer = TokenIssuer(signing_key, ttl_seconds=token_ttl_seconds)
    token_verifier = TokenVerifier(signing_key.public_key())
    user_store = JsonUserStore(users_file)    # para cambiar de almacén de usuarios, se cambia esta línea (y el import)

    servicer = Superpeer(node_id, registry, peer_tracker, user_store, token_issuer, tls_dir)
    # Primero el rol del nodo (certificado) y luego el usuario (token)
    interceptors = [RoleInterceptor(NODE_METHOD_ROLES), AuthenticationInterceptor(token_verifier, PUBLIC_METHODS)]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), interceptors=interceptors)
    rpc.add_SuperpeerServicer_to_server(servicer, server)
    server.add_secure_port(f"[::]:{port}", server_credentials(tls_dir))     # solo mTLS: no hay puerto sin cifrar
    server.start()
    print(f"superpeer {node_id} listening on :{port}" + (" (demo data)" if demo else ""))
    server.wait_for_termination()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--id", default="sp1")
    p.add_argument("--port", type=int, default=7000)
    p.add_argument("--demo", action="store_true")
    p.add_argument("--users-file", default="/data/users.json", help="usuarios (crear con manage_users.py)")
    p.add_argument("--token-key", default="/keys/token.key", help="clave privada que firma los tokens")
    p.add_argument("--token-ttl", type=float, default=3600, help="segundos que dura un token de sesión")
    p.add_argument("--tls-dir", required=True, help="carpeta con node.key, node.crt y ca.crt (make_certs.py)")
    p.add_argument("--registry-dir", default="/data/registry", help="log y snapshot del registro")
    p.add_argument("--snapshot-every", type=int, default=DEFAULT_SNAPSHOT_EVERY,
                   help="cada cuántas modificaciones se toma un snapshot y se vacía el log")
    a = p.parse_args()
    serve(a.id, a.port, a.demo, a.users_file, a.token_key, a.token_ttl, a.tls_dir, a.registry_dir, a.snapshot_every)
