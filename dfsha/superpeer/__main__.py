import argparse
import posixpath
import threading
import time

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.common.auth_interceptor import AuthenticationInterceptor, RoleInterceptor, current_user
from dfsha.common.auth_tokens import TokenIssuer, TokenVerifier, load_or_create_signing_key
from dfsha.common.errors import FileSystemError, translate_file_system_errors_to_grpc
from dfsha.superpeer.dict_registry import DEFAULT_PENDING_PLAN_TTL_SECONDS, DictRegistry      # la ÚNICA línea que elige la implementación del registro
from dfsha.superpeer.json_user_store import JsonUserStore
from dfsha.superpeer.peer_tracker import PeerTracker
from dfsha.superpeer.block_cleaner import DEFAULT_GRACE_SECONDS, DEFAULT_INTERVAL_SECONDS, BlockCleaner
from dfsha.superpeer import replication_monitor
from dfsha.superpeer.replication_monitor import ReplicationMonitor
from dfsha.superpeer.persistent_registry import DEFAULT_SNAPSHOT_EVERY, PersistentRegistry
from dfsha.superpeer.registry import BlockInfo
from dfsha.superpeer.federation import FederationServicer, STARTUP_GRACE_SECONDS, SuperpeerFederation, parse_superpeers
from dfsha.superpeer.replicated_registry import NotServingError, ReplicatedRegistry
from dfsha.common.tls import create_secure_server, secure_channel

DEMO_USER = "demo"                  # dueño de las entradas de ejemplo de --demo (crear su cuenta con manage_users.py)
# RPCs que no piden token: Login (es el que lo entrega) y los de los peers, que no son usuarios.
# Todos los demás, incluidos los que se agreguen, lo exigen.
FEDERATION_METHODS = [
    "/dfsha.Federation/Heartbeat",
    "/dfsha.Federation/AppendLog",
    "/dfsha.Federation/FetchUser",
    "/dfsha.Federation/FetchUserRecords",
]
PUBLIC_METHODS = {
    "/dfsha.Superpeer/Login",
    "/dfsha.Superpeer/RegisterPeer",
    "/dfsha.Superpeer/Heartbeat",
}
# Los de los peers se protegen con su certificado (mTLS): solo un nodo con rol "peer" puede llamarlos,
# así nadie más puede registrar un peer falso para recibir bloques ajenos.
# Los de la federación, igual: solo otro superpeer puede enviar operaciones o pedir usuarios
NODE_METHOD_ROLES = {
    "/dfsha.Superpeer/RegisterPeer": "peer",
    "/dfsha.Superpeer/Heartbeat": "peer",
}
for federation_method in FEDERATION_METHODS:
    PUBLIC_METHODS.add(federation_method)           # no son de un usuario: no llevan token
    NODE_METHOD_ROLES[federation_method] = "superpeer"

HEARTBEAT_INTERVAL_SECONDS = 3.0    # cada cuánto envían heartbeat los peers; muertos tras 3 perdidos (9 s)
PING_TIMEOUT_SECONDS = 2.0          # cuánto espera RegisterPeer la respuesta del peer
REPLICATE_TIMEOUT_SECONDS = 15.0    # una copia de re-replicación: el destino lee hasta 1 MiB del origen
REPLICATION_TOKEN_SUBJECT = "replication"   # a nombre de quién va el permiso de lectura de una copia
BLOCK_SIZE_BYTES = 1024 * 1024      # 1 MiB por bloque: el registro parte los archivos con este tamaño,
                                    # y un peer recibe bloques nuevos solo si le cabe al menos uno

def clean_path(path):
    """Normaliza la ruta y asegura que comience con '/'"""
    return posixpath.normpath("/" + path.lstrip("/"))

class Superpeer(rpc.SuperpeerServicer):
    def __init__(self, node_id, registry, peer_tracker, user_store, token_issuer, tls_dir, block_cleaner, federation):
        self.node_id = node_id
        self.federation = federation       # los demás superpeers: quién es el dueño de cada usuario
        self.tls_dir = tls_dir             # certificado de este superpeer, para hablar con los peers
        self.block_cleaner = block_cleaner # borra de los peers los bloques que quedan obsoletos
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
    # Las operaciones de un usuario usan _served_user(): con varios superpeers, solo el dueño del
    # usuario las atiende; los demás responden EAGAIN (UNAVAILABLE) y el cliente se cambia al dueño.

    def _served_user(self):
        """El usuario de la llamada, si este superpeer lo atiende ahora; si no, lanza NotServingError."""
        user = current_user()
        if not self.federation.serves(user):
            raise NotServingError(user)
        return user

    def Login(self, request, context):
        """
        RPC: usuario y contraseña -> token de sesión. Lo usa el cliente al conectarse y cuando su token vence.
        Si falla responde lo mismo sin importar si el usuario no existe o la contraseña es incorrecta,
        para no revelar qué usuarios existen.
        """
        if not self.user_store.authenticate(request.username, request.password):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid username or password")
        token = self.token_issuer.issue(request.username)
        # Cualquier superpeer acepta el login (todos tienen los mismos usuarios), y le dice al cliente
        # cuál atiende a ese usuario ahora, para que se conecte a ese
        owner = self.federation.owner_of(request.username)
        owner_address = ""
        if owner is not None:
            owner_address = owner.address
        return pb.LoginReply(token=token, expires_in_seconds=self.token_issuer.ttl_seconds,
                             owner_address=owner_address)

    def Stat(self, request, context):
        """RPC: metadatos de un path. Lo usa getattr en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            entry = self.registry.stat(self._served_user(), path)
        return pb.Attr(is_dir=entry.is_dir, size=entry.size, mtime=entry.mtime)

    def ListDir(self, request, context):
        """RPC: nombres de los hijos directos de un directorio. Lo usa readdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            child_names = self.registry.list_directory(self._served_user(), path)
        return pb.DirListing(names=child_names)

    def MakeDir(self, request, context):
        """RPC: crea un directorio vacío. Lo usa mkdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.make_directory(self._served_user(), path, now=time.time())
        return pb.Empty()

    def RemoveDir(self, request, context):
        """RPC: borra un directorio vacío. Lo usa rmdir en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.remove_directory(self._served_user(), path, now=time.time())
        return pb.Empty()

    def CreateFile(self, request, context):
        """RPC: crea un archivo vacío. Lo usa create (touch, primer paso de cp) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            self.registry.create_file(self._served_user(), path, now=time.time())
        return pb.Empty()

    def Delete(self, request, context):
        """RPC: borra un archivo. Lo usa unlink (comando rm) en el mount."""
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            obsolete_blocks = self.registry.delete_file(self._served_user(), path, now=time.time())
        self.block_cleaner.schedule(obsolete_blocks)     # sus bloques ya no los usa nadie
        return pb.Empty()

    def Rename(self, request, context):
        """RPC: mueve o renombra un archivo o directorio. Lo usa rename (comando mv) en el mount."""
        source_path = clean_path(request.source_path)
        destination_path = clean_path(request.destination_path)
        with translate_file_system_errors_to_grpc(context):
            obsolete_blocks = self.registry.rename(self._served_user(), source_path, destination_path, now=time.time())
        self.block_cleaner.schedule(obsolete_blocks)     # los del archivo reemplazado en el destino, si había uno
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

    def ListSuperpeers(self, request, context):
        """RPC: los superpeers según este (vivos, listos, a qué usuarios atiende cada uno) y su posición en cada log."""
        return self.federation.view()

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
            blocks = self.registry.get_block_locations(self._served_user(), path)
        return self._block_list_reply(blocks, "read")

    def AllocateBlocks(self, request, context):
        """
        RPC: el plan para subir una versión nueva de un archivo de request.size bytes:
        bloques nuevos y en qué peers escribir cada uno. El archivo no cambia hasta CommitFile.
        """
        path = clean_path(request.path)
        with translate_file_system_errors_to_grpc(context):
            blocks = self.registry.allocate_blocks(self._served_user(), path, request.size)
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
            obsolete_blocks = self.registry.commit_file(self._served_user(), path, request.size, blocks, now=time.time())
        # Los de la versión anterior: se borran de sus peers después del período de gracia, para que
        # quien estaba leyendo la versión vieja alcance a terminar
        self.block_cleaner.schedule(obsolete_blocks)
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


def serve(node_id, port, superpeers, demo, users_file, token_key_file, token_ttl_seconds, tls_dir, registry_dir,
          snapshot_every, deletion_grace_seconds, cleaner_interval_seconds, pending_plan_ttl_seconds,
          replication_delay_seconds, replication_interval_seconds):
    # La tabla de peers va primero: el registro la consulta para elegir dónde poner los bloques
    peer_tracker = PeerTracker(heartbeat_interval_seconds=HEARTBEAT_INTERVAL_SECONDS)

    def peers_available_for_blocks():
        """peer_id de los peers vivos donde cabe al menos un bloque más. El registro la llama en cada allocate_blocks."""
        return peer_tracker.alive_peer_ids_with_free_bytes(BLOCK_SIZE_BYTES)

    # El registro en memoria, envuelto por la persistencia: cada modificación queda en un log en
    # registry_dir y al arrancar se reconstruye desde ahí (para cambiar de registro, se cambia esta línea)
    persistent_registry = PersistentRegistry(DictRegistry(live_peers=peers_available_for_blocks,
                                                          block_size=BLOCK_SIZE_BYTES,
                                                          pending_plan_ttl_seconds=pending_plan_ttl_seconds),
                                             registry_dir, snapshot_every)

    # Autenticación: la clave privada firma los tokens y la pública los verifica.
    # La clave se crea la primera vez y vive en un volumen aparte (/keys), nunca en el repositorio.
    # Cada superpeer tiene la suya: los peers guardan la pública de cada uno (se la dan al registrarse)
    signing_key = load_or_create_signing_key(token_key_file)
    token_issuer = TokenIssuer(signing_key, ttl_seconds=token_ttl_seconds)
    token_verifier = TokenVerifier(signing_key.public_key())
    user_store = JsonUserStore(users_file)    # para cambiar de almacén de usuarios, se cambia esta línea (y el import)

    # Federación: los demás superpeers, y el registro replicado que el resto del superpeer usa.
    # Con un solo superpeer en la lista todo funciona igual que antes: es el dueño de todos los usuarios
    federation = SuperpeerFederation(node_id, superpeers, tls_dir, user_store)
    registry = ReplicatedRegistry(persistent_registry, federation)
    federation.use_registry(registry)

    # Limpiador y monitor de réplicas: trabajan en segundo plano y hablan con los peers con el
    # certificado de este superpeer (solo un superpeer puede usar DeleteBlock y ReplicateBlock)
    peer_stubs = {}                            # dirección -> PeerStub: un canal por peer, reutilizado
    peer_stubs_lock = threading.Lock()         # lo usan dos hilos: el limpiador y el monitor

    def peer_stub(address):
        with peer_stubs_lock:
            if address not in peer_stubs:
                peer_stubs[address] = rpc.PeerStub(secure_channel(address, tls_dir))
            return peer_stubs[address]

    def delete_block_on_peer(address, block_id):
        peer_stub(address).DeleteBlock(pb.BlockRequest(block_id=block_id), timeout=PING_TIMEOUT_SECONDS)

    def replicate_block_on_peer(target_address, block_id, source_address, size):
        # El destino le presenta este permiso al origen para leer el bloque, como haría un cliente
        access_token = token_issuer.issue_block_token(REPLICATION_TOKEN_SUBJECT, block_id, "read")
        request = pb.ReplicateBlockRequest(block_id=block_id, source_address=source_address,
                                           access_token=access_token, size=size)
        peer_stub(target_address).ReplicateBlock(request, timeout=REPLICATE_TIMEOUT_SECONDS)

    block_cleaner = BlockCleaner(peer_tracker, delete_block_on_peer, registry,
                                 grace_seconds=deletion_grace_seconds, interval_seconds=cleaner_interval_seconds)
    block_cleaner.start()
    monitor = ReplicationMonitor(registry, peer_tracker, replicate_block_on_peer, block_cleaner,
                                 delay_seconds=replication_delay_seconds, interval_seconds=replication_interval_seconds)
    monitor.start()

    servicer = Superpeer(node_id, registry, peer_tracker, user_store, token_issuer, tls_dir, block_cleaner, federation)
    # Primero el rol del nodo (certificado) y luego el usuario (token)
    interceptors = [RoleInterceptor(NODE_METHOD_ROLES), AuthenticationInterceptor(token_verifier, PUBLIC_METHODS)]
    server = create_secure_server(port, tls_dir, interceptors=interceptors)   # solo mTLS: no hay puerto sin cifrar
    rpc.add_SuperpeerServicer_to_server(servicer, server)
    rpc.add_FederationServicer_to_server(FederationServicer(federation, registry, user_store), server)
    server.start()
    federation.start()                     # después del servidor: los demás superpeers ya pueden responderle
    superpeer_ids = []
    for superpeer in superpeers:
        superpeer_ids.append(superpeer.superpeer_id)
    print(f"superpeer {node_id} listening on :{port}, federation: {', '.join(superpeer_ids)}", flush=True)

    # Las entradas de ejemplo se crean como cualquier operación: en el dueño del usuario demo y replicadas.
    # Por eso se espera a que la federación esté lista (y solo las crea el superpeer que atiende a demo)
    if demo:
        federation.wait_until_ready(STARTUP_GRACE_SECONDS + 5)
        if federation.serves(DEMO_USER):
            seed_demo_entries(registry)
            print("demo data ready", flush=True)
    server.wait_for_termination()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--id", default="sp1")
    p.add_argument("--port", type=int, default=7000)
    p.add_argument("--superpeers", default=None,
                   help="todos los superpeers, incluido este, en orden: sp1=sp1:7000,sp2=sp2:7000 "
                        "(sin esta opción, solo este, en <id>:<port>)")
    p.add_argument("--demo", action="store_true")
    p.add_argument("--users-file", default="/data/users.json", help="usuarios (crear con manage_users.py)")
    p.add_argument("--token-key", default="/keys/token.key", help="clave privada que firma los tokens")
    p.add_argument("--token-ttl", type=float, default=3600, help="segundos que dura un token de sesión")
    p.add_argument("--tls-dir", required=True, help="carpeta con node.key, node.crt y ca.crt (make_certs.py)")
    p.add_argument("--registry-dir", default="/data/registry", help="log y snapshot del registro")
    p.add_argument("--snapshot-every", type=int, default=DEFAULT_SNAPSHOT_EVERY,
                   help="cada cuántas modificaciones se toma un snapshot y se vacía el log")
    p.add_argument("--deletion-grace", type=float, default=DEFAULT_GRACE_SECONDS,
                   help="segundos que espera un bloque obsoleto antes de borrarse (deja terminar lecturas en curso)")
    p.add_argument("--cleaner-interval", type=float, default=DEFAULT_INTERVAL_SECONDS,
                   help="cada cuántos segundos el limpiador revisa su cola")
    p.add_argument("--pending-plan-ttl", type=float, default=DEFAULT_PENDING_PLAN_TTL_SECONDS,
                   help="segundos que vive un plan de escritura sin commit")
    p.add_argument("--replication-delay", type=float, default=replication_monitor.DEFAULT_DELAY_SECONDS,
                   help="segundos que un peer debe estar muerto para copiar sus bloques (y espera al arrancar)")
    p.add_argument("--replication-interval", type=float, default=replication_monitor.DEFAULT_INTERVAL_SECONDS,
                   help="cada cuántos segundos el monitor busca bloques subreplicados")
    a = p.parse_args()
    if a.superpeers is None:
        superpeer_list = parse_superpeers(f"{a.id}={a.id}:{a.port}")
    else:
        superpeer_list = parse_superpeers(a.superpeers)
    serve(a.id, a.port, superpeer_list, a.demo, a.users_file, a.token_key, a.token_ttl, a.tls_dir, a.registry_dir,
          a.snapshot_every, a.deletion_grace, a.cleaner_interval, a.pending_plan_ttl, a.replication_delay,
          a.replication_interval)
