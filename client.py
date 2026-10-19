"""
Cliente para interactuar con el sistema de archivos distribuido.

Habla con dos tipos de nodo:
  - el superpeer: metadatos (stat, ls, mkdir...) y ubicación de los bloques;
  - los peers: los bytes de los bloques, directamente, sin pasar por el superpeer.
"""

import errno
import os
import threading
from concurrent import futures

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from auth_client import BearerTokenInterceptor, LoginSession
from errors import FileSystemError, errno_from_rpc_error
from tls import secure_channel

PARALLEL_BLOCK_TRANSFERS = 4      # bloques que se bajan o suben a la vez (en general, a peers distintos)


def describe_block_attempt(block_id, tried_peer_ids):
    """Texto para el mensaje de error de un bloque, p. ej. "block 9f3a1c0b (tried: peer1, peer2)"."""
    if len(tried_peer_ids) == 0:
        tried_text = "no replicas"
    else:
        tried_text = ", ".join(tried_peer_ids)
    return f"block {block_id[:8]} (tried: {tried_text})"


class DFSClient:
    """Se comunica con los superpeers y los peers por gRPC"""

    def __init__(self, superpeers, username, password, tls_dir, timeout=5):
        self.superpeers = list(superpeers)
        self.username = username
        self.password = password
        self.tls_dir = tls_dir             # certificado de esta máquina cliente (mTLS)
        self.timeout = timeout
        self.addr = None
        self.channel = None
        self.stub = None
        self._peer_stubs = {}                    # dirección del peer -> PeerStub (un canal por peer)
        self._peer_stubs_lock = threading.Lock() # varios hilos pueden pedir el mismo peer a la vez
        self._connect()

    def _connect(self):
        """
        Itera a través de la lista de superpeers, se conecta al primero que esté disponible y hace Login.
        Si las credenciales no sirven lanza grpc.RpcError (UNAUTHENTICATED): no tiene sentido probar
        otro superpeer, porque todos conocen los mismos usuarios.
        """
        for addr in self.superpeers:
            channel = secure_channel(addr, self.tls_dir)
            try:
                grpc.channel_ready_future(channel).result(timeout=self.timeout)
            except grpc.FutureTimeoutError:
                channel.close()
                continue
            login_session = LoginSession(channel, self.username, self.password, self.timeout)
            login_session.token()                     # Login ya: unas credenciales malas fallan al conectar
            self.addr, self.channel = addr, channel
            # Todo lo que pase por este canal lleva el token; los métodos de abajo no cambian
            authenticated_channel = grpc.intercept_channel(channel, BearerTokenInterceptor(login_session))
            self.stub = rpc.SuperpeerStub(authenticated_channel)
            return
        raise ConnectionError(f"no superpeer reachable: {self.superpeers}")

    # Cada método envía un RPC y devuelve la respuesta.
    # Los errores llegan como grpc.RpcError; traducirlos es trabajo de quien llama (el mount).

    def stat(self, path):
        """Metadatos de un path (tipo, tamaño, fecha)."""
        return self.stub.Stat(pb.PathRequest(path=path), timeout=self.timeout)

    def ls(self, path):
        """Nombres de los hijos directos de un directorio."""
        return list(self.stub.ListDir(pb.PathRequest(path=path), timeout=self.timeout).names)

    def mkdir(self, path):
        """Crea un directorio vacío."""
        self.stub.MakeDir(pb.PathRequest(path=path), timeout=self.timeout)

    def rmdir(self, path):
        """Borra un directorio vacío."""
        self.stub.RemoveDir(pb.PathRequest(path=path), timeout=self.timeout)

    def rm(self, path):
        """Borra un archivo."""
        self.stub.Delete(pb.PathRequest(path=path), timeout=self.timeout)

    def mv(self, source_path, destination_path):
        """Mueve o renombra un archivo o directorio."""
        request = pb.RenameRequest(source_path=source_path, destination_path=destination_path)
        self.stub.Rename(request, timeout=self.timeout)

    def create(self, path):
        """Crea un archivo vacío."""
        self.stub.CreateFile(pb.PathRequest(path=path), timeout=self.timeout)

    def list_peers(self):
        """Estado de todos los peers que conoce el superpeer (lista de pb.PeerStatus)."""
        return list(self.stub.ListPeers(pb.Empty(), timeout=self.timeout).peers)

    # ---------- Peers ----------
    # Los bytes de los archivos van directo entre este cliente y los peers.
    # Además de grpc.RpcError, los métodos de bloques pueden lanzar FileSystemError:
    #   ENODATA  al leer, si ninguna réplica del bloque pudo entregarlo ("No data available")
    #   EIO      al escribir, si el bloque no quedó en ninguna réplica ("Input/output error")

    def _peer_stub(self, address):
        """El stub del peer en address. Su canal se crea la primera vez y después se reutiliza."""
        with self._peer_stubs_lock:
            if address not in self._peer_stubs:
                self._peer_stubs[address] = rpc.PeerStub(secure_channel(address, self.tls_dir))   # mTLS
            return self._peer_stubs[address]

    # ---------- Lectura ----------

    def get_block_locations(self, path):
        """pb.BlockList de path: sus bloques en orden, las réplicas de cada uno (vivas primero) y block_size."""
        return self.stub.GetBlockLocations(pb.PathRequest(path=path), timeout=self.timeout)

    def read_block(self, block_location):
        """
        Bytes de un bloque. Prueba sus réplicas en orden: si una falla (peer caído, no tiene el bloque,
        o entrega un tamaño distinto al esperado), sigue con la siguiente.
        Si fallan todas, lanza FileSystemError(ENODATA): el archivo existe, pero sus datos no están disponibles.
        """
        tried_peer_ids = []
        for replica in block_location.replicas:
            tried_peer_ids.append(replica.peer_id)
            # El permiso de lectura que entregó el superpeer: sin él, el peer no entrega el bloque
            request = pb.BlockRequest(block_id=block_location.block_id, access_token=block_location.access_token)
            try:
                data = self._peer_stub(replica.address).ReadBlock(request, timeout=self.timeout).data
            except grpc.RpcError:
                continue                                    # peer caído o sin el bloque: siguiente réplica
            if len(data) != block_location.size:
                continue                                    # bloque incompleto o equivocado: siguiente réplica
            return data

        raise FileSystemError(errno.ENODATA, describe_block_attempt(block_location.block_id, tried_peer_ids))

    def read_blocks(self, block_locations):
        """
        Lee varios bloques en paralelo (hasta PARALLEL_BLOCK_TRANSFERS a la vez).
        Devuelve sus bytes en el mismo orden en que se pidieron.
        """
        with futures.ThreadPoolExecutor(max_workers=PARALLEL_BLOCK_TRANSFERS) as executor:
            pending_reads = []
            for block_location in block_locations:
                pending_read = executor.submit(self.read_block, block_location)
                pending_reads.append(pending_read)
            blocks_data = []
            for pending_read in pending_reads:
                blocks_data.append(pending_read.result())   # si un bloque falló, aquí se lanza su error
            return blocks_data

    def download_to_file(self, path, local_path):
        """
        Copia el contenido completo de path a local_path y devuelve los bytes escritos.
        Baja los bloques de PARALLEL_BLOCK_TRANSFERS en PARALLEL_BLOCK_TRANSFERS: en memoria nunca
        hay más que esos bloques, sin importar el tamaño del archivo.
        """
        block_list = self.get_block_locations(path)
        all_block_locations = list(block_list.blocks)
        written_bytes = 0
        with open(local_path, "wb") as local_file:
            for group_start in range(0, len(all_block_locations), PARALLEL_BLOCK_TRANSFERS):
                group = all_block_locations[group_start:group_start + PARALLEL_BLOCK_TRANSFERS]
                for block_data in self.read_blocks(group):
                    local_file.write(block_data)
                    written_bytes += len(block_data)
        return written_bytes

    # ---------- Escritura (mínima: la usa dfs_cli.py hasta que el mount escriba) ----------

    def write_block(self, block_location, data):
        """
        Escribe data en cada réplica que el plan le asignó al bloque.
        Si una réplica falla (p. ej. el peer acaba de caer), se omite: el commit acepta menos réplicas.
        Devuelve las réplicas (pb.Replica) donde sí quedó escrito.
        Si no quedó en ninguna, lanza FileSystemError(EIO).
        """
        written_replicas = []
        tried_peer_ids = []
        for replica in block_location.replicas:
            tried_peer_ids.append(replica.peer_id)
            request = pb.WriteBlockRequest(block_id=block_location.block_id, data=data,
                                           access_token=block_location.access_token)   # permiso de escritura del plan
            try:
                self._peer_stub(replica.address).WriteBlock(request, timeout=self.timeout)
            except grpc.RpcError:
                continue                                    # esta réplica no: probar las demás
            written_replicas.append(replica)

        if len(written_replicas) == 0:
            raise FileSystemError(errno.EIO, describe_block_attempt(block_location.block_id, tried_peer_ids))
        return written_replicas

    def _upload_block_from_file(self, file_descriptor, block_location, offset):
        """
        Lee de file_descriptor el pedazo del bloque (block_location.size bytes desde offset),
        lo escribe en sus réplicas y devuelve el pb.BlockLocation para el commit, con las réplicas escritas.
        os.pread lee en una posición dada sin mover el cursor del archivo: varios hilos pueden usarlo a la vez.
        """
        data = os.pread(file_descriptor, block_location.size, offset)
        written_replicas = self.write_block(block_location, data)
        return pb.BlockLocation(block_id=block_location.block_id, size=block_location.size,
                                replicas=written_replicas)

    def upload_from_file(self, path, local_path):
        """
        Sube el archivo local local_path como el contenido de path y devuelve los bloques del commit.
        Si path no existe, lo crea (su directorio padre sí debe existir); si existe, lo sobrescribe.
        Lee local_path bloque a bloque: nunca carga el archivo completo en memoria.
        Si algo falla antes del commit, path conserva su versión anterior completa.
        """
        # 1. Primero el archivo local: si no existe, falla aquí, antes de crear nada en DFSha
        file_size = os.path.getsize(local_path)

        # 2. El archivo debe existir en el registro antes de pedir el plan
        try:
            self.create(path)
        except grpc.RpcError as rpc_error:
            if errno_from_rpc_error(rpc_error) != errno.EEXIST:
                raise                                       # ya existía: se sobrescribe; otro error: se propaga

        # 3. El plan: qué bloques y en qué peers
        allocate_request = pb.AllocateRequest(path=path, size=file_size)
        plan = self.stub.AllocateBlocks(allocate_request, timeout=self.timeout)

        # 4. Subir cada bloque a sus réplicas, varios a la vez.
        #    El bloque i empieza donde terminan los anteriores (i * block_size).
        with open(local_path, "rb") as local_file:
            file_descriptor = local_file.fileno()
            with futures.ThreadPoolExecutor(max_workers=PARALLEL_BLOCK_TRANSFERS) as executor:
                pending_uploads = []
                offset = 0
                for block_location in plan.blocks:
                    pending_upload = executor.submit(self._upload_block_from_file,
                                                     file_descriptor, block_location, offset)
                    pending_uploads.append(pending_upload)
                    offset += block_location.size
                committed_blocks = []
                for pending_upload in pending_uploads:
                    committed_blocks.append(pending_upload.result())   # si un bloque falló, aquí se lanza su error

        # 5. Hacer visible la versión nueva, con las réplicas que sí quedaron escritas
        commit_request = pb.CommitRequest(path=path, size=file_size, blocks=committed_blocks)
        self.stub.CommitFile(commit_request, timeout=self.timeout)
        return committed_blocks
