"""
Cliente para interactuar con el sistema de archivos distribuido.
"""

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from auth_client import BearerTokenInterceptor, LoginSession

class DFSClient:
    """Se comunica con los superpeers por gRPC"""

    def __init__(self, superpeers, username, password, timeout=5):
        self.superpeers = list(superpeers)
        self.username = username
        self.password = password
        self.timeout = timeout
        self.addr = None
        self.channel = None
        self.stub = None
        self._connect()

    def _connect(self):
        """
        Itera a través de la lista de superpeers, se conecta al primero que esté disponible y hace Login.
        Si las credenciales no sirven lanza grpc.RpcError (UNAUTHENTICATED): no tiene sentido probar
        otro superpeer, porque todos conocen los mismos usuarios.
        """
        for addr in self.superpeers:
            # AÑADIR TLS
            channel = grpc.insecure_channel(addr)
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
