"""
Cliente para interactuar con el sistema de archivos distribuido.
"""

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc

class DFSClient:
    """Se comunica con los superpeers por gRPC"""

    def __init__(self, superpeers, token=None, timeout=5):
        self.superpeers = list(superpeers)
        self.token = token
        self.timeout = timeout
        self.addr = None
        self.channel = None
        self.stub = None
        self._connect()

    def _connect(self):
        """Itera a través de la lista de superpeers y se conecta al primero que esté disponible"""
        for addr in self.superpeers:
            # AÑADIR TLS, y Autenticación con token
            channel = grpc.insecure_channel(addr)  
            try:
                grpc.channel_ready_future(channel).result(timeout=self.timeout)
            except grpc.FutureTimeoutError:
                channel.close()
                continue
            self.addr, self.channel = addr, channel
            self.stub = rpc.SuperpeerStub(channel)
            return
        raise ConnectionError(f"no superpeer reachable: {self.superpeers}")

    def stat(self, path):
        return self.stub.Stat(pb.PathRequest(path=path), timeout=self.timeout)

    def ls(self, path):
        return list(self.stub.ListDir(pb.PathRequest(path=path), timeout=self.timeout).names)