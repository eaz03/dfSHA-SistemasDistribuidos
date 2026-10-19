"""
Peer de DFSha: guarda bloques y se mantiene registrado en el superpeer.

Su API (servicio Peer en dfsha.proto): WriteBlock, ReadBlock y DeleteBlock por block_id, más Ping.
No sabe a qué archivo pertenece cada bloque: eso solo lo sabe el registro del superpeer.
Dónde y cómo se guardan los bytes lo decide una implementación de BlockStorage (hoy DiskBlockStorage).

Al arrancar:
  1. Levanta su servidor gRPC (servicio Peer). Va primero porque el superpeer
     le hace Ping durante el registro, para comprobar que la dirección anunciada responde.
  2. Se registra en el superpeer, reintentando hasta que lo acepte.
  3. Envía un heartbeat cada heartbeat_interval_seconds (el intervalo lo decide el superpeer).
     - known=False (el superpeer se reinició y no lo conoce): se registra de nuevo.
     - el superpeer no responde: sigue intentando; cuando vuelva, recibirá known=False.
Al recibir SIGTERM (docker compose stop) o Ctrl+C: deja de enviar heartbeats y apaga su servidor.
El superpeer no recibe ningún aviso: lo da por muerto cuando dejan de llegar heartbeats.
"""

import argparse
import os
import shutil
import signal
import threading
from concurrent import futures

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from auth_interceptor import RoleInterceptor
from disk_block_storage import DiskBlockStorage
from encrypted_block_storage import EncryptedBlockStorage
from errors import translate_file_system_errors_to_grpc
from key_files import load_or_create_secret_key
from tls import secure_channel, server_credentials

REGISTER_TIMEOUT_SECONDS = 5.0      # mayor que el Ping que hace el superpeer durante el registro (2 s)
HEARTBEAT_TIMEOUT_SECONDS = 2.0
FIRST_RETRY_DELAY_SECONDS = 1.0     # espera entre intentos de registro: 1, 2, 4, 8, 10, 10... segundos
MAX_RETRY_DELAY_SECONDS = 10.0
BYTES_PER_MB = 1024 * 1024
# Métodos que solo puede llamar un superpeer (según su certificado): nadie más debe poder dar órdenes al peer
SUPERPEER_ONLY_METHODS = {
    "/dfsha.Peer/Ping": "superpeer",
    "/dfsha.Peer/DeleteBlock": "superpeer",     # si no, cualquier cliente podría borrar bloques ajenos
}


# ---------- Servidor: lo que el peer responde ----------

class Peer(rpc.PeerServicer):
    """
    Servidor gRPC del peer: la API que exponen los peers.
    Mismo patrón que los handlers del superpeer: cada RPC llama a block_storage dentro de
    translate_file_system_errors_to_grpc, que convierte un FileSystemError en un error gRPC con su errno:
      block_id inválido   -> EINVAL -> INVALID_ARGUMENT
      bloque inexistente  -> ENOENT -> NOT_FOUND
      disco lleno         -> ENOSPC -> RESOURCE_EXHAUSTED
    """

    def __init__(self, peer_id, block_storage):
        self.peer_id = peer_id
        self.block_storage = block_storage       # cualquier implementación de block_storage.BlockStorage

    def Ping(self, request, context):
        """RPC: responde para demostrar que este peer atiende en esta dirección. Lo llama el superpeer."""
        return pb.Empty()

    def WriteBlock(self, request, context):
        """
        RPC: guarda un bloque. Lo llama el cliente al subir un archivo, una vez por réplica.
        Si el bloque ya existía lo reemplaza: es un reintento del cliente (los IDs nunca se reutilizan).
        """
        with translate_file_system_errors_to_grpc(context):
            self.block_storage.write_block(request.block_id, request.data)
        return pb.Empty()

    def ReadBlock(self, request, context):
        """RPC: devuelve los bytes de un bloque. Lo llama el cliente al leer un archivo."""
        with translate_file_system_errors_to_grpc(context):
            data = self.block_storage.read_block(request.block_id)
        return pb.BlockData(data=data)

    def DeleteBlock(self, request, context):
        """
        RPC: borra un bloque que ya ningún archivo usa. Lo llama el superpeer.
        Borrar un bloque que no existe no es error: el superpeer puede reintentar sin peligro.
        """
        with translate_file_system_errors_to_grpc(context):
            self.block_storage.delete_block(request.block_id)
        return pb.Empty()


# ---------- Cliente: lo que el peer le pide al superpeer ----------

class SuperpeerMembership:
    """
    Mantiene a este peer registrado en el superpeer:
    registro inicial con reintentos, heartbeats periódicos y re-registro cuando el superpeer lo olvida.
    Todo corre en el hilo que llama a run(); stop_event lo detiene.
    """

    def __init__(self, superpeer_address, peer_info, data_directory, stop_event, tls_dir):
        self.superpeer_address = superpeer_address
        self.peer_info = peer_info                   # pb.PeerInfo: id, dirección anunciada, capacidad
        self.data_directory = data_directory
        self.stop_event = stop_event
        self.channel = secure_channel(superpeer_address, tls_dir)   # mTLS; gRPC reconecta solo si el superpeer se reinicia
        self.superpeer_stub = rpc.SuperpeerStub(self.channel)
        self.heartbeat_interval_seconds = None       # lo fija el superpeer en la respuesta a RegisterPeer
        self.superpeer_reachable = True              # solo para no repetir el mismo mensaje en cada heartbeat fallido

    def run(self):
        """
        Se registra y luego envía heartbeats hasta que stop_event se active.
        El primer heartbeat sale apenas termina el registro: el superpeer asume
        todo el espacio libre al registrar, y así recibe el valor real de inmediato.
        """
        registered = self.register_until_accepted()
        if not registered:
            return
        while True:
            self.send_heartbeat()
            # Espera un intervalo; termina antes si llega la orden de apagar.
            stop_requested = self.stop_event.wait(timeout=self.heartbeat_interval_seconds)
            if stop_requested:
                return

    def register_until_accepted(self):
        """
        Llama a RegisterPeer hasta que el superpeer lo acepte.
        Entre intentos espera cada vez más (1 s, 2 s, 4 s... hasta 10 s),
        para no saturar a un superpeer que está arrancando.
        Devuelve True si quedó registrado, False si llegó la orden de apagar antes.
        """
        retry_delay_seconds = FIRST_RETRY_DELAY_SECONDS
        while True:
            try:
                register_reply = self.superpeer_stub.RegisterPeer(self.peer_info, timeout=REGISTER_TIMEOUT_SECONDS)
                self.heartbeat_interval_seconds = register_reply.heartbeat_interval_seconds
                print(f"registered in superpeer {self.superpeer_address} as {self.peer_info.address}, "
                      f"heartbeat every {self.heartbeat_interval_seconds:g} s")
                return True
            except grpc.RpcError as rpc_error:
                if rpc_error.code() == grpc.StatusCode.INVALID_ARGUMENT:
                    # Error de configuración (p. ej. id vacío): reintentar no lo arregla.
                    raise SystemExit(f"superpeer rejected this peer: {rpc_error.details()}")
                print(f"register failed ({rpc_error.code().name}: {rpc_error.details()}), "
                      f"retrying in {retry_delay_seconds:g} s")

            stop_requested = self.stop_event.wait(timeout=retry_delay_seconds)
            if stop_requested:
                return False
            retry_delay_seconds = min(retry_delay_seconds * 2, MAX_RETRY_DELAY_SECONDS)

    def send_heartbeat(self):
        """
        Envía un heartbeat con el espacio libre actual. Tres resultados posibles:
          known=True  -> todo bien.
          known=False -> el superpeer no conoce este peer (se reinició): registrarse de nuevo.
          error gRPC  -> el superpeer no responde: se intenta otra vez en el siguiente intervalo.
        """
        free_bytes = compute_free_bytes(self.data_directory, self.peer_info.capacity_bytes)
        heartbeat_request = pb.HeartbeatRequest(peer_id=self.peer_info.peer_id, free_bytes=free_bytes)
        try:
            heartbeat_reply = self.superpeer_stub.Heartbeat(heartbeat_request, timeout=HEARTBEAT_TIMEOUT_SECONDS)
        except grpc.RpcError as rpc_error:
            if self.superpeer_reachable:
                print(f"superpeer {self.superpeer_address} not answering ({rpc_error.code().name}), will keep trying")
                self.superpeer_reachable = False
            return

        if not self.superpeer_reachable:
            print(f"superpeer {self.superpeer_address} answering again")
            self.superpeer_reachable = True
        if not heartbeat_reply.known:
            print("superpeer does not know this peer (it restarted?), registering again")
            self.register_until_accepted()

    def close(self):
        self.channel.close()


# ---------- Espacio en disco ----------

def used_bytes_in(directory):
    """
    Bytes que ocupan los archivos dentro de directory (los bloques, a partir del paso 2).
    Recorre la carpeta completa: suficiente por ahora; en el paso 2 el almacenamiento
    de bloques llevará la cuenta sin recorrer el disco en cada heartbeat.
    """
    total_bytes = 0
    for directory_path, subdirectory_names, file_names in os.walk(directory):
        for file_name in file_names:
            file_path = os.path.join(directory_path, file_name)
            try:
                total_bytes += os.path.getsize(file_path)
            except FileNotFoundError:
                pass                                   # se borró mientras se recorría la carpeta
    return total_bytes


def compute_free_bytes(data_directory, capacity_bytes):
    """
    Espacio libre que este peer anuncia: lo que le queda de su capacidad,
    sin pasar del espacio libre real del disco (varios peers pueden compartir un disco).
    """
    free_within_capacity = capacity_bytes - used_bytes_in(data_directory)
    free_on_disk = shutil.disk_usage(data_directory).free
    free_bytes = min(free_within_capacity, free_on_disk)
    return max(free_bytes, 0)


# ---------- Arranque y apagado ----------

def install_stop_handlers(stop_event):
    """
    Hace que SIGTERM (docker compose stop) y Ctrl+C (SIGINT) activen stop_event
    en lugar de matar el proceso a la fuerza.
    En Docker es necesario: el proceso principal de un contenedor (PID 1) ignora SIGTERM
    si no tiene un manejador, y docker compose stop esperaría 10 s para matarlo con SIGKILL.
    """
    def request_stop(signal_number, stack_frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def serve(peer_id, host, port, superpeer_address, capacity_bytes, data_directory, tls_dir, block_key_path):
    # La ÚNICA línea que elige dónde y cómo se guardan los bloques: en disco (DiskBlockStorage crea la
    # carpeta y borra temporales a medias), cifrados con AES-256-GCM (cifrado en reposo). La clave va
    # en otro lugar que los bloques: si estuvieran juntos, quien se lleve el disco se lleva las dos cosas
    block_storage = EncryptedBlockStorage(DiskBlockStorage(data_directory), load_or_create_secret_key(block_key_path))

    # 1. Servidor primero: el superpeer le hará Ping durante el registro.
    #    gRPC acepta mensajes de hasta 4 MB por defecto: alcanza para bloques de 1 MiB.
    #    Con bloques más grandes habría que subir ese límite o enviar el bloque en streaming.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16),
                         interceptors=[RoleInterceptor(SUPERPEER_ONLY_METHODS)])
    rpc.add_PeerServicer_to_server(Peer(peer_id, block_storage), server)
    server.add_secure_port(f"[::]:{port}", server_credentials(tls_dir))     # solo mTLS
    server.start()
    announced_address = f"{host}:{port}"
    print(f"peer {peer_id} listening on :{port}, announced as {announced_address}, "
          f"capacity {capacity_bytes // BYTES_PER_MB} MB, data in {data_directory}")

    # 2. Apagado ordenado.
    stop_event = threading.Event()
    install_stop_handlers(stop_event)

    # 3. Registro y heartbeats en este hilo, hasta la orden de apagar.
    peer_info = pb.PeerInfo(peer_id=peer_id, address=announced_address, capacity_bytes=capacity_bytes)
    membership = SuperpeerMembership(superpeer_address, peer_info, data_directory, stop_event, tls_dir)
    try:
        membership.run()
    finally:
        print(f"peer {peer_id} stopping")
        membership.close()
        server.stop(grace=2).wait()      # termina las llamadas en curso, máximo 2 s


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Peer de DFSha")
    parser.add_argument("--id", required=True,
                        help="nombre único del peer, p. ej. peer1")
    parser.add_argument("--host", required=True,
                        help="nombre o IP con que lo encuentran el superpeer y los clientes: "
                             "peer1 en Docker Compose, la IP de la VPN en las VMs")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--superpeer", default="sp1:7000",
                        help="dirección del superpeer, p. ej. sp1:7000")
    parser.add_argument("--capacity-mb", type=int, default=1024,
                        help="espacio que este peer ofrece para bloques")
    parser.add_argument("--data-dir", default="/data",
                        help="carpeta donde se guardan los bloques")
    parser.add_argument("--block-key", default="/keys/blocks.key",
                        help="clave que cifra los bloques en disco; en otro lugar que --data-dir")
    parser.add_argument("--tls-dir", required=True,
                        help="carpeta con node.key, node.crt y ca.crt de este peer (make_certs.py)")
    arguments = parser.parse_args()
    if arguments.capacity_mb <= 0:
        parser.error("--capacity-mb must be positive")

    serve(peer_id=arguments.id,
          host=arguments.host,
          port=arguments.port,
          superpeer_address=arguments.superpeer,
          capacity_bytes=arguments.capacity_mb * BYTES_PER_MB,
          data_directory=arguments.data_dir,
          tls_dir=arguments.tls_dir,
          block_key_path=arguments.block_key)
