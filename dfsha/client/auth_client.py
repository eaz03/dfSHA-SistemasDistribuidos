"""
Autenticación del lado del cliente: hace Login, guarda el token y lo adjunta en cada llamada con
un interceptor de gRPC, así los métodos de DFSClient no cambian.

Si el superpeer responde UNAUTHENTICATED (el token venció), se hace Login otra vez y se reintenta
la llamada una vez, sin que el usuario del montaje lo note. Para eso se guardan el usuario y la
contraseña en memoria mientras el montaje está activo.
"""

import os
import sys
import threading

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.common.auth_interceptor import bearer_metadata
from dfsha.common.tls import secure_channel

DEFAULT_CLIENT_TLS_DIR = "certs/client"


def credentials_from_environment():
    """
    (usuario, contraseña, carpeta del certificado) de las variables DFSHA_USER, DFSHA_PASSWORD y
    DFSHA_TLS_DIR, las mismas que usa el montaje. Termina el programa con un mensaje si faltan.
    En Docker Compose el contenedor client ya las tiene (vienen del .env).
    """
    username = os.environ.get("DFSHA_USER")
    password = os.environ.get("DFSHA_PASSWORD")
    if not username or not password:
        sys.exit("set DFSHA_USER and DFSHA_PASSWORD (see .env.example), or run this in the client container")
    return username, password, os.environ.get("DFSHA_TLS_DIR", DEFAULT_CLIENT_TLS_DIR)


DEFAULT_SUPERPEERS = "sp1:7000,sp2:7000"


def superpeers_from_environment():
    """Las direcciones de los superpeers para las herramientas: DFSHA_SUPERPEERS, o las del docker-compose."""
    addresses = []
    for item in os.environ.get("DFSHA_SUPERPEERS", DEFAULT_SUPERPEERS).split(","):
        item = item.strip()
        if item != "":
            addresses.append(item)
    return addresses


def authenticated_superpeer_stub(address, username, password, tls_dir, timeout=5):
    """SuperpeerStub por mTLS que hace Login y adjunta el token en cada llamada (para scripts y pruebas)."""
    channel = secure_channel(address, tls_dir)
    login_session = LoginSession(channel, username, password, timeout)
    login_session.token()
    return rpc.SuperpeerStub(grpc.intercept_channel(channel, BearerTokenInterceptor(login_session)))


class LoginSession:
    """El token de este cliente: lo obtiene con Login y lo renueva cuando vence."""

    def __init__(self, channel, username, password, timeout):
        """
        channel: canal SIN el interceptor de token, porque Login es justamente la llamada que no
            lleva token (y si pasara por el interceptor, este intentaría hacer Login para hacer Login).
        """
        self._superpeer_stub = rpc.SuperpeerStub(channel)
        self._username = username
        self._password = password
        self._timeout = timeout
        self._lock = threading.Lock()     # varios hilos podrían pedir el token a la vez
        self._token = None
        self.owner_address = ""           # el superpeer que atiende a este usuario, según el último Login

    def token(self):
        """El token actual; hace Login la primera vez. Lanza grpc.RpcError (UNAUTHENTICATED) si las credenciales no sirven."""
        with self._lock:
            if self._token is None:
                self._login()
            return self._token

    def renew(self, rejected_token):
        """
        Hace Login de nuevo porque el superpeer rechazó rejected_token, y devuelve el token nuevo.
        Si otro hilo ya lo renovó mientras tanto (el token actual ya no es el rechazado), no repite el Login.
        """
        with self._lock:
            if self._token == rejected_token:
                self._login()
            return self._token

    def _login(self):
        login_request = pb.LoginRequest(username=self._username, password=self._password)
        login_reply = self._superpeer_stub.Login(login_request, timeout=self._timeout)
        self._token = login_reply.token
        self.owner_address = login_reply.owner_address


class _CallDetails(grpc.ClientCallDetails):
    """Copia de los detalles de una llamada con otra metadata (los originales no se pueden modificar)."""

    def __init__(self, original, metadata):
        self.method = original.method
        self.timeout = original.timeout
        self.metadata = metadata
        self.credentials = original.credentials
        self.wait_for_ready = original.wait_for_ready
        self.compression = original.compression


class BearerTokenInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
                             grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):
    """Agrega "authorization: Bearer <token>" a cada llamada que sale por el canal."""

    def __init__(self, login_session):
        self._login_session = login_session

    def _with_token(self, call_details, token):
        metadata = list(call_details.metadata or []) + list(bearer_metadata(token))
        return _CallDetails(call_details, metadata)

    def intercept_unary_unary(self, continuation, call_details, request):
        token = self._login_session.token()
        outcome = continuation(self._with_token(call_details, token), request)
        # Solo aquí se reintenta: la petición es un único mensaje que se puede volver a enviar.
        # En las que envían un stream, los mensajes ya se consumieron y no se pueden repetir
        if outcome.code() == grpc.StatusCode.UNAUTHENTICATED:
            new_token = self._login_session.renew(token)
            outcome = continuation(self._with_token(call_details, new_token), request)
        return outcome

    def intercept_unary_stream(self, continuation, call_details, request):
        return continuation(self._with_token(call_details, self._login_session.token()), request)

    def intercept_stream_unary(self, continuation, call_details, request_iterator):
        return continuation(self._with_token(call_details, self._login_session.token()), request_iterator)

    def intercept_stream_stream(self, continuation, call_details, request_iterator):
        return continuation(self._with_token(call_details, self._login_session.token()), request_iterator)
