"""
Autenticación del lado del cliente: hace Login, guarda el token y lo adjunta en cada llamada con
un interceptor de gRPC, así los métodos de DFSClient no cambian.

Si el superpeer responde UNAUTHENTICATED (el token venció), se hace Login otra vez y se reintenta
la llamada una vez, sin que el usuario del montaje lo note. Para eso se guardan el usuario y la
contraseña en memoria mientras el montaje está activo.
"""

import threading

import grpc

import dfsha_pb2 as pb
import dfsha_pb2_grpc as rpc
from auth_interceptor import bearer_metadata


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
