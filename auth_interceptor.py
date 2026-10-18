"""
Autenticación del lado del servidor: un interceptor de gRPC que verifica el token de cada llamada
antes de que llegue al handler, como un portero.

Es seguro por defecto: todo RPC queda protegido, incluidos los que se agreguen después, salvo los
de una lista corta y explícita de métodos públicos (p. ej. Login). Si cada handler revisara el
token por su cuenta, bastaría con olvidarlo en uno para dejar un agujero.

Los handlers obtienen el usuario autenticado con current_user(), que reemplaza al DEMO_USER fijo.
"""

import contextvars

import grpc

from auth_tokens import InvalidTokenError

AUTHORIZATION_METADATA_KEY = "authorization"     # gRPC exige las llaves de metadata en minúscula
BEARER_PREFIX = "Bearer "                        # mismo formato que el encabezado Authorization de HTTP

# Una variable de contexto tiene un valor propio en cada llamada en curso: si dos usuarios llaman
# a la vez (cada llamada en su hilo), cada handler ve el suyo
_current_user = contextvars.ContextVar("dfsha_current_user")


def current_user():
    """
    Usuario autenticado de la llamada en curso. Solo se puede usar dentro de un handler protegido:
    en uno público (p. ej. Login) no hay usuario y lanza LookupError, porque sería un error de programación.
    """
    return _current_user.get()


def bearer_metadata(token):
    """La metadata que el cliente adjunta a cada llamada: (("authorization", "Bearer <token>"),)."""
    return ((AUTHORIZATION_METADATA_KEY, BEARER_PREFIX + token),)


class AuthenticationInterceptor(grpc.ServerInterceptor):
    """Exige un token válido en toda llamada, salvo en los métodos de public_methods."""

    def __init__(self, token_verifier, public_methods):
        """
        public_methods: nombres completos de los métodos que no piden token,
            p. ej. {"/dfsha.Superpeer/Login"} (formato "/paquete.Servicio/Método").
        """
        self._token_verifier = token_verifier
        self._public_methods = frozenset(public_methods)

    def intercept_service(self, continuation, handler_call_details):
        """
        gRPC lo llama en cada llamada, antes de elegir el handler. continuation(...) devuelve el
        handler normal; aquí se devuelve uno envuelto que primero verifica el token.
        """
        handler = continuation(handler_call_details)
        if handler is None or handler_call_details.method in self._public_methods:
            return handler                         # método inexistente (gRPC responde UNIMPLEMENTED) o público
        token = _bearer_token(handler_call_details.invocation_metadata)
        return _with_authentication(handler, token, self._token_verifier)


def _bearer_token(metadata):
    """Saca el token de la metadata "authorization: Bearer <token>"; None si no vino o no tiene ese formato."""
    for key, value in metadata or ():
        if key == AUTHORIZATION_METADATA_KEY and value.startswith(BEARER_PREFIX):
            return value[len(BEARER_PREFIX):]
    return None


def _with_authentication(handler, token, token_verifier):
    """
    Devuelve una copia de handler cuya función primero verifica el token (y si no sirve, termina
    la llamada con UNAUTHENTICATED sin ejecutar el handler) y luego deja el usuario en current_user().
    La verificación va dentro de la función envuelta, y no en intercept_service, porque solo ahí
    está el 'context' con el que se puede abortar la llamada.
    """
    def authenticate(context):
        if token is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing token: call Login first")
        try:
            return token_verifier.verify(token)
        except InvalidTokenError as error:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, str(error))   # abort lanza: no sigue

    def call_returning_one_response(behavior):
        def authenticated_behavior(request_or_iterator, context):
            context_token = _current_user.set(authenticate(context))
            try:
                return behavior(request_or_iterator, context)
            finally:
                _current_user.reset(context_token)    # el hilo vuelve al pool sin usuario
        return authenticated_behavior

    def call_returning_a_stream(behavior):
        # Si el handler devuelve un stream de respuestas, el usuario debe seguir puesto mientras
        # se recorre, no solo mientras se crea: por eso es un generador
        def authenticated_behavior(request_or_iterator, context):
            context_token = _current_user.set(authenticate(context))
            try:
                yield from behavior(request_or_iterator, context)
            finally:
                _current_user.reset(context_token)
        return authenticated_behavior

    # Un RPC tiene una de cuatro formas según si recibe y devuelve un mensaje o un stream
    if handler.unary_unary:
        return grpc.unary_unary_rpc_method_handler(
            call_returning_one_response(handler.unary_unary),
            handler.request_deserializer, handler.response_serializer)
    if handler.stream_unary:
        return grpc.stream_unary_rpc_method_handler(
            call_returning_one_response(handler.stream_unary),
            handler.request_deserializer, handler.response_serializer)
    if handler.unary_stream:
        return grpc.unary_stream_rpc_method_handler(
            call_returning_a_stream(handler.unary_stream),
            handler.request_deserializer, handler.response_serializer)
    return grpc.stream_stream_rpc_method_handler(
        call_returning_a_stream(handler.stream_stream),
        handler.request_deserializer, handler.response_serializer)
