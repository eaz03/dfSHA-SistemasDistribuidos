"""
Tokens de DFSha: credenciales temporales que el cliente adjunta en cada llamada después del
login, para no enviar la contraseña cada vez.

Son JWT (RFC 7519) firmados con EdDSA (Ed25519), una firma asimétrica:
  - El superpeer firma con la clave PRIVADA (TokenIssuer). Solo él puede fabricar tokens.
  - Cualquiera con la clave PÚBLICA puede verificarlos (TokenVerifier), pero no fabricarlos.
    Así, más adelante, los peers podrán comprobar permisos de bloque sin poder falsificarlos.

Cualquiera puede leer los datos de un token (no van cifrados), pero nadie puede cambiarlos sin
invalidar la firma. Por eso el token solo debe viajar por canales cifrados (TLS).

El servidor no guarda los tokens emitidos: le basta con verificar la firma. La contrapartida es
que un token no se puede revocar antes de vencer; se mitiga con una duración corta y, en una
emergencia, cambiando la clave (eso invalida todos los tokens a la vez).

Este módulo envuelve PyJWT: el resto del código solo ve TokenIssuer, TokenVerifier e
InvalidTokenError, así que cambiar de librería no lo afectaría.
"""

import time

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from dfsha.common.key_files import create_file_exclusively

SIGNING_ALGORITHM = "EdDSA"
SESSION_TOKEN_TYPE = "session"          # token de login: identifica al usuario ante el superpeer
BLOCK_TOKEN_TYPE = "block"              # permiso de bloque: autoriza leer o escribir un bloque en un peer
BLOCK_OPERATIONS = ("read", "write")
DEFAULT_SESSION_TTL_SECONDS = 60 * 60   # 1 hora
DEFAULT_BLOCK_TTL_SECONDS = 30 * 60     # lo mismo que vive un plan sin commit: después, escribir ya no sirve
CLOCK_SKEW_SECONDS = 30                 # margen por si los relojes de las máquinas no coinciden
REQUIRED_CLAIMS = ["sub", "typ", "iat", "exp"]
REQUIRED_BLOCK_CLAIMS = REQUIRED_CLAIMS + ["bid", "op"]


class InvalidTokenError(Exception):
    """El token no sirve: mal formado, firma inválida, vencido o de otro tipo. El mensaje dice por qué."""


class WrongBlockPermissionError(InvalidTokenError):
    """
    Un permiso de bloque auténtico y vigente, pero para otro bloque u otra operación.
    Es un caso aparte para poder responder "no tienes permiso" (PERMISSION_DENIED) en vez de
    "no sé quién eres" (UNAUTHENTICATED).
    """


# ---------- Claves ----------

def load_or_create_signing_key(private_key_path):
    """
    Devuelve la clave privada guardada en private_key_path; si el archivo no existe, crea una
    nueva, la guarda (permisos 600) y guarda también su clave pública en private_key_path + ".pub".
    La clave privada nunca debe ir a git ni salir de los superpeers.
    """
    try:
        return _load_private_key(private_key_path)
    except FileNotFoundError:
        pass

    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    created = create_file_exclusively(private_key_path, private_pem, mode=0o600)
    if not created:
        # Otro proceso la creó justo al mismo tiempo: se usa la suya, para que todos firmen igual
        return _load_private_key(private_key_path)

    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    create_file_exclusively(private_key_path + ".pub", public_pem, mode=0o644)   # la pública sí se puede compartir
    return private_key


def load_verification_key(public_key_path):
    """Devuelve la clave pública guardada en public_key_path (la que usan quienes solo verifican)."""
    with open(public_key_path, "rb") as public_key_file:
        return load_verification_key_from_pem(public_key_file.read())


def load_verification_key_from_pem(public_key_pem):
    """Clave pública a partir de sus bytes PEM (p. ej. la que el superpeer le envía a un peer al registrarlo)."""
    return serialization.load_pem_public_key(public_key_pem)


def _load_private_key(private_key_path):
    with open(private_key_path, "rb") as private_key_file:
        return serialization.load_pem_private_key(private_key_file.read(), password=None)


# ---------- Emitir y verificar ----------

class TokenIssuer:
    """Emite tokens de sesión. Lo usa el superpeer después de un login correcto; necesita la clave privada."""

    def __init__(self, signing_key, ttl_seconds=DEFAULT_SESSION_TTL_SECONDS, clock=time.time,
                 block_ttl_seconds=DEFAULT_BLOCK_TTL_SECONDS):
        """
        clock: la hora actual en segundos. Es la hora del calendario (time.time) y no monotonic,
            porque el token lo puede verificar otra máquina y monotonic no se puede comparar entre
            máquinas. Las pruebas pasan un reloj falso para fabricar tokens ya vencidos.
        """
        self._signing_key = signing_key
        self.ttl_seconds = ttl_seconds      # público: el Login le dice al cliente cuánto dura su token
        self._block_ttl_seconds = block_ttl_seconds
        self._clock = clock

    def public_key_pem(self):
        """La clave pública que verifica lo que firma este emisor, en PEM: se puede compartir con cualquiera."""
        return self._signing_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    def issue(self, username):
        """Devuelve un token que identifica a username durante ttl_seconds."""
        issued_at = int(self._clock())
        claims = {
            "sub": username,                    # "subject": a quién identifica
            "typ": SESSION_TOKEN_TYPE,
            "iat": issued_at,                   # "issued at": cuándo se emitió
            "exp": issued_at + self.ttl_seconds,   # "expiration": desde cuándo deja de servir
        }
        return jwt.encode(claims, self._signing_key, algorithm=SIGNING_ALGORITHM)

    def issue_block_token(self, username, block_id, operation):
        """
        Permiso para que username haga operation ("read" o "write") sobre el bloque block_id en un peer.
        Lo emite el superpeer al entregar un plan (write) o las ubicaciones de un archivo (read), porque
        él es quien sabe de quién es cada bloque; el peer lo verifica con la clave pública, sin preguntarle.
        """
        if operation not in BLOCK_OPERATIONS:
            raise ValueError(f"operation must be one of {BLOCK_OPERATIONS}")
        issued_at = int(self._clock())
        claims = {
            "sub": username,
            "typ": BLOCK_TOKEN_TYPE,
            "bid": block_id,                    # el único bloque al que da acceso
            "op": operation,                    # y la única operación
            "iat": issued_at,
            "exp": issued_at + self._block_ttl_seconds,
        }
        return jwt.encode(claims, self._signing_key, algorithm=SIGNING_ALGORITHM)


class TokenVerifier:
    """Verifica tokens de sesión. Solo necesita la clave pública: puede fabricar tokens quien firma, no quien verifica."""

    def __init__(self, verification_key):
        self._verification_key = verification_key

    def verify(self, token):
        """
        Devuelve el usuario del token. Lanza InvalidTokenError si la firma no coincide, si está
        vencido (con CLOCK_SKEW_SECONDS de margen), si le falta un dato o si no es de sesión.
        """
        claims = self._decode(token, REQUIRED_CLAIMS)
        if claims["typ"] != SESSION_TOKEN_TYPE:
            raise InvalidTokenError(f"wrong token type: {claims['typ']}")
        return claims["sub"]

    def verify_block_token(self, token, block_id, operation):
        """
        Comprueba que token sea un permiso vigente, firmado por el superpeer, para hacer operation
        sobre block_id. Devuelve el usuario al que se le dio.
        Lanza InvalidTokenError si no es un permiso válido (firma, vencimiento, tipo), o
        WrongBlockPermissionError si es válido pero para otro bloque u otra operación.
        """
        claims = self._decode(token, REQUIRED_BLOCK_CLAIMS)
        if claims["typ"] != BLOCK_TOKEN_TYPE:
            raise InvalidTokenError(f"wrong token type: {claims['typ']}")     # p. ej. un token de sesión
        if claims["bid"] != block_id:
            raise WrongBlockPermissionError("block permission is for another block")
        if claims["op"] != operation:
            raise WrongBlockPermissionError(f"block permission is for '{claims['op']}', not '{operation}'")
        return claims["sub"]

    def _decode(self, token, required_claims):
        """Verifica firma, vencimiento y datos obligatorios, y devuelve los datos del token."""
        try:
            return jwt.decode(
                token,
                self._verification_key,
                # Lista de algoritmos aceptados: nunca se le cree al token cuál usar, si no un
                # atacante podría mandar uno con algoritmo "none" (sin firma)
                algorithms=[SIGNING_ALGORITHM],
                leeway=CLOCK_SKEW_SECONDS,
                options={"require": required_claims},
            )
        except jwt.ExpiredSignatureError:
            raise InvalidTokenError("token expired") from None
        except jwt.InvalidTokenError as error:
            raise InvalidTokenError(f"invalid token: {error}") from None
