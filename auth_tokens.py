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

import contextlib
import os
import time
import uuid

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

SIGNING_ALGORITHM = "EdDSA"
SESSION_TOKEN_TYPE = "session"          # token de login; los permisos de bloque tendrán otro tipo
DEFAULT_SESSION_TTL_SECONDS = 60 * 60   # 1 hora
CLOCK_SKEW_SECONDS = 30                 # margen por si los relojes de las máquinas no coinciden
REQUIRED_CLAIMS = ["sub", "typ", "iat", "exp"]


class InvalidTokenError(Exception):
    """El token no sirve: mal formado, firma inválida, vencido o de otro tipo. El mensaje dice por qué."""


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
    created = _create_file_exclusively(private_key_path, private_pem, mode=0o600)
    if not created:
        # Otro proceso la creó justo al mismo tiempo: se usa la suya, para que todos firmen igual
        return _load_private_key(private_key_path)

    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    _create_file_exclusively(private_key_path + ".pub", public_pem, mode=0o644)   # la pública sí se puede compartir
    return private_key


def load_verification_key(public_key_path):
    """Devuelve la clave pública guardada en public_key_path (la que usan quienes solo verifican)."""
    with open(public_key_path, "rb") as public_key_file:
        return serialization.load_pem_public_key(public_key_file.read())


def _load_private_key(private_key_path):
    with open(private_key_path, "rb") as private_key_file:
        return serialization.load_pem_private_key(private_key_file.read(), password=None)


def _create_file_exclusively(path, content, mode):
    """
    Crea path con content solo si no existe todavía. Devuelve True si lo creó, False si ya existía.
    Se escribe completo en un temporal y luego se "engancha" con os.link, que falla si path ya
    existe: así nunca queda un archivo a medias y nunca se pisa una clave que otro proceso ya creó
    (os.replace sí la pisaría).
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    temporary_path = f"{path}.tmp-{uuid.uuid4().hex}"
    try:
        with open(temporary_path, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.chmod(temporary_path, mode)
        try:
            os.link(temporary_path, path)
            return True
        except FileExistsError:
            return False
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.remove(temporary_path)


# ---------- Emitir y verificar ----------

class TokenIssuer:
    """Emite tokens de sesión. Lo usa el superpeer después de un login correcto; necesita la clave privada."""

    def __init__(self, signing_key, ttl_seconds=DEFAULT_SESSION_TTL_SECONDS, clock=time.time):
        """
        clock: la hora actual en segundos. Es la hora del calendario (time.time) y no monotonic,
            porque el token lo puede verificar otra máquina y monotonic no se puede comparar entre
            máquinas. Las pruebas pasan un reloj falso para fabricar tokens ya vencidos.
        """
        self._signing_key = signing_key
        self._ttl_seconds = ttl_seconds
        self._clock = clock

    def issue(self, username):
        """Devuelve un token que identifica a username durante ttl_seconds."""
        issued_at = int(self._clock())
        claims = {
            "sub": username,                    # "subject": a quién identifica
            "typ": SESSION_TOKEN_TYPE,
            "iat": issued_at,                   # "issued at": cuándo se emitió
            "exp": issued_at + self._ttl_seconds,   # "expiration": desde cuándo deja de servir
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
        try:
            claims = jwt.decode(
                token,
                self._verification_key,
                # Lista de algoritmos aceptados: nunca se le cree al token cuál usar, si no un
                # atacante podría mandar uno con algoritmo "none" (sin firma)
                algorithms=[SIGNING_ALGORITHM],
                leeway=CLOCK_SKEW_SECONDS,
                options={"require": REQUIRED_CLAIMS},
            )
        except jwt.ExpiredSignatureError:
            raise InvalidTokenError("token expired") from None
        except jwt.InvalidTokenError as error:
            raise InvalidTokenError(f"invalid token: {error}") from None

        if claims["typ"] != SESSION_TOKEN_TYPE:
            raise InvalidTokenError(f"wrong token type: {claims['typ']}")
        return claims["sub"]
