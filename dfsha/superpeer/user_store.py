"""
Interfaz del almacén de usuarios de DFSha, y las reglas de contraseñas que comparten todas
sus implementaciones.

Las contraseñas nunca se guardan: se guarda un hash (scrypt) del que no se puede volver a la
contraseña. Para comprobar un login se hashea la contraseña recibida y se compara.
Si alguien roba el almacén, no obtiene las contraseñas.

Implementaciones:
  - JsonUserStore (json_user_store.py): un archivo JSON.
  - Más adelante podría vivir junto a la persistencia del registro, sin tocar a quien la usa.
"""

import base64
import hashlib
import hmac
import re
import secrets
from abc import ABC, abstractmethod

# Los nombres son la llave del namespace en el registro y viajarán dentro del token:
# solo caracteres sin significado especial (nada de "/", espacios ni mayúsculas que confundan)
USERNAME_PATTERN = re.compile(r"[a-z0-9_-]{3,32}")
MIN_PASSWORD_LENGTH = 8

# Parámetros de scrypt: lento y con uso de memoria a propósito (unos 16 MB y decenas de ms por
# intento). En un login no se nota, pero hace carísimo probar millones de contraseñas.
SCRYPT_N = 2 ** 14       # costo de CPU y memoria
SCRYPT_R = 8             # tamaño del bloque interno
SCRYPT_P = 1             # paralelismo
SALT_BYTES = 16
HASH_BYTES = 32


def validate_username(username):
    """Lanza ValueError si username no cumple USERNAME_PATTERN."""
    if not isinstance(username, str) or USERNAME_PATTERN.fullmatch(username) is None:
        raise ValueError("username must be 3-32 characters among a-z, 0-9, '_' and '-'")


def validate_password(password):
    """Lanza ValueError si password es demasiado corta."""
    if not isinstance(password, str) or len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"password must have at least {MIN_PASSWORD_LENGTH} characters")


def _scrypt(password, salt, n, r, p):
    return hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=HASH_BYTES)


def hash_password(password):
    """
    Devuelve "scrypt$N$r$p$<sal base64>$<hash base64>".
    La sal es aleatoria y distinta en cada llamada: dos usuarios con la misma contraseña
    quedan con hashes distintos, y no sirven tablas precalculadas de contraseñas comunes.
    Se guardan los parámetros junto al hash para poder subirlos en el futuro sin romper
    los hashes que ya existen.
    """
    salt = secrets.token_bytes(SALT_BYTES)      # secrets y no random: random es predecible
    password_hash = _scrypt(password, salt, SCRYPT_N, SCRYPT_R, SCRYPT_P)
    encoded_salt = base64.b64encode(salt).decode("ascii")
    encoded_hash = base64.b64encode(password_hash).decode("ascii")
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${encoded_salt}${encoded_hash}"


def verify_password(password, stored_hash):
    """
    True si password es la contraseña con la que se creó stored_hash.
    Lanza ValueError si stored_hash no tiene el formato de hash_password (almacén corrupto).
    """
    parts = stored_hash.split("$")
    if len(parts) != 6 or parts[0] != "scrypt":
        raise ValueError("unknown password hash format")
    _, n, r, p, encoded_salt, encoded_hash = parts
    salt = base64.b64decode(encoded_salt)
    expected_hash = base64.b64decode(encoded_hash)
    received_hash = _scrypt(password, salt, int(n), int(r), int(p))
    # compare_digest tarda lo mismo sin importar dónde difieran: con '==' un atacante podría
    # medir tiempos de respuesta e ir adivinando el hash byte por byte
    return hmac.compare_digest(received_hash, expected_hash)


# Hash de una contraseña al azar que nadie conoce. Se verifica contra él cuando el usuario no
# existe, para que esa respuesta tarde lo mismo que una contraseña incorrecta (ver authenticate).
_DUMMY_PASSWORD_HASH = hash_password(secrets.token_urlsafe(16))


class UserStore(ABC):
    """
    Interfaz del almacén de usuarios. Las implementaciones heredan de esta clase y definen
    sus métodos abstractos; authenticate ya viene hecho y no se redefine.
    Los errores de administración (usuario repetido, nombre inválido...) son ValueError:
    no son errores del sistema de archivos, así que no usan FileSystemError.
    """

    @abstractmethod
    def add_user(self, username, password, home=None):
        """
        Crea el usuario username con la contraseña password (se guarda solo su hash).
        home: el id del superpeer que lo atiende normalmente (p. ej. "sp2"); None = el primero de la lista.
        Errores (ValueError): nombre inválido, contraseña demasiado corta, o el usuario ya existe.
        """

    @abstractmethod
    def remove_user(self, username):
        """
        Borra el usuario username.
        Errores (ValueError): el usuario no existe.
        """

    @abstractmethod
    def list_users(self):
        """Devuelve los nombres de todos los usuarios, en orden alfabético."""

    @abstractmethod
    def _password_hash_of(self, username):
        """Devuelve el hash guardado de username, o None si no existe. Solo lo usa authenticate."""

    # ---------- Federación: los usuarios son los mismos en todos los superpeers ----------

    @abstractmethod
    def home_of(self, username):
        """El id del superpeer que atiende normalmente a username, o None (no tiene, o no existe)."""

    @abstractmethod
    def export_records(self):
        """
        Todos los registros, incluidos los borrados, como datos JSON:
        {usuario: {"password_hash", "home", "updated_at"}} o {usuario: {"deleted": True, "updated_at"}}.
        """

    @abstractmethod
    def merge_records(self, records):
        """
        Fusiona los registros de otro superpeer (lo devuelto por su export_records): por cada usuario
        queda el registro cambiado más recientemente (updated_at), también si es un borrado. Así dos
        superpeers que fusionan sus registros quedan iguales, sin importar el orden. Devuelve cuántos cambiaron.
        """

    @abstractmethod
    def fingerprint(self):
        """Un resumen (hash) de todos los registros: dos superpeers con la misma huella tienen los mismos usuarios."""

    def authenticate(self, username, password):
        """
        True si username existe y password es su contraseña; False en cualquier otro caso.
        Está implementado aquí, y no en cada almacén, porque es la parte delicada:
          - Devuelve lo mismo (False) si el usuario no existe o si la contraseña es incorrecta,
            para no revelar qué usuarios existen.
          - Si el usuario no existe, igual calcula un hash completo: sin eso, esa respuesta
            sería instantánea y midiendo el tiempo se sabría qué usuarios existen.
        """
        stored_hash = self._password_hash_of(username)
        if stored_hash is None:
            verify_password(password, _DUMMY_PASSWORD_HASH)    # gasta el mismo tiempo, y se descarta
            return False
        return verify_password(password, stored_hash)
