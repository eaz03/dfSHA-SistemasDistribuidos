"""
Archivos de claves: crearlos una sola vez y de forma segura, y reutilizarlos después.
Lo usan los tokens (auth_tokens.py) y el cifrado de bloques (encrypted_block_storage.py).

Las claves nunca van al repositorio (.gitignore) y deberían vivir lejos de los datos que protegen:
una clave guardada en el mismo disco que los bloques cifrados no protege nada si roban el disco.
"""

import contextlib
import os
import secrets
import uuid


def create_file_exclusively(path, content, mode):
    """
    Crea path con content (bytes) solo si no existe todavía. Devuelve True si lo creó, False si ya existía.
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


def load_or_create_secret_key(path, size_bytes=32):
    """
    Devuelve los bytes de la clave secreta guardada en path; si no existe, crea una al azar
    (permisos 600: solo el dueño la lee). Si otro proceso la crea al mismo tiempo, se usa la suya.
    """
    # Si ya existe no hace nada; en ambos casos se lee del archivo, así todos usan la misma clave
    create_file_exclusively(path, secrets.token_bytes(size_bytes), mode=0o600)
    with open(path, "rb") as key_file:
        key = key_file.read()
    if len(key) != size_bytes:
        raise ValueError(f"key file {path} must have {size_bytes} bytes, has {len(key)}")
    return key
