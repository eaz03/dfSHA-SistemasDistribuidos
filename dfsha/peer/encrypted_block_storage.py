"""
Implementación de BlockStorage que cifra cada bloque antes de guardarlo (cifrado en reposo).

No escribe en disco por su cuenta: envuelve otro BlockStorage (normalmente DiskBlockStorage) y le
pasa los datos ya cifrados (patrón decorador). Para el peer se usa igual que cualquier BlockStorage:

    storage = EncryptedBlockStorage(DiskBlockStorage(data_directory), load_or_create_secret_key(key_path))

Cifrado: AES-256-GCM. GCM además de cifrar autentica: si cambia un solo bit del bloque guardado,
al leerlo se detecta y se rechaza, en vez de entregar datos corruptos o manipulados.

Formato de cada bloque guardado:
    [versión: 1 byte][nonce: 12 bytes][datos cifrados + tag: tamaño original + 16 bytes]
"""

import errno
import secrets

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from dfsha.peer.block_storage import BlockStorage, validate_block_id
from dfsha.common.errors import FileSystemError

FORMAT_VERSION = b"\x01"     # permite cambiar el formato después (p. ej. rotar claves) y seguir leyendo los viejos
NONCE_BYTES = 12             # el tamaño estándar para GCM
TAG_BYTES = 16               # la "etiqueta" que GCM agrega para detectar cambios
KEY_BYTES = 32               # AES-256


class EncryptedBlockStorage(BlockStorage):
    """Cifra al escribir y descifra al leer; el almacenamiento envuelto solo ve bytes cifrados."""

    def __init__(self, inner_storage, key):
        if len(key) != KEY_BYTES:
            raise ValueError(f"block encryption key must have {KEY_BYTES} bytes")
        self._inner_storage = inner_storage
        self._cipher = AESGCM(key)

    def write_block(self, block_id, data):
        # Se valida aquí también: el block_id se usa en el cifrado, antes de llegar al almacenamiento envuelto
        validate_block_id(block_id)
        # Un nonce nuevo en cada escritura: GCM se rompe si se repite un nonce con la misma clave.
        # Efecto visible: el mismo contenido cifrado dos veces da bytes distintos en disco
        nonce = secrets.token_bytes(NONCE_BYTES)
        # El block_id va como "dato asociado": no se cifra, pero queda atado al bloque cifrado.
        # Si alguien copia el archivo de otro bloque sobre este nombre, al leerlo se detecta
        encrypted_data = self._cipher.encrypt(nonce, data, block_id.encode("ascii"))
        self._inner_storage.write_block(block_id, FORMAT_VERSION + nonce + encrypted_data)

    def read_block(self, block_id):
        validate_block_id(block_id)
        stored = self._inner_storage.read_block(block_id)       # ENOENT viene de aquí, tal cual

        header_bytes = len(FORMAT_VERSION) + NONCE_BYTES
        if len(stored) < header_bytes + TAG_BYTES or stored[:1] != FORMAT_VERSION:
            raise FileSystemError(errno.EIO, block_id)
        nonce = stored[1:header_bytes]
        encrypted_data = stored[header_bytes:]
        try:
            return self._cipher.decrypt(nonce, encrypted_data, block_id.encode("ascii"))
        except InvalidTag:
            # Corrupto, manipulado, de otro bloque, o la clave no es la correcta: no hay forma de
            # distinguirlos. No se borra: si la clave está mal configurada, borrar destruiría datos
            # sanos. Quien lea debe usar otra réplica (y la re-replicación lo reemplazará).
            raise FileSystemError(errno.EIO, block_id) from None

    def delete_block(self, block_id):
        self._inner_storage.delete_block(block_id)       # nada que descifrar: borrar es igual
