"""
Interfaz del almacenamiento de bloques de un peer: guardar, entregar y borrar pedazos de bytes
por su ID. No sabe nada de red ni de archivos de usuario.

El service Peer (gRPC) usa esta interfaz, nunca una implementación concreta; así se puede cambiar
dónde y cómo se guardan los bloques sin tocar peer.py:
  - DiskBlockStorage (disk_block_storage.py): un archivo por bloque en el disco local.
  - Más adelante: una en memoria para probar peer.py sin disco, o una que cifre los bloques
    antes de delegar en la de disco (cifrado en reposo).
"""

import errno
import re
from abc import ABC, abstractmethod

from errors import FileSystemError

# El registro genera los IDs con uuid4().hex: 32 caracteres hexadecimales en minúscula.
# Se acepta solo eso (lista blanca): así un ID como "../../etc/passwd" nunca llega a ser un path.
BLOCK_ID_PATTERN = re.compile(r"[0-9a-f]{32}")


def validate_block_id(block_id):
    """
    Lanza FileSystemError(EINVAL) si block_id no tiene la forma de un ID generado por el registro.
    Es parte del contrato: toda implementación la llama antes de tocar un bloque, para que un
    block_id inválido dé el mismo error sin importar dónde se guarden los bloques.
    """
    if not isinstance(block_id, str) or BLOCK_ID_PATTERN.fullmatch(block_id) is None:
        raise FileSystemError(errno.EINVAL, str(block_id))


class BlockStorage(ABC):
    """
    Interfaz del almacenamiento de bloques. Las implementaciones heredan de esta clase
    y definen todos sus métodos.
    Los errores se lanzan como FileSystemError, igual que en Registry, para que el service Peer
    los traduzca a gRPC con translate_file_system_errors_to_grpc.
    """

    @abstractmethod
    def write_block(self, block_id, data):
        """
        Guarda data (bytes) como el bloque block_id. Si ya existía lo reemplaza: es un reintento
        del cliente, con los mismos bytes, porque cada versión de un archivo usa IDs nuevos.
        Una escritura interrumpida nunca deja un bloque a medias: o queda completo o no queda.
        Errores: EINVAL si block_id no es válido.
        """

    @abstractmethod
    def read_block(self, block_id):
        """
        Devuelve los bytes del bloque block_id.
        Errores: EINVAL si block_id no es válido; ENOENT si este peer no tiene ese bloque.
        """

    @abstractmethod
    def delete_block(self, block_id):
        """
        Borra el bloque block_id. Si no existe no es error (idempotente): el superpeer puede
        reintentar un borrado que sí se hizo pero cuya respuesta se perdió en la red.
        Errores: EINVAL si block_id no es válido.
        """
