"""
Implementación de BlockStorage en el disco local de un peer: cada bloque es un archivo
root_directory/<block_id> con sus bytes.

Garantías:
  - Una escritura interrumpida nunca deja un bloque a medias: se escribe en un temporal y se
    renombra al nombre final solo cuando los bytes ya están completos en el disco.
  - Un block_id que llega por la red nunca puede apuntar fuera de root_directory.
"""

import contextlib
import errno
import os
import uuid

from dfsha.peer.block_storage import BlockStorage, validate_block_id
from dfsha.common.errors import FileSystemError

TEMPORARY_MARKER = ".tmp-"           # "<block_id>.tmp-<uuid>": un bloque que se está escribiendo


class DiskBlockStorage(BlockStorage):
    """Bloques en un directorio local: un archivo por bloque, con el block_id como nombre."""

    def __init__(self, root_directory):
        self._root_directory = root_directory
        os.makedirs(root_directory, exist_ok=True)      # exist_ok: al reiniciar el peer ya existe
        self._remove_leftover_temporaries()

    # ---------- Helpers internos ----------

    def _block_path(self, block_id):
        """
        Devuelve el path en disco del bloque block_id.
        Lanza FileSystemError(EINVAL) si block_id no es válido: como el ID pasa a ser parte de un
        path, validarlo aquí impide que se salga de root_directory.
        """
        validate_block_id(block_id)
        return os.path.join(self._root_directory, block_id)

    def _remove_leftover_temporaries(self):
        """
        Borra los temporales de escrituras que quedaron a medias porque el peer se cayó.
        Solo se llama al arrancar: en ese momento nadie puede estar escribiendo uno.
        """
        for file_name in os.listdir(self._root_directory):
            if TEMPORARY_MARKER in file_name:
                os.remove(os.path.join(self._root_directory, file_name))

    # ---------- Operaciones ----------
    # No hace falta un lock: cada bloque es un archivo distinto, y dos operaciones sobre el mismo
    # bloque no se mezclan porque os.replace es atómico (se ve el bloque completo o no se ve).

    def write_block(self, block_id, data):
        final_path = self._block_path(block_id)
        # Un temporal propio de esta escritura: dos reintentos simultáneos no se pisan
        temporary_path = final_path + TEMPORARY_MARKER + uuid.uuid4().hex
        try:
            with open(temporary_path, "wb") as temporary_file:
                temporary_file.write(data)
                temporary_file.flush()                   # de Python al sistema operativo
                os.fsync(temporary_file.fileno())        # del sistema operativo al disco
            # Mismo directorio que el temporal, así que el rename es atómico
            os.replace(temporary_path, final_path)      # sobreescribe si ya existía: es un reintento
        except OSError as os_error:
            # Disco lleno: es un error esperable, no un bug. Como FileSystemError llega al cliente
            # como ENOSPC (RESOURCE_EXHAUSTED); como OSError llegaría como UNKNOWN.
            # Cualquier otro OSError (permisos, disco dañado) sí es un problema del peer: se deja pasar.
            if os_error.errno == errno.ENOSPC:
                raise FileSystemError(errno.ENOSPC, block_id) from None
            raise
        finally:
            # Si algo falló (disco lleno, permisos...), no dejar el temporal tirado.
            # Si todo salió bien, el replace ya lo convirtió en el bloque y no queda nada que borrar.
            with contextlib.suppress(FileNotFoundError):
                os.remove(temporary_path)

    def read_block(self, block_id):
        block_path = self._block_path(block_id)
        # Se intenta abrir directamente en vez de preguntar antes si existe: entre la pregunta
        # y el open, otro hilo podría borrarlo
        try:
            with open(block_path, "rb") as block_file:
                return block_file.read()
        except FileNotFoundError:
            raise FileSystemError(errno.ENOENT, block_id) from None

    def delete_block(self, block_id):
        block_path = self._block_path(block_id)
        with contextlib.suppress(FileNotFoundError):     # no existe: ya estaba borrado (idempotente)
            os.remove(block_path)
