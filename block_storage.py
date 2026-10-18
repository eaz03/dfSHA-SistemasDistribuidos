"""
Almacenamiento de bloques en el disco local de un peer: cada bloque es un archivo
root_directory/<block_id> con sus bytes.

No sabe nada de red ni de archivos de usuario: solo guarda, entrega y borra pedazos de bytes
por su ID. El service Peer (gRPC) se apoya en esta clase, igual que el superpeer en Registry.

Garantías:
  - Una escritura interrumpida nunca deja un bloque a medias: se escribe en un temporal y se
    renombra al nombre final solo cuando los bytes ya están completos en el disco.
  - Un block_id que llega por la red nunca puede apuntar fuera de root_directory.
"""

import contextlib
import errno
import os
import re
import uuid

from errors import FileSystemError

# El registro genera los IDs con uuid4().hex: 32 caracteres hexadecimales en minúscula.
# Se acepta solo eso (lista blanca): así un ID como "../../etc/passwd" nunca llega a ser un path.
BLOCK_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
TEMPORARY_MARKER = ".tmp-"           # "<block_id>.tmp-<uuid>": un bloque que se está escribiendo


class BlockStorage:
    """Bloques en un directorio local: un archivo por bloque, con el block_id como nombre."""

    def __init__(self, root_directory):
        self._root_directory = root_directory
        os.makedirs(root_directory, exist_ok=True)      # exist_ok: al reiniciar el peer ya existe
        self._remove_leftover_temporaries()

    # ---------- Helpers internos ----------

    def _block_path(self, block_id):
        """
        Devuelve el path en disco del bloque block_id.
        Lanza FileSystemError(EINVAL) si block_id no tiene la forma de un ID generado por el registro.
        """
        if not isinstance(block_id, str) or BLOCK_ID_PATTERN.fullmatch(block_id) is None:
            raise FileSystemError(errno.EINVAL, str(block_id))
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
        """
        Guarda data (bytes) como el bloque block_id. Si ya existía lo reemplaza: es un reintento
        del cliente, con los mismos bytes, porque cada versión de un archivo usa IDs nuevos.
        Errores: EINVAL si block_id no es válido.
        """
        final_path = self._block_path(block_id)
        # Un temporal propio de esta escritura: dos reintentos simultáneos no se pisan
        temporary_path = final_path + TEMPORARY_MARKER + uuid.uuid4().hex
        try:
            with open(temporary_path, "wb") as temporary_file:
                temporary_file.write(data)
                temporary_file.flush()                   # de Python al sistema operativo
                os.fsync(temporary_file.fileno())        # del sistema operativo al disco
            # Mismo directorio que el temporal, así que el rename es atómico
            os.replace(temporary_path, final_path)
        finally:
            # Si algo falló (disco lleno, permisos...), no dejar el temporal tirado.
            # Si todo salió bien, el replace ya lo convirtió en el bloque y no queda nada que borrar.
            with contextlib.suppress(FileNotFoundError):
                os.remove(temporary_path)

    def read_block(self, block_id):
        """
        Devuelve los bytes del bloque block_id.
        Errores: EINVAL si block_id no es válido; ENOENT si este peer no tiene ese bloque.
        """
        block_path = self._block_path(block_id)
        # Se intenta abrir directamente en vez de preguntar antes si existe: entre la pregunta
        # y el open, otro hilo podría borrarlo
        try:
            with open(block_path, "rb") as block_file:
                return block_file.read()
        except FileNotFoundError:
            raise FileSystemError(errno.ENOENT, block_id) from None

    def delete_block(self, block_id):
        """
        Borra el bloque block_id. Si no existe no es error (idempotente): el superpeer puede
        reintentar un borrado que sí se hizo pero cuya respuesta se perdió en la red.
        Errores: EINVAL si block_id no es válido.
        """
        block_path = self._block_path(block_id)
        with contextlib.suppress(FileNotFoundError):
            os.remove(block_path)
