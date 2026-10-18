"""
 Acceso al sistema de archivos distribuidos de manera transparente para el usuario,
 montando un filesystem FUSE que se comunica con los superpeers por gRPC meadiante DFSClient.
"""

import os
import stat
import sys
import tempfile

import grpc
from fuse import FUSE, Operations, FuseOSError

from client import DFSClient
from errors import errno_from_rpc_error


class DFShaFS(Operations):
    """Genera transparencia de acceso convirtiendo requests al kernel en llamadas al cliente DFSClient"""

    def __init__(self, client):
        self.client = client
        self.last_file_handle = 0      # último número de handle entregado al kernel (ver _new_file_handle)
        self.temp_files = {}           # Tarea 4: handle -> copia local temporal del archivo abierto
        self.uid, self.gid = os.getuid(), os.getgid()

    # ---------- Helpers ----------

    def _new_file_handle(self):
        """
        Entrega un número nuevo (1, 2, 3...) para identificar un archivo abierto.
        El kernel lo devuelve en cada read/write/release de ese archivo.
        Empieza en 1 porque 0 es lo que devuelven los métodos por defecto de fusepy.
        """
        self.last_file_handle += 1
        return self.last_file_handle

    # ---------- Consultas (no modifican nada) ----------

    def getattr(self, path, fh=None):
        """
        El kernel pregunta "¿qué es este path?" (en ls, cd, stat y al recorrer cualquier ruta).
        Pedimos los metadatos al superpeer y los traducimos al formato que espera el kernel.
        """
        # 1. Pedir los metadatos al superpeer (RPC Stat)
        try:
            attributes = self.client.stat(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))   # p. ej. errno 2 -> "No such file or directory"

        # 2. Decidir tipo de archivo y permisos
        if attributes.is_dir:
            file_type = stat.S_IFDIR               # directorio
            permissions = 0o755                    # rwxr-xr-x (la x es la que permite hacer cd)
            link_count = 2                         # convención POSIX para directorios
        else:
            file_type = stat.S_IFREG               # archivo regular
            permissions = 0o644                    # rw-r--r--
            link_count = 1

        # 3. Responder en el formato de "struct stat" (fusepy lo convierte a la estructura de C)
        return {
            "st_mode": file_type | permissions,    # tipo y permisos van juntos en un solo número
            "st_nlink": link_count,
            "st_size": attributes.size,            # bytes; el kernel nunca lee más allá de esto
            "st_mtime": attributes.mtime,          # última modificación (la que muestra ls -l)
            "st_atime": attributes.mtime,          # último acceso: no lo registramos, usamos mtime
            "st_ctime": attributes.mtime,          # último cambio de metadatos: ídem
            "st_uid": self.uid,                    # dueño: el usuario que montó el filesystem
            "st_gid": self.gid,
        }

    def readdir(self, path, fh):
        """
        El kernel pregunta "¿qué nombres hay dentro de este directorio?" (ls, find, autocompletar).
        Solo devolvemos nombres; los detalles de cada uno el kernel los pide después con getattr.
        """
        try:
            child_names = self.client.ls(path)     # RPC ListDir, p. ej. ["notes.txt", "report.pdf"]
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))

        # Todo directorio POSIX lista "." (él mismo) y ".." (su padre)
        return [".", ".."] + child_names

    # ---------- Operaciones sobre el espacio de nombres (solo tocan el registro del superpeer) ----------

    def mkdir(self, path, mode):
        """
        El kernel pide crear un directorio (comando mkdir).
        mode: permisos pedidos; los ignoramos porque todos los directorios se muestran como 755.
        """
        try:
            self.client.mkdir(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))
        return 0                                   # 0 = éxito

    def rmdir(self, path):
        """El kernel pide borrar un directorio vacío (comando rmdir; rm -r al final de cada carpeta)."""
        try:
            self.client.rmdir(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))   # p. ej. "Directory not empty"
        return 0

    def unlink(self, path):
        """El kernel pide borrar un archivo (comando rm)."""
        try:
            self.client.rm(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))   # p. ej. "Is a directory"
        return 0

    def rename(self, old, new):
        """
        El kernel pide mover o renombrar (comando mv, y los editores al guardar).
        old y new llegan como paths completos: mv a.txt docs/ llega como ("/a.txt", "/docs/a.txt").
        """
        try:
            self.client.mv(old, new)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))
        return 0

    def create(self, path, mode, fi=None):
        """
        El kernel pide crear un archivo nuevo y dejarlo abierto (touch, echo > nuevo, cp hacia el mount).
        Por ahora el archivo queda vacío: escribir contenido llega en la Tarea 4 (write/flush/release).
        Devuelve un handle porque el archivo queda abierto; fusepy cierra con su release por defecto.
        """
        try:
            self.client.create(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))
        return self._new_file_handle()


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python mount.py <superpeer host:port> <mountpoint>")

    client = DFSClient([sys.argv[1]])
    print(f"connected to: {client.addr}")
    FUSE(DFShaFS(client), sys.argv[2], foreground=True, nothreads=True,
        fsname="dfsha", debug=os.environ.get("DFSHA_DEBUG") == "1")

if __name__ == "__main__":
    main()
