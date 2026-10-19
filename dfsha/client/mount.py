"""
 Acceso al sistema de archivos distribuidos de manera transparente para el usuario,
 montando un filesystem FUSE que se comunica con los superpeers por gRPC meadiante DFSClient.
"""

import errno
import os
import stat
import sys
import tempfile

import grpc
from fuse import FUSE, Operations, FuseOSError

from dfsha.client.client import DFSClient
from dfsha.common.errors import FileSystemError, errno_from_rpc_error

# ---------- Lectura: cuándo bajar bloques por adelantado ----------
# El kernel nunca dice "quiero el archivo completo": pide pedazos de hasta 128 KiB.
# Si los pedazos llegan uno detrás de otro (cada read empieza donde terminó el anterior),
# alguien está leyendo el archivo de principio a fin (cat, cp) y conviene bajar el resto de una vez.
SEQUENTIAL_READS_BEFORE_PREFETCH = 2           # 2: head (una sola lectura) no baja el archivo entero
FULL_PREFETCH_LIMIT_BYTES = 64 * 1024 * 1024   # hasta este tamaño, la lectura secuencial baja TODO lo que falta
PREFETCH_WINDOW_BLOCKS = 8                     # archivos más grandes: solo los 8 bloques siguientes (memoria fija)


class ReadHandle:
    """
    Estado de un archivo abierto para lectura (uno por cada open): dónde están sus bloques
    y cuáles ya se bajaron. Se descarta en release.
    """

    def __init__(self, path, block_list):
        self.path = path
        self.block_locations = list(block_list.blocks)    # en orden: el byte N está en el bloque N // block_size
        self.block_size = block_list.block_size
        self.file_size = 0                                # suma de los tamaños de los bloques
        for block_location in self.block_locations:
            self.file_size += block_location.size
        self.cached_blocks = {}                           # número de bloque -> bytes ya bajados
        self.failed_prefetch_blocks = set()               # bloques que la descarga anticipada no pudo leer: no reintentarlos ahí
        self.next_sequential_offset = 0                   # dónde empezaría la próxima lectura si es secuencial
        self.consecutive_reads = 0                        # lecturas seguidas, cada una donde terminó la anterior

    def block_ids(self):
        """Los IDs de los bloques en orden: identifican la versión del archivo que se abrió."""
        identifiers = []
        for block_location in self.block_locations:
            identifiers.append(block_location.block_id)
        return identifiers


class DFShaFS(Operations):
    """Genera transparencia de acceso convirtiendo requests al kernel en llamadas al cliente DFSClient"""

    def __init__(self, client):
        self.client = client
        self.last_file_handle = 0      # último número de handle entregado al kernel (ver _new_file_handle)
        self.handles = {}              # handle -> ReadHandle (subtarea 6: también los archivos abiertos para escribir)
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

    # ---------- Lectura del contenido (bloques en los peers) ----------

    def open(self, path, flags):
        """
        El kernel abre un archivo (cat, less, cp desde el mount, open() en un programa).
        Por ahora solo lectura: pide al superpeer las ubicaciones de los bloques UNA vez
        y guarda el estado del archivo abierto. Los bytes se bajan después, en read.
        Abrir para escribir da EROFS ("Read-only file system") hasta la subtarea 6.
        """
        access_mode = flags & os.O_ACCMODE              # O_RDONLY, O_WRONLY u O_RDWR
        if access_mode != os.O_RDONLY:
            raise FuseOSError(errno.EROFS)
        try:
            block_list = self.client.get_block_locations(path)
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))
        file_handle = self._new_file_handle()
        self.handles[file_handle] = ReadHandle(path, block_list)
        return file_handle

    def read(self, path, size, offset, fh):
        """
        El kernel pide size bytes desde offset (en pedazos de hasta 128 KiB).
        1. Baja los bloques del rango pedido que no estén ya en el caché.
        2. Si la lectura es secuencial, baja además el resto del archivo (o la ventana siguiente), en paralelo.
        3. Arma la respuesta con el pedazo pedido de cada bloque.
        """
        read_handle = self.handles.get(fh)
        if read_handle is None:
            raise FuseOSError(errno.EBADF)              # el kernel nunca debería pedir un handle desconocido
        if offset >= read_handle.file_size:
            return b""                                  # fin del archivo

        end = min(offset + size, read_handle.file_size)
        first_block_number = offset // read_handle.block_size
        last_block_number = (end - 1) // read_handle.block_size

        # 1. ¿Esta lectura empieza donde terminó la anterior?
        if offset == read_handle.next_sequential_offset:
            read_handle.consecutive_reads += 1
        else:
            read_handle.consecutive_reads = 1           # un salto (tail, dd skip): se empieza a contar de nuevo
        read_handle.next_sequential_offset = end

        # 2. Los bloques del rango pedido son obligatorios: si alguno no se puede leer, esta lectura falla
        required_block_numbers = []
        for block_number in range(first_block_number, last_block_number + 1):
            if block_number not in read_handle.cached_blocks:
                required_block_numbers.append(block_number)
        try:
            self._fetch_blocks(read_handle, required_block_numbers)
        except FileSystemError as file_system_error:
            raise FuseOSError(file_system_error.error_number)   # ENODATA: "No data available"
        except grpc.RpcError as rpc_error:
            raise FuseOSError(errno_from_rpc_error(rpc_error))

        # 3. Si la lectura es secuencial, bajar por adelantado (en paralelo) lo que sigue.
        #    Es opcional: un bloque que falla aquí no hace fallar esta lectura; su error aparecerá
        #    cuando se lea de verdad, como pasa con un sector dañado en un disco (cat entrega todo hasta ahí)
        if read_handle.consecutive_reads >= SEQUENTIAL_READS_BEFORE_PREFETCH:
            prefetch_block_numbers = []
            for block_number in self._prefetch_block_numbers(read_handle, last_block_number):
                already_cached = block_number in read_handle.cached_blocks
                already_failed = block_number in read_handle.failed_prefetch_blocks
                if not already_cached and not already_failed:
                    prefetch_block_numbers.append(block_number)
            self._prefetch_into_cache(read_handle, prefetch_block_numbers)

        # 4. En archivos grandes, soltar los bloques que ya quedaron atrás: la memoria no crece con el archivo
        if read_handle.file_size > FULL_PREFETCH_LIMIT_BYTES:
            for block_number in list(read_handle.cached_blocks):
                if block_number < first_block_number:
                    del read_handle.cached_blocks[block_number]

        # 5. La respuesta: de cada bloque del rango, solo la parte que cae entre offset y end
        pieces = []
        for block_number in range(first_block_number, last_block_number + 1):
            block_data = read_handle.cached_blocks[block_number]
            block_start = block_number * read_handle.block_size      # posición del bloque dentro del archivo
            piece_start = max(offset, block_start) - block_start
            piece_end = min(end, block_start + len(block_data)) - block_start
            pieces.append(block_data[piece_start:piece_end])
        return b"".join(pieces)

    def release(self, path, fh):
        """El kernel cerró el archivo (el último close): se descarta su estado y su caché."""
        self.handles.pop(fh, None)
        return 0

    # ---------- Helpers de lectura ----------

    def _prefetch_block_numbers(self, read_handle, last_block_number):
        """
        Bloques a bajar por adelantado cuando la lectura es secuencial:
        todos los que siguen si el archivo es chico (<= FULL_PREFETCH_LIMIT_BYTES),
        o solo los PREFETCH_WINDOW_BLOCKS siguientes si es grande.
        """
        total_blocks = len(read_handle.block_locations)
        if read_handle.file_size <= FULL_PREFETCH_LIMIT_BYTES:
            stop = total_blocks
        else:
            stop = min(last_block_number + 1 + PREFETCH_WINDOW_BLOCKS, total_blocks)
        return range(last_block_number + 1, stop)

    def _fetch_blocks(self, read_handle, block_numbers):
        """
        Baja esos bloques en paralelo y los guarda en read_handle.cached_blocks.
        Si alguno falla, pide las ubicaciones otra vez y reintenta una vez: los permisos de lectura
        que firma el superpeer vencen a los 30 min, y las réplicas vivas pueden haber cambiado.
        Si el archivo cambió de versión entretanto (otros block_id), no se mezclan versiones: se lanza el error.
        """
        if len(block_numbers) == 0:
            return
        try:
            self._read_into_cache(read_handle, block_numbers)
            return
        except FileSystemError as first_error:
            fresh_block_list = self.client.get_block_locations(read_handle.path)
            fresh_handle = ReadHandle(read_handle.path, fresh_block_list)
            if fresh_handle.block_ids() != read_handle.block_ids():
                raise first_error                       # el archivo se sobrescribió: no mezclar versiones
            read_handle.block_locations = fresh_handle.block_locations
        self._read_into_cache(read_handle, block_numbers)   # si vuelve a fallar, el error sube a read

    def _read_into_cache(self, read_handle, block_numbers):
        """Lee esos bloques (client.read_blocks, en paralelo) y los guarda en el caché. Si uno falla, lanza su error."""
        block_locations = []
        for block_number in block_numbers:
            block_locations.append(read_handle.block_locations[block_number])
        blocks_data = self.client.read_blocks(block_locations)
        for block_number, block_data in zip(block_numbers, blocks_data):
            read_handle.cached_blocks[block_number] = block_data

    def _prefetch_into_cache(self, read_handle, block_numbers):
        """Descarga anticipada: guarda en el caché los bloques que se pudieron leer y omite los que fallaron."""
        if len(block_numbers) == 0:
            return
        block_locations = []
        for block_number in block_numbers:
            block_locations.append(read_handle.block_locations[block_number])
        blocks_data = self.client.read_blocks_allowing_failures(block_locations)
        for block_number, block_data in zip(block_numbers, blocks_data):
            if block_data is None:
                read_handle.failed_prefetch_blocks.add(block_number)   # se pedirá recién cuando se lea
            else:
                read_handle.cached_blocks[block_number] = block_data


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python -m dfsha.client.mount <superpeer host:port> <mountpoint>")

    # Credenciales por variables de entorno: en Docker Compose vienen del archivo .env (fuera de git)
    username = os.environ.get("DFSHA_USER")
    password = os.environ.get("DFSHA_PASSWORD")
    if not username or not password:
        sys.exit("set DFSHA_USER and DFSHA_PASSWORD (see .env.example)")
    tls_dir = os.environ.get("DFSHA_TLS_DIR", "certs/client")    # certificado de esta máquina (make_certs.py)

    try:
        client = DFSClient([sys.argv[1]], username, password, tls_dir)
    except grpc.RpcError as rpc_error:
        sys.exit(f"login failed: {rpc_error.details()}")
    print(f"connected to: {client.addr}")
    FUSE(DFShaFS(client), sys.argv[2], foreground=True, nothreads=True,
        fsname="dfsha", debug=os.environ.get("DFSHA_DEBUG") == "1")

if __name__ == "__main__":
    main()
