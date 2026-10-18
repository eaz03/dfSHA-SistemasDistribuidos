"""
Errores del sistema de archivos y su traducción entre capas.

Un mismo error pasa por tres representaciones:
  1. FileSystemError   dentro del superpeer (reglas del registro, sin red)
  2. error gRPC        en la red (código + mensaje + errno en la metadata)
  3. FuseOSError       en el mount, para el kernel (lo crea mount.py)
Este módulo define la 1 y las traducciones 1 -> 2 (servidor) y 2 -> errno (cliente).
No importa fuse, para que el superpeer no dependa de libfuse.
"""

import contextlib
import errno
import os

import grpc


class FileSystemError(Exception):
    """
    Error de sistema de archivos, independiente de la red.
    error_number: errno de Linux, p. ej. errno.ENOENT.  path: el path afectado.
    Se lanza a propósito cuando se viola una regla del registro (p. ej. "el padre no existe").
    """

    def __init__(self, error_number, path):
        self.error_number = error_number
        self.path = path
        readable_message = f"{path}: {os.strerror(error_number)}"   # "/docs: No such file or directory"
        super().__init__(readable_message)


# Nombre de la llave con la que el errno viaja en la metadata final de la respuesta gRPC
ERRNO_METADATA_KEY = "errno"

# errno -> código gRPC, para clientes que no leen la metadata (scripts, pruebas).
# Los errno que no están aquí (ENOTDIR, EISDIR, EINVAL, EBUSY...) viajan como INVALID_ARGUMENT.
GRPC_CODE_FOR_ERRNO = {
    errno.ENOENT: grpc.StatusCode.NOT_FOUND,                 # No such file or directory
    errno.EEXIST: grpc.StatusCode.ALREADY_EXISTS,            # File exists
    errno.EACCES: grpc.StatusCode.PERMISSION_DENIED,         # Permission denied
    errno.ENOTEMPTY: grpc.StatusCode.FAILED_PRECONDITION,    # Directory not empty
    errno.ENOSPC: grpc.StatusCode.RESOURCE_EXHAUSTED,        # No space left on device (faltan peers)
}

# código gRPC -> errno, solo para cuando NO llegó metadata (p. ej. el superpeer está caído).
# Cualquier código que no esté aquí se convierte en EIO ("Input/output error").
# RESOURCE_EXHAUSTED no está a propósito: gRPC también lo usa por su cuenta (p. ej. un mensaje
# que pasa del límite de tamaño), y sin metadata no sabemos si de verdad faltó espacio.
ERRNO_FOR_GRPC_CODE = {
    grpc.StatusCode.NOT_FOUND: errno.ENOENT,
    grpc.StatusCode.ALREADY_EXISTS: errno.EEXIST,
    grpc.StatusCode.PERMISSION_DENIED: errno.EACCES,
    grpc.StatusCode.FAILED_PRECONDITION: errno.ENOTEMPTY,
}


# ---------- Lado servidor (superpeer) ----------

def abort_rpc(context, file_system_error):
    """
    Termina el RPC actual con file_system_error. Nunca retorna: context.abort lanza una excepción.
    Envía tres cosas: el errno exacto en la metadata (lo usa el mount),
    un código gRPC equivalente y un mensaje legible (los usan otros clientes y los logs).
    """
    context.set_trailing_metadata(((ERRNO_METADATA_KEY, str(file_system_error.error_number)),))
    grpc_status_code = GRPC_CODE_FOR_ERRNO.get(file_system_error.error_number,
                                               grpc.StatusCode.INVALID_ARGUMENT)
    context.abort(grpc_status_code, str(file_system_error))


@contextlib.contextmanager
def translate_file_system_errors_to_grpc(context):
    """
    Bloque 'with' para los handlers: si adentro se lanza un FileSystemError
    (a cualquier profundidad), termina el RPC con ese error.
    Solo atrapa FileSystemError: un bug (KeyError, etc.) sigue siendo un bug,
    llega al cliente como UNKNOWN y su traceback queda en los logs del superpeer.

        with translate_file_system_errors_to_grpc(context):
            self.registry.make_directory(...)   # puede lanzar FileSystemError
    """
    try:
        yield                                  # aquí corre el código de adentro del 'with'
    except FileSystemError as file_system_error:
        abort_rpc(context, file_system_error)


# ---------- Lado cliente (mount) ----------

def errno_from_rpc_error(rpc_error):
    """
    Devuelve el errno que corresponde a un grpc.RpcError:
      1. el exacto, si el superpeer lo envió en la metadata;
      2. si no, el deducido del código gRPC;
      3. si tampoco, EIO ("Input/output error").
    """
    metadata = dict(rpc_error.trailing_metadata() or ())
    if ERRNO_METADATA_KEY in metadata:
        return int(metadata[ERRNO_METADATA_KEY])
    return ERRNO_FOR_GRPC_CODE.get(rpc_error.code(), errno.EIO)
