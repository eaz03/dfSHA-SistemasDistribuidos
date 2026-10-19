"""
Contrato del registro de metadatos (la parte "ControlNode" del superpeer).

El superpeer SOLO usa esta interfaz. Cualquier clase que la implemente se puede usar
sin tocar el resto del código. Por ejemplo:
  - DictRegistry (dict_registry.py): diccionario plano en memoria (la actual)
  - un árbol de nodos en memoria
  - un registro persistente (log en disco, SQLite)
  - un registro remoto o centralizado, al que se le reenvían las llamadas por la red

Reglas que toda implementación debe cumplir:
  - Los paths llegan absolutos y normalizados ("/docs/a.txt"); el superpeer los limpia antes.
  - Cada usuario tiene su propio espacio de nombres, que empieza solo con la raíz "/".
  - Si una operación viola una regla POSIX, lanza FileSystemError con el errno que indica
    su documentación, y no modifica nada.
  - Cada método es atómico: dos llamadas simultáneas no se mezclan (la implementación se encarga).
  - Los métodos que modifican reciben 'now' (la hora del cambio) en vez de leer el reloj,
    para que repetir las mismas llamadas (desde un log o en otro superpeer) dé el mismo resultado.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class EntryInfo:
    """
    Lo que el registro informa sobre una entrada.
    Es una copia de solo lectura (frozen): modificarla no cambia el registro.
    """
    is_dir: bool
    size: int        # bytes; 0 en directorios
    mtime: float     # última modificación (segundos desde 1970)


@dataclass(frozen=True)
class BlockInfo:
    """
    Un bloque de un archivo: un pedazo de hasta block_size bytes, guardado en varios peers.
    También es una copia de solo lectura, como EntryInfo.
    """
    block_id: str     # único (uuid4): cada versión de un archivo usa bloques nuevos, nunca reutiliza IDs
    size: int         # bytes de este bloque; solo el último del archivo puede ser más chico
    peer_ids: tuple   # peer_id de los peers con una réplica, p. ej. ("peer1", "peer2").
                      # IDs y no direcciones: la dirección de un peer puede cambiar (reinicio, VPN),
                      # su ID no. El superpeer traduce ID -> dirección con su tabla de peers al responder.
                      # tuple y no list: frozen impide reasignar el campo, pero una lista se podría modificar.
                      # Puede tener menos de replication_factor IDs si había pocos peers (subreplicado).


class Registry(ABC):
    """
    Interfaz del registro de metadatos. Las implementaciones heredan de esta clase
    y deben definir todos los métodos marcados con @abstractmethod
    (Python no deja crear un objeto de una clase a la que le falte alguno).
    """

    # ---------- Consultas (no modifican) ----------

    @abstractmethod
    def stat(self, user, path):
        """
        Devuelve los metadatos de path como EntryInfo.
        Errores: ENOENT si path no existe.
        """

    @abstractmethod
    def list_directory(self, user, path):
        """
        Devuelve los nombres de los hijos directos de un directorio, ordenados.
        Errores: ENOENT si no existe; ENOTDIR si es un archivo.
        """

    # ---------- Modificaciones ----------

    @abstractmethod
    def make_directory(self, user, path, now):
        """
        Crea un directorio vacío en path.
        Errores: ENOENT si el padre no existe; ENOTDIR si el padre es un archivo;
                 EEXIST si el nombre ya está usado.
        """

    @abstractmethod
    def remove_directory(self, user, path, now):
        """
        Borra el directorio vacío path.
        Errores: EBUSY si es la raíz; ENOENT si no existe; ENOTDIR si es un archivo;
                 ENOTEMPTY si tiene contenido.
        """

    @abstractmethod
    def create_file(self, user, path, now):
        """
        Crea un archivo vacío (tamaño 0) en path.
        Errores: ENOENT si el padre no existe; ENOTDIR si el padre es un archivo;
                 EEXIST si el nombre ya está usado.
        """

    @abstractmethod
    def delete_file(self, user, path, now):
        """
        Borra el archivo path.
        Devuelve los BlockInfo que quedan obsoletos (los del archivo y los de sus planes
        pendientes): borrarlos de los peers le toca a quien llama.
        Errores: ENOENT si no existe; EISDIR si es un directorio.
        """

    @abstractmethod
    def rename(self, user, source_path, destination_path, now):
        """
        Mueve o renombra source_path a destination_path; si es un directorio, con todo su contenido.
        Si destination_path existe, lo reemplaza, siempre que sea del mismo tipo
        y, si es directorio, esté vacío. Si ambos paths son iguales, no hace nada.
        Devuelve los BlockInfo que quedan obsoletos: los del archivo reemplazado en el destino
        y los de los planes pendientes de los paths que dejan de existir (lista vacía si no hay).
        Errores: EBUSY si alguno es la raíz; ENOENT si source_path no existe o el padre
                 del destino no existe; ENOTDIR si el padre del destino es un archivo, o si se
                 quiere reemplazar un archivo con un directorio; EISDIR si se quiere reemplazar
                 un directorio con un archivo; ENOTEMPTY si el directorio destino tiene contenido;
                 EINVAL si se quiere mover un directorio dentro de sí mismo.
        """

    # ---------- Bloques ----------
    # Escribir un archivo es en dos fases: allocate_blocks entrega un plan (bloques nuevos y sus
    # peers), el cliente sube los bloques, y commit_file hace visible la versión nueva de una vez.
    # Si el cliente se cae a mitad de la subida, el archivo conserva su versión anterior completa.

    @abstractmethod
    def allocate_blocks(self, user, path, file_size):
        """
        Planifica dónde guardar una versión nueva de path, de file_size bytes: la parte en bloques
        y le asigna a cada uno replication_factor peers distintos. Si hay menos peers disponibles
        que replication_factor, usa los que haya (cada bloque queda en todos ellos, subreplicado);
        la re-replicación completará las copias cuando haya más peers.
        Devuelve una lista de BlockInfo, en orden (vacía si file_size es 0).
        No modifica el archivo, por eso no recibe 'now': no hay nada que repetir desde un log.
        Errores: ENOENT si path no existe; EISDIR si es un directorio; EINVAL si file_size < 0;
                 ENOSPC si no hay ningún peer disponible y file_size > 0.
        """

    @abstractmethod
    def commit_file(self, user, path, size, blocks, now):
        """
        Hace visible la versión nueva de path, de size bytes, formada por blocks (en orden),
        cuando el cliente ya subió todos esos bloques a sus peers.
        En cada bloque, peer_ids son los peers donde el cliente SÍ logró escribirlo: puede ser
        solo una parte de los asignados (si un peer se cayó durante la subida), pero al menos uno.
        peer_ids puede llegar como lista (así llega de protobuf): el registro lo guarda como tuple.
        Recibe los BlockInfo y 'now' para que se pueda repetir igual desde un log.
        Devuelve los BlockInfo de la versión anterior, ya obsoletos: el registro no habla con
        los peers, así que borrarlos le toca a quien llama.
        Errores: ENOENT si path no existe; EISDIR si es un directorio; EINVAL si size < 0,
                 si algún bloque no fue asignado por allocate_blocks a ese usuario y path o
                 cambió de tamaño, si un bloque se repite, si algún bloque no tiene peers, repite
                 un peer o tiene uno que no le fue asignado, si algún bloque que no es el último
                 no mide block_size, o si los tamaños de los bloques no suman size.
        """

    @abstractmethod
    def get_block_locations(self, user, path):
        """
        Devuelve los BlockInfo de la versión actual de path, en orden (vacía si mide 0 bytes):
        qué bloques lo forman y en qué peers (peer_id) está cada uno.
        Errores: ENOENT si path no existe; EISDIR si es un directorio.
        """

    @abstractmethod
    def expire_pending_blocks(self):
        """
        Descarta los planes de allocate_blocks que llevan demasiado tiempo sin commit (el cliente
        se cayó o canceló) y devuelve sus BlockInfo: el cliente pudo haber subido algunos, así que
        borrarlos de los peers le toca a quien llama. Un commit posterior de esos bloques da EINVAL.
        El registro no limpia por su cuenta: quien lo usa (el superpeer) decide cada cuánto llamarlo.
        """
