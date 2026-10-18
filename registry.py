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
        Errores: ENOENT si no existe; EISDIR si es un directorio.
        """

    @abstractmethod
    def rename(self, user, source_path, destination_path, now):
        """
        Mueve o renombra source_path a destination_path; si es un directorio, con todo su contenido.
        Si destination_path existe, lo reemplaza, siempre que sea del mismo tipo
        y, si es directorio, esté vacío. Si ambos paths son iguales, no hace nada.
        Errores: EBUSY si alguno es la raíz; ENOENT si source_path no existe o el padre
                 del destino no existe; ENOTDIR si el padre del destino es un archivo, o si se
                 quiere reemplazar un archivo con un directorio; EISDIR si se quiere reemplazar
                 un directorio con un archivo; ENOTEMPTY si el directorio destino tiene contenido;
                 EINVAL si se quiere mover un directorio dentro de sí mismo.
        """
