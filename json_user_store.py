"""
Implementación de UserStore en un archivo JSON:

    {"sofi": {"password_hash": "scrypt$16384$8$1$..."}}

Un diccionario por usuario, para poder agregar campos después (p. ej. el secreto del 2FA)
sin cambiar el formato.

El archivo se lee de nuevo en cada operación: así el superpeer ve al instante los usuarios que
se crean con manage_users.py desde otro proceso, sin reiniciarse. Son pocos usuarios, así que
leerlo cuesta muy poco comparado con el hash de la contraseña.
"""

import contextlib
import json
import os
import threading
import uuid

from user_store import UserStore, hash_password, validate_password, validate_username


class JsonUserStore(UserStore):
    """Usuarios en un archivo JSON: {usuario: {"password_hash": ...}}."""

    def __init__(self, file_path):
        self._file_path = file_path
        self._lock = threading.Lock()    # el superpeer atiende varios logins a la vez, en hilos distintos

    # ---------- Helpers internos (quien llama debe tener self._lock) ----------

    def _load(self):
        """Devuelve el diccionario de usuarios; vacío si el archivo todavía no existe."""
        try:
            with open(self._file_path, encoding="utf-8") as users_file:
                return json.load(users_file)
        except FileNotFoundError:
            return {}

    def _save(self, users):
        """
        Escribe el archivo de forma atómica (temporal + fsync + replace), como los bloques:
        si se corta a la mitad, el archivo anterior queda intacto en vez de corrupto.
        """
        directory = os.path.dirname(os.path.abspath(self._file_path))
        os.makedirs(directory, exist_ok=True)
        temporary_path = f"{self._file_path}.tmp-{uuid.uuid4().hex}"
        try:
            with open(temporary_path, "w", encoding="utf-8") as temporary_file:
                json.dump(users, temporary_file, indent=2, sort_keys=True)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            # Solo el dueño puede leerlo: aunque no tenga contraseñas, los hashes no deben andar sueltos
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, self._file_path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temporary_path)

    # ---------- Operaciones ----------

    def add_user(self, username, password):
        validate_username(username)
        validate_password(password)
        password_hash = hash_password(password)   # fuera del lock: es lento y no toca el archivo
        with self._lock:
            users = self._load()
            if username in users:
                raise ValueError(f"user '{username}' already exists")
            users[username] = {"password_hash": password_hash}
            self._save(users)

    def remove_user(self, username):
        with self._lock:
            users = self._load()
            if username not in users:
                raise ValueError(f"user '{username}' does not exist")
            del users[username]
            self._save(users)

    def list_users(self):
        with self._lock:
            return sorted(self._load())

    def _password_hash_of(self, username):
        with self._lock:
            user = self._load().get(username)
        return None if user is None else user["password_hash"]
