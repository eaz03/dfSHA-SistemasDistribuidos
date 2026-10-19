"""
Implementación de UserStore en un archivo JSON:

    {"sofi": {"password_hash": "scrypt$16384$8$1$...", "home": "sp2", "updated_at": 1760900000.5},
     "luis": {"deleted": true, "updated_at": 1760900100.0}}

Un diccionario por usuario, para poder agregar campos después (p. ej. el secreto del 2FA)
sin cambiar el formato.
  - home: el superpeer que atiende normalmente al usuario (si no está, el primero de la lista).
  - updated_at: cuándo cambió el registro. Con varios superpeers, cada uno tiene su archivo y se
    fusionan (merge_records): por cada usuario gana el registro más reciente.
  - Un usuario borrado no desaparece: queda marcado como borrado ("deleted"). Si se quitara del
    archivo, al fusionar con otro superpeer que todavía lo tiene volvería a aparecer.

El archivo se lee de nuevo en cada operación: así el superpeer ve al instante los usuarios que
se crean con manage_users.py desde otro proceso, sin reiniciarse. Son pocos usuarios, así que
leerlo cuesta muy poco comparado con el hash de la contraseña.
"""

import contextlib
import hashlib
import json
import os
import threading
import time
import uuid

from dfsha.superpeer.user_store import UserStore, hash_password, validate_password, validate_username


def _is_active(record):
    """True si record es un usuario existente (no un borrado)."""
    return record is not None and not record.get("deleted", False)


def _is_newer(candidate, current):
    """
    True si el registro candidate debe reemplazar a current al fusionar: es más reciente, o tiene la
    misma fecha y gana el desempate (comparar el texto da el mismo resultado en todos los superpeers).
    Los registros de antes de la federación no tienen updated_at: cuentan como del instante 0.
    """
    candidate_time = candidate.get("updated_at", 0)
    current_time = current.get("updated_at", 0)
    if candidate_time != current_time:
        return candidate_time > current_time
    candidate_text = json.dumps(candidate, sort_keys=True)
    current_text = json.dumps(current, sort_keys=True)
    return candidate_text > current_text


class JsonUserStore(UserStore):
    """Usuarios en un archivo JSON: {usuario: {"password_hash", "home", "updated_at"}}."""

    def __init__(self, file_path, clock=time.time):
        self._file_path = file_path
        self._clock = clock              # hora del calendario: se compara entre máquinas al fusionar
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

    def add_user(self, username, password, home=None):
        validate_username(username)
        validate_password(password)
        password_hash = hash_password(password)   # fuera del lock: es lento y no toca el archivo
        record = {"password_hash": password_hash, "updated_at": self._clock()}
        if home is not None:
            record["home"] = home
        with self._lock:
            users = self._load()
            if _is_active(users.get(username)):
                raise ValueError(f"user '{username}' already exists")
            users[username] = record          # también reemplaza un borrado: el usuario vuelve a existir
            self._save(users)

    def remove_user(self, username):
        with self._lock:
            users = self._load()
            if not _is_active(users.get(username)):
                raise ValueError(f"user '{username}' does not exist")
            users[username] = {"deleted": True, "updated_at": self._clock()}
            self._save(users)

    def list_users(self):
        with self._lock:
            users = self._load()
        active_usernames = []
        for username, record in users.items():
            if _is_active(record):
                active_usernames.append(username)
        return sorted(active_usernames)

    def _password_hash_of(self, username):
        with self._lock:
            record = self._load().get(username)
        if not _is_active(record):
            return None
        return record["password_hash"]

    # ---------- Federación ----------

    def home_of(self, username):
        with self._lock:
            record = self._load().get(username)
        if not _is_active(record):
            return None
        return record.get("home")

    def export_records(self):
        with self._lock:
            return self._load()

    def merge_records(self, records):
        with self._lock:
            users = self._load()
            changed_count = 0
            for username, their_record in records.items():
                my_record = users.get(username)
                if my_record is None or _is_newer(their_record, my_record):
                    users[username] = their_record
                    changed_count += 1
            if changed_count > 0:
                self._save(users)
            return changed_count

    def fingerprint(self):
        with self._lock:
            users = self._load()
        canonical_text = json.dumps(users, sort_keys=True)     # mismo contenido -> mismo texto -> misma huella
        return hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
