"""
Tabla de peers del superpeer: quién está registrado, dónde se lo encuentra,
cuánto espacio tiene y si sigue vivo.

Es estado "blando": vive solo en memoria, no se guarda ni se sincroniza con otros superpeers.
Si el superpeer se reinicia, la tabla empieza vacía y cada peer se vuelve a registrar solo:
su siguiente heartbeat recibe known=False.

No sabe nada de gRPC: los manejadores del superpeer la usan (subtarea 3).
"""

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class PeerSnapshot:
    """
    Copia de solo lectura del estado de un peer en un momento dado.
    Es lo que la tabla entrega hacia afuera: modificarla no cambia la tabla.
    """
    peer_id: str
    address: str                     # dónde lo encuentran el superpeer y los clientes, p. ej. "peer1:7100"
    capacity_bytes: int              # espacio total que ofrece
    free_bytes: int                  # espacio libre según su último heartbeat
    seconds_since_heartbeat: float
    alive: bool


@dataclass
class _PeerRecord:
    """Lo que la tabla guarda de cada peer (interno: afuera solo salen PeerSnapshot)."""
    address: str
    capacity_bytes: int
    free_bytes: int
    last_heartbeat: float            # momento del último registro o heartbeat, según self._clock


class PeerTracker:
    """
    Tabla de peers, segura para usar desde varios hilos (gRPC atiende cada llamada en un hilo).

    Un peer está vivo si su último heartbeat fue hace menos de
    heartbeat_interval_seconds * missed_heartbeats_before_dead segundos.
    Un peer muerto NO se borra de la tabla: sigue apareciendo como alive=False
    (sirve para saber qué bloques hay que re-replicar) y revive si vuelve a enviar heartbeats.
    """

    def __init__(self, heartbeat_interval_seconds=3.0, missed_heartbeats_before_dead=3, clock=time.monotonic):
        """
        heartbeat_interval_seconds: cada cuánto deben enviar heartbeat los peers.
            El superpeer se lo comunica a cada peer al registrarlo, así el valor vive en un solo lugar.
        missed_heartbeats_before_dead: cuántos heartbeats seguidos puede perder un peer antes de darlo por muerto.
        clock: función que devuelve la hora actual en segundos. Por defecto time.monotonic,
            que no salta si alguien cambia la hora del sistema. Las pruebas pasan un reloj falso
            para simular el paso del tiempo sin esperar.
        """
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.dead_after_seconds = heartbeat_interval_seconds * missed_heartbeats_before_dead
        self._clock = clock
        self._lock = threading.Lock()
        self._peers = {}                 # peer_id -> _PeerRecord

    # ---------- Cambios (los llaman los manejadores RegisterPeer y Heartbeat) ----------

    def register(self, peer_id, address, capacity_bytes):
        """
        Agrega un peer, o reemplaza su información si ya estaba (p. ej. se reinició con otra dirección).
        Mientras no llegue su primer heartbeat, se asume que tiene todo su espacio libre.
        """
        with self._lock:
            self._peers[peer_id] = _PeerRecord(
                address=address,
                capacity_bytes=capacity_bytes,
                free_bytes=capacity_bytes,
                last_heartbeat=self._clock(),
            )

    def heartbeat(self, peer_id, free_bytes):
        """
        Anota que el peer sigue vivo y cuánto espacio libre tiene.
        Devuelve True si el peer está en la tabla, False si no (entonces debe registrarse de nuevo).
        """
        with self._lock:
            record = self._peers.get(peer_id)
            if record is None:
                return False
            record.free_bytes = free_bytes
            record.last_heartbeat = self._clock()
            return True

    # ---------- Consultas ----------

    def all_peers(self):
        """Todos los peers conocidos, vivos o muertos, como PeerSnapshot ordenados por peer_id (para ListPeers)."""
        with self._lock:
            now = self._clock()
            snapshots = []
            for peer_id in sorted(self._peers):
                record = self._peers[peer_id]
                snapshot = self._snapshot(peer_id, record, now)
                snapshots.append(snapshot)
            return snapshots

    def alive_peers(self):
        """Solo los peers vivos. Lo usará la elección de peers para bloques nuevos (pasos 3 y 4)."""
        living_peers = []
        for peer in self.all_peers():
            if peer.alive:
                living_peers.append(peer)
        return living_peers

    # ---------- Interno ----------

    def _snapshot(self, peer_id, record, now):
        seconds_since_heartbeat = now - record.last_heartbeat
        return PeerSnapshot(
            peer_id=peer_id,
            address=record.address,
            capacity_bytes=record.capacity_bytes,
            free_bytes=record.free_bytes,
            seconds_since_heartbeat=seconds_since_heartbeat,
            alive=seconds_since_heartbeat <= self.dead_after_seconds,
        )
