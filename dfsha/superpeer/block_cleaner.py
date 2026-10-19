"""
Limpiador de bloques: borra de los peers los bloques que ya ningún archivo usa.

Los bloques quedan obsoletos cuando un archivo se sobrescribe (CommitFile), se borra (Delete) o se
reemplaza con mv (Rename): el registro los devuelve y el superpeer se los entrega a schedule().
También se borran los de los planes que nunca llegaron a commit (expire_pending_blocks), porque el
cliente pudo haber subido algunos.

Trabaja en segundo plano: el cliente no espera a que los peers confirmen el borrado.
Cada tarea es "este bloque en este peer": si un peer está caído, el bloque se borra igual en los
demás y en ese peer se reintenta cuando vuelva. Borrar es idempotente en el peer, así que reintentar
nunca hace daño.

La cola vive en memoria: si el superpeer se reinicia con borrados pendientes, esos bloques quedan
huérfanos en los peers. Los recogerá la limpieza completa de huérfanos (los peers reportan qué bloques
tienen y el superpeer borra los que nadie usa), que es la red de seguridad para todos estos casos.
"""

import sys
import threading
import time
from dataclasses import dataclass

DEFAULT_GRACE_SECONDS = 2 * 60           # quien estaba leyendo la versión vieja alcanza a terminar
DEFAULT_INTERVAL_SECONDS = 5             # cada cuánto revisa la cola
DEFAULT_GIVE_UP_SECONDS = 24 * 60 * 60   # un peer que no vuelve en un día: se deja para la limpieza de huérfanos


@dataclass(frozen=True)
class _Deletion:
    """Borrar block_id en peer_id. Interno: no sale del limpiador."""
    block_id: str
    peer_id: str
    scheduled_at: float        # cuándo quedó obsoleto (para rendirse después de give_up_seconds)
    delete_after: float        # no antes de esto (período de gracia)


class BlockCleaner:
    """Cola de borrados pendientes y el hilo que los envía a los peers."""

    def __init__(self, peer_tracker, delete_block, registry, grace_seconds=DEFAULT_GRACE_SECONDS,
                 interval_seconds=DEFAULT_INTERVAL_SECONDS, give_up_seconds=DEFAULT_GIVE_UP_SECONDS,
                 clock=time.monotonic):
        """
        peer_tracker: la tabla de peers, para saber la dirección de cada peer_id y si está vivo.
        delete_block: función (dirección, block_id) que borra un bloque en un peer; lanza una excepción
            si no pudo. Es inyectable: el superpeer pasa una que usa gRPC con su certificado (solo un
            superpeer puede borrar), y las pruebas una falsa que anota qué se borró.
        registry: para pedirle los planes vencidos (expire_pending_blocks).
        clock: la hora en segundos; monotonic porque solo mide cuánto pasó en esta máquina.
            Las pruebas pasan un reloj falso para no esperar.
        """
        self._peer_tracker = peer_tracker
        self._delete_block = delete_block
        self._registry = registry
        self._grace_seconds = grace_seconds
        self._interval_seconds = interval_seconds
        self._give_up_seconds = give_up_seconds
        self._clock = clock
        self._lock = threading.Lock()          # los handlers agregan tareas mientras el hilo las procesa
        self._pending = []                     # lista de _Deletion
        self._stop_event = threading.Event()
        self._thread = None

    def schedule(self, blocks):
        """
        Anota para borrar cada réplica de blocks (BlockInfo), después del período de gracia.
        Lo llaman los handlers con lo que devuelve el registro; vuelve enseguida.
        """
        now = self._clock()
        with self._lock:
            for block in blocks:
                for peer_id in block.peer_ids:
                    self._pending.append(_Deletion(block_id=block.block_id, peer_id=peer_id, scheduled_at=now,
                                                   delete_after=now + self._grace_seconds))

    def pending_count(self):
        with self._lock:
            return len(self._pending)

    def run_once(self):
        """
        Una pasada: anota los bloques de los planes vencidos y envía los borrados cuya gracia ya pasó.
        Devuelve (borrados, por reintentar, abandonados). El hilo la llama cada interval_seconds.
        """
        self.schedule(self._registry.expire_pending_blocks())

        # Se sacan de la cola las tareas que ya se pueden intentar; el resto se queda. Los envíos van
        # fuera del lock: son llamadas por red, y mientras tanto los handlers deben poder seguir anotando
        now = self._clock()
        with self._lock:
            due = [deletion for deletion in self._pending if deletion.delete_after <= now]
            self._pending = [deletion for deletion in self._pending if deletion.delete_after > now]

        deleted = 0
        retry_later = []
        given_up = 0
        for deletion in due:
            if self._try_delete(deletion):
                deleted += 1
            elif now - deletion.scheduled_at > self._give_up_seconds:
                given_up += 1
                print(f"cleaner: giving up on block {deletion.block_id} in {deletion.peer_id} "
                      f"(left as orphan)", file=sys.stderr, flush=True)
            else:
                retry_later.append(deletion)

        with self._lock:
            self._pending.extend(retry_later)
        return deleted, len(retry_later), given_up

    def _try_delete(self, deletion):
        """True si el bloque quedó borrado en ese peer; False si hay que reintentar (peer caído o desconocido)."""
        peer = self._peer_tracker.peer_snapshot(deletion.peer_id)
        if peer is None or not peer.alive:
            return False                       # no se le habla a un peer muerto: se espera a que vuelva
        try:
            self._delete_block(peer.address, deletion.block_id)
            return True
        except Exception:                      # cualquier fallo de red o del peer: se reintenta después
            return False

    # ---------- Hilo ----------

    def start(self):
        """Arranca el hilo en segundo plano (daemon: no impide que el proceso termine)."""
        self._thread = threading.Thread(target=self._run, name="block-cleaner", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _run(self):
        # wait(timeout) espera un intervalo, o termina antes si llega stop()
        while not self._stop_event.wait(timeout=self._interval_seconds):
            try:
                deleted, retry_later, given_up = self.run_once()
                if deleted or given_up:
                    print(f"cleaner: deleted {deleted} replicas, {retry_later} to retry, {given_up} given up",
                          flush=True)
            except Exception as error:
                # Un error inesperado no debe matar el hilo: el superpeer quedaría sin limpieza en silencio
                print(f"cleaner: unexpected error: {error!r}", file=sys.stderr, flush=True)
