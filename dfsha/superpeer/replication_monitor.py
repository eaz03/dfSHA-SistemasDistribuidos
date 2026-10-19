"""
Monitor de réplicas: devuelve a cada bloque las réplicas que perdió.

Un bloque queda con menos de replication_factor réplicas si un peer muere, o si se escribió cuando
había pocos peers vivos (el registro guarda el bloque igual, subreplicado). Si después se cae el
último peer que lo tiene, el bloque se pierde. Este monitor, en segundo plano:
  1. busca en el registro los bloques con menos réplicas de las acordadas;
  2. elige un peer destino: vivo, con espacio y que no tenga ya el bloque (el de más espacio libre);
  3. le ordena al destino copiar el bloque directamente desde un peer que sí lo tiene (ReplicateBlock):
     los bytes van de peer a peer, el superpeer nunca ve el contenido;
  4. anota la réplica nueva en el registro (queda en el log de la persistencia).

No reacciona al instante: un peer que se reinicia vuelve en segundos, y copiar todos sus bloques
sería un desperdicio. Un peer muerto cuenta como perdido solo después de delay_seconds sin heartbeat,
y el monitor no revisa nada durante los primeros delay_seconds después de arrancar el superpeer
(su tabla de peers empieza vacía hasta que los peers se vuelven a registrar: sin esta espera creería
que todas las réplicas desaparecieron).
"""

import sys
import threading
import time

from dfsha.common.errors import FileSystemError
from dfsha.superpeer.registry import BlockInfo

DEFAULT_DELAY_SECONDS = 60               # cuánto debe estar muerto un peer para copiar sus bloques
DEFAULT_INTERVAL_SECONDS = 10            # cada cuánto revisa
DEFAULT_MAX_COPIES_PER_PASS = 10         # para no saturar la red ni a los peers que atienden clientes


class ReplicationMonitor:
    """Busca bloques subreplicados y ordena copiarlos a otros peers."""

    def __init__(self, registry, peer_tracker, replicate_block, block_cleaner,
                 delay_seconds=DEFAULT_DELAY_SECONDS, interval_seconds=DEFAULT_INTERVAL_SECONDS,
                 max_copies_per_pass=DEFAULT_MAX_COPIES_PER_PASS, clock=time.monotonic):
        """
        replicate_block: función (dirección destino, block_id, dirección origen, tamaño) que le ordena al
            destino copiar el bloque desde el origen; lanza una excepción si no se pudo. Es inyectable:
            el superpeer pasa una que usa gRPC y un permiso de lectura firmado; las pruebas una falsa.
        block_cleaner: recibe las copias que resultaron innecesarias (el archivo cambió durante la copia).
        """
        self._registry = registry
        self._peer_tracker = peer_tracker
        self._replicate_block = replicate_block
        self._block_cleaner = block_cleaner
        self._delay_seconds = delay_seconds
        self._interval_seconds = interval_seconds
        self._max_copies_per_pass = max_copies_per_pass
        self._clock = clock
        self._started_at = clock()
        self._stop_event = threading.Event()
        self._thread = None

    def run_once(self):
        """Una pasada. Devuelve cuántas réplicas nuevas se hicieron (0 durante la espera de arranque)."""
        if self._clock() - self._started_at < self._delay_seconds:
            return 0                              # recién arrancado: la tabla de peers todavía no está completa

        peers = {peer.peer_id: peer for peer in self._peer_tracker.all_peers()}
        # Presentes: vivos, o muertos hace poco (pueden estar reiniciándose). Esos todavía cuentan como réplica
        give_up_after = self._peer_tracker.dead_after_seconds + self._delay_seconds
        present_peer_ids = {peer_id for peer_id, peer in peers.items()
                            if peer.alive or peer.seconds_since_heartbeat <= give_up_after}

        copies = 0
        for user, path, block in self._registry.under_replicated_blocks(present_peer_ids):
            if copies >= self._max_copies_per_pass:
                break                             # el resto en la siguiente pasada
            source = self._alive_holder(block, peers)
            target = self._choose_target(block, peers)
            if source is None or target is None:
                continue                          # ahora no hay de dónde copiar o a dónde: se reintenta después
            try:
                self._replicate_block(target.address, block.block_id, source.address, block.size)
            except Exception as error:
                print(f"replication: copying block {block.block_id} from {source.peer_id} to {target.peer_id} "
                      f"failed: {error!r}", file=sys.stderr, flush=True)
                continue
            copies += 1
            try:
                self._registry.add_block_replica(user, path, block.block_id, target.peer_id)
            except FileSystemError:
                # El archivo se sobrescribió o se borró mientras se copiaba: la copia sobra
                self._block_cleaner.schedule([BlockInfo(block_id=block.block_id, size=block.size,
                                                        peer_ids=(target.peer_id,))])
        return copies

    def _alive_holder(self, block, peers):
        """Un peer vivo que tiene el bloque (de donde copiar), o None."""
        for peer_id in block.peer_ids:
            peer = peers.get(peer_id)
            if peer is not None and peer.alive:
                return peer
        return None

    def _choose_target(self, block, peers):
        """El peer vivo con más espacio libre que no tiene el bloque y donde cabe, o None."""
        candidates = [peer for peer_id, peer in peers.items()
                      if peer.alive and peer_id not in block.peer_ids and peer.free_bytes >= block.size]
        if not candidates:
            return None
        return max(candidates, key=lambda peer: peer.free_bytes)

    # ---------- Hilo ----------

    def start(self):
        self._thread = threading.Thread(target=self._run, name="replication-monitor", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def _run(self):
        while not self._stop_event.wait(timeout=self._interval_seconds):
            try:
                copies = self.run_once()
                if copies:
                    print(f"replication: {copies} new replicas", flush=True)
            except Exception as error:
                # Un error inesperado no debe matar el hilo: el sistema quedaría sin re-replicación en silencio
                print(f"replication: unexpected error: {error!r}", file=sys.stderr, flush=True)
