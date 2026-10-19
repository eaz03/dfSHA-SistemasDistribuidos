"""
Federación de superpeers: varios superpeers que se reparten los usuarios y se respaldan entre sí.

Idea: cada usuario tiene un solo superpeer dueño, el único que modifica su espacio de nombres.
Como los usuarios son disjuntos, dos superpeers nunca modifican los mismos datos y no hay que
coordinar escrituras. Los demás superpeers guardan una copia al día de cada usuario: el dueño
les envía cada operación (AppendLog) antes de responderle al cliente. Si el dueño cae, otro ya
tiene todo y toma a sus usuarios.

  - Membresía: lista fija de superpeers (--superpeers). Cada uno le envía un Heartbeat a los
    demás cada FEDERATION_HEARTBEAT_SECONDS; uno que no se oye en SUPERPEER_DEAD_AFTER_SECONDS
    está muerto. El heartbeat lleva si el superpeer está listo y hasta dónde va el log de cada usuario.
  - Dueño de un usuario: su "home" (users.json) si está vivo y listo; si no, el siguiente de la
    lista que lo esté. Todos calculan lo mismo con la misma información, sin votar.
  - Listo (ready): un superpeer que arranca no atiende usuarios hasta ponerse al día con los demás.
  - Ponerse al día: si otro superpeer va más adelante con un usuario (su posición es mayor),
    se le pide el estado completo de ese usuario (FetchUser) y se reemplaza la copia propia.
  - Épocas: cada vez que un superpeer empieza a atender a un usuario abre una época nueva (ver
    ReplicatedRegistry). Una operación de una época vieja (de un dueño anterior que no se enteró
    de que perdió al usuario) se rechaza, y ese superpeer se pone al día antes de volver a atender.

Con dos superpeers que siguen vivos pero dejan de verse (se cae la red entre ellos), los dos pueden
atender al mismo usuario por un rato. Al reconectarse gana la época más nueva y se pierden los
cambios del otro lado. Con tres superpeers y votación por mayoría eso no pasaría (ver docs/PLAN.md).
"""

import json
import sys
import threading
import time
from dataclasses import dataclass, field

import grpc

from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.common.tls import secure_channel
from dfsha.superpeer.persistent_registry import UserPosition

FEDERATION_HEARTBEAT_SECONDS = 1.0     # cada cuánto se avisan los superpeers que siguen vivos
SUPERPEER_DEAD_AFTER_SECONDS = 4.0     # sin noticias de un superpeer en este tiempo: muerto
STARTUP_GRACE_SECONDS = 5.0            # si al arrancar ningún otro responde en este tiempo, se arranca solo
FEDERATION_RPC_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class SuperpeerInfo:
    superpeer_id: str
    address: str                       # dónde lo encuentran los clientes y los demás superpeers, p. ej. "sp2:7000"


@dataclass
class _RemoteSuperpeer:
    """Lo último que se sabe de otro superpeer (interno)."""
    info: SuperpeerInfo
    stub: object                       # FederationStub
    last_heard: float = None           # según el reloj de este superpeer; None si nunca respondió
    ready: bool = False
    positions: dict = field(default_factory=dict)    # usuario -> UserPosition, según su último heartbeat
    users_fingerprint: str = ""
    announced_dead: bool = False       # para avisar en el log una sola vez que se murió


def parse_superpeers(text):
    """
    "sp1=sp1:7000,sp2=sp2:7000" -> [SuperpeerInfo("sp1", "sp1:7000"), SuperpeerInfo("sp2", "sp2:7000")].
    El orden importa: un usuario sin home es del primero, y si su dueño cae lo toma el siguiente.
    """
    superpeers = []
    for item in text.split(","):
        item = item.strip()
        if item == "":
            continue
        superpeer_id, separator, address = item.partition("=")
        if separator == "" or superpeer_id == "" or address == "":
            raise ValueError(f"expected id=host:port, got {item!r}")
        superpeers.append(SuperpeerInfo(superpeer_id=superpeer_id, address=address))
    return superpeers


def position_to_pb(user, position):
    return pb.UserPosition(user=user, epoch=position.epoch, epoch_owner=position.epoch_owner, seq=position.seq)


def position_from_pb(position_message):
    return UserPosition(epoch=position_message.epoch, epoch_owner=position_message.epoch_owner,
                        seq=position_message.seq)


class SuperpeerFederation:
    """La vista que este superpeer tiene de los demás, y el hilo que la mantiene al día."""

    def __init__(self, my_id, superpeers, tls_dir, user_store, clock=time.monotonic):
        """
        superpeers: lista de SuperpeerInfo de TODOS los superpeers, incluido este, en el orden de la configuración.
        user_store: de donde sale el home de cada usuario, y los usuarios que se fusionan con los demás.
        """
        self.my_id = my_id
        self._superpeers = list(superpeers)
        self._me = None
        for superpeer in self._superpeers:
            if superpeer.superpeer_id == my_id:
                self._me = superpeer
        if self._me is None:
            raise ValueError(f"this superpeer ({my_id}) is not in the superpeer list")
        self._user_store = user_store
        self._clock = clock
        self._lock = threading.Lock()                # protege self._remote: lo usan los handlers y el hilo
        self._remote = {}                            # superpeer_id -> _RemoteSuperpeer (todos menos este)
        for superpeer in self._superpeers:
            if superpeer.superpeer_id != my_id:
                stub = rpc.FederationStub(secure_channel(superpeer.address, tls_dir))   # mTLS con certificado de superpeer
                self._remote[superpeer.superpeer_id] = _RemoteSuperpeer(info=superpeer, stub=stub)
        self.registry = None                         # ReplicatedRegistry: se conecta con use_registry
        self.ready = len(self._remote) == 0          # solo: no hay con quién ponerse al día
        self._started_at = clock()
        self._stop_event = threading.Event()

    def use_registry(self, registry):
        """El registro replicado (se crea después de la federación, porque la necesita)."""
        self.registry = registry

    # ---------- Consultas ----------

    def is_alive(self, superpeer_id):
        if superpeer_id == self.my_id:
            return True
        with self._lock:
            remote = self._remote.get(superpeer_id)
            return self._remote_is_alive(remote)

    def _remote_is_alive(self, remote):
        """Quien llama debe tener self._lock."""
        if remote is None or remote.last_heard is None:
            return False
        return self._clock() - remote.last_heard <= SUPERPEER_DEAD_AFTER_SECONDS

    def _can_serve_users(self, superpeer_id):
        """True si superpeer_id puede ser dueño de usuarios ahora: está vivo y listo (ya se puso al día)."""
        if superpeer_id == self.my_id:
            return self.ready
        with self._lock:
            remote = self._remote.get(superpeer_id)
            return self._remote_is_alive(remote) and remote.ready

    def owner_of(self, user):
        """
        El SuperpeerInfo del dueño de user según lo que sabe este superpeer, o None si ninguno puede serlo.
        Se recorre la lista empezando por el home del usuario: el primero vivo y listo es el dueño.
        """
        home_id = self._user_store.home_of(user)
        start_index = 0
        for index, superpeer in enumerate(self._superpeers):
            if superpeer.superpeer_id == home_id:
                start_index = index
        for offset in range(len(self._superpeers)):
            candidate = self._superpeers[(start_index + offset) % len(self._superpeers)]
            if self._can_serve_users(candidate.superpeer_id):
                return candidate
        return None

    def is_behind(self, user):
        """True si algún superpeer vivo va más adelante que este con user: hay que ponerse al día antes de atenderlo."""
        my_position = self.registry.position(user)
        with self._lock:
            for remote in self._remote.values():
                if not self._remote_is_alive(remote):
                    continue
                their_position = remote.positions.get(user)
                if their_position is not None and their_position > my_position:
                    return True
        return False

    def serves(self, user):
        """True si este superpeer debe atender a user ahora: es su dueño y su copia está al día."""
        owner = self.owner_of(user)
        if owner is None or owner.superpeer_id != self.my_id:
            return False
        return not self.is_behind(user)

    def highest_known_epoch(self, user):
        """La época más alta de user que este superpeer haya visto en los demás (0 si ninguna)."""
        highest_epoch = 0
        with self._lock:
            for remote in self._remote.values():
                their_position = remote.positions.get(user)
                if their_position is not None and their_position.epoch > highest_epoch:
                    highest_epoch = their_position.epoch
        return highest_epoch

    def my_status(self):
        """pb.FederationStatus de este superpeer: lo que va en cada heartbeat."""
        position_messages = []
        for user, position in self.registry.positions().items():
            position_messages.append(position_to_pb(user, position))
        return pb.FederationStatus(superpeer_id=self.my_id, ready=self.ready, positions=position_messages,
                                   users_fingerprint=self._user_store.fingerprint())

    def view(self):
        """pb.SuperpeerView para ListSuperpeers: cada superpeer, si está vivo y listo, y qué usuarios atiende."""
        served_users = {}
        for superpeer in self._superpeers:
            served_users[superpeer.superpeer_id] = []
        for user in self._user_store.list_users():
            owner = self.owner_of(user)
            if owner is not None:
                served_users[owner.superpeer_id].append(user)

        statuses = []
        for superpeer in self._superpeers:
            status = pb.SuperpeerStatus(superpeer_id=superpeer.superpeer_id, address=superpeer.address,
                                        alive=self.is_alive(superpeer.superpeer_id),
                                        ready=self._can_serve_users(superpeer.superpeer_id),
                                        served_users=served_users[superpeer.superpeer_id])
            statuses.append(status)
        position_messages = []
        for user, position in self.registry.positions().items():
            position_messages.append(position_to_pb(user, position))
        return pb.SuperpeerView(superpeer_id=self.my_id, superpeers=statuses, positions=position_messages)

    # ---------- Noticias de los demás ----------

    def record_status(self, status):
        """Anota lo que otro superpeer contó de sí mismo (en su heartbeat, o en la respuesta al nuestro)."""
        with self._lock:
            remote = self._remote.get(status.superpeer_id)
            if remote is None:
                return                                # no está en la configuración: se ignora
            if not self._remote_is_alive(remote):
                print(f"federation: superpeer {status.superpeer_id} is alive", flush=True)
            remote.announced_dead = False
            remote.last_heard = self._clock()
            remote.ready = status.ready
            positions = {}
            for position_message in status.positions:
                positions[position_message.user] = position_from_pb(position_message)
            remote.positions = positions
            remote.users_fingerprint = status.users_fingerprint

    def record_position(self, superpeer_id, user, position):
        """Anota la posición de user en superpeer_id (llega en la respuesta de AppendLog, antes del próximo heartbeat)."""
        with self._lock:
            remote = self._remote.get(superpeer_id)
            if remote is not None:
                remote.positions[user] = position

    def send_record(self, record):
        """
        Envía una operación nueva (una línea del log) a cada superpeer vivo y espera sus respuestas.
        Devuelve [(superpeer_id, pb.AppendLogReply)] de los que respondieron. Uno que no responde
        a tiempo se omite: se pondrá al día después, con FetchUser.
        """
        log_record = pb.LogRecord(user=record["user"], record_json=json.dumps(record))
        with self._lock:
            alive_remotes = []
            for remote in self._remote.values():
                if self._remote_is_alive(remote):
                    alive_remotes.append(remote)
        replies = []
        for remote in alive_remotes:
            try:
                reply = remote.stub.AppendLog(log_record, timeout=FEDERATION_RPC_TIMEOUT_SECONDS)
            except grpc.RpcError:
                continue
            self.record_position(remote.info.superpeer_id, record["user"], position_from_pb(reply.position))
            replies.append((remote.info.superpeer_id, reply))
        return replies

    # ---------- Hilo: heartbeats, ponerse al día y fusión de usuarios ----------

    def start(self):
        thread = threading.Thread(target=self._run, name="federation", daemon=True)
        thread.start()

    def stop(self):
        self._stop_event.set()

    def wait_until_ready(self, timeout_seconds):
        """Espera hasta que este superpeer esté listo para atender usuarios. Devuelve self.ready."""
        deadline = self._clock() + timeout_seconds
        while not self.ready and self._clock() < deadline:
            time.sleep(0.1)
        return self.ready

    def _run(self):
        while True:
            try:
                self._send_heartbeats()
                self._catch_up()
                self._update_readiness()
            except Exception as error:
                # Un error inesperado no debe matar el hilo: el superpeer quedaría aislado sin avisar
                print(f"federation: unexpected error: {error!r}", file=sys.stderr, flush=True)
            if self._stop_event.wait(timeout=FEDERATION_HEARTBEAT_SECONDS):
                return

    def _send_heartbeats(self):
        my_status = self.my_status()
        with self._lock:
            remotes = list(self._remote.values())
        for remote in remotes:
            try:
                their_status = remote.stub.Heartbeat(my_status, timeout=FEDERATION_RPC_TIMEOUT_SECONDS)
            except grpc.RpcError:
                self._announce_if_dead(remote)
                continue
            self.record_status(their_status)

    def _announce_if_dead(self, remote):
        """Avisa en el log, una sola vez, que un superpeer que respondía dejó de hacerlo."""
        with self._lock:
            heard_before = remote.last_heard is not None
            if heard_before and not self._remote_is_alive(remote) and not remote.announced_dead:
                remote.announced_dead = True
                print(f"federation: superpeer {remote.info.superpeer_id} is dead", flush=True)

    def _catch_up(self):
        """
        Por cada superpeer vivo: copia los usuarios con los que va más adelante (FetchUser) y, si
        sus usuarios son distintos (otra huella), los fusiona con los propios (FetchUserRecords).
        """
        with self._lock:
            alive_remotes = []
            for remote in self._remote.values():
                if self._remote_is_alive(remote):
                    alive_remotes.append(remote)
            work = []
            for remote in alive_remotes:
                work.append((remote, dict(remote.positions), remote.users_fingerprint))

        for remote, their_positions, their_fingerprint in work:
            for user, their_position in their_positions.items():
                if their_position > self.registry.position(user):
                    self._copy_user_from(remote, user)
            if their_fingerprint != "" and their_fingerprint != self._user_store.fingerprint():
                self._merge_users_from(remote)

    def _copy_user_from(self, remote, user):
        try:
            user_state = remote.stub.FetchUser(pb.UserRequest(user=user), timeout=FEDERATION_RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as rpc_error:
            print(f"federation: cannot fetch user {user} from {remote.info.superpeer_id}: {rpc_error.code().name}",
                  file=sys.stderr, flush=True)
            return
        exported_user = json.loads(user_state.state_json)
        if self.registry.install_from(user, exported_user):
            position = UserPosition.from_dict(exported_user["position"])
            print(f"federation: copied user {user} from {remote.info.superpeer_id} "
                  f"(epoch {position.epoch} by {position.epoch_owner}, seq {position.seq})", flush=True)

    def _merge_users_from(self, remote):
        try:
            user_records = remote.stub.FetchUserRecords(pb.Empty(), timeout=FEDERATION_RPC_TIMEOUT_SECONDS)
        except grpc.RpcError:
            return
        changed_count = self._user_store.merge_records(json.loads(user_records.records_json))
        if changed_count > 0:
            print(f"federation: merged {changed_count} user records from {remote.info.superpeer_id}", flush=True)

    def _update_readiness(self):
        """
        Al arrancar, este superpeer queda listo cuando ya habló con todos los demás (o pasó
        STARTUP_GRACE_SECONDS sin que nadie responda: los demás están caídos) y no va atrasado
        con ningún usuario.
        """
        if self.ready:
            return
        with self._lock:
            heard_from_all = True
            for remote in self._remote.values():
                if remote.last_heard is None:
                    heard_from_all = False
            known_users = set()
            for remote in self._remote.values():
                for user in remote.positions:
                    known_users.add(user)
        grace_over = self._clock() - self._started_at > STARTUP_GRACE_SECONDS
        if not heard_from_all and not grace_over:
            return
        for user in known_users:
            if self.is_behind(user):
                return                                # todavía se está poniendo al día
        self.ready = True
        print(f"federation: superpeer {self.my_id} ready", flush=True)


class FederationServicer(rpc.FederationServicer):
    """Lo que este superpeer responde a los demás superpeers (servicio Federation del .proto)."""

    def __init__(self, federation, registry, user_store):
        self.federation = federation
        self.registry = registry            # ReplicatedRegistry
        self.user_store = user_store

    def Heartbeat(self, request, context):
        """RPC: otro superpeer cuenta cómo está; se le responde cómo está este."""
        self.federation.record_status(request)
        return self.federation.my_status()

    def AppendLog(self, request, context):
        """RPC: el dueño de un usuario envía una operación nueva para aplicarla en la copia de este superpeer."""
        record = json.loads(request.record_json)
        outcome, position = self.registry.apply_replicated(record)
        return pb.AppendLogReply(applied=outcome in ("applied", "duplicate"), stale_epoch=outcome == "stale",
                                 position=position_to_pb(record["user"], position))

    def FetchUser(self, request, context):
        """RPC: otro superpeer está atrasado con este usuario y pide su estado completo."""
        exported_user = self.registry.export_user(request.user)
        return pb.UserState(user=request.user, state_json=json.dumps(exported_user))

    def FetchUserRecords(self, request, context):
        """RPC: los usuarios de este superpeer (hashes, homes, borrados), para que el otro los fusione."""
        return pb.UserRecords(records_json=json.dumps(self.user_store.export_records()))
