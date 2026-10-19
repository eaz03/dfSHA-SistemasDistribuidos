"""
Pruebas del registro replicado (federación): dos superpeers en el mismo proceso, conectados con una
federación falsa (sin red), para comprobar replicación, dueño único, épocas y ponerse al día.
"""

import errno
from types import SimpleNamespace

import pytest

from dfsha.common.errors import FileSystemError
from dfsha.superpeer.dict_registry import DictRegistry
from dfsha.superpeer.persistent_registry import PersistentRegistry
from dfsha.superpeer.replicated_registry import NotServingError, ReplicatedRegistry


class FakeFederation:
    """Lo mínimo que ReplicatedRegistry le pide a la federación, controlado por la prueba."""

    def __init__(self, my_id):
        self.my_id = my_id
        self.served_users = set()
        self.others = []              # registros replicados de los demás superpeers "vivos"

    def serves(self, user):
        return user in self.served_users

    def highest_known_epoch(self, user):
        return max([other.position(user).epoch for other in self.others], default=0)

    def send_record(self, record):
        replies = []
        for other in self.others:
            result, position = other.apply_replicated(dict(record))
            replies.append((other, SimpleNamespace(stale_epoch=(result == "stale"), applied=(result == "applied"))))
        return replies


def make_superpeer(tmp_path, superpeer_id):
    federation = FakeFederation(superpeer_id)
    persistent = PersistentRegistry(DictRegistry(block_size=4), str(tmp_path / superpeer_id))
    return federation, ReplicatedRegistry(persistent, federation)


@pytest.fixture
def pair(tmp_path):
    fed1, sp1 = make_superpeer(tmp_path, "sp1")
    fed2, sp2 = make_superpeer(tmp_path, "sp2")
    fed1.others, fed2.others = [sp2], [sp1]
    fed1.served_users.add("u")                          # sp1 es el dueño de u
    return SimpleNamespace(fed1=fed1, sp1=sp1, fed2=fed2, sp2=sp2)


def test_owner_writes_replicate_to_the_other_superpeer(pair):
    pair.sp1.make_directory("u", "/docs", 1.0)
    pair.sp1.create_file("u", "/docs/a", 2.0)
    assert pair.sp2.list_directory("u", "/docs") == ["a"]               # la copia ya lo tiene
    assert pair.sp2.position("u") == pair.sp1.position("u")
    assert pair.sp1.position("u").epoch_owner == "sp1"


def test_only_the_owner_can_modify(pair):
    with pytest.raises(NotServingError) as raised:
        pair.sp2.make_directory("u", "/x", 1.0)
    assert raised.value.error_number == errno.EAGAIN                    # el cliente se cambia al dueño
    with pytest.raises(FileSystemError):
        pair.sp2.allocate_blocks("u", "/x", 10)


def test_failed_operations_do_not_change_the_copies(pair):
    pair.sp1.make_directory("u", "/docs", 1.0)
    with pytest.raises(FileSystemError):
        pair.sp1.make_directory("u", "/docs", 2.0)                       # EEXIST
    assert pair.sp2.position("u") == pair.sp1.position("u")


def test_failover_opens_a_newer_epoch_and_the_old_owner_is_rejected(pair):
    pair.sp1.make_directory("u", "/antes", 1.0)
    first_epoch = pair.sp1.position("u").epoch

    # sp1 deja de verse: sp2 toma a u y abre una época nueva
    pair.fed2.served_users.add("u")
    pair.fed2.others = []
    pair.sp2.make_directory("u", "/durante", 2.0)
    assert pair.sp2.position("u").epoch > first_epoch
    assert pair.sp2.position("u").epoch_owner == "sp2"

    # sp1 cree que todavía es el dueño (no se enteró) y escribe: sp2 rechaza la época vieja
    with pytest.raises(NotServingError):
        pair.sp1.make_directory("u", "/tarde", 3.0)
    assert "tarde" not in pair.sp2.list_directory("u", "/")             # el dueño nuevo no lo aceptó


def test_a_superpeer_behind_catches_up_with_the_full_state(pair):
    pair.fed1.others = []                                               # sp2 caído: no recibe nada
    for number in range(3):
        pair.sp1.make_directory("u", f"/d{number}", float(number))
    assert pair.sp2.position("u") < pair.sp1.position("u")

    pair.fed1.others = [pair.sp2]                                       # vuelve, pero le faltan operaciones
    pair.sp1.make_directory("u", "/d3", 4.0)
    assert pair.sp2.position("u") < pair.sp1.position("u")             # "behind": no aplica sueltas

    assert pair.sp2.install_from("u", pair.sp1.export_user("u"))        # FetchUser: el estado completo
    assert pair.sp2.position("u") == pair.sp1.position("u")
    assert pair.sp2.list_directory("u", "/") == ["d0", "d1", "d2", "d3"]
    assert not pair.sp2.install_from("u", pair.sp1.export_user("u"))    # ya está al día: no hace nada


def test_repeated_records_are_ignored(pair):
    sent = []
    original = pair.fed1.send_record
    pair.fed1.send_record = lambda record: (sent.append(dict(record)), original(record))[1]
    pair.sp1.make_directory("u", "/x", 1.0)
    for record in sent:
        result, _ = pair.sp2.apply_replicated(dict(record))             # un reintento de la red
        assert result == "duplicate"
    assert pair.sp2.list_directory("u", "/") == ["x"]
