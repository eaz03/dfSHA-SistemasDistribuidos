"""
Pruebas del registro de metadatos (DictRegistry): espacio de nombres, plan y commit de bloques,
bloques obsoletos, planes pendientes y re-replicación. Sin red ni disco.
"""

import errno
from dataclasses import replace

import pytest

from dfsha.common.errors import FileSystemError
from dfsha.superpeer.dict_registry import DictRegistry


def expect_errno(error_number, function, *arguments):
    with pytest.raises(FileSystemError) as raised:
        function(*arguments)
    assert raised.value.error_number == error_number


@pytest.fixture
def clock():
    return [1000.0]


@pytest.fixture
def registry(clock):
    # Bloques de 4 bytes para que todo sea chico; tres peers y k = 2
    return DictRegistry(live_peers=lambda: ["p1", "p2", "p3"], block_size=4, replication_factor=2,
                        pending_plan_ttl_seconds=60, clock=lambda: clock[0])


def upload(registry, user, path, size, now=2.0):
    """Plan + commit con todas las réplicas asignadas (como un cliente sin fallas)."""
    plan = registry.allocate_blocks(user, path, size)
    registry.commit_file(user, path, size, plan, now)
    return plan


# ---------- Espacio de nombres ----------

def test_namespace_operations_follow_posix(registry):
    registry.make_directory("u", "/docs", 1.0)
    registry.create_file("u", "/docs/a.txt", 1.0)
    assert registry.list_directory("u", "/") == ["docs"]
    assert registry.stat("u", "/docs/a.txt").size == 0
    expect_errno(errno.EEXIST, registry.make_directory, "u", "/docs", 1.0)
    expect_errno(errno.ENOENT, registry.create_file, "u", "/nada/x", 1.0)
    expect_errno(errno.ENOTEMPTY, registry.remove_directory, "u", "/docs", 1.0)
    expect_errno(errno.EISDIR, registry.delete_file, "u", "/docs", 1.0)
    expect_errno(errno.EBUSY, registry.remove_directory, "u", "/", 1.0)
    registry.rename("u", "/docs", "/archivo", 2.0)                    # mueve el subárbol
    assert registry.list_directory("u", "/archivo") == ["a.txt"]
    expect_errno(errno.EINVAL, registry.rename, "u", "/archivo", "/archivo/dentro", 3.0)


def test_each_user_has_its_own_namespace(registry):
    registry.create_file("sofi", "/secreto", 1.0)
    assert registry.list_directory("esteban", "/") == []
    expect_errno(errno.ENOENT, registry.stat, "esteban", "/secreto")


# ---------- Plan y commit ----------

def test_allocate_splits_into_blocks_with_distinct_replicas(registry):
    registry.create_file("u", "/f", 1.0)
    plan = registry.allocate_blocks("u", "/f", 10)
    assert [block.size for block in plan] == [4, 4, 2]                # el último se queda con lo que sobra
    for block in plan:
        assert len(block.peer_ids) == 2 and len(set(block.peer_ids)) == 2
    assert registry.stat("u", "/f").size == 0                          # el plan no cambia el archivo


def test_round_robin_spreads_replicas(registry):
    registry.create_file("u", "/f", 1.0)
    plan = registry.allocate_blocks("u", "/f", 12)
    assert [block.peer_ids for block in plan] == [("p1", "p2"), ("p3", "p1"), ("p2", "p3")]


def test_allocate_with_fewer_peers_than_k_and_without_peers():
    one_peer = DictRegistry(live_peers=lambda: ["p1"], block_size=4)
    one_peer.create_file("u", "/f", 1.0)
    assert [block.peer_ids for block in one_peer.allocate_blocks("u", "/f", 8)] == [("p1",), ("p1",)]
    no_peers = DictRegistry(live_peers=lambda: [], block_size=4)
    no_peers.create_file("u", "/f", 1.0)
    expect_errno(errno.ENOSPC, no_peers.allocate_blocks, "u", "/f", 8)
    assert no_peers.allocate_blocks("u", "/f", 0) == []               # un archivo vacío no necesita peers


def test_commit_makes_new_version_visible_and_returns_obsolete_blocks(registry):
    registry.create_file("u", "/f", 1.0)
    first = upload(registry, "u", "/f", 10)
    assert registry.get_block_locations("u", "/f") == first
    assert registry.stat("u", "/f").size == 10
    second = registry.allocate_blocks("u", "/f", 4)
    assert registry.commit_file("u", "/f", 4, second, 3.0) == first   # la versión anterior queda obsoleta


def test_commit_accepts_a_subset_of_replicas_as_list(registry):
    registry.create_file("u", "/f", 1.0)
    plan = registry.allocate_blocks("u", "/f", 4)
    written = [replace(plan[0], peer_ids=[plan[0].peer_ids[0]])]       # una sola réplica, como lista (protobuf)
    registry.commit_file("u", "/f", 4, written, 2.0)
    assert registry.get_block_locations("u", "/f")[0].peer_ids == (plan[0].peer_ids[0],)


def test_commit_rejects_anything_outside_the_plan(registry):
    registry.create_file("u", "/f", 1.0)
    registry.create_file("u", "/g", 1.0)
    plan = registry.allocate_blocks("u", "/f", 10)
    other = registry.allocate_blocks("u", "/g", 4)
    invalid_commits = [
        (10, [other[0], plan[1], plan[2]]),                            # bloque de otro archivo
        (10, [plan[0], plan[0], plan[2]]),                             # bloque repetido
        (10, [plan[2], plan[1], plan[0]]),                             # el bloque chico no va al final
        (11, plan),                                                    # no suman size
        (-1, plan),                                                    # size negativo
        (10, [replace(plan[0], peer_ids=("intruso",)), plan[1], plan[2]]),   # peer no asignado
        (10, [replace(plan[0], peer_ids=()), plan[1], plan[2]]),       # sin réplicas
    ]
    for size, blocks in invalid_commits:
        expect_errno(errno.EINVAL, registry.commit_file, "u", "/f", size, blocks, 2.0)
    expect_errno(errno.ENOENT, registry.commit_file, "otro", "/f", 10, plan, 2.0)   # para otro usuario ni existe
    assert registry.stat("u", "/f").size == 0                          # ningún intento cambió el archivo
    # Limitación conocida: dos bloques completos intercambiados entre sí sí se aceptan (los tamaños
    # encajan); el registro no exige el orden del plan, así que un cliente con errores desordenaría su archivo
    registry.commit_file("u", "/f", 10, plan, 2.0)                     # el plan sigue sirviendo
    expect_errno(errno.EINVAL, registry.commit_file, "u", "/f", 10, plan, 3.0)      # pero solo una vez


# ---------- Bloques obsoletos y planes pendientes ----------

def test_delete_and_replacing_rename_return_obsolete_blocks(registry):
    registry.create_file("u", "/a", 1.0)
    registry.create_file("u", "/b", 1.0)
    blocks_a = upload(registry, "u", "/a", 4)
    blocks_b = upload(registry, "u", "/b", 4)
    assert registry.rename("u", "/a", "/b", 3.0) == blocks_b          # /b se reemplaza
    assert registry.get_block_locations("u", "/b") == blocks_a        # los bloques viajan con el archivo
    assert registry.delete_file("u", "/b", 4.0) == blocks_a


def test_late_commit_cannot_land_on_a_new_file_with_the_same_name(registry):
    """Problema ABA: un plan viejo no puede caer sobre un archivo nuevo con el mismo nombre."""
    registry.create_file("u", "/a", 1.0)
    old_plan = registry.allocate_blocks("u", "/a", 4)
    assert registry.delete_file("u", "/a", 2.0) == old_plan           # el plan se descarta con el archivo
    registry.create_file("u", "/a", 3.0)
    expect_errno(errno.EINVAL, registry.commit_file, "u", "/a", 4, old_plan, 4.0)


def test_abandoned_plans_expire(registry, clock):
    registry.create_file("u", "/viejo", 1.0)
    registry.create_file("u", "/joven", 1.0)
    old = registry.allocate_blocks("u", "/viejo", 4)
    clock[0] += 50
    young = registry.allocate_blocks("u", "/joven", 4)
    clock[0] += 11                                                     # el viejo tiene 61 s, el joven 11 s
    assert registry.expire_pending_blocks() == old
    assert registry.expire_pending_blocks() == []
    expect_errno(errno.EINVAL, registry.commit_file, "u", "/viejo", 4, old, 5.0)
    registry.commit_file("u", "/joven", 4, young, 5.0)


# ---------- Re-replicación ----------

def test_under_replicated_blocks_and_add_replica(registry):
    registry.create_file("u", "/f", 1.0)
    plan = registry.allocate_blocks("u", "/f", 8)
    registry.commit_file("u", "/f", 8, [plan[0], replace(plan[1], peer_ids=[plan[1].peer_ids[0]])], 2.0)
    under = registry.under_replicated_blocks({"p1", "p2", "p3"})
    assert [block.block_id for _, _, block in under] == [plan[1].block_id]
    registry.add_block_replica("u", "/f", plan[1].block_id, "p9")
    registry.add_block_replica("u", "/f", plan[1].block_id, "p9")     # repetido: no hace nada
    assert registry.get_block_locations("u", "/f")[1].peer_ids == (plan[1].peer_ids[0], "p9")
    assert registry.under_replicated_blocks({"p1", "p2", "p3", "p9"}) == []
    expect_errno(errno.ENOENT, registry.add_block_replica, "u", "/f", "f" * 32, "p9")    # bloque que ya no es del archivo
    expect_errno(errno.ENOENT, registry.add_block_replica, "u", "/nada", plan[0].block_id, "p9")


def test_blocks_without_any_present_replica_are_not_listed(registry):
    """Sin ninguna réplica presente no hay de dónde copiar."""
    registry.create_file("u", "/f", 1.0)
    upload(registry, "u", "/f", 4)
    assert registry.under_replicated_blocks(set()) == []
