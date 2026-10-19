"""
Pruebas de las tareas en segundo plano del superpeer: el limpiador de bloques y el monitor de
réplicas. Usan una tabla de peers, un reloj y unas funciones de red falsas: no hay red ni esperas.
"""

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dfsha.superpeer.block_cleaner import BlockCleaner
from dfsha.superpeer.dict_registry import DictRegistry
from dfsha.superpeer.registry import BlockInfo
from dfsha.superpeer.replication_monitor import ReplicationMonitor


def fake_peer(peer_id, alive=True, seconds_since_heartbeat=1.0, free_bytes=100):
    return SimpleNamespace(peer_id=peer_id, address=f"{peer_id}:7100", alive=alive,
                           seconds_since_heartbeat=seconds_since_heartbeat, free_bytes=free_bytes)


class FakeTracker:
    def __init__(self, peers):
        self.peers = {peer.peer_id: peer for peer in peers}
        self.dead_after_seconds = 9

    def peer_snapshot(self, peer_id):
        return self.peers.get(peer_id)

    def all_peers(self):
        return list(self.peers.values())


# ---------- Limpiador ----------

@pytest.fixture
def cleaner_setup():
    clock = [0.0]
    tracker = FakeTracker([fake_peer("p1"), fake_peer("p2"), fake_peer("broken")])
    tracker.peers["broken"].address = "broken:7100"
    deleted = []

    def delete_block(address, block_id):
        if address == "broken:7100":
            raise RuntimeError("peer roto")
        deleted.append((address, block_id))

    expired = []
    registry = SimpleNamespace(expire_pending_blocks=lambda: [expired.pop()] if expired else [])
    cleaner = BlockCleaner(tracker, delete_block, registry, grace_seconds=120, give_up_seconds=3600,
                           clock=lambda: clock[0])
    return SimpleNamespace(clock=clock, tracker=tracker, deleted=deleted, expired=expired, cleaner=cleaner)


def test_cleaner_waits_for_the_grace_period_and_deletes_every_replica(cleaner_setup):
    setup = cleaner_setup
    setup.cleaner.schedule([BlockInfo("1" * 32, 10, ("p1", "p2"))])
    assert setup.cleaner.run_once() == (0, 0, 0) and setup.deleted == []
    setup.clock[0] = 121
    assert setup.cleaner.run_once() == (2, 0, 0)
    assert sorted(setup.deleted) == [("p1:7100", "1" * 32), ("p2:7100", "1" * 32)]


def test_cleaner_retries_a_dead_peer_when_it_comes_back(cleaner_setup):
    setup = cleaner_setup
    setup.tracker.peers["p2"].alive = False
    setup.cleaner.schedule([BlockInfo("1" * 32, 10, ("p1", "p2"))])
    setup.clock[0] = 121
    assert setup.cleaner.run_once() == (1, 1, 0)
    setup.tracker.peers["p2"].alive = True
    setup.clock[0] += 5
    assert setup.cleaner.run_once() == (1, 0, 0)


def test_cleaner_gives_up_after_the_limit(cleaner_setup):
    setup = cleaner_setup
    setup.cleaner.schedule([BlockInfo("3" * 32, 10, ("broken", "unknown"))])
    setup.clock[0] = 121
    assert setup.cleaner.run_once() == (0, 2, 0)
    setup.clock[0] += 3600
    assert setup.cleaner.run_once() == (0, 0, 2) and setup.cleaner.pending_count() == 0


def test_cleaner_deletes_expired_plans(cleaner_setup):
    setup = cleaner_setup
    setup.expired.append(BlockInfo("4" * 32, 10, ("p1",)))
    setup.cleaner.run_once()
    setup.clock[0] = 121
    assert setup.cleaner.run_once() == (1, 0, 0)


def test_cleaner_thread_runs_in_the_background(cleaner_setup):
    setup = cleaner_setup
    cleaner = BlockCleaner(setup.tracker, lambda address, block_id: setup.deleted.append(block_id),
                           SimpleNamespace(expire_pending_blocks=lambda: []), grace_seconds=0, interval_seconds=0.05)
    cleaner.start()
    cleaner.schedule([BlockInfo("5" * 32, 10, ("p1",))])
    time.sleep(0.3)
    cleaner.stop()
    assert "5" * 32 in setup.deleted


# ---------- Monitor de réplicas ----------

@pytest.fixture
def monitor_setup():
    clock = [0.0]
    tracker = FakeTracker([fake_peer("p1"), fake_peer("p2"), fake_peer("p3", free_bytes=50),
                           fake_peer("p4", free_bytes=90)])
    copies = []
    cleaned = []
    registry = DictRegistry(live_peers=lambda: ["p1", "p2"], block_size=4)
    registry.create_file("u", "/a", 1.0)
    plan = registry.allocate_blocks("u", "/a", 4)
    registry.commit_file("u", "/a", 4, plan, 2.0)                      # bloque en (p1, p2)
    monitor = ReplicationMonitor(registry, tracker,
                                 lambda target, block_id, source, size: copies.append((target, source)),
                                 SimpleNamespace(schedule=cleaned.extend), delay_seconds=60,
                                 clock=lambda: clock[0])
    return SimpleNamespace(clock=clock, tracker=tracker, copies=copies, cleaned=cleaned,
                           registry=registry, plan=plan, monitor=monitor)


def test_monitor_waits_after_start_and_for_recently_dead_peers(monitor_setup):
    setup = monitor_setup
    setup.tracker.peers["p2"] = fake_peer("p2", alive=False, seconds_since_heartbeat=80)
    assert setup.monitor.run_once() == 0                               # gracia de arranque
    setup.clock[0] = 61
    setup.tracker.peers["p2"] = fake_peer("p2", alive=False, seconds_since_heartbeat=20)
    assert setup.monitor.run_once() == 0                               # muerto hace poco: puede volver
    assert setup.copies == []


def test_monitor_copies_from_a_live_holder_to_the_peer_with_most_space(monitor_setup):
    setup = monitor_setup
    setup.clock[0] = 61
    setup.tracker.peers["p2"] = fake_peer("p2", alive=False, seconds_since_heartbeat=80)
    assert setup.monitor.run_once() == 1
    assert setup.copies == [("p4:7100", "p1:7100")]
    assert setup.registry.get_block_locations("u", "/a")[0].peer_ids == ("p1", "p2", "p4")
    assert setup.monitor.run_once() == 0                               # ya tiene 2 réplicas presentes


def test_monitor_sends_unneeded_copies_to_the_cleaner(monitor_setup):
    setup = monitor_setup
    setup.clock[0] = 61
    setup.registry.delete_file("u", "/a", 3.0)                         # el archivo cambió durante la copia
    orphan = replace(setup.plan[0], peer_ids=("p1",))
    setup.registry.under_replicated_blocks = lambda present: [("u", "/a", orphan)]
    assert setup.monitor.run_once() == 1
    assert [block.peer_ids for block in setup.cleaned] == [("p2",)]   # el destino con más espacio sin el bloque


def test_monitor_limits_copies_per_pass():
    registry = DictRegistry(live_peers=lambda: ["p1"], block_size=4)
    registry.create_file("u", "/f", 1.0)
    plan = registry.allocate_blocks("u", "/f", 60)                     # 15 bloques con una réplica
    registry.commit_file("u", "/f", 60, plan, 1.0)
    tracker = FakeTracker([fake_peer("p1", free_bytes=10**6), fake_peer("p2", free_bytes=10**6)])
    monitor = ReplicationMonitor(registry, tracker, lambda *arguments: None, SimpleNamespace(schedule=print),
                                 delay_seconds=0, max_copies_per_pass=10, clock=lambda: 100.0)
    assert [monitor.run_once(), monitor.run_once(), monitor.run_once()] == [10, 5, 0]
