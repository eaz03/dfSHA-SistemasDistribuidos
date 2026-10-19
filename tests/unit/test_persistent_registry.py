"""
Pruebas del registro persistente: reinicios, caídas a mitad de escritura, snapshot y concurrencia.
"""

import os
import threading
from dataclasses import replace

import pytest

from dfsha.common.errors import FileSystemError
from dfsha.superpeer.dict_registry import DictRegistry
from dfsha.superpeer.persistent_registry import (LOG_FILE_NAME, SNAPSHOT_FILE_NAME, CorruptLogError,
                                                 PersistentRegistry)


def open_registry(directory, snapshot_every=1000):
    inner = DictRegistry(live_peers=lambda: ["p1", "p2", "p3"], block_size=4)
    return PersistentRegistry(inner, str(directory), snapshot_every)


def log_lines(directory):
    with open(os.path.join(directory, LOG_FILE_NAME), encoding="utf-8") as log_file:
        return log_file.read().splitlines()


def test_everything_survives_a_restart(tmp_path):
    registry = open_registry(tmp_path)
    registry.make_directory("sofi", "/tesis", 1.0)
    registry.create_file("sofi", "/tesis/c1", 2.0)
    plan = registry.allocate_blocks("sofi", "/tesis/c1", 10)
    registry.commit_file("sofi", "/tesis/c1", 10, [replace(plan[0], peer_ids=["p1"]), plan[1], plan[2]], 3.0)
    registry.add_block_replica("sofi", "/tesis/c1", plan[0].block_id, "p3")
    registry.create_file("sofi", "/borrar", 4.0)
    registry.delete_file("sofi", "/borrar", 5.0)
    registry.make_directory("sofi", "/a", 6.0)
    registry.rename("sofi", "/a", "/b", 7.0)
    registry.create_file("esteban", "/suyo", 8.0)
    before = registry.export_state()

    restarted = open_registry(tmp_path)
    assert restarted.export_state() == before
    assert restarted.get_block_locations("sofi", "/tesis/c1")[0].peer_ids == ("p1", "p3")
    assert log_lines(tmp_path) == []                                   # al arrancar: snapshot y log vacío


def test_failed_operations_are_not_logged(tmp_path):
    registry = open_registry(tmp_path)
    registry.make_directory("u", "/x", 1.0)
    with pytest.raises(FileSystemError):
        registry.make_directory("u", "/x", 2.0)
    assert len(log_lines(tmp_path)) == 1


def test_torn_last_line_is_discarded(tmp_path):
    registry = open_registry(tmp_path)
    registry.make_directory("u", "/x", 1.0)
    registry.make_directory("u", "/y", 2.0)
    with open(os.path.join(tmp_path, LOG_FILE_NAME), "a", encoding="utf-8") as log_file:
        log_file.write('{"seq": 3, "op": "make_dir')                   # el proceso se cayó escribiendo
    assert open_registry(tmp_path).list_directory("u", "/") == ["x", "y"]


def test_damaged_line_in_the_middle_refuses_to_start(tmp_path):
    registry = open_registry(tmp_path)
    for path in ["/x", "/y", "/z"]:
        registry.make_directory("u", path, 1.0)
    lines = log_lines(tmp_path)
    lines[1] = "basura"
    with open(os.path.join(tmp_path, LOG_FILE_NAME), "w", encoding="utf-8") as log_file:
        log_file.write("\n".join(lines) + "\n")
    with pytest.raises(CorruptLogError):
        open_registry(tmp_path)


def test_crash_between_snapshot_and_log_truncation(tmp_path):
    registry = open_registry(tmp_path)
    registry.make_directory("u", "/x", 1.0)
    registry.make_directory("u", "/y", 2.0)
    with open(os.path.join(tmp_path, LOG_FILE_NAME), encoding="utf-8") as log_file:
        saved_log = log_file.read()
    registry._take_snapshot()                                          # la foto ya incluye /x y /y...
    with open(os.path.join(tmp_path, LOG_FILE_NAME), "w", encoding="utf-8") as log_file:
        log_file.write(saved_log)                                      # ...pero el log no alcanzó a vaciarse
    assert open_registry(tmp_path).list_directory("u", "/") == ["x", "y"]


def test_periodic_snapshot_keeps_the_log_short(tmp_path):
    registry = open_registry(tmp_path, snapshot_every=3)
    for number in range(7):
        registry.make_directory("u", f"/d{number}", float(number))
    assert len(log_lines(tmp_path)) == 1
    assert os.path.exists(os.path.join(tmp_path, SNAPSHOT_FILE_NAME))
    assert len(open_registry(tmp_path, snapshot_every=3).list_directory("u", "/")) == 7


def test_concurrent_writers_replay_to_the_same_state(tmp_path):
    registry = open_registry(tmp_path, snapshot_every=50)

    def worker(user):
        for number in range(40):
            registry.make_directory(user, f"/d{number}", 1.0)
            registry.create_file(user, f"/d{number}/f", 1.0)
            if number % 3 == 0:
                registry.rename(user, f"/d{number}", f"/m{number}", 1.0)

    threads = [threading.Thread(target=worker, args=(f"user{number}",)) for number in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert open_registry(tmp_path).export_state() == registry.export_state()
