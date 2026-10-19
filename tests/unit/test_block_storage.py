"""
Pruebas del almacenamiento de bloques del peer: en disco (escritura atómica, IDs validados) y
cifrado en reposo (AES-256-GCM).
"""

import errno
import os
import shutil
import uuid

import pytest

from dfsha.common.errors import FileSystemError
from dfsha.common.key_files import load_or_create_secret_key
from dfsha.peer.block_storage import BlockStorage
from dfsha.peer.disk_block_storage import DiskBlockStorage
from dfsha.peer.encrypted_block_storage import EncryptedBlockStorage

INVALID_BLOCK_IDS = ["../../etc/passwd", "A" * 32, "a" * 31, "a" * 33, "", None, b"a" * 32, "a" * 32 + ".tmp-x"]


def new_id():
    return uuid.uuid4().hex


def expect_errno(error_number, function, *arguments):
    with pytest.raises(FileSystemError) as raised:
        function(*arguments)
    assert raised.value.error_number == error_number


@pytest.fixture
def disk(tmp_path):
    return DiskBlockStorage(str(tmp_path / "blocks"))


@pytest.fixture
def encrypted(tmp_path, disk):
    return EncryptedBlockStorage(disk, load_or_create_secret_key(str(tmp_path / "keys" / "blocks.key")))


def test_interface_cannot_be_instantiated():
    with pytest.raises(TypeError):
        BlockStorage()


def test_disk_write_read_retry_and_idempotent_delete(disk, tmp_path):
    block_id = new_id()
    disk.write_block(block_id, b"hola")
    disk.write_block(block_id, b"hola")                                # reintento: reemplaza
    assert disk.read_block(block_id) == b"hola"
    assert os.listdir(tmp_path / "blocks") == [block_id]               # sin temporales
    disk.delete_block(block_id)
    disk.delete_block(block_id)                                        # borrar dos veces no es error
    expect_errno(errno.ENOENT, disk.read_block, block_id)


@pytest.mark.parametrize("bad_id", INVALID_BLOCK_IDS)
def test_disk_rejects_invalid_block_ids(disk, bad_id):
    expect_errno(errno.EINVAL, disk.read_block, bad_id)
    expect_errno(errno.EINVAL, disk.write_block, bad_id, b"x")
    expect_errno(errno.EINVAL, disk.delete_block, bad_id)


def test_disk_failed_write_leaves_no_temporary(disk, tmp_path):
    with pytest.raises(TypeError):
        disk.write_block(new_id(), "no son bytes")
    assert os.listdir(tmp_path / "blocks") == []


def test_disk_cleans_leftover_temporaries_on_start(disk, tmp_path):
    kept = new_id()
    disk.write_block(kept, b"dato")
    (tmp_path / "blocks" / f"{kept}.tmp-{new_id()}").write_bytes(b"a medias")
    restarted = DiskBlockStorage(str(tmp_path / "blocks"))
    assert os.listdir(tmp_path / "blocks") == [kept]
    assert restarted.read_block(kept) == b"dato"


def test_encrypted_round_trip_and_nothing_readable_on_disk(encrypted, tmp_path):
    block_id = new_id()
    secret = b"contenido secreto " * 100
    encrypted.write_block(block_id, secret)
    assert encrypted.read_block(block_id) == secret
    on_disk = (tmp_path / "blocks" / block_id).read_bytes()
    assert b"secreto" not in on_disk
    assert len(on_disk) == len(secret) + 29                           # versión + nonce + tag
    other = new_id()
    encrypted.write_block(other, secret)
    assert (tmp_path / "blocks" / other).read_bytes() != on_disk      # nonce distinto en cada escritura


def test_encrypted_detects_tampering_swapping_and_wrong_key(encrypted, disk, tmp_path):
    first, second = new_id(), new_id()
    encrypted.write_block(first, b"A" * 50)
    encrypted.write_block(second, b"B" * 50)

    tampered = bytearray((tmp_path / "blocks" / first).read_bytes())
    tampered[30] ^= 1                                                  # un bit cambiado
    (tmp_path / "blocks" / first).write_bytes(bytes(tampered))
    expect_errno(errno.EIO, encrypted.read_block, first)
    assert (tmp_path / "blocks" / first).exists()                      # no se borra: podría ser la clave

    shutil.copy(tmp_path / "blocks" / second, tmp_path / "blocks" / first)   # bloque intercambiado
    expect_errno(errno.EIO, encrypted.read_block, first)
    expect_errno(errno.EIO, EncryptedBlockStorage(disk, os.urandom(32)).read_block, second)   # otra clave
    disk.write_block(second, b"texto plano")                           # bloque sin cifrar
    expect_errno(errno.EIO, encrypted.read_block, second)
    expect_errno(errno.ENOENT, encrypted.read_block, new_id())
    expect_errno(errno.EINVAL, encrypted.write_block, "../x", b"x")


def test_secret_key_is_created_once_with_private_permissions(tmp_path):
    key_path = str(tmp_path / "keys" / "blocks.key")
    key = load_or_create_secret_key(key_path)
    assert len(key) == 32 and load_or_create_secret_key(key_path) == key
    if os.name == "posix":
        assert os.stat(key_path).st_mode & 0o777 == 0o600
