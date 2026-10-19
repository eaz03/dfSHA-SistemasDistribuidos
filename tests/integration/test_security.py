"""
Prueba de la seguridad de DFSha sobre los contenedores: login y tokens, mTLS y roles de los nodos,
aislamiento entre usuarios y permisos de bloque (los ataques que cada mecanismo debe frenar).

Se ejecuta dentro del contenedor sp1, con todos los servicios levantados:
  docker compose exec sp1 python -m tests.integration.test_security
Crea dos usuarios temporales (en /data/users.json) y los borra al terminar, junto con sus archivos.
"""

import os
import secrets
import socket
import subprocess
import sys

import grpc

from dfsha.client.auth_client import LoginSession, authenticated_superpeer_stub
from dfsha.client.client import DFSClient
from dfsha.common.auth_interceptor import bearer_metadata
from dfsha.common.tls import secure_channel
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as rpc
from dfsha.superpeer.json_user_store import JsonUserStore

SUPERPEER = "sp1:7000"
CLIENT_TLS = "certs/client"
USERS_FILE = "/data/users.json"
RPC_TIMEOUT_SECONDS = 5

results = []    # True o False por cada caso


def report(case_name, passed, detail):
    results.append(passed)
    print(f"[{'PASS' if passed else 'FAIL'}] {case_name}: {detail}")


def outcome(rpc_call):
    """'OK' si la llamada funcionó, o el código gRPC con que falló."""
    try:
        rpc_call()
        return "OK"
    except grpc.RpcError as rpc_error:
        return rpc_error.code().name


def main():
    users = JsonUserStore(USERS_FILE)
    alice = ("test-alice-" + secrets.token_hex(3), secrets.token_urlsafe(12))
    bob = ("test-bob-" + secrets.token_hex(3), secrets.token_urlsafe(12))
    for username, password in (alice, bob):
        users.add_user(username, password)
    try:
        run_cases(alice, bob)
    finally:
        for username, _ in (alice, bob):
            users.remove_user(username)

    passed_count = results.count(True)
    print(f"\n{passed_count}/{len(results)} passed")
    if passed_count < len(results):
        sys.exit(1)


def run_cases(alice, bob):
    plain_superpeer = rpc.SuperpeerStub(secure_channel(SUPERPEER, CLIENT_TLS))   # sin token

    # 1. Login
    wrong_password = outcome(lambda: plain_superpeer.Login(pb.LoginRequest(username=alice[0], password="x"),
                                                           timeout=RPC_TIMEOUT_SECONDS))
    unknown_user = outcome(lambda: plain_superpeer.Login(pb.LoginRequest(username="nadie", password="x"),
                                                         timeout=RPC_TIMEOUT_SECONDS))
    report("1. wrong password and unknown user look the same", wrong_password == unknown_user == "UNAUTHENTICATED",
           f"{wrong_password} / {unknown_user}")

    # 2. Tokens
    token = LoginSession(secure_channel(SUPERPEER, CLIENT_TLS), alice[0], alice[1], RPC_TIMEOUT_SECONDS).token()
    stat_root = pb.PathRequest(path="/")
    without_token = outcome(lambda: plain_superpeer.Stat(stat_root, timeout=RPC_TIMEOUT_SECONDS))
    tampered = outcome(lambda: plain_superpeer.Stat(stat_root, metadata=bearer_metadata(token[:-3] + "AAA"),
                                                    timeout=RPC_TIMEOUT_SECONDS))
    valid = outcome(lambda: plain_superpeer.Stat(stat_root, metadata=bearer_metadata(token), timeout=RPC_TIMEOUT_SECONDS))
    report("2. a valid token is required", (without_token, tampered, valid) == ("UNAUTHENTICATED", "UNAUTHENTICATED", "OK"),
           f"without: {without_token}, tampered: {tampered}, valid: {valid}")

    # 3. mTLS: sin certificado, de otra CA o con un nombre que no está en el certificado del servidor
    with open("certs/ca.crt", "rb") as ca_file:
        no_client_cert = grpc.secure_channel(SUPERPEER, grpc.ssl_channel_credentials(ca_file.read()))
    subprocess.run([sys.executable, "-m", "tools.make_certs", "--dir", "/tmp/other-ca", "dev"], capture_output=True)
    login = pb.LoginRequest(username=alice[0], password=alice[1])
    handshakes = {
        "no client certificate": outcome(lambda: rpc.SuperpeerStub(no_client_cert).Login(login, timeout=RPC_TIMEOUT_SECONDS)),
        "certificate from another CA": outcome(lambda: rpc.SuperpeerStub(secure_channel(SUPERPEER, "/tmp/other-ca/client"))
                                               .Login(login, timeout=RPC_TIMEOUT_SECONDS)),
        "address not in the certificate": outcome(lambda: rpc.SuperpeerStub(
            secure_channel(f"{socket.gethostbyname('sp1')}:7000", CLIENT_TLS)).Login(login, timeout=RPC_TIMEOUT_SECONDS)),
    }
    report("3. mTLS rejects unknown machines", all(code == "UNAVAILABLE" for code in handshakes.values()),
           ", ".join(f"{name}: {code}" for name, code in handshakes.items()))

    # 4. Roles: un cliente no puede hacer lo que le toca a un peer o a un superpeer
    client_superpeer = authenticated_superpeer_stub(SUPERPEER, alice[0], alice[1], CLIENT_TLS)
    client_peer = rpc.PeerStub(secure_channel("peer1:7100", CLIENT_TLS))
    roles = {
        "RegisterPeer": outcome(lambda: client_superpeer.RegisterPeer(
            pb.PeerInfo(peer_id="falso", address="falso:1", capacity_bytes=1), timeout=RPC_TIMEOUT_SECONDS)),
        "DeleteBlock": outcome(lambda: client_peer.DeleteBlock(pb.BlockRequest(block_id="0" * 32), timeout=RPC_TIMEOUT_SECONDS)),
        "ReplicateBlock": outcome(lambda: client_peer.ReplicateBlock(
            pb.ReplicateBlockRequest(block_id="0" * 32, source_address="peer2:7100"), timeout=RPC_TIMEOUT_SECONDS)),
        "Ping": outcome(lambda: client_peer.Ping(pb.Empty(), timeout=RPC_TIMEOUT_SECONDS)),
    }
    report("4. a client cannot act as a node", all(code == "PERMISSION_DENIED" for code in roles.values()),
           ", ".join(f"{name}: {code}" for name, code in roles.items()))

    # 5. Aislamiento entre usuarios
    alice_client = DFSClient([SUPERPEER], alice[0], alice[1], CLIENT_TLS)
    original = os.urandom(300_000)
    local_path = f"/tmp/{alice[0]}.bin"
    with open(local_path, "wb") as local_file:
        local_file.write(original)
    alice_client.upload_from_file("/privado.bin", local_path)
    bob_superpeer = authenticated_superpeer_stub(SUPERPEER, bob[0], bob[1], CLIENT_TLS)
    bob_view = outcome(lambda: bob_superpeer.GetBlockLocations(pb.PathRequest(path="/privado.bin"), timeout=RPC_TIMEOUT_SECONDS))
    bob_listing = list(bob_superpeer.ListDir(pb.PathRequest(path="/"), timeout=RPC_TIMEOUT_SECONDS).names)
    report("5. users only see their own files", bob_view == "NOT_FOUND" and "privado.bin" not in bob_listing,
           f"bob asks for alice's file: {bob_view}, bob's root: {bob_listing}")

    # 6. Permisos de bloque: bob conoce el block_id de un bloque de alice e intenta usarlo en el peer
    block = alice_client.get_block_locations("/privado.bin").blocks[0]
    peer = rpc.PeerStub(secure_channel(block.replicas[0].address, CLIENT_TLS))
    bob_superpeer.CreateFile(pb.PathRequest(path="/mio.bin"), timeout=RPC_TIMEOUT_SECONDS)
    bob_block = bob_superpeer.AllocateBlocks(pb.AllocateRequest(path="/mio.bin", size=10), timeout=RPC_TIMEOUT_SECONDS).blocks[0]
    bob_session = LoginSession(secure_channel(SUPERPEER, CLIENT_TLS), bob[0], bob[1], RPC_TIMEOUT_SECONDS).token()
    attacks = {
        "read without permission": outcome(lambda: peer.ReadBlock(pb.BlockRequest(block_id=block.block_id),
                                                                  timeout=RPC_TIMEOUT_SECONDS)),
        "read with his own block's permission": outcome(lambda: peer.ReadBlock(
            pb.BlockRequest(block_id=block.block_id, access_token=bob_block.access_token), timeout=RPC_TIMEOUT_SECONDS)),
        "overwrite without permission": outcome(lambda: peer.WriteBlock(
            pb.WriteBlockRequest(block_id=block.block_id, data=b"basura"), timeout=RPC_TIMEOUT_SECONDS)),
        "write with a read permission": outcome(lambda: peer.WriteBlock(
            pb.WriteBlockRequest(block_id=block.block_id, data=b"basura", access_token=block.access_token),
            timeout=RPC_TIMEOUT_SECONDS)),
        "session token as block permission": outcome(lambda: peer.ReadBlock(
            pb.BlockRequest(block_id=block.block_id, access_token=bob_session), timeout=RPC_TIMEOUT_SECONDS)),
    }
    report("6. block permissions stop every attack", all(code in ("UNAUTHENTICATED", "PERMISSION_DENIED")
                                                         for code in attacks.values()),
           ", ".join(f"{name}: {code}" for name, code in attacks.items()))
    alice_client.download_to_file("/privado.bin", local_path + ".copy")
    with open(local_path + ".copy", "rb") as copy_file:
        intact = copy_file.read() == original
    allowed = outcome(lambda: peer.ReadBlock(pb.BlockRequest(block_id=block.block_id, access_token=block.access_token),
                                             timeout=RPC_TIMEOUT_SECONDS))
    report("7. the owner still reads an intact file", intact and allowed == "OK",
           f"file intact: {intact}, alice reads her block: {allowed}")

    # Limpieza: los archivos de prueba (sus bloques los borra el limpiador)
    alice_client.rm("/privado.bin")
    bob_superpeer.Delete(pb.PathRequest(path="/mio.bin"), timeout=RPC_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
