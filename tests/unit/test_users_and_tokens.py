"""
Pruebas de usuarios (hash scrypt, validaciones, tiempo constante) y de tokens JWT EdDSA
(sesión y permisos de bloque).
"""

import base64
import json
import os
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from dfsha.common.auth_tokens import (InvalidTokenError, TokenIssuer, TokenVerifier, WrongBlockPermissionError,
                                      load_or_create_signing_key, load_verification_key)
from dfsha.superpeer.json_user_store import JsonUserStore
from dfsha.superpeer.user_store import UserStore, hash_password, verify_password

BLOCK_ID = "a" * 32


# ---------- Usuarios ----------

def test_password_hashes_are_salted_and_verifiable():
    first, second = hash_password("secreto123"), hash_password("secreto123")
    assert first != second and first.startswith("scrypt$")
    assert verify_password("secreto123", first)
    assert not verify_password("secreto124", first)


def test_user_store(tmp_path):
    path = str(tmp_path / "users.json")
    store = JsonUserStore(path)
    store.add_user("sofi", "secreto123")
    assert JsonUserStore(path).authenticate("sofi", "secreto123")      # otra instancia lee el archivo
    assert not store.authenticate("sofi", "incorrecta")
    assert not store.authenticate("nadie", "secreto123")
    for bad in [("sofi", "secreto123"), ("Sofi", "secreto123"), ("a/b", "secreto123"),
                ("ab", "secreto123"), ("corta", "1234567")]:
        with pytest.raises(ValueError):
            store.add_user(*bad)
    assert "secreto123" not in open(path, encoding="utf-8").read()
    store.remove_user("sofi")
    assert store.list_users() == []
    with pytest.raises(TypeError):
        UserStore()


def test_unknown_user_takes_as_long_as_a_wrong_password(tmp_path):
    """Sin esto, midiendo el tiempo de respuesta se sabría qué usuarios existen."""
    store = JsonUserStore(str(tmp_path / "users.json"))
    store.add_user("sofi", "secreto123")

    def average_seconds(username):
        start = time.perf_counter()
        for _ in range(5):
            store.authenticate(username, "incorrecta")
        return (time.perf_counter() - start) / 5

    ratio = average_seconds("nadie") / average_seconds("sofi")
    assert 0.5 < ratio < 2


# ---------- Tokens ----------

@pytest.fixture
def signing_key():
    return Ed25519PrivateKey.generate()


@pytest.fixture
def verifier(signing_key):
    return TokenVerifier(signing_key.public_key())


def test_signing_key_files(tmp_path):
    key_path = str(tmp_path / "keys" / "token.key")
    key = load_or_create_signing_key(key_path)
    token = TokenIssuer(key).issue("sofi")
    assert TokenVerifier(load_verification_key(key_path + ".pub")).verify(token) == "sofi"   # solo con la pública
    reused = load_or_create_signing_key(key_path)
    assert TokenVerifier(reused.public_key()).verify(token) == "sofi"
    if os.name == "posix":
        assert os.stat(key_path).st_mode & 0o777 == 0o600


def test_session_token_rejections(signing_key, verifier):
    token = TokenIssuer(signing_key).issue("sofi")
    assert verifier.verify(token) == "sofi"

    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=="))
    claims["sub"] = "esteban"
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    now = time.time()
    rejected = [
        f"{header}.{forged}.{signature}",                                              # usuario cambiado
        TokenIssuer(Ed25519PrivateKey.generate()).issue("sofi"),                       # otra clave
        jwt.encode({"sub": "sofi", "typ": "session", "iat": int(now), "exp": int(now) + 99},
                   None, algorithm="none"),                                            # sin firma
        TokenIssuer(signing_key, ttl_seconds=60, clock=lambda: now - 120).issue("sofi"),   # vencido
        TokenIssuer(signing_key).issue_block_token("sofi", BLOCK_ID, "read"),          # otro tipo
        "", "abc", token + "x",
    ]
    for bad_token in rejected:
        with pytest.raises(InvalidTokenError):
            verifier.verify(bad_token)
    # vencido hace 10 s: dentro del margen de 30 s por relojes desfasados
    assert verifier.verify(TokenIssuer(signing_key, ttl_seconds=60, clock=lambda: now - 70).issue("sofi")) == "sofi"


def test_block_permissions(signing_key, verifier):
    issuer = TokenIssuer(signing_key)
    read_token = issuer.issue_block_token("sofi", BLOCK_ID, "read")
    assert verifier.verify_block_token(read_token, BLOCK_ID, "read") == "sofi"
    with pytest.raises(WrongBlockPermissionError):
        verifier.verify_block_token(read_token, "b" * 32, "read")      # otro bloque
    with pytest.raises(WrongBlockPermissionError):
        verifier.verify_block_token(read_token, BLOCK_ID, "write")     # otra operación
    with pytest.raises(InvalidTokenError):
        verifier.verify_block_token(issuer.issue("sofi"), BLOCK_ID, "read")    # token de sesión
    expired = TokenIssuer(signing_key, clock=lambda: time.time() - 3600).issue_block_token("sofi", BLOCK_ID, "read")
    with pytest.raises(InvalidTokenError):
        verifier.verify_block_token(expired, BLOCK_ID, "read")
    with pytest.raises(ValueError):
        issuer.issue_block_token("sofi", BLOCK_ID, "delete")
