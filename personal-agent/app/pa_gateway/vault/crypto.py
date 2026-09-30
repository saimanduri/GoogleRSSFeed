"""Thin wrappers over vetted crypto libraries (spec 3, rule 35.2: never implement primitives).

- AES-256-GCM            : cryptography (OpenSSL)
- HKDF-SHA-256           : cryptography
- HMAC-SHA-256           : stdlib hmac
- Argon2id               : argon2-cffi (reference implementation)
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass

from argon2.low_level import Type, hash_secret_raw
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

KEY_LEN = 32
NONCE_LEN = 12


class CryptoError(Exception):
    """Raised on any authentication/decryption failure. Deliberately carries no detail."""


def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"), validate=True)


def random_key() -> bytes:
    return secrets.token_bytes(KEY_LEN)


def aead_encrypt(key: bytes, plaintext: bytes, aad: bytes = b"") -> bytes:
    """Returns nonce || ciphertext+tag."""
    nonce = os.urandom(NONCE_LEN)
    return nonce + AESGCM(bytes(key)).encrypt(nonce, plaintext, aad)


def aead_decrypt(key: bytes, blob: bytes, aad: bytes = b"") -> bytes:
    if len(blob) < NONCE_LEN + 16:
        raise CryptoError("ciphertext too short")
    try:
        return AESGCM(bytes(key)).decrypt(blob[:NONCE_LEN], blob[NONCE_LEN:], aad)
    except InvalidTag as e:
        raise CryptoError("authentication failed") from e


def hkdf(ikm: bytes, info: str, length: int = KEY_LEN, salt: bytes | None = None) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=salt, info=info.encode()).derive(bytes(ikm))


def hmac_sha256(key: bytes, data: bytes) -> bytes:
    return hmac.new(bytes(key), data, hashlib.sha256).digest()


def consteq(a: bytes, b: bytes) -> bool:
    return hmac.compare_digest(a, b)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Argon2Params:
    time_cost: int
    memory_kib: int
    parallelism: int

    def to_dict(self) -> dict:
        return {"t": self.time_cost, "m": self.memory_kib, "p": self.parallelism}

    @classmethod
    def from_dict(cls, d: dict) -> "Argon2Params":
        return cls(int(d["t"]), int(d["m"]), int(d["p"]))


def argon2id(secret: bytes, salt: bytes, params: Argon2Params, length: int = KEY_LEN) -> bytes:
    return hash_secret_raw(
        secret=bytes(secret), salt=salt, time_cost=params.time_cost, memory_cost=params.memory_kib,
        parallelism=params.parallelism, hash_len=length, type=Type.ID,
    )
