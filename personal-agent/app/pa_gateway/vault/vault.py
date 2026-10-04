"""Vault header and key hierarchy (spec 4.3 - 4.6).

  VMK (256-bit random)
   +-- HKDF -> K_db, K_files, K_log, K_secret, K_ipc, K_hdr, K_dlp, K_skill

  vault.header (JSON, versioned):
    W_pw    = AEAD(KEK_pw, VMK)        KEK_pw    = Argon2id(password, salt_pw)
    W_reset = AEAD(KEK_reset, VMK)     KEK_reset = HKDF(S_tpm || Argon2id(recovery_key, salt_rk))
    protector = PIN-authorised key descriptor holding Enc(S_tpm)
    mac     = HMAC(K_hdr, canonical header without mac)   -> detects tampering after unlock

There is no wrapped copy of the VMK that the PIN alone can open.
"""
from __future__ import annotations

import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pa_common.devmode import fast_kdf_for_tests

from . import recovery
from .crypto import (Argon2Params, CryptoError, aead_decrypt, aead_encrypt, argon2id, b64d, b64e, consteq,
                     hkdf, hmac_sha256, random_key)
from .protector import KeyProtector, protector_for
from .secretmem import SecretBytes

HEADER_VERSION = 1
DEFAULT_ASSISTANT_NAME = "ChiRAG Agent"
AAD_PW = b"pa/vault/W_pw/v1"
AAD_RESET = b"pa/vault/W_reset/v1"
SUBKEYS = ("K_db", "K_files", "K_log", "K_secret", "K_ipc", "K_hdr", "K_dlp", "K_skill", "K_backup_local")


class VaultError(Exception):
    pass


class WrongPassword(VaultError):
    pass


class WrongRecovery(VaultError):
    pass


class HeaderTampered(VaultError):
    pass


def calibrate_password_params(target_seconds: float = 1.0) -> Argon2Params:
    """Spec 4.3: >= 1 s on this PC, memory >= 256 MiB, iterations >= 3, parallelism 4."""
    if fast_kdf_for_tests():
        return Argon2Params(1, 8 * 1024, 1)
    params = Argon2Params(3, 256 * 1024, 4)
    salt = os.urandom(16)
    for _ in range(8):
        start = time.perf_counter()
        argon2id(b"calibration", salt, params)
        if time.perf_counter() - start >= target_seconds:
            break
        params = Argon2Params(params.time_cost + 1, params.memory_kib, params.parallelism)
    return params


def recovery_params() -> Argon2Params:
    return Argon2Params(1, 8 * 1024, 1) if fast_kdf_for_tests() else Argon2Params(2, 64 * 1024, 4)


def _canonical(d: dict[str, Any]) -> bytes:
    return json.dumps({k: v for k, v in d.items() if k != "mac"}, sort_keys=True, separators=(",", ":")).encode()


@dataclass
class UnlockedKeys:
    """Sub-keys derived from the VMK; lives only in pa-gateway memory while unlocked."""
    vmk: SecretBytes
    subkeys: dict[str, SecretBytes]

    def key(self, name: str) -> bytes:
        return self.subkeys[name].get()

    def wipe(self) -> None:
        self.vmk.wipe()
        for k in self.subkeys.values():
            k.wipe()

    @classmethod
    def from_vmk(cls, vmk: bytes) -> "UnlockedKeys":
        return cls(SecretBytes(vmk), {n: SecretBytes(hkdf(vmk, f"pa/{n}/v1")) for n in SUBKEYS})


class VaultHeader:
    def __init__(self, path: Path):
        self.path = path
        self.data: dict[str, Any] = {}

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> None:
        try:
            self.data = json.loads(self.path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise HeaderTampered("vault header unreadable") from e
        if self.data.get("version") != HEADER_VERSION:
            raise HeaderTampered("unsupported vault header version")

    def save(self, keys: UnlockedKeys) -> None:
        self.data["version"] = HEADER_VERSION
        self.data["mac"] = b64e(hmac_sha256(keys.key("K_hdr"), _canonical(self.data)))
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True), "utf-8")
        os.replace(tmp, self.path)

    def verify_mac(self, keys: UnlockedKeys) -> None:
        mac = self.data.get("mac")
        if not mac or not consteq(b64d(mac), hmac_sha256(keys.key("K_hdr"), _canonical(self.data))):
            raise HeaderTampered("vault header integrity check failed")


class Vault:
    """All operations on the wrapped VMK. Stateless apart from the header file."""

    def __init__(self, header_path: Path):
        self.header = VaultHeader(header_path)

    # ------------------------------------------------------------------ setup
    def create(self, username: str, password: str, pin: str, protector: KeyProtector,
               display_name: str = "", assistant_name: str = "") -> tuple[UnlockedKeys, str]:
        if self.header.exists():
            raise VaultError("vault already exists")
        vmk = random_key()
        keys = UnlockedKeys.from_vmk(vmk)
        rk = recovery.generate()
        self.header.data = {"version": HEADER_VERSION, "username": username, "created": int(time.time()),
                            "display_name": display_name, "assistant_name": assistant_name}
        self._wrap_password(vmk, password)
        self._wrap_reset(vmk, pin, rk, protector)
        self.header.data["rk_check"] = self._rk_check(keys, rk)
        self.header.save(keys)
        return keys, rk

    @staticmethod
    def _rk_check(keys: "UnlockedKeys", rk: str) -> str:
        return b64e(hmac_sha256(keys.key("K_hdr"), b"rk:" + recovery.normalize(rk).encode()))

    def _wrap_password(self, vmk: bytes, password: str) -> None:
        params = calibrate_password_params()
        salt = os.urandom(16)
        kek = argon2id(password.encode("utf-8"), salt, params)
        self.header.data["pw"] = {"salt": b64e(salt), "params": params.to_dict(),
                                  "W_pw": b64e(aead_encrypt(kek, vmk, AAD_PW))}

    def _kek_reset(self, s_tpm: bytes, rk: str, salt: bytes, params: Argon2Params) -> bytes:
        rk_stretched = argon2id(recovery.normalize(rk).encode("ascii"), salt, params)
        return hkdf(s_tpm + rk_stretched, "pa/KEK_reset/v1")

    def _wrap_reset(self, vmk: bytes, pin: str, rk: str, protector: KeyProtector) -> None:
        old = self.header.data.get("reset")
        s_tpm = secrets.token_bytes(32)
        desc = protector.create(pin, s_tpm)
        salt = os.urandom(16)
        params = recovery_params()
        kek = self._kek_reset(s_tpm, rk, salt, params)
        self.header.data["reset"] = {"protector": desc, "salt_rk": b64e(salt), "params_rk": params.to_dict(),
                                     "W_reset": b64e(aead_encrypt(kek, vmk, AAD_RESET)),
                                     "pin_policy": {"created": int(time.time())}}
        if old:
            try:
                protector_for(old["protector"]["kind"]).destroy(old["protector"])
            except Exception:  # noqa: BLE001 - old key may already be gone (TPM cleared)
                pass

    # ------------------------------------------------------------------ unlock
    @property
    def username(self) -> str:
        return self.header.data.get("username", "")

    def load(self) -> None:
        self.header.load()

    def unlock_with_password(self, password: str) -> UnlockedKeys:
        pw = self.header.data["pw"]
        kek = argon2id(password.encode("utf-8"), b64d(pw["salt"]), Argon2Params.from_dict(pw["params"]))
        try:
            vmk = aead_decrypt(kek, b64d(pw["W_pw"]), AAD_PW)
        except CryptoError as e:
            raise WrongPassword("wrong username or password") from e
        keys = UnlockedKeys.from_vmk(vmk)
        self.header.verify_mac(keys)
        return keys

    def protector(self) -> KeyProtector:
        return protector_for(self.header.data["reset"]["protector"]["kind"])

    def protector_desc(self) -> dict[str, Any]:
        return self.header.data["reset"]["protector"]

    def verify_pin(self, pin: str) -> bool:
        return self.protector().verify_pin(self.protector_desc(), pin)

    def unlock_with_pin_and_recovery(self, pin: str, rk: str) -> UnlockedKeys:
        """Forgot-password path: needs THIS PC's TPM + PIN + recovery key."""
        reset = self.header.data["reset"]
        s_tpm = self.protector().decrypt(reset["protector"], pin)  # raises PinRejected
        try:
            kek = self._kek_reset(s_tpm, rk, b64d(reset["salt_rk"]), Argon2Params.from_dict(reset["params_rk"]))
            vmk = aead_decrypt(kek, b64d(reset["W_reset"]), AAD_RESET)
        except (CryptoError, ValueError) as e:
            raise WrongRecovery("recovery key rejected") from e
        keys = UnlockedKeys.from_vmk(vmk)
        self.header.verify_mac(keys)
        return keys

    # ------------------------------------------------------------------ changes (caller holds keys)
    def change_password(self, keys: UnlockedKeys, new_password: str) -> None:
        self._wrap_password(keys.vmk.get(), new_password)
        self.header.save(keys)

    def set_pin(self, keys: UnlockedKeys, new_pin: str, rk: str, protector: KeyProtector | None = None) -> None:
        """New TPM key + W_reset (requires the CURRENT recovery key, spec 4.6 'Forgot PIN')."""
        self.verify_recovery_key(keys, rk)
        self._wrap_reset(keys.vmk.get(), new_pin, rk, protector or self.protector())
        self.header.save(keys)

    def rotate_recovery_key(self, keys: UnlockedKeys, pin: str, protector: KeyProtector | None = None) -> str:
        """Generate a new recovery key; old W_reset is destroyed. Caller must have verified the password."""
        rk = recovery.generate()
        self._wrap_reset(keys.vmk.get(), pin, rk, protector or self.protector())
        self.header.data["rk_check"] = self._rk_check(keys, rk)
        self.header.save(keys)
        return rk

    def verify_recovery_key(self, keys: UnlockedKeys, rk: str) -> None:
        """Checks rk without the PIN: we cannot open W_reset without S_tpm, so we compare against a keyed hash."""
        expected = self.header.data.get("rk_check")
        if expected is None:
            raise WrongRecovery("no recovery key check value")
        try:
            got = self._rk_check(keys, rk)
        except ValueError as e:
            raise WrongRecovery("recovery key format is invalid") from e
        if not consteq(b64d(expected), b64d(got)):
            raise WrongRecovery("recovery key rejected")

    @property
    def profile(self) -> dict[str, str]:
        """Your name and the assistant's name. Not secret (shown on the sign-in screen, like the username),
        but covered by the header MAC so they cannot be changed without the vault key."""
        d = self.header.data
        return {"display_name": d.get("display_name") or d.get("username", ""),
                "assistant_name": d.get("assistant_name") or DEFAULT_ASSISTANT_NAME}

    def set_profile(self, keys: UnlockedKeys, display_name: str, assistant_name: str) -> None:
        self.header.data["display_name"] = display_name
        self.header.data["assistant_name"] = assistant_name
        self.header.save(keys)

    def rename(self, keys: UnlockedKeys, username: str) -> None:
        self.header.data["username"] = username
        self.header.save(keys)

    def destroy(self) -> None:
        """Crypto-erase: delete the TPM key and overwrite the header (spec 28)."""
        try:
            self.header.load()
            self.protector().destroy(self.protector_desc())
        except Exception:  # noqa: BLE001
            pass
        if self.header.path.exists():
            size = self.header.path.stat().st_size
            with open(self.header.path, "r+b") as f:
                f.write(os.urandom(max(size, 1)))
                f.flush()
                os.fsync(f.fileno())
            self.header.path.unlink()
