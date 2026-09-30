"""PIN-authorised key protectors (spec 4.3/4.4).

A protector encrypts the 256-bit secret S_tpm so that it can be decrypted ONLY with the PIN.

* TpmProtector (production): a non-exportable RSA-2048 key in the TPM, created through the
  Windows "Microsoft Platform Crypto Provider" (CNG / NCrypt) with the PIN as its usage
  authorisation. The TPM's dictionary-attack logic rate-limits PIN guessing in hardware.
* SoftwareProtector (developer mode ONLY): Argon2id(PIN) wraps S_tpm. Exists so the app runs on
  CI runners and PCs without a usable TPM during development. It is refused in release builds and
  shown as a High finding on the Security Posture page, because a 6-digit PIN protected only by
  Argon2id can be brute-forced offline by anyone who copies the data folder.
"""
from __future__ import annotations

import ctypes
import secrets
import sys
from typing import Any

from pa_common.devmode import dev_mode, fast_kdf_for_tests

from .crypto import Argon2Params, CryptoError, aead_decrypt, aead_encrypt, argon2id, b64d, b64e


class ProtectorError(Exception):
    pass


class PinRejected(ProtectorError):
    pass


class TpmLockedOut(ProtectorError):
    pass


class ProtectorUnavailable(ProtectorError):
    pass


class KeyProtector:
    kind = "abstract"
    hardware_bound = False

    def create(self, pin: str, secret: bytes) -> dict[str, Any]:
        """Create a new PIN-authorised key and return a descriptor containing the encrypted secret."""
        raise NotImplementedError

    def decrypt(self, desc: dict[str, Any], pin: str) -> bytes:
        raise NotImplementedError

    def verify_pin(self, desc: dict[str, Any], pin: str) -> bool:
        try:
            self.decrypt(desc, pin)
            return True
        except PinRejected:
            return False

    def destroy(self, desc: dict[str, Any]) -> None:
        pass


# --------------------------------------------------------------------------------------------
# Software protector (developer mode only)
# --------------------------------------------------------------------------------------------

class SoftwareProtector(KeyProtector):
    kind = "software"
    hardware_bound = False

    def __init__(self) -> None:
        if not dev_mode():
            raise ProtectorUnavailable("software key protector is only available in developer mode")

    @staticmethod
    def _params() -> Argon2Params:
        return Argon2Params(1, 8 * 1024, 1) if fast_kdf_for_tests() else Argon2Params(3, 256 * 1024, 4)

    def create(self, pin: str, secret: bytes) -> dict[str, Any]:
        salt = secrets.token_bytes(16)
        params = self._params()
        kek = argon2id(pin.encode("utf-8"), salt, params)
        return {"kind": self.kind, "salt": b64e(salt), "params": params.to_dict(),
                "blob": b64e(aead_encrypt(kek, secret, b"pa/protector/software/v1"))}

    def decrypt(self, desc: dict[str, Any], pin: str) -> bytes:
        kek = argon2id(pin.encode("utf-8"), b64d(desc["salt"]), Argon2Params.from_dict(desc["params"]))
        try:
            return aead_decrypt(kek, b64d(desc["blob"]), b"pa/protector/software/v1")
        except CryptoError as e:
            raise PinRejected("PIN rejected") from e


# --------------------------------------------------------------------------------------------
# TPM protector via CNG (Windows only)
# --------------------------------------------------------------------------------------------

MS_PLATFORM_CRYPTO_PROVIDER = "Microsoft Platform Crypto Provider"
NCRYPT_SILENT_FLAG = 0x40
NCRYPT_OVERWRITE_KEY_FLAG = 0x80
NCRYPT_PAD_OAEP_FLAG = 0x4
NTE_BAD_KEYSET = 0x80090016
# TPM lockout / auth failure HRESULTs (TBS/PCP)
TPM_LOCKOUT_CODES = {0x80280921, 0x80280803, 0x80290406}
TPM_AUTHFAIL_CODES = {0x8028008E, 0x80280022, 0x80280098, 0x80090027, 0x8028009A}


class _OAEP(ctypes.Structure):
    _fields_ = [("pszAlgId", ctypes.c_wchar_p), ("pbLabel", ctypes.c_void_p), ("cbLabel", ctypes.c_ulong)]


def _hr(code: int) -> int:
    return code & 0xFFFFFFFF


class TpmProtector(KeyProtector):
    kind = "tpm"
    hardware_bound = True

    def __init__(self) -> None:
        if sys.platform != "win32":
            raise ProtectorUnavailable("TPM protector requires Windows")
        self._nc = ctypes.WinDLL("ncrypt.dll")
        self._nc.NCryptOpenStorageProvider.restype = ctypes.c_long
        self._nc.NCryptCreatePersistedKey.restype = ctypes.c_long
        self._nc.NCryptOpenKey.restype = ctypes.c_long
        self._nc.NCryptSetProperty.restype = ctypes.c_long
        self._nc.NCryptFinalizeKey.restype = ctypes.c_long
        self._nc.NCryptEncrypt.restype = ctypes.c_long
        self._nc.NCryptDecrypt.restype = ctypes.c_long
        self._nc.NCryptDeleteKey.restype = ctypes.c_long
        self._nc.NCryptFreeObject.restype = ctypes.c_long

    # -- helpers -------------------------------------------------------------------------
    def _provider(self) -> ctypes.c_void_p:
        h = ctypes.c_void_p()
        rc = self._nc.NCryptOpenStorageProvider(ctypes.byref(h), ctypes.c_wchar_p(MS_PLATFORM_CRYPTO_PROVIDER), 0)
        if rc != 0:
            raise ProtectorUnavailable(f"TPM provider unavailable (0x{_hr(rc):08X})")
        return h

    def _set_pin(self, key: ctypes.c_void_p, pin: str) -> None:
        buf = ctypes.create_unicode_buffer(pin)
        rc = self._nc.NCryptSetProperty(key, ctypes.c_wchar_p("SmartCardPin"), buf,
                                        ctypes.c_ulong(ctypes.sizeof(buf)), 0)
        ctypes.memset(buf, 0, ctypes.sizeof(buf))
        if rc != 0:
            raise ProtectorError(f"cannot set PIN (0x{_hr(rc):08X})")

    def available(self) -> bool:
        try:
            h = self._provider()
            self._nc.NCryptFreeObject(h)
            return True
        except ProtectorError:
            return False

    def _encrypt(self, key: ctypes.c_void_p, data: bytes, hash_alg: str) -> bytes:
        pad = _OAEP(hash_alg, None, 0)
        out_len = ctypes.c_ulong(0)
        rc = self._nc.NCryptEncrypt(key, data, len(data), ctypes.byref(pad), None, 0, ctypes.byref(out_len), NCRYPT_PAD_OAEP_FLAG)
        if rc != 0:
            raise ProtectorError(f"TPM encrypt size failed (0x{_hr(rc):08X})")
        out = ctypes.create_string_buffer(out_len.value)
        rc = self._nc.NCryptEncrypt(key, data, len(data), ctypes.byref(pad), out, out_len, ctypes.byref(out_len), NCRYPT_PAD_OAEP_FLAG)
        if rc != 0:
            raise ProtectorError(f"TPM encrypt failed (0x{_hr(rc):08X})")
        return out.raw[:out_len.value]

    # -- API -----------------------------------------------------------------------------
    def create(self, pin: str, secret: bytes) -> dict[str, Any]:
        prov = self._provider()
        key = ctypes.c_void_p()
        name = f"PersonalAgent-{secrets.token_hex(8)}"
        try:
            rc = self._nc.NCryptCreatePersistedKey(prov, ctypes.byref(key), ctypes.c_wchar_p("RSA"),
                                                   ctypes.c_wchar_p(name), 0, NCRYPT_OVERWRITE_KEY_FLAG)
            if rc != 0:
                raise ProtectorUnavailable(f"cannot create TPM key (0x{_hr(rc):08X})")
            length = ctypes.c_ulong(2048)
            self._nc.NCryptSetProperty(key, ctypes.c_wchar_p("Length"), ctypes.byref(length), 4, 0)
            self._set_pin(key, pin)
            rc = self._nc.NCryptFinalizeKey(key, 0)
            if rc != 0:
                raise ProtectorUnavailable(f"cannot finalize TPM key (0x{_hr(rc):08X})")
            hash_alg = "SHA256"
            try:
                blob = self._encrypt(key, secret, hash_alg)
            except ProtectorError:
                hash_alg = "SHA1"
                blob = self._encrypt(key, secret, hash_alg)
            return {"kind": self.kind, "key_name": name, "oaep_hash": hash_alg, "blob": b64e(blob)}
        finally:
            if key:
                self._nc.NCryptFreeObject(key)
            self._nc.NCryptFreeObject(prov)

    def decrypt(self, desc: dict[str, Any], pin: str) -> bytes:
        prov = self._provider()
        key = ctypes.c_void_p()
        try:
            rc = self._nc.NCryptOpenKey(prov, ctypes.byref(key), ctypes.c_wchar_p(desc["key_name"]), 0, 0)
            if rc != 0:
                raise ProtectorUnavailable(f"TPM key missing - was the TPM cleared? (0x{_hr(rc):08X})")
            self._set_pin(key, pin)
            data = b64d(desc["blob"])
            pad = _OAEP(desc.get("oaep_hash", "SHA256"), None, 0)
            out = ctypes.create_string_buffer(512)
            out_len = ctypes.c_ulong(0)
            rc = self._nc.NCryptDecrypt(key, data, len(data), ctypes.byref(pad), out, 512, ctypes.byref(out_len),
                                        NCRYPT_PAD_OAEP_FLAG | NCRYPT_SILENT_FLAG)
            if rc != 0:
                code = _hr(rc)
                if code in TPM_LOCKOUT_CODES:
                    raise TpmLockedOut("TPM is in dictionary-attack lockout; wait and try again")
                raise PinRejected(f"PIN rejected (0x{code:08X})")
            return out.raw[:out_len.value]
        finally:
            if key:
                self._nc.NCryptFreeObject(key)
            self._nc.NCryptFreeObject(prov)

    def verify_pin(self, desc: dict[str, Any], pin: str) -> bool:
        """Quick-unlock proof of presence: the PIN-authorised TPM key must decrypt a FRESH challenge."""
        prov = self._provider()
        key = ctypes.c_void_p()
        try:
            rc = self._nc.NCryptOpenKey(prov, ctypes.byref(key), ctypes.c_wchar_p(desc["key_name"]), 0, 0)
            if rc != 0:
                raise ProtectorUnavailable("TPM key missing")
            challenge = secrets.token_bytes(32)
            ct = self._encrypt(key, challenge, desc.get("oaep_hash", "SHA256"))
            self._set_pin(key, pin)
            pad = _OAEP(desc.get("oaep_hash", "SHA256"), None, 0)
            out = ctypes.create_string_buffer(512)
            out_len = ctypes.c_ulong(0)
            rc = self._nc.NCryptDecrypt(key, ct, len(ct), ctypes.byref(pad), out, 512, ctypes.byref(out_len),
                                        NCRYPT_PAD_OAEP_FLAG | NCRYPT_SILENT_FLAG)
            if rc != 0:
                if _hr(rc) in TPM_LOCKOUT_CODES:
                    raise TpmLockedOut("TPM lockout")
                return False
            return secrets.compare_digest(out.raw[:out_len.value], challenge)
        finally:
            if key:
                self._nc.NCryptFreeObject(key)
            self._nc.NCryptFreeObject(prov)

    def destroy(self, desc: dict[str, Any]) -> None:
        try:
            prov = self._provider()
        except ProtectorError:
            return
        key = ctypes.c_void_p()
        try:
            if self._nc.NCryptOpenKey(prov, ctypes.byref(key), ctypes.c_wchar_p(desc["key_name"]), 0, 0) == 0:
                self._nc.NCryptDeleteKey(key, 0)  # frees the handle
                key = ctypes.c_void_p()
        finally:
            if key:
                self._nc.NCryptFreeObject(key)
            self._nc.NCryptFreeObject(prov)


def tpm_status() -> dict[str, Any]:
    if sys.platform != "win32":
        return {"present": False, "usable": False, "detail": "not Windows"}
    try:
        ok = TpmProtector().available()
        return {"present": ok, "usable": ok, "detail": "Microsoft Platform Crypto Provider available" if ok else "provider unavailable"}
    except ProtectorError as e:
        return {"present": False, "usable": False, "detail": str(e)}


def protector_for(kind: str) -> KeyProtector:
    if kind == "tpm":
        return TpmProtector()
    if kind == "software":
        return SoftwareProtector()
    raise ProtectorUnavailable(f"unknown protector {kind}")


def default_protector() -> KeyProtector:
    """TPM when usable; otherwise the software stand-in in developer mode; otherwise refuse (spec 4.2 step 1)."""
    if tpm_status()["usable"]:
        return TpmProtector()
    if dev_mode():
        return SoftwareProtector()
    raise ProtectorUnavailable("TPM 2.0 is not usable on this PC. Setup cannot continue.")
