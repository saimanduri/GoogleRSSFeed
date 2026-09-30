"""Named-pipe IPC server (spec 2.4, 39.1). Windows only.

- pipe name contains a random per-launch component; FILE_FLAG_FIRST_PIPE_INSTANCE prevents squatting
- DACL: current user + SYSTEM only; NETWORK logon SID explicitly denied; PIPE_REJECT_REMOTE_CLIENTS
- client verification: process image path (release: inside the install folder + valid Authenticode
  signature from the publisher) and, for pa-core, the exact PID the gateway spawned
- per-launch token handshake on every connection; 5 failed handshakes -> security event
- frames: 4-byte length + JSON (pa_common.protocol), schema-checked by the dispatcher
"""
from __future__ import annotations

import ctypes
import ctypes.wintypes as wt
import json
import os
import secrets
import sys
import threading
from pathlib import Path
from typing import Any

from pa_common.buildinfo import RELEASE_BUILD
from pa_common.devmode import dev_mode
from pa_common.ids import new_id
from pa_common.protocol import FrameDecoder, FrameError, encode_frame

from .dispatch import ClientInfo, Dispatcher

BUF = 64 * 1024
UI_EXE = "pa-ui.exe"
CORE_EXE = "pa-core.exe"


def _image_path(pid: int) -> str:
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    h = k32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not h:
        return ""
    try:
        buf = ctypes.create_unicode_buffer(32768)
        size = wt.DWORD(len(buf))
        if k32.QueryFullProcessImageNameW(h, 0, buf, ctypes.byref(size)):
            return buf.value
        return ""
    finally:
        k32.CloseHandle(h)


class _GUID(ctypes.Structure):
    _fields_ = [("Data1", wt.DWORD), ("Data2", wt.WORD), ("Data3", wt.WORD), ("Data4", ctypes.c_ubyte * 8)]


class _WINTRUST_FILE_INFO(ctypes.Structure):
    _fields_ = [("cbStruct", wt.DWORD), ("pcwszFilePath", wt.LPCWSTR), ("hFile", wt.HANDLE), ("pgKnownSubject", ctypes.c_void_p)]


class _WINTRUST_DATA(ctypes.Structure):
    _fields_ = [("cbStruct", wt.DWORD), ("pPolicyCallbackData", ctypes.c_void_p), ("pSIPClientData", ctypes.c_void_p),
                ("dwUIChoice", wt.DWORD), ("fdwRevocationChecks", wt.DWORD), ("dwUnionChoice", wt.DWORD),
                ("pFile", ctypes.POINTER(_WINTRUST_FILE_INFO)), ("dwStateAction", wt.DWORD), ("hWVTStateData", wt.HANDLE),
                ("pwszURLReference", wt.LPCWSTR), ("dwProvFlags", wt.DWORD), ("dwUIContext", wt.DWORD),
                ("pSignatureSettings", ctypes.c_void_p)]


def authenticode_valid(path: str) -> bool:
    """WinVerifyTrust(WINTRUST_ACTION_GENERIC_VERIFY_V2) on the client executable."""
    action = _GUID(0x00AAC56B, 0xCD44, 0x11D0, (ctypes.c_ubyte * 8)(0x8C, 0xC2, 0x00, 0xC0, 0x4F, 0xC2, 0x95, 0xEE))
    fi = _WINTRUST_FILE_INFO(ctypes.sizeof(_WINTRUST_FILE_INFO), path, None, None)
    wd = _WINTRUST_DATA()
    wd.cbStruct = ctypes.sizeof(_WINTRUST_DATA)
    wd.dwUIChoice = 2  # WTD_UI_NONE
    wd.fdwRevocationChecks = 0
    wd.dwUnionChoice = 1  # WTD_CHOICE_FILE
    wd.pFile = ctypes.pointer(fi)
    wd.dwStateAction = 0
    wd.dwProvFlags = 0x00000080  # WTD_CACHE_ONLY_URL_RETRIEVAL
    rc = ctypes.WinDLL("wintrust").WinVerifyTrust(None, ctypes.byref(action), ctypes.byref(wd))
    return rc == 0


class PipeServer:
    def __init__(self, dispatcher: Dispatcher, rendezvous: Path):
        import win32security  # type: ignore[import-not-found]
        self.dispatcher = dispatcher
        self.gw = dispatcher.gw
        self.pipe_name = rf"\\.\pipe\PersonalAgent-{secrets.token_hex(12)}"
        self.ui_token = secrets.token_urlsafe(32)
        self._core_pid: int | None = None
        self._core_token: str | None = None
        self._stop = threading.Event()
        self._failures = 0
        self.rendezvous = rendezvous
        self.install_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else None
        self._sa = self._security_attributes(win32security)
        self._first = True

    @staticmethod
    def _security_attributes(win32security):
        import ntsecuritycon as con  # type: ignore[import-not-found]
        import win32api  # type: ignore[import-not-found]

        token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32security.TOKEN_QUERY)
        user_sid = win32security.GetTokenInformation(token, win32security.TokenUser)[0]
        system_sid = win32security.CreateWellKnownSid(win32security.WinLocalSystemSid)
        network_sid = win32security.CreateWellKnownSid(win32security.WinNetworkSid)
        dacl = win32security.ACL()
        dacl.AddAccessDeniedAce(win32security.ACL_REVISION, con.GENERIC_ALL, network_sid)
        dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.GENERIC_READ | con.GENERIC_WRITE, user_sid)
        dacl.AddAccessAllowedAce(win32security.ACL_REVISION, con.GENERIC_ALL, system_sid)
        sd = win32security.SECURITY_DESCRIPTOR()
        sd.SetSecurityDescriptorOwner(user_sid, False)
        sd.SetSecurityDescriptorDacl(True, dacl, False)
        sa = win32security.SECURITY_ATTRIBUTES()
        sa.SECURITY_DESCRIPTOR = sd
        return sa

    def expect_core_pid(self, pid: int, token: str) -> None:
        self._core_pid = pid
        self._core_token = token

    def write_rendezvous(self) -> None:
        self.rendezvous.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.rendezvous.with_suffix(".tmp")
        tmp.write_text(json.dumps({"pipe": self.pipe_name, "ui_token": self.ui_token, "pid": os.getpid()}))
        os.replace(tmp, self.rendezvous)

    def remove_rendezvous(self) -> None:
        try:
            self.rendezvous.unlink()
        except OSError:
            pass

    def start(self) -> None:
        self.write_rendezvous()
        threading.Thread(target=self._accept_loop, daemon=True, name="pipe-accept").start()

    def stop(self) -> None:
        self._stop.set()
        self.remove_rendezvous()

    # ---------------------------------------------------------------- accept
    def _create_instance(self):
        import win32file  # type: ignore[import-not-found]
        import win32pipe  # type: ignore[import-not-found]
        open_mode = win32pipe.PIPE_ACCESS_DUPLEX | win32file.FILE_FLAG_OVERLAPPED
        if self._first:
            open_mode |= 0x00080000  # FILE_FLAG_FIRST_PIPE_INSTANCE
            self._first = False
        pipe_mode = win32pipe.PIPE_TYPE_BYTE | win32pipe.PIPE_READMODE_BYTE | win32pipe.PIPE_WAIT | 0x00000008  # REJECT_REMOTE
        return win32pipe.CreateNamedPipe(self.pipe_name, open_mode, pipe_mode, win32pipe.PIPE_UNLIMITED_INSTANCES,
                                         BUF, BUF, 0, self._sa)

    def _accept_loop(self) -> None:
        import pywintypes  # type: ignore[import-not-found]
        import win32event  # type: ignore[import-not-found]
        import win32pipe  # type: ignore[import-not-found]
        while not self._stop.is_set():
            h = self._create_instance()
            ov = pywintypes.OVERLAPPED()
            ov.hEvent = win32event.CreateEvent(None, True, False, None)
            try:
                rc = win32pipe.ConnectNamedPipe(h, ov)
            except pywintypes.error as e:
                if e.winerror == 535:  # ERROR_PIPE_CONNECTED
                    rc = 0
                else:
                    h.Close()
                    continue
            if rc != 0:
                while not self._stop.is_set():
                    if win32event.WaitForSingleObject(ov.hEvent, 500) == win32event.WAIT_OBJECT_0:
                        break
                if self._stop.is_set():
                    h.Close()
                    break
            threading.Thread(target=self._serve, args=(h,), daemon=True, name="pipe-conn").start()

    # ---------------------------------------------------------------- one connection
    def _serve(self, h) -> None:
        import pywintypes  # type: ignore[import-not-found]
        import win32event  # type: ignore[import-not-found]
        import win32file  # type: ignore[import-not-found]
        import win32pipe  # type: ignore[import-not-found]

        write_lock = threading.Lock()

        def send(msg: dict[str, Any]) -> None:
            data = encode_frame(msg)
            with write_lock:
                ov = pywintypes.OVERLAPPED()
                ov.hEvent = win32event.CreateEvent(None, True, False, None)
                win32file.WriteFile(h, data, ov)
                win32file.GetOverlappedResult(h, ov, True)

        try:
            pid = win32pipe.GetNamedPipeClientProcessId(h)
        except pywintypes.error:
            h.Close()
            return
        image = _image_path(pid)
        info = ClientInfo(role="", client_id=f"pipe-{new_id('c')}", pid=pid, image=image, send=None)
        decoder = FrameDecoder()
        authed = False
        try:
            while not self._stop.is_set():
                ov = pywintypes.OVERLAPPED()
                ov.hEvent = win32event.CreateEvent(None, True, False, None)
                buf = win32file.AllocateReadBuffer(BUF)
                try:
                    win32file.ReadFile(h, buf, ov)
                    n = win32file.GetOverlappedResult(h, ov, True)
                except pywintypes.error:
                    break
                if n == 0:
                    break
                for msg in decoder.feed(bytes(buf[:n])):
                    if not authed:
                        authed = self._handshake(msg, info)
                        if not authed:
                            send({"type": "res", "id": 0, "ok": False, "error": {"code": "handshake_failed", "message": "rejected", "details": {}}})
                            return
                        info.send = send if info.role == "ui" else None
                        self.dispatcher.register_client(info)
                        send({"type": "res", "id": 0, "ok": True, "result": {"hello": "ok", "role": info.role}})
                        continue
                    fut = self.dispatcher.pool.submit(self.dispatcher.handle, info, msg)
                    fut.add_done_callback(lambda f: _safe_send(send, f.result()))
        except FrameError:
            self.gw.audit.write("ipc.bad_frame", "security", pid=pid, severity="medium")
        finally:
            if authed:
                self.dispatcher.unregister_client(info.client_id)
            try:
                win32pipe.DisconnectNamedPipe(h)
            except pywintypes.error:
                pass
            h.Close()

    def _handshake(self, msg: dict[str, Any], info: ClientInfo) -> bool:
        role, token = msg.get("role"), str(msg.get("token", ""))
        ok = False
        reason = "bad token"
        if msg.get("type") == "hello" and role == "ui":
            ok = secrets.compare_digest(token, self.ui_token)
            ok = ok and self._image_ok(info.image, UI_EXE)
            reason = "ui image/signature" if not ok else ""
        elif msg.get("type") == "hello" and role == "core" and self._core_token:
            ok = secrets.compare_digest(token, self._core_token) and info.pid == self._core_pid
            ok = ok and self._image_ok(info.image, CORE_EXE)
            reason = "core pid/token/image" if not ok else ""
        if ok:
            info.role = str(role)
            self._failures = 0
            return True
        self._failures += 1
        self.gw.audit.write("ipc.handshake_failed", "security", pid=info.pid, image=info.image, reason=reason, severity="high")
        if self._failures >= 5:
            self.gw.home_event("ipc_attack", "high", "Repeated failed connections to the agent",
                               "Another program tried to connect to the gateway without the right credentials.")
            self._failures = 0
        return False

    def _image_ok(self, image: str | None, expected_exe: str) -> bool:
        if not RELEASE_BUILD and dev_mode():
            return True  # developer mode: clients run as python.exe / dev Tauri build (Posture shows this)
        if not image or self.install_dir is None:
            return False
        p = Path(image)
        if p.name.lower() != expected_exe or p.parent.resolve() != self.install_dir.resolve():
            return False
        return authenticode_valid(str(p))


def _safe_send(send, msg) -> None:
    try:
        send(msg)
    except Exception:  # noqa: BLE001
        pass
