"""Windows session / power events (spec 4.7):
  - Windows locks                -> lock the UI (if "Lock when Windows locks" is on)
  - sign-out, shutdown, restart  -> wipe keys (missions pause until the next password sign-in)
  - sleep/hibernate              -> wipe keys unless BitLocker is on and the setting allows keeping them
Implemented with a hidden message-only window + WTSRegisterSessionNotification.
"""
from __future__ import annotations

import threading

WM_WTSSESSION_CHANGE = 0x02B1
WM_POWERBROADCAST = 0x0218
WM_QUERYENDSESSION = 0x0011
WM_ENDSESSION = 0x0016
WTS_SESSION_LOCK = 0x7
WTS_SESSION_LOGOFF = 0x6
PBT_APMSUSPEND = 0x4


def start(gw) -> None:
    threading.Thread(target=_run, args=(gw,), daemon=True, name="win-session").start()


def _run(gw) -> None:
    import win32api  # type: ignore[import-not-found]
    import win32con  # type: ignore[import-not-found]
    import win32gui  # type: ignore[import-not-found]
    import win32ts  # type: ignore[import-not-found]

    from .posture import bitlocker_status

    def wndproc(hwnd, msg, wparam, lparam):
        try:
            if msg == WM_WTSSESSION_CHANGE:
                if wparam == WTS_SESSION_LOCK and gw.db is not None and gw.settings.get("security.lock_on_windows_lock"):
                    gw.lock_ui("windows locked")
                elif wparam == WTS_SESSION_LOGOFF:
                    gw.sign_out("windows sign-out")
            elif msg == WM_POWERBROADCAST and wparam == PBT_APMSUSPEND:
                keep = gw.db is not None and gw.settings.get("security.wipe_keys_on_sleep") == "if_no_bitlocker" \
                    and bitlocker_status() == "on"
                if not keep:
                    gw.sign_out("sleep/hibernate")
                else:
                    gw.lock_ui("sleep")
            elif msg in (WM_QUERYENDSESSION, WM_ENDSESSION):
                gw.sign_out("windows shutdown")
                return True
        except Exception:  # noqa: BLE001
            pass
        return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)

    wc = win32gui.WNDCLASS()
    wc.lpfnWndProc = wndproc
    wc.lpszClassName = "PersonalAgentSessionWatcher"
    wc.hInstance = win32api.GetModuleHandle(None)
    atom = win32gui.RegisterClass(wc)
    hwnd = win32gui.CreateWindow(atom, "PersonalAgentSessionWatcher", 0, 0, 0, 0, 0, 0, 0, wc.hInstance, None)
    win32ts.WTSRegisterSessionNotification(hwnd, win32ts.NOTIFY_FOR_THIS_SESSION)
    _ = win32con
    win32gui.PumpMessages()


def harden_process() -> None:
    """No crash dumps with key material: exclude from WER and suppress fault dialogs (spec 4.7)."""
    import ctypes
    import sys
    try:
        ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002 | 0x8000)  # FAILCRITICALERRORS|NOGPFAULTERRORBOX|NOOPENFILEERRORBOX
        ctypes.windll.wer.WerAddExcludedApplication(ctypes.c_wchar_p(sys.executable), False)
        ctypes.windll.kernel32.SetProcessDEPPolicy(1)
    except Exception:  # noqa: BLE001
        pass


def single_instance() -> object | None:
    import win32api  # type: ignore[import-not-found]
    import win32event  # type: ignore[import-not-found]
    import winerror  # type: ignore[import-not-found]
    m = win32event.CreateMutex(None, False, "Local\\PersonalAgent-Gateway")
    if win32api.GetLastError() == winerror.ERROR_ALREADY_EXISTS:
        return None
    return m
