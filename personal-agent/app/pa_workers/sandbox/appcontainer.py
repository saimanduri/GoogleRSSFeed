"""AppContainer launcher (spec 19.2 "Standard isolation").

Runs a process inside an AppContainer with NO capabilities: no network (no internetClient /
privateNetwork capability, loopback exempt NOT granted), no access to user folders, low integrity.
Only the per-run folder (and the Python runtime, read/execute) are granted to the container SID.
The process is also placed in a Job Object (CPU/memory/process-count/wall-time limits).
"""
from __future__ import annotations

import ctypes
import ctypes.wintypes as wt
import subprocess
from pathlib import Path

PROFILE_NAME = "PersonalAgent.Sandbox"
PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES = 0x00020009
EXTENDED_STARTUPINFO_PRESENT = 0x00080000
CREATE_SUSPENDED = 0x00000004
CREATE_NO_WINDOW = 0x08000000
CREATE_UNICODE_ENVIRONMENT = 0x00000400
STARTF_USESTDHANDLES = 0x00000100
HRESULT_ALREADY_EXISTS = 0x800700B7


class SECURITY_CAPABILITIES(ctypes.Structure):
    _fields_ = [("AppContainerSid", ctypes.c_void_p), ("Capabilities", ctypes.c_void_p),
                ("CapabilityCount", wt.DWORD), ("Reserved", wt.DWORD)]


class STARTUPINFOW(ctypes.Structure):
    _fields_ = [("cb", wt.DWORD), ("lpReserved", wt.LPWSTR), ("lpDesktop", wt.LPWSTR), ("lpTitle", wt.LPWSTR),
                ("dwX", wt.DWORD), ("dwY", wt.DWORD), ("dwXSize", wt.DWORD), ("dwYSize", wt.DWORD),
                ("dwXCountChars", wt.DWORD), ("dwYCountChars", wt.DWORD), ("dwFillAttribute", wt.DWORD),
                ("dwFlags", wt.DWORD), ("wShowWindow", wt.WORD), ("cbReserved2", wt.WORD), ("lpReserved2", ctypes.c_void_p),
                ("hStdInput", wt.HANDLE), ("hStdOutput", wt.HANDLE), ("hStdError", wt.HANDLE)]


class STARTUPINFOEXW(ctypes.Structure):
    _fields_ = [("StartupInfo", STARTUPINFOW), ("lpAttributeList", ctypes.c_void_p)]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [("hProcess", wt.HANDLE), ("hThread", wt.HANDLE), ("dwProcessId", wt.DWORD), ("dwThreadId", wt.DWORD)]


def container_sid() -> tuple[ctypes.c_void_p, str]:
    """Create (or open) the AppContainer profile; returns (PSID, string SID)."""
    userenv = ctypes.WinDLL("userenv")
    advapi = ctypes.WinDLL("advapi32")
    sid = ctypes.c_void_p()
    hr = userenv.CreateAppContainerProfile(ctypes.c_wchar_p(PROFILE_NAME), ctypes.c_wchar_p("Personal Agent sandbox"),
                                           ctypes.c_wchar_p("Isolated Python analysis"), None, 0, ctypes.byref(sid))
    if hr & 0xFFFFFFFF == HRESULT_ALREADY_EXISTS:
        hr = userenv.DeriveAppContainerSidFromAppContainerName(ctypes.c_wchar_p(PROFILE_NAME), ctypes.byref(sid))
    if hr != 0:
        raise OSError(f"AppContainer profile error 0x{hr & 0xFFFFFFFF:08X}")
    s = ctypes.c_wchar_p()
    advapi.ConvertSidToStringSidW(sid, ctypes.byref(s))
    return sid, s.value or ""


def grant(path: Path, sid_str: str, write: bool) -> None:
    """Grant the container SID access to one folder (icacls, inheritable)."""
    perm = "(OI)(CI)M" if write else "(OI)(CI)RX"
    subprocess.run(["icacls", str(path), "/grant", f"*{sid_str}:{perm}", "/T", "/Q"], capture_output=True,
                   creationflags=CREATE_NO_WINDOW, check=False)


def available() -> bool:
    try:
        container_sid()
        return True
    except (OSError, AttributeError):
        return False


def run_in_appcontainer(cmdline: str, cwd: Path, stdout_path: Path, stderr_path: Path, timeout: float,
                        memory_mb: int = 1024) -> int:
    """Launch `cmdline` in the AppContainer; returns exit code (or -1 on timeout, process killed)."""
    import msvcrt

    import win32job  # type: ignore[import-not-found]

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    sid, _ = container_sid()
    caps = SECURITY_CAPABILITIES(sid, None, 0, 0)
    size = ctypes.c_size_t()
    k32.InitializeProcThreadAttributeList(None, 1, 0, ctypes.byref(size))
    attr = ctypes.create_string_buffer(size.value)
    if not k32.InitializeProcThreadAttributeList(attr, 1, 0, ctypes.byref(size)):
        raise ctypes.WinError(ctypes.get_last_error())
    if not k32.UpdateProcThreadAttribute(attr, 0, ctypes.c_size_t(PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES),
                                         ctypes.byref(caps), ctypes.sizeof(caps), None, None):
        raise ctypes.WinError(ctypes.get_last_error())
    out_f = open(stdout_path, "wb")
    err_f = open(stderr_path, "wb")
    try:
        for f in (out_f, err_f):
            k32.SetHandleInformation(wt.HANDLE(msvcrt.get_osfhandle(f.fileno())), 1, 1)  # HANDLE_FLAG_INHERIT
        si = STARTUPINFOEXW()
        si.StartupInfo.cb = ctypes.sizeof(STARTUPINFOEXW)
        si.StartupInfo.dwFlags = STARTF_USESTDHANDLES
        si.StartupInfo.hStdOutput = msvcrt.get_osfhandle(out_f.fileno())
        si.StartupInfo.hStdError = msvcrt.get_osfhandle(err_f.fileno())
        si.StartupInfo.hStdInput = None
        si.lpAttributeList = ctypes.cast(attr, ctypes.c_void_p)
        pi = PROCESS_INFORMATION()
        buf = ctypes.create_unicode_buffer(cmdline)
        env = "SYSTEMROOT=C:\\Windows\0PYTHONDONTWRITEBYTECODE=1\0PYTHONNOUSERSITE=1\0PYTHONIOENCODING=utf-8\0\0"
        ok = k32.CreateProcessW(None, buf, None, None, True,
                                EXTENDED_STARTUPINFO_PRESENT | CREATE_SUSPENDED | CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT,
                                ctypes.c_wchar_p(env), ctypes.c_wchar_p(str(cwd)), ctypes.byref(si), ctypes.byref(pi))
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())
        job = win32job.CreateJobObject(None, "")
        info = win32job.QueryInformationJobObject(job, win32job.JobObjectExtendedLimitInformation)
        info["ProcessMemoryLimit"] = memory_mb * 1024 * 1024
        info["BasicLimitInformation"]["ActiveProcessLimit"] = 1
        info["BasicLimitInformation"]["PerProcessUserTimeLimit"] = int(timeout) * 10_000_000
        info["BasicLimitInformation"]["LimitFlags"] = (win32job.JOB_OBJECT_LIMIT_PROCESS_MEMORY | win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                                                       | win32job.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | win32job.JOB_OBJECT_LIMIT_PROCESS_TIME)
        win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, info)
        import win32api  # type: ignore[import-not-found]
        win32job.AssignProcessToJobObject(job, win32api.OpenProcess(0x1F0FFF, False, pi.dwProcessId))
        k32.ResumeThread(pi.hThread)
        rc = k32.WaitForSingleObject(pi.hProcess, int(timeout * 1000))
        if rc != 0:  # WAIT_TIMEOUT
            k32.TerminateProcess(pi.hProcess, 1)
            code = -1
        else:
            ec = wt.DWORD()
            k32.GetExitCodeProcess(pi.hProcess, ctypes.byref(ec))
            code = int(ec.value)
        k32.CloseHandle(pi.hThread)
        k32.CloseHandle(pi.hProcess)
        del job
        return code
    finally:
        out_f.close()
        err_f.close()
        k32.DeleteProcThreadAttributeList(attr)
