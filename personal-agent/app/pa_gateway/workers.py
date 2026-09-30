"""Launching helper processes (pa-parser, pa-outlook-worker, sandbox runners) with least privilege.

- scrubbed environment (no tokens, no proxy settings, no user profile paths beyond what Windows needs)
- Windows: created SUSPENDED, assigned to a Job Object (memory cap, 1 active process, kill-on-close,
  UI restrictions), then resumed - so limits apply before the first instruction runs
- firewall: the installer blocks outbound traffic for every worker executable (spec 14.2);
  in developer mode (python.exe) that per-program rule cannot distinguish processes - the
  Security Posture page reports this.
"""
from __future__ import annotations

import ctypes
import os
import subprocess
import sys
from pathlib import Path

WORKER_EXES = {
    "pa_workers.parser": "pa-parser.exe",
    "pa_workers.outlook": "pa-outlook-worker.exe",
    "pa_workers.sandbox.runner": "pa-sandbox-runner.exe",
    "pa_core": "pa-core.exe",
}
SAFE_ENV_KEYS = ("SYSTEMROOT", "WINDIR", "TEMP", "TMP", "PATHEXT", "COMSPEC", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE",
                 "LOCALAPPDATA", "APPDATA", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "PA_DEV_MODE", "PA_TEST_FAST_KDF", "LANG")


def frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def worker_command(module: str) -> list[str]:
    if frozen():
        return [str(Path(sys.executable).with_name(WORKER_EXES[module]))]
    return [sys.executable, "-m", module]


def scrubbed_env(extra: dict[str, str] | None = None, tmp: Path | None = None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k.upper() in SAFE_ENV_KEYS}
    if not frozen():
        # developer mode: the worker needs to import our packages
        app_dir = str(Path(__file__).resolve().parents[1])
        env["PYTHONPATH"] = app_dir
        if sys.platform == "win32":
            env["PATH"] = os.environ.get("PATH", "")
        else:
            env["PATH"] = "/usr/bin:/bin"
    elif sys.platform == "win32":
        env["PATH"] = os.environ.get("SYSTEMROOT", r"C:\Windows") + r"\System32"
    if tmp:
        env["TEMP"] = env["TMP"] = str(tmp)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONNOUSERSITE"] = "1"
    env.update(extra or {})
    return env


class JobLimits:
    def __init__(self, memory_mb: int = 1024, cpu_seconds: int | None = None, max_processes: int = 1):
        self.memory_mb = memory_mb
        self.cpu_seconds = cpu_seconds
        self.max_processes = max_processes


def _job_for(limits: JobLimits):
    import win32job  # type: ignore[import-not-found]

    job = win32job.CreateJobObject(None, "")
    info = win32job.QueryInformationJobObject(job, win32job.JobObjectExtendedLimitInformation)
    flags = (win32job.JOB_OBJECT_LIMIT_PROCESS_MEMORY | win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
             | win32job.JOB_OBJECT_LIMIT_ACTIVE_PROCESS | win32job.JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION)
    info["ProcessMemoryLimit"] = limits.memory_mb * 1024 * 1024
    info["BasicLimitInformation"]["ActiveProcessLimit"] = limits.max_processes
    if limits.cpu_seconds:
        flags |= win32job.JOB_OBJECT_LIMIT_PROCESS_TIME
        info["BasicLimitInformation"]["PerProcessUserTimeLimit"] = limits.cpu_seconds * 10_000_000
    info["BasicLimitInformation"]["LimitFlags"] = flags
    win32job.SetInformationJobObject(job, win32job.JobObjectExtendedLimitInformation, info)
    ui = win32job.QueryInformationJobObject(job, win32job.JobObjectBasicUIRestrictions)
    ui["UIRestrictionsClass"] = (win32job.JOB_OBJECT_UILIMIT_DESKTOP | win32job.JOB_OBJECT_UILIMIT_DISPLAYSETTINGS
                                 | win32job.JOB_OBJECT_UILIMIT_EXITWINDOWS | win32job.JOB_OBJECT_UILIMIT_GLOBALATOMS
                                 | win32job.JOB_OBJECT_UILIMIT_HANDLES | win32job.JOB_OBJECT_UILIMIT_READCLIPBOARD
                                 | win32job.JOB_OBJECT_UILIMIT_WRITECLIPBOARD | win32job.JOB_OBJECT_UILIMIT_SYSTEMPARAMETERS)
    win32job.SetInformationJobObject(job, win32job.JobObjectBasicUIRestrictions, ui)
    return job


def popen_limited(cmd: list[str], *, cwd: Path | None, env: dict[str, str], limits: JobLimits | None,
                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) -> tuple[subprocess.Popen, object]:
    """Start a process with Job Object limits (Windows). Returns (process, job_handle_or_None)."""
    if sys.platform != "win32" or limits is None:
        p = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=stdin, stdout=stdout, stderr=stderr)
        return p, None
    import win32api  # type: ignore[import-not-found]
    import win32con  # type: ignore[import-not-found]
    import win32job  # type: ignore[import-not-found]

    CREATE_SUSPENDED = 0x00000004
    CREATE_NO_WINDOW = 0x08000000
    job = _job_for(limits)
    p = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=stdin, stdout=stdout, stderr=stderr,
                         creationflags=CREATE_SUSPENDED | CREATE_NO_WINDOW)
    try:
        h = win32api.OpenProcess(win32con.PROCESS_SET_QUOTA | win32con.PROCESS_TERMINATE | 0x0800, False, p.pid)
        win32job.AssignProcessToJobObject(job, h)
        ctypes.windll.ntdll.NtResumeProcess(ctypes.c_void_p(int(h)))
        win32api.CloseHandle(h)
    except Exception:
        p.kill()
        raise
    return p, job


def run_worker(module: str, stdin_bytes: bytes, timeout: float, cwd: Path, memory_mb: int = 1024) -> bytes:
    p, job = popen_limited(worker_command(module), cwd=cwd, env=scrubbed_env(tmp=cwd),
                           limits=JobLimits(memory_mb=memory_mb, cpu_seconds=int(timeout),
                                                            max_processes=1 if frozen() else 4))  # venv launcher = 2 procs
    try:
        out, _err = p.communicate(stdin_bytes, timeout=timeout)
        return out
    except subprocess.TimeoutExpired:
        p.kill()
        p.communicate()
        raise
    finally:
        del job  # closing the job handle kills anything left (KILL_ON_JOB_CLOSE)
