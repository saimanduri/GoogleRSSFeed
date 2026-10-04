"""CPU, memory, GPU and storage use of THIS application and of the whole PC (Settings > Diagnostics & About).

Read-only and local: Windows API through ctypes on Windows (no extra dependency), /proc on Linux (developer runs and
tests). "This application" = the gateway, every process it started (pa-core, workers, model runtime) and the window
(pa-ui.exe). Process names and numbers only - nothing about other programs is kept or returned.
"""
from __future__ import annotations

import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Any

RED_AT = 90.0      # the user's threshold: shown in red
ORANGE_AT = 75.0   # early warning: shown in orange
APP_EXES = ("pa-ui.exe", "pa-gateway.exe", "pa-core.exe", "pa-parser.exe", "pa-outlook-worker.exe", "llama-server.exe")
DIR_SIZE_TTL = 120.0


def level(pct: float | None) -> str:
    """ok / warn / critical for one percentage (None = unknown)."""
    if pct is None:
        return "unknown"
    return "critical" if pct >= RED_AT else "warn" if pct >= ORANGE_AT else "ok"


def meter(label: str, pct: float | None, detail: str, app_pct: float | None = None, app_detail: str = "") -> dict[str, Any]:
    p = None if pct is None else round(max(0.0, min(100.0, pct)), 1)
    a = None if app_pct is None else round(max(0.0, min(100.0, app_pct)), 1)
    worst = max([x for x in (p, a) if x is not None], default=None)
    return {"label": label, "pct": p, "detail": detail, "app_pct": a, "app_detail": app_detail, "level": level(worst)}


def app_pids(all_procs: list[tuple[int, int, str]], root_pid: int) -> set[int]:
    """The gateway, all its descendants, and any process of the app by name (the window is started by Windows, not by us).
    all_procs: (pid, parent_pid, exe_name)."""
    children: dict[int, list[int]] = {}
    for pid, ppid, _ in all_procs:
        children.setdefault(ppid, []).append(pid)
    out, todo = set(), [root_pid]
    while todo:
        p = todo.pop()
        if p in out:
            continue
        out.add(p)
        todo += children.get(p, [])
    out |= {pid for pid, _, name in all_procs if name.lower() in APP_EXES}
    return out


def dir_size(path: Path, limit_files: int = 200_000) -> int:
    total, n, stack = 0, 0, [path]
    while stack and n < limit_files:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    n += 1
                    try:
                        if e.is_symlink():
                            continue
                        if e.is_dir(follow_symlinks=False):
                            stack.append(Path(e.path))
                        else:
                            total += e.stat(follow_symlinks=False).st_size
                    except OSError:
                        pass
        except OSError:
            pass
    return total


class _Probe:
    """Platform part: process list, per-process CPU time + memory, system CPU times + memory."""

    def processes(self) -> list[tuple[int, int, str]]:
        raise NotImplementedError

    def proc_stats(self, pid: int) -> tuple[float, int] | None:
        """(cpu seconds used so far, resident memory bytes)"""
        raise NotImplementedError

    def system_cpu(self) -> tuple[float, float]:
        """(busy seconds, total seconds) since boot, all cores together"""
        raise NotImplementedError

    def system_memory(self) -> tuple[int, int]:
        """(used bytes, total bytes)"""
        raise NotImplementedError


class _LinuxProbe(_Probe):
    tick = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
    page = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096

    def processes(self):
        out = []
        for d in os.listdir("/proc"):
            if d.isdigit():
                try:
                    stat = Path(f"/proc/{d}/stat").read_text()
                    name = stat[stat.index("(") + 1:stat.rindex(")")]
                    ppid = int(stat[stat.rindex(")") + 2:].split()[1])
                    out.append((int(d), ppid, name))
                except (OSError, ValueError):
                    pass
        return out

    def proc_stats(self, pid):
        try:
            f = Path(f"/proc/{pid}/stat").read_text()[1:].split(")")[-1].split()
            cpu = (int(f[11]) + int(f[12])) / self.tick
            rss = int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * self.page
            return cpu, rss
        except (OSError, ValueError, IndexError):
            return None

    def system_cpu(self):
        v = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
        idle = v[3] + (v[4] if len(v) > 4 else 0)
        return (sum(v) - idle) / self.tick, sum(v) / self.tick

    def system_memory(self):
        info = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            k, _, rest = line.partition(":")
            info[k] = int(rest.split()[0]) * 1024
        total = info.get("MemTotal", 0)
        return total - info.get("MemAvailable", info.get("MemFree", 0)), total


class _WinProbe(_Probe):
    def __init__(self) -> None:
        import ctypes
        from ctypes import wintypes as wt
        self.c, self.wt = ctypes, wt
        self.k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self.psapi = ctypes.WinDLL("psapi", use_last_error=True)

        class PE(ctypes.Structure):
            _fields_ = [("dwSize", wt.DWORD), ("cntUsage", wt.DWORD), ("th32ProcessID", wt.DWORD), ("th32DefaultHeapID", ctypes.c_void_p),
                        ("th32ModuleID", wt.DWORD), ("cntThreads", wt.DWORD), ("th32ParentProcessID", wt.DWORD), ("pcPriClassBase", wt.LONG),
                        ("dwFlags", wt.DWORD), ("szExeFile", wt.WCHAR * 260)]

        class PMC(ctypes.Structure):
            _fields_ = [("cb", wt.DWORD), ("PageFaultCount", wt.DWORD), ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]

        class MS(ctypes.Structure):
            _fields_ = [("dwLength", wt.DWORD), ("dwMemoryLoad", wt.DWORD), ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong), ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong), ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
        self.PE, self.PMC, self.MS = PE, PMC, MS
        k = self.k32
        k.CreateToolhelp32Snapshot.argtypes, k.CreateToolhelp32Snapshot.restype = [wt.DWORD, wt.DWORD], wt.HANDLE
        k.Process32FirstW.argtypes = k.Process32NextW.argtypes = [wt.HANDLE, ctypes.POINTER(PE)]
        k.OpenProcess.argtypes, k.OpenProcess.restype = [wt.DWORD, wt.BOOL, wt.DWORD], wt.HANDLE
        k.CloseHandle.argtypes = [wt.HANDLE]
        k.GetProcessTimes.argtypes = [wt.HANDLE] + [ctypes.POINTER(wt.FILETIME)] * 4
        k.GetSystemTimes.argtypes = [ctypes.POINTER(wt.FILETIME)] * 3
        k.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(MS)]
        self.psapi.GetProcessMemoryInfo.argtypes = [wt.HANDLE, ctypes.POINTER(PMC), wt.DWORD]

    @staticmethod
    def _ft(ft) -> float:
        return ((ft.dwHighDateTime << 32) | ft.dwLowDateTime) / 1e7

    def processes(self):
        c, k = self.c, self.k32
        snap = k.CreateToolhelp32Snapshot(0x2, 0)          # TH32CS_SNAPPROCESS
        if not snap or snap == self.wt.HANDLE(-1).value:
            return []
        out = []
        try:
            e = self.PE()
            e.dwSize = c.sizeof(self.PE)
            ok = k.Process32FirstW(snap, c.byref(e))
            while ok:
                out.append((int(e.th32ProcessID), int(e.th32ParentProcessID), e.szExeFile))
                ok = k.Process32NextW(snap, c.byref(e))
        finally:
            k.CloseHandle(snap)
        return out

    def proc_stats(self, pid):
        c, k, wt = self.c, self.k32, self.wt
        h = k.OpenProcess(0x1000 | 0x0010, False, pid)     # QUERY_LIMITED_INFORMATION | VM_READ
        if not h:
            h = k.OpenProcess(0x1000, False, pid)
        if not h:
            return None
        try:
            t = [wt.FILETIME() for _ in range(4)]
            if not k.GetProcessTimes(h, *[c.byref(x) for x in t]):
                return None
            pmc = self.PMC()
            pmc.cb = c.sizeof(self.PMC)
            rss = int(pmc.WorkingSetSize) if self.psapi.GetProcessMemoryInfo(h, c.byref(pmc), pmc.cb) else 0
            return self._ft(t[2]) + self._ft(t[3]), rss
        finally:
            k.CloseHandle(h)

    def system_cpu(self):
        wt, c = self.wt, self.c
        idle, kern, user = wt.FILETIME(), wt.FILETIME(), wt.FILETIME()
        self.k32.GetSystemTimes(c.byref(idle), c.byref(kern), c.byref(user))
        total = self._ft(kern) + self._ft(user)               # kernel time includes idle time
        return total - self._ft(idle), total

    def system_memory(self):
        m = self.MS()
        m.dwLength = self.c.sizeof(self.MS)
        self.k32.GlobalMemoryStatusEx(self.c.byref(m))
        return int(m.ullTotalPhys - m.ullAvailPhys), int(m.ullTotalPhys)


class ResourceMonitor:
    def __init__(self, data_dir: Path, gpu=None, root_pid: int | None = None, probe: _Probe | None = None) -> None:
        self.data_dir = Path(data_dir)
        self.gpu = gpu
        self.root_pid = root_pid or os.getpid()
        self._probe = probe
        self._prev: tuple[float, float, float, float] | None = None     # (sys busy, sys total, app cpu, wall time)
        self._dir_cache: tuple[float, int] | None = None
        self._lock = threading.Lock()

    @property
    def probe(self) -> _Probe | None:
        if self._probe is None:
            try:
                self._probe = _WinProbe() if sys.platform == "win32" else _LinuxProbe()
            except Exception:  # noqa: BLE001 - diagnostics must never break the screen
                return None
        return self._probe

    def _data_size(self) -> int:
        now = time.time()
        if self._dir_cache is None or now - self._dir_cache[0] > DIR_SIZE_TTL:
            self._dir_cache = (now, dir_size(self.data_dir))
        return self._dir_cache[1]

    def snapshot(self) -> dict[str, Any]:
        """One reading. CPU % is measured between two calls (the first call samples for a quarter of a second)."""
        with self._lock:
            return self._snapshot()

    def _snapshot(self) -> dict[str, Any]:
        pr = self.probe
        meters: list[dict[str, Any]] = []
        procs: list[dict[str, Any]] = []
        ncpu = os.cpu_count() or 1
        if pr is not None:
            try:
                plist = pr.processes()
                mine = app_pids(plist, self.root_pid)
                names = {pid: name for pid, _, name in plist}

                def app_cpu_and_mem() -> tuple[float, int]:
                    cpu, mem = 0.0, 0
                    procs.clear()
                    for pid in sorted(mine):
                        st = pr.proc_stats(pid)
                        if st:
                            cpu += st[0]
                            mem += st[1]
                            procs.append({"name": names.get(pid, "?"), "pid": pid, "memory_mb": round(st[1] / 2**20)})
                    return cpu, mem

                busy, total = pr.system_cpu()
                app_cpu, app_mem = app_cpu_and_mem()
                wall = time.monotonic()
                if self._prev is None:                     # prime the rate counters once
                    time.sleep(0.25)
                    self._prev = (busy, total, app_cpu, wall)
                    busy, total = pr.system_cpu()
                    app_cpu, app_mem = app_cpu_and_mem()
                    wall = time.monotonic()
                pb, pt, pa, pw = self._prev
                self._prev = (busy, total, app_cpu, wall)
                sys_pct = 100.0 * (busy - pb) / (total - pt) if total > pt else None
                elapsed = max(wall - pw, 1e-6)
                app_pct = 100.0 * (app_cpu - pa) / (elapsed * ncpu) if app_cpu >= pa else None
                meters.append(meter("CPU", sys_pct, f"{ncpu} logical processors", app_pct, "this app"))
                used, mtotal = pr.system_memory()
                meters.append(meter("Memory", 100.0 * used / mtotal if mtotal else None,
                                    f"{used / 2**30:.1f} of {mtotal / 2**30:.1f} GB used",
                                    100.0 * app_mem / mtotal if mtotal else None, f"this app {app_mem / 2**20:.0f} MB"))
            except Exception:  # noqa: BLE001
                meters.append(meter("CPU", None, "not available"))
                meters.append(meter("Memory", None, "not available"))
        if self.gpu is not None:
            g = self.gpu.usage()
            if g.get("available") and g.get("history") is not None:
                meters.append(meter("GPU", float(g.get("util") or 0.0), f"whole PC · {g.get('vram_mb', 0)} MB video memory in use"))
            else:
                meters.append(meter("GPU", None, "no GPU counters on this PC"))
        try:
            du = shutil.disk_usage(self.data_dir)
            size = self._data_size()
            meters.append(meter("Storage", 100.0 * du.used / du.total if du.total else None,
                                f"{du.free / 1e9:.1f} GB free of {du.total / 1e9:.0f} GB on the data drive",
                                100.0 * size / du.total if du.total else None, f"app data {size / 2**20:.0f} MB"))
        except OSError:
            meters.append(meter("Storage", None, "not available"))
        return {"meters": meters, "processes": procs, "red_at": RED_AT, "orange_at": ORANGE_AT}
