"""Sandboxed Python execution (spec 19). Model-written code runs ONLY here; no host shell tool exists.

Strength (shown in the UI and on the Security Posture page):
  STRONG     Windows Sandbox (Hyper-V): disposable VM, networking/vGPU/audio/video/printer/clipboard
             disabled, inputs mapped read-only, one output folder, destroyed after the run
  STANDARD   AppContainer (no capabilities => no network, no user folders, low integrity) + Job Object
  UNAVAILABLE python.run is disabled and Settings explains how to enable Windows Sandbox
Only files explicitly granted to the task are copied in (by file ID). Outputs are untrusted, go
through the file pipeline and inherit the task's sensitivity. No secrets, tokens, vault or profile
paths are ever visible inside the sandbox (fresh folder, scrubbed environment).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from pa_common.ids import new_id

from .tools.base import ExecContext, ToolFailed, ToolResult

WSB_EXE = Path(os.environ.get("SYSTEMROOT", r"C:\Windows")) / "System32" / "WindowsSandbox.exe"
MAX_STDOUT = 64 * 1024
MAX_OUTPUT_FILES = 10


def _sandbox_python() -> Path | None:
    """Pinned portable Python for sandbox runs. Release: <install>/sandbox-python/python.exe."""
    if getattr(sys, "frozen", False):
        p = Path(sys.executable).with_name("sandbox-python") / "python.exe"
        return p if p.exists() else None
    env = os.environ.get("PA_SANDBOX_PYTHON")
    if env and Path(env).exists():
        return Path(env)
    base = Path(getattr(sys, "_base_executable", sys.executable))
    return base if base.exists() else None


class SandboxService:
    def __init__(self, gw):
        self.gw = gw
        self._wsb_lock = threading.Lock()  # Windows allows one Windows Sandbox at a time
        self._current: subprocess.Popen | None = None
        self._strength: str | None = None

    def strength(self, refresh: bool = False) -> str:
        if self._strength is not None and not refresh:
            return self._strength
        if os.environ.get("PA_SANDBOX_FORCE") in ("STRONG", "STANDARD", "UNAVAILABLE") and not getattr(sys, "frozen", False):
            self._strength = os.environ["PA_SANDBOX_FORCE"]
        elif sys.platform != "win32" or _sandbox_python() is None:
            self._strength = "UNAVAILABLE"
        elif WSB_EXE.exists():
            self._strength = "STRONG"
        else:
            from pa_workers.sandbox import appcontainer
            self._strength = "STANDARD" if appcontainer.available() else "UNAVAILABLE"
        return self._strength

    def available(self) -> bool:
        return (self.strength() != "UNAVAILABLE" and bool(self.gw.settings.get("tools.python_enabled"))
                and not self.gw.killswitch.active("disable_sandbox"))

    def describe(self) -> dict[str, Any]:
        s = self.strength()
        return {"strength": s, "label": {"STRONG": "Strong isolation (Windows Sandbox)", "STANDARD": "Standard isolation (AppContainer)",
                                         "UNAVAILABLE": "Unavailable - Python analysis is disabled"}[s],
                "help": "" if s == "STRONG" else "Enable 'Windows Sandbox' in Windows Features (Windows 11 Pro/Enterprise/Education, "
                                                  "virtualization on in firmware) for strong isolation."}

    def terminate(self) -> None:
        p = self._current
        if p and p.poll() is None:
            p.kill()
        if sys.platform == "win32":
            for name in ("WindowsSandboxClient.exe", "WindowsSandboxRemoteSession.exe", "WindowsSandbox.exe"):
                subprocess.run(["taskkill", "/F", "/IM", name], capture_output=True, check=False,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

    # ------------------------------------------------------------------ tool
    def run_tool(self, args: dict[str, Any], ctx: ExecContext) -> ToolResult:
        if not self.available():
            raise ToolFailed("the sandbox is not available")
        run_dir = self.gw.paths.tmp_dir / f"sbx-{new_id('r')}"
        in_dir, out_dir = run_dir / "in", run_dir / "out"
        in_dir.mkdir(parents=True)
        out_dir.mkdir()
        try:
            for fid in args["file_ids"]:
                f = self.gw.files.get(fid)
                safe = "".join(c for c in f["name"] if c.isalnum() or c in "._- ")[:100] or fid
                (in_dir / safe).write_bytes(self.gw.files.read_bytes(fid))
            (in_dir / "run.py").write_text(_WRAPPER + "\n" + args["code"], "utf-8")
            self.gw.audit.write("sandbox.run", "sandbox", task_id=ctx.task["id"], strength=self.strength(),
                                files=len(args["file_ids"]), code_bytes=len(args["code"]))
            timeout = int(args["timeout_seconds"])
            if self.strength() == "STRONG":
                code = self._run_wsb(run_dir, timeout, ctx)
            else:
                code = self._run_appcontainer(run_dir, timeout)
            stdout = (out_dir / "stdout.txt").read_bytes()[:MAX_STDOUT].decode("utf-8", "replace") if (out_dir / "stdout.txt").exists() else ""
            stderr = (out_dir / "stderr.txt").read_bytes()[:16384].decode("utf-8", "replace") if (out_dir / "stderr.txt").exists() else ""
            produced = []
            files_out = sorted(p for p in (out_dir / "files").glob("*") if p.is_file())[:MAX_OUTPUT_FILES] if (out_dir / "files").exists() else []
            for p in files_out:
                f = self.gw.files.ingest(name=p.name, data=p.read_bytes()[:50 * 1024 * 1024], source="sandbox",
                                         sensitivity=ctx.hwm, folder="/Sandbox output", run_async=False)
                produced.append({"file_id": f["id"], "name": f["name"], "status": f["status"]})
            self.gw.audit.write("sandbox.result", "sandbox", task_id=ctx.task["id"], exit_code=code, outputs=len(produced))
            text = f"exit code: {code}{' (timed out)' if code == -1 else ''}\n--- stdout ---\n{stdout}"
            if stderr.strip():
                text += f"\n--- stderr ---\n{stderr}"
            if produced:
                text += "\n--- files saved to My Files ---\n" + "\n".join(f"{x['file_id']} {x['name']} ({x['status']})" for x in produced)
            return ToolResult(text, ctx.hwm, "sandbox", {"exit_code": code, "files": produced})
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    def _run_wsb(self, run_dir: Path, timeout: int, ctx: ExecContext) -> int:
        py = _sandbox_python()
        assert py is not None
        wsb = run_dir / "run.wsb"
        wsb.write_text(f"""<Configuration>
  <VGpu>Disable</VGpu><Networking>Disable</Networking><AudioInput>Disable</AudioInput><VideoInput>Disable</VideoInput>
  <PrinterRedirection>Disable</PrinterRedirection><ClipboardRedirection>Disable</ClipboardRedirection>
  <ProtectedClient>Enable</ProtectedClient><MemoryInMB>2048</MemoryInMB>
  <MappedFolders>
    <MappedFolder><HostFolder>{run_dir / 'in'}</HostFolder><SandboxFolder>C:\\pa\\in</SandboxFolder><ReadOnly>true</ReadOnly></MappedFolder>
    <MappedFolder><HostFolder>{py.parent}</HostFolder><SandboxFolder>C:\\pa\\py</SandboxFolder><ReadOnly>true</ReadOnly></MappedFolder>
    <MappedFolder><HostFolder>{run_dir / 'out'}</HostFolder><SandboxFolder>C:\\pa\\out</SandboxFolder><ReadOnly>false</ReadOnly></MappedFolder>
  </MappedFolders>
  <LogonCommand><Command>cmd /c "cd /d C:\\pa\\in &amp;&amp; C:\\pa\\py\\{py.name} -I C:\\pa\\in\\run.py &gt; C:\\pa\\out\\stdout.txt 2&gt; C:\\pa\\out\\stderr.txt &amp; echo %errorlevel% &gt; C:\\pa\\out\\DONE &amp; shutdown /s /t 0"</Command></LogonCommand>
</Configuration>""", "utf-8")
        with self._wsb_lock:
            self._current = subprocess.Popen([str(WSB_EXE), str(wsb)], creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            done = run_dir / "out" / "DONE"
            end = time.time() + timeout + 90  # VM boot time
            while time.time() < end:
                if done.exists():
                    break
                if ctx.cancelled():
                    break
                time.sleep(1)
            finished = done.exists()
            self.terminate()
            self._current = None
        if not finished:
            return -1
        try:
            return int((run_dir / "out" / "DONE").read_text().strip() or 0)
        except ValueError:
            return 0

    def _run_appcontainer(self, run_dir: Path, timeout: int) -> int:
        from pa_workers.sandbox import appcontainer
        py = _sandbox_python()
        assert py is not None
        _, sid = appcontainer.container_sid()
        appcontainer.grant(run_dir / "in", sid, write=False)
        appcontainer.grant(run_dir / "out", sid, write=True)
        appcontainer.grant(py.parent, sid, write=False)
        cmd = f'"{py}" -I "{run_dir / "in" / "run.py"}"'
        return appcontainer.run_in_appcontainer(cmd, run_dir / "out", run_dir / "out" / "stdout.txt",
                                                run_dir / "out" / "stderr.txt", float(timeout))


# Prepended to model code: output files go to ./files next to stdout; inputs are in the working dir.
_WRAPPER = """
import os, sys
_OUT = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])).replace('\\\\in', '\\\\out'), 'files')
try:
    os.makedirs(_OUT, exist_ok=True)
except OSError:
    _OUT = os.path.join(os.getcwd(), 'files'); os.makedirs(_OUT, exist_ok=True)
OUTPUT_DIR = _OUT
INPUT_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
"""
