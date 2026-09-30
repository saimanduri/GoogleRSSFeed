"""Built-in model runtime: bundled llama.cpp server (spec 20.1, 39.1).

Started by pa-gateway with:  --host 127.0.0.1 --port <random> --api-key <random 256-bit per launch>
GGUF files only; SHA-256 verified against the model registry before every load. No model download
capability inside the runtime. Outbound traffic is blocked by the firewall rule for llama-server.exe.

Listener hardening (39.1): llama.cpp itself cannot reject Origin/Sec-Fetch headers, so the defences
are: loopback bind, random port, 256-bit key known only to pa-gateway (pa-core never talks to it),
and a watcher that counts 401 responses in the server log - after 5 failed key attempts the runtime
is restarted with a new port and key and a security event is raised on Home.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from pa_common.errors import PAError

from ..workers import JobLimits, popen_limited, scrubbed_env


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def find_llama_server() -> Path | None:
    cands = []
    if getattr(sys, "frozen", False):
        cands.append(Path(sys.executable).with_name("llama-server.exe"))
    env = os.environ.get("PA_LLAMA_SERVER")
    if env:
        cands.append(Path(env))
    root = Path(__file__).resolve().parents[3]
    cands += [root / "llm-runtime" / "bin" / "llama-server.exe", root / "llm-runtime" / "bin" / "llama-server"]
    return next((c for c in cands if c.exists()), None)


class BuiltinRuntime:
    def __init__(self, gw):
        self.gw = gw
        self.proc: subprocess.Popen | None = None
        self._job = None
        self.port = 0
        self.api_key = ""
        self.model_id: str | None = None
        self._failed_auth = 0
        self._lock = threading.RLock()
        self.restarts = 0

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1"

    @property
    def host_header(self) -> str:
        return f"127.0.0.1:{self.port}"

    def available(self) -> bool:
        return find_llama_server() is not None

    def running_model(self, model_id: str) -> bool:
        return self.proc is not None and self.proc.poll() is None and self.model_id == model_id

    def start(self, model: dict[str, Any]) -> None:
        with self._lock:
            self.stop()
            exe = find_llama_server()
            if exe is None:
                raise PAError("the built-in runtime (llama-server) is not installed", code="runtime_missing")
            path = Path(model["path"] or "")
            if path.suffix.lower() != ".gguf" or not path.exists():
                raise PAError("built-in runtime accepts only existing .gguf files", code="invalid_model")
            digest = sha256_file(path)
            if not model.get("sha256") or digest != model["sha256"]:
                self.gw.audit.write("model.verify_failed", "model", model_id=model["id"], severity="high")
                raise PAError("model file SHA-256 does not match the registry - refusing to load", code="model_tampered")
            self.gw.audit.write("model.verified", "model", model_id=model["id"], sha256=digest)
            self.port = _free_port()
            self.api_key = secrets.token_hex(32)
            s = self.gw.settings
            cmd = [str(exe), "--host", "127.0.0.1", "--port", str(self.port), "--api-key", self.api_key,
                   "-m", str(path), "--ctx-size", str(s.get("llm.context_tokens")), "--n-gpu-layers", str(s.get("llm.gpu_layers")),
                   "--parallel", str(s.get("llm.max_concurrent")), "--no-webui"]
            # prompt/response logging stays off; there is no slot persistence to disk (spec 20.5)
            env = scrubbed_env({"LLAMA_ARG_HOST": "127.0.0.1"})
            self.proc, self._job = popen_limited(cmd, cwd=path.parent, env=env, limits=JobLimits(memory_mb=256 * 1024, max_processes=1),
                                                 stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.model_id = model["id"]
            self._failed_auth = 0
            threading.Thread(target=self._watch, daemon=True, name="llama-watch").start()
            self._wait_ready()
            self.gw.audit.write("model.loaded", "model", model_id=model["id"], runtime="builtin", port=self.port)

    def _wait_ready(self, timeout: float = 180) -> None:
        end = time.time() + timeout
        while time.time() < end:
            if self.proc is None or self.proc.poll() is not None:
                raise PAError("the built-in runtime exited during start-up", code="runtime_failed")
            try:
                r = httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=2, trust_env=False)
                if r.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        raise PAError("the built-in runtime did not become ready", code="runtime_failed")

    def _watch(self) -> None:
        proc = self.proc
        if proc is None or proc.stdout is None:
            return
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode(errors="ignore")
            if " 401" in line or "Invalid API Key" in line:
                self._failed_auth += 1
                if self._failed_auth >= 5:
                    self.gw.audit.write("llm.key_bruteforce", "security", severity="high")
                    self.gw.home_event("llm_bruteforce", "high", "Someone tried to use the local model without the key",
                                       "The model runtime was restarted with a new port and key.")
                    model_id = self.model_id
                    threading.Thread(target=self._rotate, args=(model_id,), daemon=True).start()
                    return

    def _rotate(self, model_id: str | None) -> None:
        self.restarts += 1
        if model_id:
            try:
                self.start(self.gw.llm.get_model(model_id))
            except PAError:
                self.stop()

    def stop(self) -> None:
        with self._lock:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(10)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
            self.proc = None
            self._job = None
            self.model_id = None
            self.api_key = ""
