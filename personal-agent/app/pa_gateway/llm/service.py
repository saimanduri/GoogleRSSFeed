"""Model access (spec 20, 39.1, 39.5) - all model traffic goes through pa-gateway.

Providers (Settings > AI Model):
  builtin   bundled llama.cpp server started by the gateway: 127.0.0.1, random port, random 256-bit
            API key per launch, GGUF only, SHA-256 verified before load (runtime.py)
  ollama    Ollama on this PC (OpenAI-compatible /v1 API). "Reduced isolation".
  openai    any OpenAI-compatible server: vLLM, LM Studio, LocalAI, Run:ai inference endpoints...
            Loopback endpoints are "Reduced isolation"; non-loopback endpoints are REMOTE: data leaves
            this PC, so adding one is a loosening change and RESTRICTED context is never sent to them.
  dev_mock  deterministic scripted model for development and tests (never in release builds)

Rules enforced here:
  - nothing reaches a model unless it is in the session log (verify_messages_logged) and the full
    request is recorded first (spec 39.5 / rule 35.19)
  - the model never chooses endpoints; the router maps roles -> model records
  - external runtimes that answer browser-origin requests are refused (39.1)
"""
from __future__ import annotations

import ipaddress
import json
import socket
import threading
import time
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit

import httpx

from pa_common.devmode import dev_mode
from pa_common.errors import PAError
from pa_common.ids import new_id
from pa_common.sensitivity import Sensitivity, Trust
from pa_common.timeutil import now_iso

PROVIDERS = ("builtin", "ollama", "openai", "dev_mock")
ROLES = ("fast", "standard", "reasoning", "vision", "embedding", "stt")


def endpoint_location(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower()
    if host in ("localhost",):
        return "loopback"
    try:
        return "loopback" if ipaddress.ip_address(host).is_loopback else "remote"
    except ValueError:
        return "remote"


class LLMService:
    def __init__(self, gw):
        self.gw = gw
        self._sem = threading.BoundedSemaphore(8)
        self._inflight = threading.Semaphore(int(gw.settings.get("llm.max_concurrent")))
        self._interactive_waiting = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ model registry
    def models(self) -> list[dict[str, Any]]:
        rows = self.gw.db.all("SELECT * FROM models ORDER BY created_at")
        for r in rows:
            r["test_report"] = json.loads(r.pop("test_report_json") or "null")
            r["location"] = "builtin" if r["provider"] == "builtin" else (
                "dev" if r["provider"] == "dev_mock" else endpoint_location(r["endpoint"] or ""))
            r["isolation"] = {"builtin": "Built-in (strong)", "loopback": "Reduced isolation",
                              "remote": "REMOTE - data leaves this PC", "dev": "Developer mock"}[r["location"]]
        return rows

    def get_model(self, mid: str) -> dict[str, Any]:
        for m in self.models():
            if m["id"] == mid:
                return m
        raise PAError("model not found", code="not_found")

    def add_model(self, spec: dict[str, Any]) -> str:
        provider = spec.get("provider")
        if provider not in PROVIDERS:
            raise PAError("unknown provider", code="invalid_request")
        if provider == "dev_mock" and not dev_mode():
            raise PAError("the developer mock model is not available in this build", code="forbidden")
        kind = spec.get("kind", "chat")
        if kind not in ("chat", "embedding", "stt"):
            raise PAError("invalid model kind", code="invalid_request")
        endpoint = (spec.get("endpoint") or "").rstrip("/")
        if provider in ("ollama", "openai"):
            parts = urlsplit(endpoint)
            if parts.scheme not in ("http", "https") or not parts.hostname:
                raise PAError("endpoint must be an http(s) URL", code="invalid_request")
            if endpoint_location(endpoint) == "remote" and parts.scheme != "https" and not spec.get("allow_insecure_remote"):
                raise PAError("remote endpoints must use https", code="invalid_request")
        mid = new_id("mdl")
        self.gw.db.insert("models", {
            "id": mid, "name": str(spec.get("name") or spec.get("model_name") or provider)[:120], "provider": provider,
            "endpoint": endpoint or None, "model_name": (spec.get("model_name") or "")[:200] or None,
            "path": spec.get("path"), "sha256": spec.get("sha256"), "size_bytes": spec.get("size_bytes"),
            "source": spec.get("source"), "license": spec.get("license"), "quantization": spec.get("quantization"),
            "kind": kind, "api_key_secret_id": None, "tested": 1 if provider == "dev_mock" else 0,
            "created_at": now_iso()})
        if spec.get("api_key"):
            self.gw.secrets.upsert_bound(f"llm:{mid}", f"API key for model {spec.get('name') or mid}", spec["api_key"], "api_key")
        self.gw.audit.write("model.added", "model", model_id=mid, provider=provider, kind=kind,
                            location=endpoint_location(endpoint) if endpoint else provider)
        return mid

    def remove_model(self, mid: str) -> None:
        self.gw.db.execute("DELETE FROM models WHERE id=?", (mid,))
        self.gw.secrets.delete_bound(f"llm:{mid}")
        self.gw.audit.write("model.removed", "model", model_id=mid)

    def role_model(self, role: str) -> dict[str, Any] | None:
        mid = self.gw.settings.get(f"llm.role.{role}") if role in ROLES else ""
        if not mid and role in ("fast", "reasoning", "vision"):
            mid = self.gw.settings.get("llm.role.standard")
        if not mid:
            cands = [m for m in self.models() if m["kind"] == ("chat" if role not in ("embedding", "stt") else role) and m["tested"]]
            return cands[0] if cands else None
        try:
            return self.get_model(mid)
        except PAError:
            return None

    # ------------------------------------------------------------------ hardening checks (39.1 / 20.4)
    def exposure_check(self, m: dict[str, Any]) -> dict[str, Any]:
        if m["provider"] not in ("ollama", "openai") or m["location"] != "loopback":
            return {"checked": False}
        parts = urlsplit(m["endpoint"])
        port = parts.port or (443 if parts.scheme == "https" else 80)
        result: dict[str, Any] = {"checked": True, "browser_origin_rejected": None, "lan_reachable": None}
        try:
            r = httpx.get(m["endpoint"] + "/v1/models", headers={"Origin": "https://attacker.example",
                          "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "cross-site"}, timeout=5, trust_env=False)
            acao = r.headers.get("access-control-allow-origin", "")
            result["browser_origin_rejected"] = r.status_code in (401, 403) or (acao not in ("*", "https://attacker.example") and r.status_code >= 400)
            result["cors_allow_origin"] = acao
        except httpx.HTTPError as e:
            result["error"] = str(e)
        try:
            lan_ip = socket.gethostbyname(socket.gethostname())
            if not ipaddress.ip_address(lan_ip).is_loopback:
                with socket.create_connection((lan_ip, port), timeout=1.5):
                    result["lan_reachable"] = True
            else:
                result["lan_reachable"] = False
        except OSError:
            result["lan_reachable"] = False
        result["at_risk"] = bool(result.get("lan_reachable")) or result.get("browser_origin_rejected") is False
        return result

    # ------------------------------------------------------------------ transport
    def _endpoint(self, m: dict[str, Any]) -> tuple[str, dict[str, str]]:
        if m["provider"] == "builtin":
            rt = self.gw.runtime
            if not rt.running_model(m["id"]):
                rt.start(m)
            return rt.base_url, {"Authorization": f"Bearer {rt.api_key}", "Host": rt.host_header}
        headers = {}
        key = self.gw.secrets.value_for_binding(f"llm:{m['id']}")
        if key:
            headers["Authorization"] = f"Bearer {key}"
        base = m["endpoint"]
        if m["provider"] == "ollama" and not base.endswith("/v1"):
            base = base + "/v1"
        return base, headers

    def _client(self, m: dict[str, Any]) -> httpx.Client:
        # system proxies are ignored for loopback; remote endpoints use normal TLS verification
        return httpx.Client(timeout=httpx.Timeout(600, connect=10), trust_env=False)

    # ------------------------------------------------------------------ chat completion (pa-core via IPC)
    def complete(self, *, task: dict[str, Any] | None, session_ids: list[str], messages: list[dict[str, Any]],
                 role: str = "standard", json_mode: bool = False, max_tokens: int = 2048, stream_to_run: str | None = None,
                 interactive: bool = False, purpose: str = "agent") -> dict[str, Any]:
        gw = self.gw
        if gw.killswitch.active("stop_all") or gw.killswitch.agent_blocked():
            raise PAError("agent is paused or stopped", code="kill_switch_active")
        m = self.model_for_task(task, role)
        if m is None:
            raise PAError("no AI model is configured. Add one in Settings > AI Model.", code="no_model")
        # 39.5: every non-system message must already be in the session log
        gw.sessionlog.verify_messages_logged(session_ids, messages)
        hwm = max((gw.sessionlog.hwm(s) for s in session_ids), default=Sensitivity.PUBLIC)
        if m["location"] == "remote" and hwm >= Sensitivity.RESTRICTED:
            raise PAError("RESTRICTED data can never be sent to a remote model", code="policy_denied")
        if task is not None:
            est = sum(len(str(x.get("content", ""))) for x in messages) // 4 + max_tokens
            gw.budgets.check(task, "tokens", est)
        request = {"model": m["model_name"] or m["name"], "messages": messages, "temperature": float(gw.settings.get("llm.temperature")),
                   "max_tokens": max_tokens, "stream": True}
        if json_mode and m["provider"] in ("openai", "ollama", "builtin"):
            request["response_format"] = {"type": "json_object"}
        primary = session_ids[0]
        req_event = gw.sessionlog.append(primary, "llm.request", json.dumps(request, ensure_ascii=False), role="system",
                                         source="gateway", trust=Trust.TRUSTED, sensitivity=int(hwm),
                                         meta={"model_id": m["id"], "role": role})
        gw.audit.write("llm.request", "model", model_id=m["id"], provider=m["provider"], location=m["location"],
                       session_event=req_event["id"], task_id=task["id"] if task else None,
                       context_high_water_mark=Sensitivity(hwm).name, messages=len(messages))
        started = time.time()
        text, usage = self._with_priority(interactive, lambda: self._run(m, request, stream_to_run))
        dur = int((time.time() - started) * 1000)
        tokens_in = int(usage.get("prompt_tokens") or sum(len(str(x.get("content", ""))) for x in messages) // 4)
        tokens_out = int(usage.get("completion_tokens") or len(text) // 4)
        gw.sessionlog.append(primary, "llm.response", text, role="assistant", source="model", trust=Trust.INFERRED,
                             sensitivity=int(hwm), meta={"model_id": m["id"], "request_event": req_event["id"], "purpose": purpose,
                                                         "tokens_in": tokens_in, "tokens_out": tokens_out, "ms": dur})
        if task is not None:
            gw.budgets.add(task["id"], "tokens", tokens_in + tokens_out)
            gw.runs.add_usage(task.get("run_id"), tokens_in, tokens_out)
        gw.audit.write("llm.response", "model", model_id=m["id"], tokens_in=tokens_in, tokens_out=tokens_out, duration_ms=dur)
        return {"text": text, "model": m["name"], "model_id": m["id"], "tokens_in": tokens_in, "tokens_out": tokens_out,
                "duration_ms": dur, "request_event": req_event["id"]}

    def model_for_task(self, task: dict[str, Any] | None, role: str) -> dict[str, Any] | None:
        if task and task.get("model_id"):
            try:
                m = self.get_model(task["model_id"])
                if m["tested"]:
                    return m
            except PAError:
                pass
        return self.role_model(role)

    def _with_priority(self, interactive: bool, fn: Callable[[], Any]) -> Any:
        """Interactive chat has priority over background missions on the local model (spec 18)."""
        if interactive:
            with self._lock:
                self._interactive_waiting += 1
        else:
            while self._interactive_waiting > 0:
                time.sleep(0.2)
        self._inflight.acquire()
        try:
            return fn()
        finally:
            self._inflight.release()
            if interactive:
                with self._lock:
                    self._interactive_waiting -= 1

    def _run(self, m: dict[str, Any], request: dict[str, Any], run_id: str | None) -> tuple[str, dict[str, Any]]:
        if m["provider"] == "dev_mock":
            from .mock import mock_complete
            text = mock_complete(request["messages"])
            if run_id:
                self.gw.emit("llm.delta", {"run_id": run_id, "delta": text, "done": True})
            return text, {}
        base, headers = self._endpoint(m)
        parts: list[str] = []
        usage: dict[str, Any] = {}
        with self._client(m) as c:
            try:
                with c.stream("POST", base + "/chat/completions", json=request, headers=headers) as r:
                    if r.status_code >= 400:
                        body = r.read().decode(errors="ignore")[:300]
                        if "response_format" in request and r.status_code in (400, 422):
                            request = {k: v for k, v in request.items() if k != "response_format"}
                            return self._run(m, request, run_id)
                        raise PAError(f"model server returned {r.status_code}: {body}", code="model_error")
                    for delta, u in _iter_sse(r.iter_lines()):
                        if u:
                            usage = u
                        if delta:
                            parts.append(delta)
                            if run_id:
                                self.gw.emit("llm.delta", {"run_id": run_id, "delta": delta})
                        if self.gw.killswitch.active("stop_all"):
                            break
            except httpx.HTTPError as e:
                raise PAError(f"cannot reach the model server: {e}", code="model_unreachable") from e
        if run_id:
            self.gw.emit("llm.delta", {"run_id": run_id, "delta": "", "done": True})
        return "".join(parts), usage

    # ------------------------------------------------------------------ embeddings / speech
    def embed(self, text: str):
        import numpy as np

        from ..agentdata.memory import DIM, hash_embedding
        m = self.role_model("embedding")
        if not m or m["provider"] == "dev_mock":
            return hash_embedding(text)
        base, headers = self._endpoint(m)
        with self._client(m) as c:
            r = c.post(base + "/embeddings", json={"model": m["model_name"] or m["name"], "input": text[:8000]}, headers=headers)
            r.raise_for_status()
            v = np.asarray(r.json()["data"][0]["embedding"], dtype=np.float32)
        # project to the fixed storage dimension deterministically
        if v.shape[0] != DIM:
            idx = np.arange(v.shape[0]) % DIM
            p = np.zeros(DIM, np.float32)
            np.add.at(p, idx, v)
            v = p
        n = float(np.linalg.norm(v))
        return v / n if n else v

    def transcribe(self, audio: bytes, mime: str, language: str = "") -> dict[str, Any]:
        m = self.role_model("stt")
        if m is None:
            raise PAError("no speech-to-text model configured (Settings > AI Model > Speech-to-text)", code="no_model")
        if m["provider"] == "dev_mock":
            return {"text": "remind me to call the dentist tomorrow at 9", "model": "dev_mock"}
        if len(audio) > 25 * 1024 * 1024:
            raise PAError("recording too long", code="too_large")
        base, headers = self._endpoint(m)
        ext = {"audio/webm": "webm", "audio/ogg": "ogg", "audio/wav": "wav", "audio/mpeg": "mp3", "audio/mp4": "m4a"}.get(mime.split(";")[0], "webm")
        data = {"model": m["model_name"] or m["name"], "response_format": "json"}
        if language:
            data["language"] = language
        self.gw.audit.write("stt.request", "model", model_id=m["id"], bytes=len(audio), location=m["location"])
        with self._client(m) as c:
            r = c.post(base + "/audio/transcriptions", headers=headers, data=data,
                       files={"file": (f"audio.{ext}", audio, mime.split(";")[0])})
            if r.status_code >= 400:
                raise PAError(f"speech-to-text server returned {r.status_code}: {r.text[:200]}", code="model_error")
            text = r.json().get("text", "")
        return {"text": text.strip(), "model": m["name"]}

    # ------------------------------------------------------------------ Test model (spec 20.2, 32)
    def test_model(self, mid: str) -> dict[str, Any]:
        m = self.get_model(mid)
        report: dict[str, Any] = {"model_id": mid, "started": now_iso(), "checks": []}
        if m["kind"] == "stt":
            report["checks"].append({"name": "reachable", "ok": self._reachable(m)})
        elif m["kind"] == "embedding":
            try:
                self.embed("hello world") if self.role_model("embedding") else None
                report["checks"].append({"name": "embedding", "ok": True})
            except Exception as e:  # noqa: BLE001
                report["checks"].append({"name": "embedding", "ok": False, "error": str(e)[:200]})
        else:
            from .modeltest import run_model_checks
            report["checks"] = run_model_checks(lambda msgs, json_mode=False: self._run(m, {
                "model": m["model_name"] or m["name"], "messages": msgs, "temperature": 0, "max_tokens": 400, "stream": True,
                **({"response_format": {"type": "json_object"}} if json_mode else {})}, None)[0])
        exp = self.exposure_check(m)
        report["exposure"] = exp
        if exp.get("at_risk"):
            report["checks"].append({"name": "listener_hardening", "ok": False,
                                     "error": "runtime answers browser-origin requests or is reachable from the network"})
        quality = [c for c in report["checks"] if c.get("gate", True)]
        report["passed"] = all(c["ok"] for c in quality)
        report["finished"] = now_iso()
        self.gw.db.update("models", "id", mid, {"tested": int(report["passed"]), "test_report_json": report})
        self.gw.audit.write("model.tested", "model", model_id=mid, passed=report["passed"])
        return report

    def _reachable(self, m: dict[str, Any]) -> bool:
        try:
            base, headers = self._endpoint(m)
            with self._client(m) as c:
                return c.get(base + "/models", headers=headers).status_code < 500
        except Exception:  # noqa: BLE001
            return False

    def discover(self, provider: str, endpoint: str, api_key: str | None = None) -> list[str]:
        """List models offered by an Ollama/OpenAI-compatible server (Settings > AI Model > Add)."""
        base = endpoint.rstrip("/")
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        with httpx.Client(timeout=10, trust_env=False) as c:
            if provider == "ollama":
                r = c.get(base + "/api/tags")
                r.raise_for_status()
                return [x["name"] for x in r.json().get("models", [])]
            r = c.get(base + "/v1/models" if not base.endswith("/v1") else base + "/models", headers=headers)
            r.raise_for_status()
            return [x["id"] for x in r.json().get("data", [])]


def _iter_sse(lines: Iterator[str]) -> Iterator[tuple[str, dict[str, Any] | None]]:
    for line in lines:
        if not line or not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            break
        try:
            obj = json.loads(payload)
        except ValueError:
            continue
        choices = obj.get("choices") or [{}]
        delta = (choices[0].get("delta") or {}).get("content") or (choices[0].get("message") or {}).get("content") or ""
        yield delta, obj.get("usage")
