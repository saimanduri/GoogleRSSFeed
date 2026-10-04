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

# The reply shape the agent loop expects (pa_core/actions.py). Used for constrained decoding on local runtimes.
ACTION_SCHEMA = {
    "type": "object",
    "properties": {"thought": {"type": "string"}, "action": {"type": "string", "enum": ["tool", "final"]},
                   "tool": {"type": "string"}, "args": {"type": "object"}, "answer": {"type": "string"}},
    "required": ["thought", "action"],
    "additionalProperties": False,
}
ROLES = ("fast", "standard", "reasoning", "vision", "embedding", "stt")
KINDS = ("chat", "vision", "embedding", "stt")
# Which kind of model may fill which role. The kinds never mix: a speech model can never answer chat, a chat model never transcribes.
ROLE_KIND = {"fast": "chat", "standard": "chat", "reasoning": "chat", "vision": "vision", "embedding": "embedding", "stt": "stt"}


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
        self._testing: set[str] = set()

    # ------------------------------------------------------------------ model registry
    def models(self) -> list[dict[str, Any]]:
        rows = self.gw.db.all("SELECT * FROM models ORDER BY created_at")
        for r in rows:
            r["testing"] = r["id"] in self._testing
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
        if kind not in KINDS:
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

    def test_in_background(self, mid: str, make_default: bool = True) -> bool:
        """Run 'Test model' without blocking the window; afterwards (if it passed) make it the default for its role when
        none is set yet - local models only, a remote model never becomes a default silently (spec 5.5)."""
        with self._lock:
            if mid in self._testing or self.get_model(mid)["tested"]:
                return False
            self._testing.add(mid)
        self.gw.emit("llm.models_changed", {"model_id": mid, "state": "testing"})

        def work() -> None:
            state, passed = "failed", False
            try:
                passed = bool(self.test_model(mid).get("passed"))
                state = "passed" if passed else "failed"
                if passed and make_default:
                    self._default_if_unset(mid)
            except Exception:  # noqa: BLE001
                self.gw.audit.write("model.test_error", "model", model_id=mid)
            finally:
                with self._lock:
                    self._testing.discard(mid)
                self.gw.emit("llm.models_changed", {"model_id": mid, "state": state})
        threading.Thread(target=work, name=f"model-test-{mid}", daemon=True).start()
        return True

    def _default_if_unset(self, mid: str) -> None:
        m = self.get_model(mid)
        if m["location"] == "remote":
            return
        role = "standard" if m["kind"] == "chat" else m["kind"]
        if not self.gw.settings.get(f"llm.role.{role}"):
            self.gw.settings.apply({f"llm.role.{role}": mid})
            self.gw.audit.write("model.default_set", "model", model_id=mid, role=role, automatic=True)

    def inspect(self, provider: str, endpoint: str, model_name: str, api_key: str | None = None) -> dict[str, Any]:
        """Guess what a downloaded model is (chat / speech-to-text / embedding / vision) so 'Add model' needs no manual choices."""
        name = (model_name or "").lower()
        out: dict[str, Any] = {"kind": "chat", "capabilities": [], "detail": "", "vision": False}
        if provider == "ollama":
            base = endpoint.rstrip("/")
            headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
            with httpx.Client(timeout=15, trust_env=False) as c:
                r = c.post(base + "/api/show", json={"model": model_name}, headers=headers)
            if r.status_code < 400:
                j = r.json()
                info = j.get("model_info") or {}
                tags = [str(t).lower() for t in (info.get("general.tags") or [])]
                caps = [str(x).lower() for x in (j.get("capabilities") or [])]
                arch = str(info.get("general.architecture", "")).lower()
                out["capabilities"] = caps
                out["vision"] = "vision" in caps or any(k in arch for k in ("vl", "llava", "mllama", "gemma3", "moondream", "minicpmv")) or any(k in name for k in ("-vl", "vl:", "vision", "llava"))
                out["detail"] = " ".join(x for x in (str(info.get("general.size_label", "")), str((j.get("details") or {}).get("quantization_level", ""))) if x)
                if "automatic-speech-recognition" in tags or "speech-recognition" in tags or "whisper" in arch or "asr" in name or "whisper" in name:
                    out["kind"] = "stt"
                elif "embedding" in caps or "embed" in name or arch in ("bert", "nomic-bert"):
                    out["kind"] = "embedding"
                if out["kind"] != "chat":
                    out["vision"] = False
                return out
        if "whisper" in name or "asr" in name:
            out["kind"] = "stt"
        elif "embed" in name:
            out["kind"] = "embedding"
        return out

    def remove_model(self, mid: str) -> None:
        self.gw.db.execute("DELETE FROM models WHERE id=?", (mid,))
        self.gw.secrets.delete_bound(f"llm:{mid}")
        self.gw.audit.write("model.removed", "model", model_id=mid)

    def role_model(self, role: str) -> dict[str, Any] | None:
        mid = self.gw.settings.get(f"llm.role.{role}") if role in ROLES else ""
        if not mid and role in ("fast", "reasoning"):
            mid = self.gw.settings.get("llm.role.standard")
        if not mid:
            cands = [m for m in self.models() if m["kind"] == ROLE_KIND.get(role, "chat") and m["tested"]]
            return cands[0] if cands else None
        try:
            m = self.get_model(mid)
        except PAError:
            return None
        return m if m["kind"] == ROLE_KIND.get(role, "chat") else None      # a wrongly assigned model is never used

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
                 role: str = "standard", json_mode: bool = False, max_tokens: int = 2048, stream_to_run: str | None = None, action_schema: bool = False,
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
            # The agent loop asks for the action shape itself: local runtimes then cannot emit another shape
            # (constrained decoding). If the server rejects the schema we fall back to plain JSON mode, then to none.
            if m["provider"] == "ollama":
                # Thinking models (gemma4, qwen3, ...) otherwise spend the whole answer budget on hidden reasoning and return an
                # EMPTY reply: the agent's JSON already carries a short "thought", so ask the server not to think separately.
                request["reasoning_effort"] = "none"
            if action_schema and m["provider"] in ("ollama", "builtin"):
                request["response_format"] = {"type": "json_schema", "json_schema": {"name": "agent_action", "strict": True, "schema": ACTION_SCHEMA}}
            else:
                request["response_format"] = {"type": "json_object"}
        primary = session_ids[0]
        req_event = gw.sessionlog.append(primary, "llm.request", json.dumps(request, ensure_ascii=False), role="system",
                                         source="gateway", trust=Trust.TRUSTED, sensitivity=int(hwm),
                                         meta={"model_id": m["id"], "role": role})
        gw.audit.write("llm.request", "model", model_id=m["id"], provider=m["provider"], location=m["location"],
                       session_event=req_event["id"], task_id=task["id"] if task else None,
                       context_high_water_mark=Sensitivity(hwm).name, messages=len(messages))
        started = time.time()
        text, usage = self._with_priority(interactive, lambda: self._run_with_rescue(m, request, stream_to_run, action_schema))
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

    def _run_with_rescue(self, m: dict[str, Any], request: dict[str, Any], run_id: str | None, action_schema: bool) -> tuple[str, dict[str, Any]]:
        """An empty reply is almost never the user's fault (hidden reasoning ate the budget, the JSON constraint confused the model, the
        context was cut). Instead of failing the chat: try again without the constraint and with room to answer, then once more through
        Ollama's own API with an explicit context window and thinking switched off."""
        try:
            return self._run(m, request, run_id)
        except PAError as e:
            if e.code != "model_empty":
                raise
            self.gw.audit.write("llm.empty_retry", "model", model_id=m["id"], step=1)
        relaxed = {k: v for k, v in request.items() if k != "response_format"}
        relaxed["max_tokens"] = min(8192, int(request.get("max_tokens", 2048)) * 2)
        try:
            return self._run(m, relaxed, run_id)
        except PAError as e:
            if e.code != "model_empty" or m["provider"] != "ollama":
                raise
            self.gw.audit.write("llm.empty_retry", "model", model_id=m["id"], step=2)
        return self._ollama_native(m, request, action_schema, run_id)

    def _ollama_native(self, m: dict[str, Any], request: dict[str, Any], action_schema: bool, run_id: str | None) -> tuple[str, dict[str, Any]]:
        """Last resort for Ollama: /api/chat with a large context window, no thinking, optional JSON schema, not streamed."""
        url = m["endpoint"].rstrip("/").removesuffix("/v1") + "/api/chat"
        body: dict[str, Any] = {"model": request["model"], "messages": request["messages"], "stream": False, "think": False,
                                "options": {"num_ctx": 16384, "num_predict": min(8192, int(request.get("max_tokens", 2048)) * 2),
                                            "temperature": float(request.get("temperature", 0.2))}}
        if "response_format" in request and action_schema:
            body["format"] = ACTION_SCHEMA
        t0 = time.time()
        try:
            with httpx.Client(timeout=httpx.Timeout(600, connect=10), trust_env=False) as c:
                r = c.post(url, json=body)
        except httpx.HTTPError as e:
            raise PAError(f"cannot reach the model server: {type(e).__name__}", code="model_unreachable") from e
        self.gw.netlog.record("llm", "POST", url, r.status_code, purpose=f"model chat retry ({m['name']})", bytes_out=len(json.dumps(body)),
                              duration_ms=int((time.time() - t0) * 1000))
        if r.status_code >= 400:
            raise PAError(f"model server returned {r.status_code}", code="model_error")
        j = r.json()
        text = (j.get("message") or {}).get("content") or ""
        if not text.strip():
            raise PAError("the model returned an empty reply three times in a row (tried with and without a JSON constraint and with a larger context). "
                          "Try another chat model in Settings > AI Model, or start a new chat if this one has become very long.", code="model_empty")
        if run_id:
            self.gw.emit("llm.delta", {"run_id": run_id, "delta": text, "done": True})
        return text, {"prompt_tokens": j.get("prompt_eval_count", 0), "completion_tokens": j.get("eval_count", 0)}

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
        return self._stream(m, request, run_id, headers, parts, usage, base + "/chat/completions", time.time())

    def _stream(self, m, request, run_id, headers, parts, usage, url, t0):  # noqa: ANN001
        sent = len(json.dumps(request, ensure_ascii=False).encode())
        purpose = f"model chat ({m['provider']}: {m['name']})"
        with self._client(m) as c:
            try:
                with c.stream("POST", url, json=request, headers=headers) as r:
                    if r.status_code >= 400:
                        self.gw.netlog.record("llm", "POST", url, r.status_code, purpose=purpose, bytes_out=sent, duration_ms=int((time.time() - t0) * 1000))
                        body = r.read().decode(errors="ignore")[:300]
                        if "response_format" in request and r.status_code in (400, 422):
                            if request["response_format"].get("type") == "json_schema":
                                request = {**request, "response_format": {"type": "json_object"}}
                            else:
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
                    self.gw.netlog.record("llm", "POST", url, r.status_code, purpose=purpose, bytes_out=sent,
                                          bytes_in=len("".join(parts).encode()), duration_ms=int((time.time() - t0) * 1000))
            except httpx.HTTPError as e:
                self.gw.netlog.record("llm", "POST", url, None, outcome="error", reason=type(e).__name__, bytes_out=sent, purpose=purpose,
                                      duration_ms=int((time.time() - t0) * 1000))
                raise PAError(f"cannot reach the model server: {e}", code="model_unreachable") from e
        if run_id:
            self.gw.emit("llm.delta", {"run_id": run_id, "delta": "", "done": True})
        if not "".join(parts).strip():
            raise PAError("the model returned an empty reply (a thinking model can use its whole answer budget on hidden reasoning; "
                          "choose a non-thinking model or a larger limit)", code="model_empty")
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
        """Speech to text. Ollama models (e.g. Qwen3-ASR) are called through /api/chat with 16 kHz mono WAV in
        'images'; OpenAI-compatible servers (Whisper etc.) through /audio/transcriptions."""
        m = self.role_model("stt")
        if m is None:
            raise PAError("no speech-to-text model configured (Settings > AI Model > Speech-to-text)", code="no_model")
        if m["provider"] == "dev_mock":
            return {"text": "remind me to call the dentist tomorrow at 9", "model": "dev_mock"}
        if len(audio) > 40 * 1024 * 1024:
            raise PAError("recording too long", code="too_large")
        return self._transcribe_with(m, audio, mime, language)

    def _transcribe_with(self, m: dict[str, Any], audio: bytes, mime: str, language: str = "") -> dict[str, Any]:
        from . import audio as A
        mime0 = mime.split(";")[0]
        self.gw.audit.write("stt.request", "model", model_id=m["id"], bytes=len(audio), location=m["location"], provider=m["provider"])
        if m["provider"] == "ollama":
            if not A.is_wav(audio):
                raise PAError("this speech model needs WAV audio (the window converts it; update the app)", code="invalid_request")
            parts, lang = [], ""
            for chunk in A.split_chunks(A.wav_to_pcm16k(audio)):
                text, lg = self._ollama_asr(m, A.make_wav(chunk))
                if text:
                    parts.append(text)
                lang = lang or lg
            return {"text": " ".join(parts).strip(), "model": m["name"], "language": lang}
        base, headers = self._endpoint(m)
        ext = {"audio/webm": "webm", "audio/ogg": "ogg", "audio/wav": "wav", "audio/x-wav": "wav", "audio/mpeg": "mp3", "audio/mp4": "m4a"}.get(mime0, "webm")
        data = {"model": m["model_name"] or m["name"], "response_format": "json"}
        if language:
            data["language"] = language
        t0 = time.time()
        with self._client(m) as c:
            r = c.post(base + "/audio/transcriptions", headers=headers, data=data, files={"file": (f"audio.{ext}", audio, mime0)})
            self.gw.netlog.record("llm", "POST", base + "/audio/transcriptions", r.status_code, purpose="speech-to-text", bytes_out=len(audio),
                                  duration_ms=int((time.time() - t0) * 1000))
            if r.status_code >= 400:
                raise PAError(f"speech-to-text server returned {r.status_code}: {r.text[:200]}", code="model_error")
            text = r.json().get("text", "")
        return {"text": text.strip(), "model": m["name"]}

    def _ollama_asr(self, m: dict[str, Any], wav: bytes) -> tuple[str, str]:
        import base64

        from . import audio as A
        base = m["endpoint"].rstrip("/")
        if base.endswith("/v1"):
            base = base[:-3]
        headers = {}
        key = self.gw.secrets.value_for_binding(f"llm:{m['id']}")
        if key:
            headers["Authorization"] = f"Bearer {key}"
        body = {"model": m["model_name"] or m["name"], "stream": False,
                "messages": [{"role": "user", "content": "", "images": [base64.b64encode(wav).decode()]}]}
        url, t0 = base + "/api/chat", time.time()
        try:
            with httpx.Client(timeout=httpx.Timeout(300, connect=10), trust_env=False) as c:
                r = c.post(url, json=body, headers=headers)
        except httpx.HTTPError as e:
            self.gw.netlog.record("llm", "POST", url, None, outcome="error", reason=type(e).__name__, bytes_out=len(wav), purpose="speech-to-text",
                                  duration_ms=int((time.time() - t0) * 1000))
            raise PAError(f"cannot reach the speech model server: {type(e).__name__}", code="model_unreachable") from e
        self.gw.netlog.record("llm", "POST", url, r.status_code, purpose="speech-to-text", bytes_out=len(wav), duration_ms=int((time.time() - t0) * 1000))
        if r.status_code >= 400:
            hint = ""
            if "not found" in r.text.lower():
                hint = " (download the model first: ollama pull <name>)"
            elif r.status_code in (400, 500):
                hint = " (this model may need a newer Ollama with audio support - update Ollama)"
            raise PAError(f"speech-to-text server returned {r.status_code}{hint}", code="model_error")
        try:
            content = (r.json().get("message") or {}).get("content", "")
        except ValueError as e:
            raise PAError("speech-to-text server sent an unreadable answer", code="model_error") from e
        return A.strip_asr_prefix(content)

    # ------------------------------------------------------------------ Test model (spec 20.2, 32)
    def test_model(self, mid: str) -> dict[str, Any]:
        m = self.get_model(mid)
        report: dict[str, Any] = {"model_id": mid, "started": now_iso(), "checks": []}
        if m["kind"] == "stt":
            report["checks"].append({"name": "reachable", "ok": self._reachable(m)})
            try:
                from . import audio as A
                t0 = time.time()
                self._transcribe_with(m, A.sample_clip(), "audio/wav") if m["provider"] != "dev_mock" else None
                report["checks"].append({"name": "transcribes_audio", "ok": True, "note": f"answered a test clip in {time.time() - t0:.1f} s"})
            except Exception as e:  # noqa: BLE001
                report["checks"].append({"name": "transcribes_audio", "ok": False, "error": str(e)[:200]})
        elif m["kind"] == "vision":
            report["checks"].append({"name": "reachable", "ok": self._reachable(m)})
            try:
                from . import audio as A
                t0 = time.time()
                ans = self._vision_with(m, A.sample_png(), "image/png", "What is the main colour of this picture? Answer with one word.") if m["provider"] != "dev_mock" else "red"
                report["checks"].append({"name": "sees_images", "ok": "red" in ans.lower(), "note": f"answered in {time.time() - t0:.1f} s: {ans[:60]}",
                                         "error": None if "red" in ans.lower() else f"expected the colour red, got: {ans[:60]}"})
            except Exception as e:  # noqa: BLE001
                report["checks"].append({"name": "sees_images", "ok": False, "error": str(e)[:200]})
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

    def _vision_with(self, m: dict[str, Any], image: bytes, mime: str, prompt: str) -> str:
        """One image question against a specific model record (used by 'Test model', before the model is a default)."""
        saved = self.role_model
        try:
            self.role_model = lambda role: m if role == "vision" else saved(role)      # type: ignore[method-assign]
            return self.describe_image(image, mime, prompt, purpose="model-test")["text"]
        finally:
            self.role_model = saved                                                      # type: ignore[method-assign]

    def _reachable(self, m: dict[str, Any]) -> bool:
        try:
            base, headers = self._endpoint(m)
            with self._client(m) as c:
                return c.get(base + "/models", headers=headers).status_code < 500
        except Exception:  # noqa: BLE001
            return False

    def discover_details(self, provider: str, endpoint: str, api_key: str | None = None) -> list[dict[str, Any]]:
        """Names plus the detected kind of every model on the server, so each 'Add' card only offers what belongs to it."""
        out = []
        for name in self.discover(provider, endpoint, api_key)[:80]:
            try:
                i = self.inspect(provider, endpoint, name, api_key)
            except Exception:  # noqa: BLE001
                i = {"kind": "chat", "vision": False}
            out.append({"name": name, "kind": i["kind"], "vision": bool(i.get("vision"))})
        return out

    def check_kind(self, spec: dict[str, Any]) -> None:
        """Server-side guard for 'Add model': speech and embedding models only under their own kind (and nothing else under theirs)."""
        if spec.get("provider") != "ollama" or spec.get("kind_confirmed") or not spec.get("model_name"):
            return
        try:
            found = self.inspect("ollama", str(spec.get("endpoint") or ""), str(spec["model_name"]), None)
        except Exception:  # noqa: BLE001
            return
        want, got = spec.get("kind", "chat"), found["kind"]
        if got in ("stt", "embedding") and want != got:
            raise PAError(f"'{spec['model_name']}' looks like a {'speech-to-text' if got == 'stt' else 'embedding'} model - add it under "
                          f"{'Voice' if got == 'stt' else 'Embedding'} models, not here", code="wrong_kind")
        if want in ("stt", "embedding") and got != want:
            raise PAError(f"'{spec['model_name']}' does not look like a {'speech-to-text' if want == 'stt' else 'embedding'} model", code="wrong_kind")
        if want == "vision" and not found.get("vision"):
            raise PAError(f"'{spec['model_name']}' does not report image understanding (vision); choose a vision model such as qwen2.5vl or qwen3-vl", code="wrong_kind")

    # ------------------------------------------------------------------ vision (images, scanned pages)
    def describe_image(self, image: bytes, mime: str, prompt: str, sensitivity: int = 1, purpose: str = "vision") -> dict[str, Any]:
        """Ask the vision model about one image. Local models only for RESTRICTED data; nothing is stored here."""
        import base64
        m = self.role_model("vision")
        if m is None:
            raise PAError("no vision model is set up (Settings > AI Model > Vision models)", code="no_vision_model")
        if m["location"] == "remote" and sensitivity >= Sensitivity.RESTRICTED:
            raise PAError("RESTRICTED data can never be sent to a remote model", code="policy_denied")
        if len(image) > 20 * 1024 * 1024:
            raise PAError("image is too large for text recognition (20 MB limit)", code="too_large")
        self.gw.audit.write("vision.request", "model", model_id=m["id"], bytes=len(image), location=m["location"], purpose=purpose)
        b64 = base64.b64encode(image).decode()
        t0 = time.time()
        if m["provider"] == "dev_mock":
            return {"text": "[mock vision] a test image", "model": "dev_mock"}
        name = m["model_name"] or m["name"]
        with self._sem:
            if m["provider"] == "ollama":
                url = m["endpoint"].rstrip("/").removesuffix("/v1") + "/api/chat"
                body = {"model": name, "stream": False, "options": {"temperature": 0},
                        "messages": [{"role": "user", "content": prompt, "images": [b64]}]}
                headers = {}
            else:
                base, headers = self._endpoint(m)
                url = base + "/chat/completions"
                body = {"model": name, "temperature": 0, "stream": False, "messages": [{"role": "user", "content": [
                    {"type": "text", "text": prompt}, {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}}]}]}
            try:
                with httpx.Client(timeout=httpx.Timeout(600, connect=10), trust_env=False) as c:
                    r = c.post(url, json=body, headers=headers)
            except httpx.HTTPError as e:
                self.gw.netlog.record("llm", "POST", url, None, outcome="error", reason=type(e).__name__, bytes_out=len(image), purpose="vision",
                                      duration_ms=int((time.time() - t0) * 1000))
                raise PAError(f"cannot reach the vision model: {type(e).__name__}", code="model_unreachable") from e
        self.gw.netlog.record("llm", "POST", url, r.status_code, purpose="vision", bytes_out=len(image), duration_ms=int((time.time() - t0) * 1000))
        if r.status_code >= 400:
            raise PAError(f"vision model returned {r.status_code}", code="model_error")
        try:
            j = r.json()
            text = (j.get("message") or {}).get("content") if m["provider"] == "ollama" else j["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            raise PAError("vision model sent an unreadable answer", code="model_error") from e
        return {"text": (text or "").strip(), "model": m["name"], "ms": int((time.time() - t0) * 1000)}

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
