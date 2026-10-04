"""Live check of speech-to-text with the REAL local Ollama (default model frozenlab/qwen3-asr:1.7b).

Developer mode only (software key, throwaway data folder, throwaway account). Flow: add the model with the same call the
'Add model' button makes -> it must test itself in the background and become the speech-to-text default -> transcribe a
WAV file (default: a clip spoken by Windows' own voice) -> print the text and timings. Audio and text of your own
recordings are never stored by this script.
    python scripts\\live_asr_check.py [ollama_model] [wav_file]
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import base64
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))
from pa_common.pipeclient import PipeClient  # noqa: E402

MODEL = sys.argv[1] if len(sys.argv) > 1 else "frozenlab/qwen3-asr:1.7b"
data = Path(tempfile.mkdtemp(prefix="pa-asr-"))
wav = Path(sys.argv[2]) if len(sys.argv) > 2 else data / "say.wav"
if len(sys.argv) <= 2:  # speak a sentence with the Windows voice (local, offline)
    ps = ("Add-Type -AssemblyName System.Speech; $s=New-Object System.Speech.Synthesis.SpeechSynthesizer;"
          "$f=New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(16000,[System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen,[System.Speech.AudioFormat.AudioChannel]::Mono);"
          f"$s.SetOutputToWaveFile('{wav}',$f);$s.Speak('Remind me to call the dentist tomorrow at nine in the morning.');$s.Dispose()")
    subprocess.run(["powershell", "-NoProfile", "-Command", ps], check=True)
env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data),
           PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data)], env=env,
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    rv = data / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"])
    r = ui.call("setup.create", {"username": "asrcheck9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    ep = "http://127.0.0.1:11434"
    kind = ui.call("llm.inspect", {"provider": "ollama", "endpoint": ep, "model_name": MODEL})["kind"]
    print("detected kind:", kind)
    t0 = time.time()
    add = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": ep, "kind": kind}})
    print("added, testing in background:", add["testing"])
    while time.time() - t0 < 300:
        st = ui.call("llm.models")
        m = st["models"][0]
        if not m["testing"] and (m["tested"] or time.time() - t0 > 5):
            break
        time.sleep(2)
    print("tested:", bool(m["tested"]), "default set:", st["roles"].get("standard" if kind == "chat" else kind) == add["id"], f"({time.time() - t0:.0f} s)")
    for c in (m.get("test_report") or {}).get("checks", []):
        print("  check", c["name"], "ok" if c["ok"] else "FAIL", c.get("note") or c.get("error") or "")
    if kind == "stt":
        t1 = time.time()
        out = ui.call("voice.transcribe", {"audio_b64": base64.b64encode(wav.read_bytes()).decode(), "mime": "audio/wav"})
        print(f"transcript ({time.time() - t1:.1f} s, language {out.get('language')}): {out['text']}")
finally:
    gw.terminate()
