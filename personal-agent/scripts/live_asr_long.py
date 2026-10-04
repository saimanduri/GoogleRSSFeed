"""Long-dictation check of voice input with the REAL local speech model (default frozenlab/qwen3-asr:1.7b).

Makes a ~10 minute spoken recording with Windows' own voice, then transcribes it the way the app's microphone button does: pieces of about
30 seconds cut at quiet moments, sent one after the other through the gateway (voice.transcribe); and, for comparison, the whole recording
in a single call (the gateway cuts it itself). Prints timings, the word error rate against the script, and how long the user would wait
after pressing Stop (only the last piece). Developer mode only, throwaway account and data folder; nothing is kept.
    python scripts\\live_asr_long.py [minutes]
"""
import base64
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import wave
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))
from pa_common.pipeclient import PipeClient  # noqa: E402

MINUTES = float(sys.argv[1]) if len(sys.argv) > 1 else 10
MODEL = "frozenlab/qwen3-asr:1.7b"
SENTENCES = [
    "The quarterly planning meeting starts on Monday morning and the whole team is expected to attend.",
    "Please send me the updated budget spreadsheet before the end of the week so that finance can review it.",
    "We need to decide whether to renew the contract with the current supplier or to look for a cheaper alternative.",
    "The customer in Pune asked for a detailed report about the delivery delays during the last month.",
    "Remember to book the conference room and to order lunch for all the participants.",
    "The new software release fixes several bugs and improves the speed of the search function.",
    "Our security review found no critical problems but recommended stronger passwords and regular backups.",
    "I will call the dentist tomorrow morning to move the appointment to the following Friday.",
    "The train from Mumbai arrives at half past six, so we should leave the office a little earlier.",
    "Training for the new employees will cover safety rules, expense reports and the use of the internal portal.",
    "The marketing team prepared three different designs and we will choose the best one after the survey.",
    "Please check that every invoice has a purchase order number before it is sent for approval.",
]


def wer(ref: str, hyp: str) -> float:
    norm = lambda s: re.sub(r"[^a-z0-9 ]", "", s.lower().replace("-", " ")).split()  # noqa: E731
    r, h = norm(ref), norm(hyp)
    d = list(range(len(h) + 1))
    for i, rw in enumerate(r, 1):
        prev, d[0] = d[0], i
        for j, hw in enumerate(h, 1):
            prev, d[j] = d[j], min(d[j] + 1, d[j - 1] + 1, prev + (rw != hw))
    return d[len(h)] / max(1, len(r))


def pcm_of(path: Path) -> np.ndarray:
    with wave.open(str(path)) as w:
        return np.frombuffer(w.readframes(w.getnframes()), dtype="<i2")


def wav_b64(pcm: np.ndarray) -> str:
    b = io.BytesIO()
    with wave.open(b, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(pcm.astype("<i2").tobytes())
    return base64.b64encode(b.getvalue()).decode()


def segments(pcm: np.ndarray, target=30, mx=40, quiet_ms=350):
    """Same rule as the window (voiceStream.ts Segmenter): >= 30 s, cut where the last 350 ms are quiet, never beyond 40 s."""
    out, start, rate = [], 0, 16000
    q = int(quiet_ms / 1000 * rate)
    pos = start + target * rate
    while pos < len(pcm):
        win = pcm[max(start, pos - q):pos].astype(np.float32) / 32768
        if np.sqrt(np.mean(win ** 2)) < 0.012 or pos - start >= mx * rate:
            out.append(pcm[start:pos])
            start = pos
            pos = start + target * rate
        else:
            pos += 1024
    if len(pcm) - start > rate * 0.4:
        out.append(pcm[start:])
    return out


data = Path(tempfile.mkdtemp(prefix="pa-long-"))
script = []
n = 0
while len(" ".join(script).split()) < MINUTES * 150:          # ~150 words per minute of speech
    script.append(SENTENCES[n % len(SENTENCES)])
    n += 1
text = " ".join(script)
wav = data / "long.wav"
tmp_txt = data / "script.txt"
tmp_txt.write_text(text, encoding="utf-8")
ps = ("Add-Type -AssemblyName System.Speech; $s=New-Object System.Speech.Synthesis.SpeechSynthesizer;"
      "$f=New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(16000,[System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen,[System.Speech.AudioFormat.AudioChannel]::Mono);"
      f"$s.SetOutputToWaveFile('{wav}',$f);$s.Speak([IO.File]::ReadAllText('{tmp_txt}'));$s.Dispose()")
t0 = time.time()
subprocess.run(["powershell", "-NoProfile", "-Command", ps], check=True)
pcm = pcm_of(wav)
print(f"recording: {len(pcm) / 16000 / 60:.1f} min of speech, {len(text.split())} words (made in {time.time() - t0:.0f} s)")

env = dict(os.environ, PA_DEV_MODE="1", PA_FORCE_SOFTWARE_PROTECTOR="1", PA_TEST_FAST_KDF="1", PA_SKIP_DEFENDER="1", PA_DATA_DIR=str(data / "gw"), PYTHONPATH=str(ROOT / "app"))
gw = subprocess.Popen([str(ROOT / ".venv/Scripts/python.exe"), "-m", "pa_gateway", "--data-dir", str(data / "gw")], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    rv = data / "gw" / "run" / "gateway.json"
    for _ in range(100):
        if rv.exists():
            break
        time.sleep(0.2)
    info = json.loads(rv.read_text())
    ui = PipeClient(info["pipe"], "ui", info["ui_token"], "ui", expected_server_pid=info["pid"], timeout=60)
    r = ui.call("setup.create", {"username": "asrlong9", "password": "Zq-Live-Check-9921-abc", "pin": "480713"})
    g = r["recovery_key"].split("-")
    ui.call("setup.confirm_recovery", {"answers": {str(i): g[i] for i in r["confirm_groups"]}})
    add = ui.call("llm.add", {"model": {"provider": "ollama", "name": MODEL, "model_name": MODEL, "endpoint": "http://127.0.0.1:11434", "kind": "stt"}})
    for _ in range(120):
        m = ui.call("llm.models")["models"][0]
        if m["tested"]:
            break
        time.sleep(2)
    print("speech model ready:", bool(m["tested"]))

    segs = segments(pcm)
    print(f"A) like the microphone button: {len(segs)} pieces of {[round(len(s) / 16000) for s in segs][:8]}... seconds")
    parts, per, t0 = [], [], time.time()
    for s in segs:
        t1 = time.time()
        out = ui.call("voice.transcribe", {"audio_b64": wav_b64(s), "mime": "audio/wav"})
        per.append(time.time() - t1)
        parts.append(out["text"])
    live = " ".join(parts)
    print(f"   total {time.time() - t0:.0f} s for {len(pcm) / 16000:.0f} s of speech (x{len(pcm) / 16000 / (time.time() - t0):.1f} faster than real time); "
          f"slowest piece {max(per):.1f} s; wait after Stop = last piece {per[-1]:.1f} s; WER {wer(text, live) * 100:.1f} %")
    t0 = time.time()
    try:
        whole = ui.call("voice.transcribe", {"audio_b64": wav_b64(pcm), "mime": "audio/wav"})["text"]
        print(f"B) whole recording in one call: {time.time() - t0:.0f} s; WER {wer(text, whole) * 100:.1f} %; words {len(whole.split())} of {len(text.split())}")
    except Exception as e:  # noqa: BLE001
        print(f"B) whole recording in one call is IMPOSSIBLE: {type(e).__name__}: {e} ({len(pcm) * 2 / 1e6:.0f} MB of audio) - this is why the app now sends 30 s pieces")
finally:
    gw.terminate()
