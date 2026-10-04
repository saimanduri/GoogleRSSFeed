"""Audio helpers for speech-to-text (voice input).

The window sends 16 kHz mono WAV (it converts the microphone recording itself). The gateway re-checks and
normalises it with the standard library + numpy only - no external converter is run, nothing is written to disk -
and splits long recordings into chunks the model can handle (Qwen3-ASR: about 60 s per request).
"""
from __future__ import annotations

import io
import re
import wave

import numpy as np

from pa_common.errors import PAError

TARGET_RATE = 16000
CHUNK_SECONDS = 50          # Qwen3-ASR handles ~60 s per request; keep a margin
MAX_SECONDS = 600           # refuse anything longer than 10 minutes
_PREFIX = re.compile(r"^\s*language\s+([A-Za-z][A-Za-z \-]{0,30}?)\s*<asr_text>", re.IGNORECASE)


def is_wav(b: bytes) -> bool:
    return len(b) > 44 and b[:4] == b"RIFF" and b[8:12] == b"WAVE"


def wav_to_pcm16k(b: bytes) -> np.ndarray:
    """Decode a PCM WAV file to mono int16 samples at 16 kHz."""
    try:
        with wave.open(io.BytesIO(b), "rb") as w:
            ch, width, rate, n = w.getnchannels(), w.getsampwidth(), w.getframerate(), w.getnframes()
            if n <= 0 or rate <= 0 or ch <= 0:
                raise PAError("the recording is empty", code="invalid_request")
            if n / rate > MAX_SECONDS:
                raise PAError("recording too long", code="too_large")
            raw = w.readframes(n)
    except (wave.Error, EOFError, RuntimeError, ValueError, OverflowError) as e:
        raise PAError(f"unsupported audio (send 16-bit PCM WAV): {e}", code="invalid_request") from e
    if width == 2:
        x = np.frombuffer(raw, dtype="<i2").astype(np.float32)
    elif width == 1:
        x = (np.frombuffer(raw, dtype=np.uint8).astype(np.float32) - 128.0) * 256.0
    elif width == 4:
        x = np.frombuffer(raw, dtype="<i4").astype(np.float32) / 65536.0
    else:
        raise PAError("unsupported audio sample size (use 16-bit WAV)", code="invalid_request")
    if ch > 1:
        x = x[: (len(x) // ch) * ch].reshape(-1, ch).mean(axis=1)
    if rate != TARGET_RATE:
        x = _resample(x, rate)
    return np.clip(x, -32768, 32767).astype(np.int16)


def _resample(x: np.ndarray, rate: int) -> np.ndarray:
    if rate > TARGET_RATE:  # light low-pass (moving average) against aliasing, then linear interpolation
        k = max(1, int(round(rate / TARGET_RATE)))
        if k > 1:
            x = np.convolve(x, np.ones(k, dtype=np.float32) / k, mode="same")
    n_out = max(1, int(round(len(x) * TARGET_RATE / rate)))
    return np.interp(np.linspace(0, len(x) - 1, n_out), np.arange(len(x)), x).astype(np.float32)


def make_wav(pcm: np.ndarray) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(TARGET_RATE)
        w.writeframes(pcm.astype("<i2").tobytes())
    return buf.getvalue()


def split_chunks(pcm: np.ndarray, seconds: int = CHUNK_SECONDS) -> list[np.ndarray]:
    """Cut at the quietest moment in the last 4 s of each chunk so words are not split."""
    size = seconds * TARGET_RATE
    out, i = [], 0
    while len(pcm) - i > size:
        lo, hi = i + size - 4 * TARGET_RATE, i + size
        win = np.abs(pcm[lo:hi].astype(np.int32))
        frame = TARGET_RATE // 20
        n = len(win) // frame
        cut = hi
        if n > 0:
            e = win[: n * frame].reshape(n, frame).mean(axis=1)
            cut = lo + int(np.argmin(e)) * frame + frame // 2
        out.append(pcm[i:cut])
        i = cut
    out.append(pcm[i:])
    return [c for c in out if len(c)]


def strip_asr_prefix(text: str) -> tuple[str, str]:
    """Qwen3-ASR answers 'language English<asr_text>hello'. Returns (text, language)."""
    m = _PREFIX.match(text or "")
    if not m:
        return (text or "").replace("<asr_text>", "").strip(), ""
    lang = m.group(1).strip()
    return text[m.end():].strip(), ("" if lang.lower() == "none" else lang)


def sample_png(size: int = 96) -> bytes:
    """A solid red square as a PNG (standard library only) - 'Test model' for vision models asks for its colour."""
    import struct
    import zlib

    def chunk(t: bytes, d: bytes) -> bytes:
        return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xFFFFFFFF)
    raw = b"".join(b"\x00" + b"\xdc\x14\x14" * size for _ in range(size))
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


def sample_clip(seconds: float = 1.5) -> bytes:
    """A short synthetic clip (quiet tone sweep) used by 'Test model' for speech-to-text models."""
    t = np.arange(int(seconds * TARGET_RATE)) / TARGET_RATE
    x = 3000 * np.sin(2 * np.pi * (300 + 200 * t) * t) * np.minimum(1, np.minimum(t, seconds - t) * 8)
    return make_wav(x.astype(np.int16))
