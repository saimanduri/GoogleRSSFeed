"""Speech-to-text audio helpers (pa_gateway/llm/audio.py): normalisation, chunking, Qwen3-ASR prefix."""
import io
import wave

import numpy as np
import pytest

from pa_common.errors import PAError
from pa_gateway.llm import audio as A


def _wav(rate=16000, ch=1, seconds=1.0) -> bytes:
    n = int(rate * seconds)
    t = np.arange(n) / rate
    x = (8000 * np.sin(2 * np.pi * 440 * t)).astype("<i2")
    if ch > 1:
        x = np.repeat(x[:, None], ch, axis=1).reshape(-1)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(ch)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(x.tobytes())
    return buf.getvalue()


def test_prefix_is_stripped():
    assert A.strip_asr_prefix("language English<asr_text>Hello there.") == ("Hello there.", "English")
    assert A.strip_asr_prefix("language None<asr_text>") == ("", "")
    assert A.strip_asr_prefix("plain text") == ("plain text", "")
    assert A.strip_asr_prefix("language Hindi<asr_text>नमस्ते") == ("नमस्ते", "Hindi")


def test_resample_stereo_48k_to_16k_mono():
    pcm = A.wav_to_pcm16k(_wav(rate=48000, ch=2, seconds=2.0))
    assert pcm.dtype == np.int16 and abs(len(pcm) - 32000) <= 2 and int(np.abs(pcm).max()) > 3000


def test_not_a_wav_is_rejected_and_junk_is_safe():
    assert not A.is_wav(b"\x1aE\xdf\xa3webm...")
    for junk in (b"RIFF" + b"\x00" * 60, b"RIFFxxxxWAVE" + b"\xff" * 200):
        with pytest.raises(PAError):
            A.wav_to_pcm16k(junk)


def test_too_long_is_refused():
    with pytest.raises(PAError) as e:
        A.wav_to_pcm16k(_wav(rate=8000, seconds=A.MAX_SECONDS + 5))
    assert e.value.code == "too_large"


def test_chunks_are_short_and_complete():
    pcm = A.wav_to_pcm16k(_wav(seconds=135))
    parts = A.split_chunks(pcm)
    assert len(parts) == 3 and all(len(p) <= A.CHUNK_SECONDS * 16000 for p in parts)
    assert sum(len(p) for p in parts) == len(pcm)
    assert len(A.split_chunks(pcm[:16000 * 10])) == 1


def test_roundtrip_wav_and_sample_clip():
    clip = A.sample_clip()
    assert A.is_wav(clip) and len(A.wav_to_pcm16k(clip)) == int(1.5 * 16000)
