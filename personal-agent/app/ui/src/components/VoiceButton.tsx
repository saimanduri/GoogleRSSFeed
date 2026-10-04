// Voice input that survives long dictation: audio is cut into ~30 second pieces while you speak and each piece is transcribed in the
// background (Settings > AI Model > Voice models), so the text appears as you go and a problem late in a 10-minute dictation never loses
// what was already transcribed. Finished text is also kept in a draft (this window only) until it is sent. Nothing is stored as audio.
import { useEffect, useRef, useState } from "react";
import { errText, useApp } from "../app";
import { LiveRecorder } from "../voiceStream";
import { Button } from "./ui";

const DRAFT_KEY = "pa.voice.draft";
const MAX_MINUTES = 60;

function blobToB64(b: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(String(r.result).split(",")[1] ?? "");
    r.onerror = reject;
    r.readAsDataURL(b);
  });
}
const mmss = (s: number) => `${String(Math.floor(s / 60)).padStart(2, "0")}:${String(s % 60).padStart(2, "0")}`;

export function readVoiceDraft(): string { try { return localStorage.getItem(DRAFT_KEY) ?? ""; } catch { return ""; } }
export function clearVoiceDraft() { try { localStorage.removeItem(DRAFT_KEY); } catch { /* private window: nothing to clear */ } }

/** onPartial: text so far (while speaking); onFinal: complete text after Stop. */
export function VoiceButton({ onStart, onPartial, onFinal }: { onStart?: () => void; onPartial?: (t: string) => void; onFinal: (t: string) => void }) {
  const { call, toast } = useApp();
  const [rec, setRec] = useState(false);
  const [busy, setBusy] = useState(false);
  const [secs, setSecs] = useState(0);
  const [done, setDone] = useState(0);
  const [total, setTotal] = useState(0);
  const recorder = useRef<LiveRecorder | null>(null);
  const parts = useRef<Map<number, string>>(new Map());
  const queue = useRef<Promise<void>>(Promise.resolve());
  const pending = useRef(0);
  const failed = useRef<{ wav: Blob; index: number }[]>([]);
  const timer = useRef<number | undefined>(undefined);

  const text = () => [...parts.current.entries()].sort((a, b) => a[0] - b[0]).map((e) => e[1]).filter(Boolean).join(" ").trim();
  const publish = () => {
    const t = text();
    try { if (t) localStorage.setItem(DRAFT_KEY, t); } catch { /* storage unavailable: the text is still in the message box */ }
    onPartial?.(t);
  };
  const transcribe = async (wav: Blob, index: number, attempt = 1): Promise<boolean> => {
    try {
      const r = await call<any>("voice.transcribe", { audio_b64: await blobToB64(wav), mime: "audio/wav" });
      parts.current.set(index, String(r.text ?? ""));
      return true;
    } catch (e: any) {
      if (attempt < 3) { await new Promise((res) => setTimeout(res, 1500 * attempt)); return transcribe(wav, index, attempt + 1); }
      failed.current.push({ wav, index });
      toast(`Part ${index + 1} of your recording could not be transcribed (${errText(e)}). It will be tried again.`, "warn");
      return false;
    }
  };
  // pieces are transcribed one after the other (in order) while the next 30 seconds are still being recorded
  const enqueue = (wav: Blob, index: number) => {
    pending.current++; setTotal((n) => n + 1);
    queue.current = queue.current.then(async () => {
      if (await transcribe(wav, index)) { setDone((n) => n + 1); publish(); }
      pending.current--;
    });
  };

  const start = async () => {
    parts.current = new Map(); failed.current = []; setDone(0); setTotal(0); setSecs(0);
    const r = new LiveRecorder((wav, i) => enqueue(wav, i));
    try { await r.start(); } catch (e: any) { toast(`Microphone unavailable: ${e.message ?? e}`, "danger"); return; }
    recorder.current = r;
    onStart?.();
    setRec(true);
    timer.current = window.setInterval(() => {
      const s = Math.floor((Date.now() - r.started) / 1000);
      setSecs(s);
      if (s >= MAX_MINUTES * 60) void stop();
    }, 500);
  };
  const stop = async () => {
    window.clearInterval(timer.current);
    const last = recorder.current?.stop();
    recorder.current = null;
    setRec(false); setBusy(true);
    if (last) enqueue(last.wav, last.index);
    try {
      await queue.current;
      // one more try for pieces that failed while recording
      for (const f of failed.current.splice(0)) { if (await transcribe(f.wav, f.index)) setDone((n) => n + 1); else failed.current.push(f); }
      publish();
      const t = text();
      if (failed.current.length) toast(`${failed.current.length} part(s) could not be transcribed - the rest is in your message.`, "warn");
      if (t) { clearVoiceDraft(); onFinal(t); } else toast("Didn't catch that - try again", "warn");
    } finally { setBusy(false); }
  };
  useEffect(() => () => { window.clearInterval(timer.current); recorder.current?.stop(); }, []);

  const label = rec ? `Stop recording (${mmss(secs)}${total ? `, ${done}/${total} parts transcribed` : ""})` : "Speak (voice input) - long dictation is transcribed every 30 seconds";
  return (
    <span className="mic">
      <Button kind={rec ? "danger" : "ghost"} icon="mic" busy={busy} title={label} onClick={rec ? () => void stop() : () => void start()} />
      {rec && <span className="small faint" style={{ marginLeft: 4 }} aria-live="polite">{mmss(secs)}{total ? ` · ${done}/${total}` : ""}</span>}
    </span>
  );
}
