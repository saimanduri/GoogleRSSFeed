// Records audio in the window (MediaRecorder) and sends it to the gateway, which forwards it to the configured
// speech-to-text model (Settings > AI Model > Speech-to-text). Nothing is stored unless it becomes a chat message.
import { useRef, useState } from "react";
import { errText, useApp } from "../app";
import { Button } from "./ui";

export function VoiceButton({ onText }: { onText: (t: string) => void }) {
  const { call, toast } = useApp();
  const [rec, setRec] = useState(false);
  const [busy, setBusy] = useState(false);
  const mr = useRef<MediaRecorder | null>(null);
  const chunks = useRef<Blob[]>([]);
  const start = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const m = new MediaRecorder(stream, { mimeType: MediaRecorder.isTypeSupported("audio/webm") ? "audio/webm" : "" });
      chunks.current = [];
      m.ondataavailable = (e) => e.data.size && chunks.current.push(e.data);
      m.onstop = async () => {
        stream.getTracks().forEach((t) => t.stop());
        const blob = new Blob(chunks.current, { type: m.mimeType || "audio/webm" });
        setBusy(true);
        try {
          const b64 = await blobToB64(blob);
          const r = await call<any>("voice.transcribe", { audio_b64: b64, mime: blob.type });
          if (r.text) onText(r.text); else toast("Didn't catch that - try again", "warn");
        } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
      };
      m.start();
      mr.current = m;
      setRec(true);
      setTimeout(() => { if (mr.current?.state === "recording") stop(); }, 120000);
    } catch (e: any) {
      toast(`Microphone unavailable: ${e.message ?? e}`, "danger");
    }
  };
  const stop = () => { mr.current?.stop(); setRec(false); };
  return (
    <span className={`mic ${rec ? "" : ""}`}>
      <Button kind={rec ? "danger" : "ghost"} icon="mic" busy={busy} title={rec ? "Stop recording" : "Speak (voice input)"} onClick={rec ? stop : start} />
    </span>
  );
}

function blobToB64(b: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(String(r.result).split(",")[1] ?? "");
    r.onerror = reject;
    r.readAsDataURL(b);
  });
}
