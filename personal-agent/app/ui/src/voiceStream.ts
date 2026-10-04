// Live voice input: the microphone is captured as 16 kHz mono audio and cut into segments of about 30 seconds (at a quiet moment, so words
// are not split). Every finished segment is handed out at once - the caller transcribes it in the background - so a long dictation is never
// lost as one big recording. Pure logic (Segmenter, Resampler) is separate from the browser audio plumbing so it can be tested without a microphone.
import { encodeWav } from "./audioWav";

export const RATE = 16000;

/** Streaming downsampler (block averaging with a fractional step): any input rate -> 16 kHz. */
export class Resampler {
  private pos = 0;           // position inside the current input chunk, in input samples
  private carry: Float32Array = new Float32Array(0);
  constructor(private inRate: number) {}
  push(input: Float32Array): Float32Array {
    if (this.inRate === RATE) return input.slice();
    const data = new Float32Array(this.carry.length + input.length);
    data.set(this.carry); data.set(input, this.carry.length);
    const step = this.inRate / RATE;
    const out: number[] = [];
    let p = this.pos;
    while (p + step <= data.length) {
      const a = Math.floor(p), b = Math.min(data.length, Math.ceil(p + step));
      let sum = 0;
      for (let i = a; i < b; i++) sum += data[i];
      out.push(sum / Math.max(1, b - a));
      p += step;
    }
    const keep = Math.floor(p);
    this.carry = data.slice(keep);
    this.pos = p - keep;
    return Float32Array.from(out);
  }
}

export function rms(a: Float32Array, from = 0, to = a.length): number {
  let s = 0;
  const n = Math.max(1, to - from);
  for (let i = from; i < to; i++) s += a[i] * a[i];
  return Math.sqrt(s / n);
}

export type SegmenterOpts = { target?: number; max?: number; quietMs?: number; quietRms?: number; silentRms?: number };

/** Collects 16 kHz samples and returns finished segments: >= `target` seconds, cut in a quiet moment, never longer than `max` seconds. */
export class Segmenter {
  private chunks: Float32Array[] = [];
  private len = 0;
  private o: Required<SegmenterOpts>;
  constructor(opts: SegmenterOpts = {}) { this.o = { target: 30, max: 40, quietMs: 350, quietRms: 0.012, silentRms: 0.004, ...opts }; }
  get seconds() { return this.len / RATE; }
  private take(): Float32Array {
    const out = new Float32Array(this.len);
    let o = 0;
    for (const c of this.chunks) { out.set(c, o); o += c.length; }
    this.chunks = []; this.len = 0;
    return out;
  }
  private tailQuiet(): boolean {
    const n = Math.floor((this.o.quietMs / 1000) * RATE);
    const last = this.chunks[this.chunks.length - 1];
    if (last.length >= n) return rms(last, last.length - n, last.length) < this.o.quietRms;
    const all = this.peekTail(n);
    return rms(all) < this.o.quietRms;
  }
  private peekTail(n: number): Float32Array {
    const out = new Float32Array(Math.min(n, this.len));
    let need = out.length, o = out.length;
    for (let i = this.chunks.length - 1; i >= 0 && need > 0; i--) {
      const c = this.chunks[i], k = Math.min(c.length, need);
      o -= k; out.set(c.subarray(c.length - k), o); need -= k;
    }
    return out;
  }
  push(samples: Float32Array): Float32Array[] {
    this.chunks.push(samples); this.len += samples.length;
    const done: Float32Array[] = [];
    if (this.len >= this.o.target * RATE && (this.len >= this.o.max * RATE || this.tailQuiet())) {
      const seg = this.take();
      if (!this.isSilent(seg)) done.push(seg);
    }
    return done;
  }
  /** What is left when the user presses stop (null when it is silence or nothing). */
  flush(): Float32Array | null {
    if (!this.len) return null;
    const seg = this.take();
    return this.isSilent(seg) || seg.length < RATE * 0.4 ? null : seg;
  }
  private isSilent(a: Float32Array): boolean { return rms(a) < this.o.silentRms; }
}

export function toWavBlob(seg: Float32Array): Blob { return new Blob([encodeWav(seg, RATE)], { type: "audio/wav" }); }

/** Browser plumbing: microphone -> 16 kHz samples -> Segmenter -> onSegment(wav). Stop returns the last piece. */
export class LiveRecorder {
  private stream: MediaStream | null = null;
  private ctx: AudioContext | null = null;
  private node: ScriptProcessorNode | null = null;
  private seg = new Segmenter();
  private rs: Resampler | null = null;
  started = 0;
  constructor(private onSegment: (wav: Blob, index: number) => void, private onLevel?: (rms: number) => void) {}
  private index = 0;
  async start(): Promise<void> {
    this.stream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true } });
    const AC: typeof AudioContext = (window as any).AudioContext || (window as any).webkitAudioContext;
    this.ctx = new AC();
    this.rs = new Resampler(this.ctx.sampleRate);
    const src = this.ctx.createMediaStreamSource(this.stream);
    this.node = this.ctx.createScriptProcessor(4096, 1, 1);        // works under the app's strict content security policy (no worklet module needed)
    this.node.onaudioprocess = (e) => {
      const frame = this.rs!.push(e.inputBuffer.getChannelData(0).slice());
      this.onLevel?.(rms(frame));
      for (const s of this.seg.push(frame)) this.onSegment(toWavBlob(s), this.index++);
    };
    const mute = this.ctx.createGain(); mute.gain.value = 0;
    src.connect(this.node); this.node.connect(mute); mute.connect(this.ctx.destination);
    this.started = Date.now();
  }
  /** Stops the microphone and returns the final piece (or null) with its index. */
  stop(): { wav: Blob; index: number } | null {
    this.node?.disconnect();
    this.stream?.getTracks().forEach((t) => t.stop());
    void this.ctx?.close();
    const rest = this.seg.flush();
    return rest ? { wav: toWavBlob(rest), index: this.index++ } : null;
  }
}
