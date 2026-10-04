// Choose a picture from this PC for the assistant or for yourself. The picture is shrunk to 128x128 in the window
// (nothing is uploaded anywhere) and stored in the encrypted settings.
import { ChangeEvent, useRef, useState } from "react";
import { errText, useApp } from "../app";
import { Button } from "./ui";

async function shrink(file: File, size = 128): Promise<string> {
  const bmp = await createImageBitmap(file);
  const c = document.createElement("canvas");
  c.width = c.height = size;
  const g = c.getContext("2d")!;
  const side = Math.min(bmp.width, bmp.height);          // centre-crop to a square
  g.drawImage(bmp, (bmp.width - side) / 2, (bmp.height - side) / 2, side, side, 0, 0, size, size);
  let url = c.toDataURL("image/png");
  if (url.length * 0.75 > 60_000) url = c.toDataURL("image/jpeg", 0.85);
  if (url.length * 0.75 > 60_000) url = c.toDataURL("image/jpeg", 0.6);
  return url;
}

export function IconUpload({ settingKey, title, help, fallback }: { settingKey: string; title: string; help: string; fallback: string }) {
  const { status, call, toast, refresh } = useApp();
  const input = useRef<HTMLInputElement>(null);
  const [busy, setBusy] = useState(false);
  const cur: string = status?.ui?.[settingKey] ?? "";
  const save = async (value: string) => {
    setBusy(true);
    try { await call("settings.apply", { changes: { [settingKey]: value } }); await refresh(); toast(value ? "Picture saved" : "Picture removed", "ok"); }
    catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  const onFile = async (e: ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    e.target.value = "";
    if (!f) return;
    if (!/^image\/(png|jpeg|webp)$/.test(f.type)) { toast("Choose a PNG, JPEG or WebP picture", "warn"); return; }
    try { await save(await shrink(f)); } catch { toast("Could not read that picture", "danger"); }
  };
  return (
    <div className="row" style={{ gap: 14, padding: "6px 0" }}>
      <span className="icon-preview">{cur ? <img src={cur} alt="" /> : <span aria-hidden="true">{fallback}</span>}</span>
      <div className="grow"><b>{title}</b><div className="small muted">{help}</div></div>
      <input ref={input} type="file" accept="image/png,image/jpeg,image/webp" hidden onChange={onFile} aria-label={`Choose ${title}`} />
      <Button small icon="upload" busy={busy} onClick={() => input.current?.click()}>Choose picture…</Button>
      {cur && <Button small kind="ghost" icon="trash" title={`Remove ${title}`} busy={busy} onClick={() => save("")}>Remove</Button>}
    </div>
  );
}
