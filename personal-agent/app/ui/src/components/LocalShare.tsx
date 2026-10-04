// Sharing files and folders from this PC with ONE chat: chips (with "Allow again" after a new app session), the folder approval
// dialog (subfolders are a separate, explicit tick) and the cards that appear when the assistant asks for a subfolder.
import { useCallback, useEffect, useState } from "react";
import { onEvent } from "../api/gateway";
import { errText, useApp } from "../app";
import { Icon } from "./Icon";
import { Badge, Button, Modal } from "./ui";

const mb = (n: number) => (n >= 1e6 ? `${(n / 1e6).toFixed(0)} MB` : `${Math.max(1, Math.round(n / 1024))} KB`);

export function FolderDialog({ path, chatId, grant, onClose, onDone }: { path?: string; chatId: string; grant?: any; onClose: () => void; onDone: () => void }) {
  const { call, toast } = useApp();
  const [info, setInfo] = useState<any>(null);
  const [subs, setSubs] = useState(false);
  const [busy, setBusy] = useState(false);
  const target = grant?.folder ?? path ?? "";
  useEffect(() => { call("localfiles.folder_info", { path: target }).then(setInfo).catch((e: any) => { toast(errText(e), "warn"); onClose(); }); /* eslint-disable-next-line */ }, [target]);
  const go = async () => {
    setBusy(true);
    try {
      if (grant) await call("localfiles.reapprove", { grant_id: grant.id, include_subfolders: subs, confirm_subfolders: subs });
      else await call("localfiles.grant_folder", { chat_id: chatId, path: target, include_subfolders: subs, confirm_subfolders: subs });
      toast(subs ? "Folder and its subfolders shared with this chat" : "Folder shared with this chat (without subfolders)", "ok");
      onDone();
    } catch (e: any) { toast(errText(e), "danger"); } finally { setBusy(false); }
  };
  return (
    <Modal title={grant ? "Allow this folder again?" : "Share this folder with the chat?"} onClose={onClose}
      actions={<><Button onClick={onClose}>Cancel</Button><div className="spacer" /><Button kind="primary" busy={busy} disabled={!info} onClick={go}>{grant ? "Allow again" : "Share folder"}</Button></>}>
      {!info ? <div className="skeleton" style={{ height: 80 }} /> : (
        <div className="col">
          <div><b>{info.name}</b><div className="small faint mono ellipsis" title={info.path}>{info.path}</div></div>
          <p className="small" style={{ margin: 0 }}>The assistant may <b>read</b> the {info.files} supported file{info.files === 1 ? "" : "s"} directly inside this folder. It cannot change, delete or copy them, and it only keeps this permission for <b>this chat and this session</b> - after you sign out or restart you will be asked again.</p>
          {info.subfolders > 0 && (
            <label className="row" style={{ alignItems: "flex-start", gap: 8, padding: 10, border: "1px solid var(--border)", borderRadius: 10 }}>
              <input type="checkbox" checked={subs} onChange={(e) => setSubs(e.target.checked)} style={{ marginTop: 3 }} />
              <span><b>Also include the {info.subfolders}{info.truncated ? "+" : ""} subfolder{info.subfolders === 1 ? "" : "s"}</b> ({info.subfolder_files}{info.truncated ? "+" : ""} more file{info.subfolder_files === 1 ? "" : "s"}{info.subfolder_names.length ? `, e.g. ${info.subfolder_names.slice(0, 3).join(", ")}` : ""})
                <div className="small faint">Off by default. If you leave it off and the assistant needs a subfolder, it will ask you first.</div></span>
            </label>
          )}
          <div className="small faint">AppData, program, system and credential folders are never shared, and links that lead outside this folder are ignored.</div>
        </div>
      )}
    </Modal>
  );
}

export function ShareChips({ items, reload, chatId }: { items: any[]; reload: () => void; chatId: string }) {
  const { call, toast } = useApp();
  const [again, setAgain] = useState<any>(null);
  if (!items.length) return null;
  const allowAgain = async (f: any) => {
    if (f.scope === "folder") { setAgain(f); return; }
    try { await call("localfiles.reapprove", { grant_id: f.id }); reload(); } catch (e: any) { toast(errText(e), "danger"); }
  };
  return (
    <div className="attach-row">
      {items.map((f) => (
        <span key={f.id} className={`attach-chip ${f.expired ? "expired" : ""}`}
          title={f.scope === "folder" ? `${f.folder}\nFolder · read in place · ${f.recursive ? "subfolders allowed" : "subfolders NOT allowed"}${f.expired ? " · approval expired" : ""}`
            : `${f.folder}\n${mb(f.size)} · label ${f.sensitivity} · read in place, not uploaded${f.expired ? " · approval expired" : ""}`}>
          <Icon name={f.scope === "folder" ? "folder" : f.kind === "table" ? "files" : f.kind === "image" ? "eye" : "memory"} size={14} />
          <span className="ellipsis">{f.name}</span>
          {f.scope === "folder" ? <Badge tone={f.recursive ? "warn" : undefined}>{f.recursive ? "with subfolders" : "no subfolders"}</Badge> : <span className="faint small">{mb(f.size)}</span>}
          {f.expired && <button className="allow-again" title="This approval ended with the last session. Allow it again for this chat." onClick={() => void allowAgain(f)}>Allow again</button>}
          <button aria-label={`Remove ${f.name}`} title="Stop sharing with the chat (nothing on disk is touched)" onClick={async () => { await call("localfiles.revoke", { grant_id: f.id }); reload(); }}>✕</button>
        </span>
      ))}
      {again && <FolderDialog grant={again} chatId={chatId} onClose={() => setAgain(null)} onDone={() => { setAgain(null); reload(); }} />}
    </div>
  );
}

/** Cards shown when the assistant asked to read a subfolder that was not approved. */
export function SubfolderCards({ chatId, reload }: { chatId: string; reload: () => void }) {
  const { call, toast } = useApp();
  const [reqs, setReqs] = useState<any[]>([]);
  const load = useCallback(() => call<any[]>("localfiles.requests", { chat_id: chatId }).then(setReqs).catch(() => setReqs([])), [call, chatId]);
  useEffect(() => { void load(); return onEvent((t, d) => { if (t === "localfiles.request" && d?.chat_id === chatId) void load(); }); }, [load, chatId]);
  if (!reqs.length) return null;
  return (
    <div className="col" style={{ gap: 8, padding: "0 18px 8px" }}>
      {reqs.map((r) => (
        <div key={r.id} className="banner warn" role="alert" style={{ alignItems: "center" }}>
          <Icon name="folder" />
          <div className="grow"><b>The assistant wants to read inside the subfolder “{r.subfolder}”</b> of “{r.folder}”.
            <div className="small">Only for this chat and this session. Nothing is changed or copied.</div></div>
          <Button small kind="primary" onClick={async () => { try { await call("localfiles.allow_subfolders", { grant_id: r.grant_id, confirm: true }); toast("Subfolders allowed for this chat - ask again", "ok"); void load(); reload(); } catch (e: any) { toast(errText(e), "danger"); } }}>Allow subfolders</Button>
          <Button small onClick={async () => { await call("localfiles.deny_request", { request_id: r.id }); void load(); }}>Not now</Button>
        </div>
      ))}
    </div>
  );
}
