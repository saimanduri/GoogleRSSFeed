# Secrets handling

## Key hierarchy (spec 4.3) - `app/pa_gateway/vault/vault.py`

```
VMK  (256-bit random, generated at setup, never stored in plaintext)
 ├─ HKDF-SHA256 "pa/K_db/v1"          SQLCipher key for db/agent.db
 ├─ HKDF "pa/K_files/v1"              wraps per-file random keys (My Files, attachments, outputs)
 ├─ HKDF "pa/K_log/v1"                HMAC key of the security-log chain
 ├─ HKDF "pa/K_secret/v1"             wraps per-item keys of the Secrets vault
 ├─ HKDF "pa/K_ipc/v1"                reserved for IPC token derivation
 ├─ HKDF "pa/K_hdr/v1"                MAC over vault.header (tamper detection) + recovery-key check value
 ├─ HKDF "pa/K_dlp/v1"                keyed fingerprints of secret values (DLP without plaintext)
 ├─ HKDF "pa/K_skill/v1"              HMAC signatures of skill definitions
 └─ HKDF "pa/K_backup_local/v1"       wraps the password-derived key used by scheduled backups
```

`vault.header` (JSON, versioned) holds only wrapped forms:

| Field | Meaning |
|---|---|
| `pw.W_pw` | AES-256-GCM(KEK_pw, VMK), KEK_pw = Argon2id(password, salt_pw), params calibrated ≥ 1 s, ≥ 256 MiB, t ≥ 3, p = 4 |
| `reset.protector` | PIN-authorised key descriptor holding Enc(S_tpm) |
| `reset.W_reset` | AES-256-GCM(KEK_reset, VMK), KEK_reset = HKDF(S_tpm ‖ Argon2id(recovery key, salt_rk)) |
| `rk_check` | HMAC(K_hdr, recovery key) - lets "Set new PIN" verify the recovery key without the old PIN |
| `username`, `display_name`, `assistant_name` | Not secret. Readable before unlock so the sign-in screen can greet you; covered by the MAC, changed only while unlocked (`account.set_profile`) |
| `mac` | HMAC(K_hdr, header) - checked after every unlock; any edit → refuse |

There is **no** wrapped copy of the VMK that the PIN alone can open.

## PIN and TPM (spec 4.4) - `vault/protector.py`
- **TpmProtector** (normal operation): non-exportable RSA-2048 key created through the Windows
  *Microsoft Platform Crypto Provider* (CNG/NCrypt) with the PIN as usage authorisation; S_tpm is
  OAEP-encrypted to it. The TPM's dictionary-attack logic limits guessing in hardware. Quick unlock proves
  presence by decrypting a **fresh random challenge** with the PIN-authorised key.
- **SoftwareProtector** (developer mode only): Argon2id(PIN) wraps S_tpm. It exists so CI runners and PCs
  without a TPM can run the app for development. It is refused in release builds and flagged **High** on
  the Security Posture page (a 6-digit PIN protected only by Argon2id is offline-guessable).
- App-level limits: PIN disabled after 5 failures until the next password sign-in; recovery disabled 1 h
  after 5 failures; password delay 1,2,4,…,300 s after 3 failures (`auth/state.py`).
- TPM cleared / motherboard replaced → PIN and W_reset stop working; the password still works; set a new PIN.

## Flows (spec 4.5/4.6)
| Flow | Needs | Code |
|---|---|---|
| Cold start | username + password | `Gateway.sign_in` |
| Quick unlock | PIN (UI locked, keys still in memory, within max age) | `Gateway.quick_unlock` |
| Step-up | PIN or password; loosening always password | `Gateway.step_up`, `SettingsService.apply` |
| Forgot password | PIN (TPM) + recovery key on THIS PC → new password → **recovery key rotated**, approvals voided | `Gateway.forgot_password` |
| Change password | current password | `Gateway.change_password` |
| Forgot PIN | password + recovery key (or generate a new recovery key) | `Gateway.set_new_pin` |
| Lost recovery key | password + PIN | `Gateway.new_recovery_key` |
| Forgot password AND lost PIN or key | not recoverable by design; restore a backup with its password | - |

## Secrets vault (spec 6) - `secrets_store.py`
- Each item: random item key wrapped with K_secret (AAD = item id); the whole item (title, type, tags,
  username, URL, notes, value) is one AES-GCM blob. Stored in SQLCipher → double encryption.
- Versions kept encrypted; restore any version.
- **Bindings**: a secret can be bound to `web.search`, `llm:<model id>`, `m365.refresh_token` (managed).
  Only the gateway calls `value_for_binding()`, injecting the value into that request in memory.
  **There is no IPC method for pa-core that returns a secret.** Unbound secrets are never used.
- **DLP without plaintext**: every value ≥ 8 chars is fingerprinted as HMAC(K_dlp, value); outbound text
  (queries, URLs, email bodies, notifications) and tool results are scanned with a sliding window;
  a match blocks the call, raises a High Home event and is logged (never the value).
- Reveal/copy need step-up; reveal auto-hides after 20 s; the window is excluded from screen capture
  (`SetWindowDisplayAffinity(WDA_EXCLUDEFROMCAPTURE)`) while visible; copy sets
  `CanIncludeInClipboardHistory=0`, `CanUploadToCloudClipboard=0`, `ExcludeClipboardContentFromMonitorProcessing`
  and clears the clipboard after 30 s (10-120 s) if unchanged.
- Import CSV (optional secure delete of the CSV); export only as an encrypted archive (password).

## Memory hygiene (spec 4.7) and honest limits
- VMK and sub-keys live in `SecretBytes` (bytearray, `VirtualLock`ed on Windows) and are wiped on lock /
  sign-out / sleep without BitLocker / Windows sign-out.
- pa-gateway disables WER crash dumps for itself and sets `SetErrorMode` at start.
- **Limit (Python):** CPython and the crypto libraries copy `bytes` internally; zeroisation is best-effort.
  A Rust gateway would do better here; the process boundary (only pa-gateway has keys) is the main control.
- **SQLCipher `cipher_memory_security` is OFF.** On Windows it `VirtualLock()`s every allocation and
  overflows the stack once the working-set quota is exhausted (observed on CI). SQLCipher still wipes its
  own key material. Revisit if SQLCipher changes this behaviour (see PENDING_WORK.md).
- If malware runs as you while the vault is unlocked, it may capture keystrokes/screen (spec 4.8).

## Tokens
- M365 refresh token: Secrets vault (binding `m365.refresh_token`); access tokens in gateway memory only.
- Model API keys: vault (binding `llm:<id>`). Built-in runtime key: random per launch, memory only.
- IPC: per-launch UI token in `run\gateway.json` (ACL-protected, deleted on exit) + pa-core token passed on
  pa-core's stdin (never on disk or command line).

## Backups
`.pabk` = random backup key (AES-GCM, 4 MiB chunks with an authenticated end marker) wrapped with
Argon2id(password at backup time). Scheduled backups use a KEK derived from your password when you enabled
them, stored wrapped with K_backup_local, so they restore with that password. See RECOVERY_GUIDE.md.
