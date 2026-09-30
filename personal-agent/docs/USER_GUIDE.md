# User guide - and "How your data is protected"

## How your data is protected (the short version)
- **Everything is encrypted on this PC** with keys that only open with your password - or with your PIN
  *plus* your recovery key *plus* this PC's TPM chip. Nobody (including us) can recover your password.
- **The AI never holds the keys.** It runs in a separate program (pa-core) that has no internet and no access
  to your vault. Every action it wants to take goes through the gateway, which checks it against your rules.
- **Only the gateway can reach the internet**, and only to places you allow. Before anything leaves the PC it
  is checked for your stored secrets, card numbers, account numbers and your own blocked words.
- **Anything that sends or writes outside this PC needs your approval** - you see the exact email, query or URL.
  When a conversation contains confidential data, even web searches need your OK.
- **Your secrets are never shown to the AI.** A key you bind to a tool is inserted by the gateway at the last
  moment, in memory only.
- **Everything is logged** in a tamper-evident security log (who, what, when - never your content).
- **STOP ALL** (red button, tray menu or Ctrl+Alt+Shift+S) halts everything within 2 seconds.

## Everyday use
| I want to… | Do this |
|---|---|
| Rename the assistant / change my name | Setup asks for **Your name** and **Name your assistant**. Change either any time in **Settings → Account & Security → Profile**. The sign-in screen greets you by name and the assistant answers to its name in chat. |
| Ask something | **Chat** → type, or click the mic and speak. Watch **Steps** on the right to see exactly what it does. |
| Be reminded | Say or type "remind me to … on Friday at 9" → press **Confirm** on the card. You'll get a Windows notification. |
| Automate something | **Missions & Routines → Describe in plain words** ("every weekday at 7:30 summarise important mail") → review → **Activate**. |
| Let it read my mail | **Settings → Connectors** → turn on Local Outlook or Microsoft 365 (sign in). Choose "Use in chat / missions". |
| Add documents | **My Files → Upload** or drag and drop. Files are scanned first; problem files stay in Quarantine. |
| Teach it about me | **Memory** → type what it should remember. Review what it *inferred* in "Proposed". |
| Keep a password | **Secrets → Add**. Reveal/copy asks for your PIN; the value hides after 20 s. |
| See what happened overnight | **Home** ("While you were away") and **Activity** (every request, every step, the security log). |
| Use a local model | **Settings → AI Model → Add a model** (Ollama, vLLM, LM Studio, Run:ai or a .gguf file) → **Test model** → choose it as default. |
| Make it stricter/looser | **Settings**. Stricter applies instantly. Looser asks for your password and a 10-second read. |
| Stop everything | **STOP ALL**. Release it in Settings → Emergency Stop (password). |

## Locking
The window locks after the idle time you set (default 10 min) and when Windows locks. Unlock with your PIN.
After a reboot, sign-out or long time away, your password is needed. Missions keep running while the window
is locked (unless you turned on "Pause missions while Windows is locked").

## What the badges mean
PUBLIC / INTERNAL / CONFIDENTIAL / RESTRICTED - how sensitive the data in a chat or task is. The highest level
that has entered a conversation decides what may leave the PC (web access with CONFIDENTIAL data needs your
approval; RESTRICTED data never leaves).
"Reduced isolation" / "REMOTE" on a model - the model runs outside the built-in runtime; REMOTE means your
chat content is sent to another machine.

See also: [RECOVERY_GUIDE.md](RECOVERY_GUIDE.md).
