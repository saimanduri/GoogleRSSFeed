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
| Change how it looks | **Settings → Appearance & Voice**: pick a theme card (System, Day & night, Light, Dark, Aurora, Ocean, Forest, Sunset) and an animated background (or Off). *Day & night* switches between light and dark by your PC clock (times are editable below). *Reduce motion* turns the animation off. |
| Find an old chat or something the agent did | **History** in the left bar: chats and agent work grouped Today / Yesterday / Previous 7 days…, with search (3+ letters). Click an item to open it. |
| Keep chats organised | In the chat list hover a chat: the **pin** keeps it at the top, the **folder** button files it under a folder name. Chats are grouped Pinned, folders, then by date. |
| Type a shortcut | In the chat box type `/` for commands: `/new`, `/remind …`, `/mission …`, `/search …`, `/history`, `/steps`, `/pin`, `/theme ocean`, `/lock`, `/help`. Up/Down + Tab or Enter choose one. |
| Jump around a long chat | Floating **↑ / ↓** buttons appear at the right edge; the ↓ button shows how many new messages arrived while you were scrolled up. |
| See how much AI I used today | The meter under the chat box shows today's tokens against your daily budget (Settings → Autonomy & Budgets). |
| Run without installing | See README option C: copy the **Portable** folder anywhere and double-click `pa-ui.exe` (no admin). Note: no firewall isolation in this mode. |
| Stop everything | **STOP ALL**. Release it in Settings → Emergency Stop (password). |

Times are shown in your PC's own clock and language: "2:35 PM" today, "Yesterday 4:10 PM", "Mon 9:05 AM", then the date.

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

## New in 0.1.7
- **Attach a file from your PC** (paperclip next to the message box, `/attach`, or drag a file onto the window): spreadsheets (.xlsx/.csv), documents (.docx/.pdf/.txt...) are read where they are, not uploaded. Ask for totals, filters, groupings; a 100 MB workbook is indexed once (1-2 minutes) and then answers in seconds.
- **Outlook** (left menu) and **Settings > Email monitoring**: switch on ready-made read-only skills (hourly inbox check, emails waiting for my approval, deadline radar, morning briefing, meeting preparation, follow-up tracker, mail that needs my reply, VIP alert, weekly wrap-up, inbox clean-up).
- **Activity log > Network Logs**: every request the application made, with times and results.
- **GPU meter** above Settings; **Alt+letter** shortcuts for every menu item (shown next to the item); **pictures** for the assistant and for you (Settings > Appearance).

## When something does not work
- **Stuck on "Waiting for the ChiRAG Agent service"**: the window starts the service itself; if it stays for more than a minute close it completely (tray icon) and start again. (A stale `gateway.json` is now detected.)
- **"Temporarily blocking new keys" while creating the account**: the security chip is rate-limiting your Windows user; wait 10-30 minutes. Developer mode (`scripts\run-dev.ps1`) avoids the TPM for testing.
- **Files are rejected as "not scanned"**: Microsoft Defender is turned off (another antivirus is active). Turn Defender's real-time protection on or ask for another scanner option.
- **A routine does not run**: open Missions & Routines; a warning with a "Fix this" button explains what is missing (web search needs a provider and key; Outlook skills need Local Outlook connected).
- **Outlook shows its own security prompt**: click Allow in Outlook; the app never clicks it for you.
- **Nothing new after an update**: make sure the new bundle was installed (Settings > Diagnostics & About shows the build); the old installed copy starts automatically at login.
