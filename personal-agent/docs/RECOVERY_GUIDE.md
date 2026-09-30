# Recovery guide

| Situation | What you need | Steps |
|---|---|---|
| Forgot **password** | PIN **and** recovery key, on this PC | Sign-in screen → *Forgot password?* → PIN + recovery key + new password. A **new recovery key** is shown - store it; the old one no longer works. Pending approvals are cancelled. |
| Forgot **PIN** | Password (+ recovery key, or generate a new one) | Sign in → Settings → Account & Security → Password, PIN & recovery → *Set new PIN*. |
| Lost **recovery key** | Password + PIN | Settings → Account & Security → *Generate new recovery key*. |
| Forgot password **and** lost PIN or recovery key | Not recoverable on this PC by design | Restore a backup if you remember the password used for that backup. |
| PIN stopped working after BIOS/TPM reset or motherboard change | Password | Sign in with the password, set a new PIN (a new recovery key is generated). |
| PIN disabled after 5 wrong tries | Password | Sign in with the password; the PIN works again. |
| "Recovery temporarily disabled" | Wait 1 hour | Five wrong recovery attempts lock recovery for an hour. |
| New PC / reinstalled Windows | A `.pabk` backup + the password used when it was made | First-run wizard → *Restore from a backup instead* → choose the file → restart the app → sign in with that password → set a new PIN and recovery key. |
| Restore on the same PC | Password at backup time | Settings → Backup & Restore → *Restore* (asks you to confirm it's you) → restart. The previous data is kept in `pre-restore-<time>`. |
| Security log integrity failed | - | Home shows a High event. Export the log (Settings → Logs), check `Activity → Security log`, consider restoring from backup. |
| App won't start: "data folder inside a cloud-synced folder" | - | Move `%LOCALAPPDATA%\PersonalAgent` out of OneDrive/Dropbox; backups may go to cloud folders (they're encrypted). |

## Backups
- Settings → Backup & Restore: choose a folder (external drive recommended), schedule daily/weekly, keep N versions.
- *Verify* performs a full test decryption. A warning appears if there was no backup for 14 days.
- Backups are encrypted with a key protected by your password (Argon2id). TPM-bound parts are not in the
  backup - that's why a restore on a new PC asks for a new PIN and recovery key.
