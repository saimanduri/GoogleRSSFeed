# UI review against the "apple-design" skill (2026-10-01)

Source: https://github.com/emilkowalski/skills/blob/main/skills/apple-design/SKILL.md (16 rules: response, direct
manipulation, interruptibility, springs, velocity hand-off, momentum, spatial consistency, rubber-banding, gesture
feel, frame smoothness, materials & depth, multimodal feedback, reduced motion/transparency/contrast, typography,
eight design foundations, process). Evidence below comes from the current CSS/TSX (`app/ui/src`) and measured
contrast ratios of the eight themes. Status of this work: **implemented in 0.1.6** (see "Implementation status" at the end); the text below is the original review.

Relevance note: the skill is written for touch/gesture interfaces. Personal Agent is a Windows desktop app (mouse +
keyboard), so rules about flick momentum, rubber-banding and velocity hand-off mostly do **not** apply; the rules
about response, interruptible/spring motion for panels and dialogs, materials, accessibility, typography, wayfinding
and forgiveness **do**.

## Scorecard
| # | Rule | Now | Verdict |
|---|---|---|---|
| 1 | Response (feedback on press, no artificial delay) | `:active` press state exists on `.btn` only (1 rule); nav items, list rows, theme cards, float buttons, slash items have hover but no pressed state; actions fire on `click` (release) | Partial |
| 2 | Direct manipulation | no drag interactions except native file drop; side panels cannot be resized; nothing is draggable (pins/folders by menu only) | Gap (low value) |
| 3 | Interruptibility | all motion is CSS `transition`/`@keyframes` with fixed durations; modals, palette, slash menu and toasts **unmount instantly** (no exit animation); nav collapse is a hard grid change | Gap |
| 4 | Springs / behaviour over animation | none; single easing `cubic-bezier(.2,.8,.2,1)`, 180 ms | Gap |
| 5-6 | Velocity hand-off, momentum projection | not applicable on desktop | N/A |
| 7 | Spatial consistency / anchored origin | modal and palette fade in place; they do not originate from the control that opened them; side panels appear without a path | Gap |
| 8 | Hint in direction of gesture | n/a | N/A |
| 9 | Rubber-banding | native scroll only | N/A |
| 10 | Gesture feel (hysteresis, cancel) | buttons fire on release; no drag-away-to-cancel (OK for desktop) | Fine |
| 11 | Frame smoothness | animates mostly `transform`/`opacity`, but the aurora backdrop animates 3 viewport-size elements with `filter: blur(90px)`; no `will-change` anywhere | Risk |
| 12 | Materials & depth | translucent chrome only when a background is on (78 % + blur 14 + saturate); scrim has static blur(3px); **13 hard 1 px dividers** instead of scroll-edge fades; modal enter is a plain fade; stacked dialogs do not push the parent back; no bright top edge on glass | Partial |
| 13 | Multimodal feedback | visual only; no sound/haptics | Fine (desktop; add sound only for approvals/errors, optional) |
| 14 | Reduced motion / transparency / contrast | `prefers-reduced-motion` only zeroes `--dur`; infinite keyframes (typing dots, pulse, spinner, backdrop) are not replaced by cross-fades; **no** `prefers-reduced-transparency`, **no** `prefers-contrast`; the animated backdrop is a full-viewport moving background (the skill's own "avoid" list) | Gap |
| 15 | Typography | system font (Segoe UI Variable) **good**; text-size setting scales via `calc(14px*scale)` **good**; tracking only `-0.01em` on h1/brand and `-0.02em` stats - no size-specific scale; headings have no explicit leading; some paddings in px | Partial |
| 16 | Foundations | see below | Mixed |
| 17 | Process | `?demo` mock preview is an interactive prototype **good**; no frame-by-frame motion review; no user test | Partial |

## Concrete defects found (measured)
1. **Contrast (WCAG AA 4.5:1):** `--text-3` on cards fails in Light 3.9, Ocean 3.6, Sunset 3.9; white button text on the
   accent fails in Dark 3.3, Ocean 4.1, Sunset 3.6; accent-coloured text/links on cards fail in Ocean 4.1 and Sunset 3.6.
   (Aurora and Forest pass everywhere.) This is also the skill's "vibrancy" rule: text over blurred/animated
   backgrounds must stay legible.
2. **Animated backdrop vs. reduced-motion guidance:** large moving full-viewport blobs are what the skill (and WCAG 2.3.3)
   say to avoid. We stop them with the in-app switch and the OS media query, but the default is ON. Safer default:
   "Off" unless the user opted in, or a much subtler/static gradient with slow opacity change only.
3. **No exit animations / unmount on close:** modals, palette, slash menu, toasts vanish; combined with fixed-duration
   keyframes this breaks interruptibility (you cannot reverse an opening dialog mid-way).
4. **Destructive actions use `window.confirm`** (delete chat, files, all memory): the skill's *Agency* rule prefers
   forgiveness (undo) and confirmations only for truly irreversible actions. Chat delete is irreversible in the
   gateway today, so an **undo window** needs a soft-delete (e.g. 10 s) rather than a client trick.
5. **Wayfinding gaps:** the Chat screen has no page title in the content area (title only in the header bar); top bar
   shows the app name, not the current location; settings sections are clear, but no "back to where I was".
6. **Labels:** "Home" and "Activity" are generic umbrellas; "Tasks" and "Missions & Routines" overlap in meaning.
   The skill prefers specific names (e.g. Today / Activity log / Running now). Needs a short user vocabulary check
   before renaming.
7. **Press feedback is inconsistent** (rule 1): only `.btn` has `:active`.
8. **Hard dividers** (rule 12): chat header, composer, steps panel, nav, settings nav use 1 px borders where a soft
   scroll-edge fade over content would match the translucent chrome.

## What is already good
System font + user text scaling; consistent spacing/radius tokens; instant hover/focus-visible styles; feedback in
all four kinds (status pill, toasts, inline errors, confirmations); proximity/grouping on settings rows; plain-language
copy; dark/light + 6 more themes; reduced-motion switch in Settings; an interactive prototype (`?demo`).

## Improvement plan (ordered by value/effort)
**A. Accessibility fixes (small, high value)**
 1. Re-tune tokens so every theme meets 4.5:1 (text-3, accent text, button text); add a unit test that computes the
    ratios from `themes.css` (script used for this review) so regressions fail CI.
 2. `@media (prefers-reduced-transparency: reduce)` -> solid chrome, no blur; `@media (prefers-contrast: more)` ->
    near-solid surfaces + visible borders; extend `prefers-reduced-motion` to replace slide/scale with short
    cross-fades and stop infinite loops (typing dots -> static dots, pulse -> static ring).
 3. Default animated background to **Off** for new users (keep it as an opt-in), or ship a calmer static gradient.

**B. Motion quality (medium)**
 4. Add a small motion layer (Motion / `framer-motion`, or hand-rolled spring) with two presets from the skill:
    critically damped (damping 1.0, response 0.3-0.4 s) for panels/dialogs/toasts, slight bounce (0.8) only for
    momentum-like actions (none yet). Make Modal, Palette, Slash menu, Toast and Steps panel **interruptible with
    exit animations** (`AnimatePresence`), animating only `transform`/`opacity`.
 5. Anchor origins: dialog scales from its trigger, palette from top-centre, toasts slide from their edge; materialize
    the scrim (blur + opacity together).
 6. Add press states to every interactive element via one shared rule (`:active { transform: scale(.97) }` for
    buttons/cards, background shift for rows); fire primary actions on `pointerdown` highlight (visual) and `click`
    (commit).
 7. `will-change: transform` only on the elements that are about to move; cap the backdrop to a single composited
    layer (pre-blurred gradient image instead of live `filter: blur`).

**C. Materials & type (medium)**
 8. Replace the 13 hard borders by a scroll-edge fade (gradient mask) + 1 px highlight on top edges of glass;
    heavier material for structural panes (nav), lighter for controls; dim + push back a parent dialog when a
    second one opens (loosen/step-up dialogs).
 9. Type scale tokens: display -0.02em/1.1, title -0.01em/1.2, body 0/1.5, caption +0.01em/1.4, all-caps +0.06em;
    convert remaining px paddings to `rem` so the text-size setting scales layout; `font-optical-sizing: auto`.

**D. Agency, forgiveness, wayfinding (needs a little backend)**
 10. Soft-delete with an **Undo toast** for chats, files and memory (`deleted_at` + purge after 10-60 s; the audit log
     records both delete and undo).
 11. Page title + breadcrumb in the content header on every screen (Chat included); remember last screen on unlock.
 12. Vocabulary review of nav labels with real users, then rename.

**E. Optional**: resizable steps panel and chat list with pointer capture (rule 2); drag-to-pin; sound cue for
approval requests only (rule 13: utility); frame-by-frame review of each animation in the Tauri window.

## Not worth doing (does not fit a desktop agent)
Momentum projection, velocity hand-off, rubber-banding, flick gestures, haptics.

## Done-when
Contrast test green for all themes; OS reduced-motion/transparency/contrast respected; every overlay animates in and
out and can be interrupted; undo available for deletions; 60 fps while the backdrop is on (Tauri dev tools); manual
checklist items H1-H9 plus a new "motion review" pass in TESTS.md.

## Implementation status (0.1.6, 2026-10-01)
Done: A1-A3 (contrast + test, OS reduced motion/transparency/contrast, background default Off); B4-B7 (CSS spring tokens,
exit animations via ghost copy, anchored dialog origin, press states, no live blur); C8-C9 (soft edges, type scale,
scale-aware units); D10-D11 (Undo, breadcrumb, remembered screen). Differences from the plan: no motion library was added
(CSS transitions + `@starting-style` + `linear()` spring; fewer dependencies); Undo is a UI-side deferral (8 s) that is
flushed on lock/close, not a backend soft delete (a crash inside the window keeps the item); only "Activity" was renamed
(the rest of D12 needs your wording). Not done: stacked-dialog depth, resizable panes, drag-to-pin, sounds (section E).
