# Build a Local Outlook Proactive Personal Assistant

## 1. Objective

Build a **local-first personal work assistant** that monitors the user's locally installed Microsoft Outlook, understands incoming and outgoing email activity, maintains a lightweight **six-month working memory**, tracks topics/tasks/deadlines/commitments, and proactively highlights what requires the user's attention.

The assistant should behave like a **personal executive/work assistant**, not merely an email summarizer.

Its purpose is to continuously answer:

> **What happened? What matters? What do I need to do? What am I waiting for? What are others waiting for? What deadlines are approaching? What did I promise? What has been completed? What changed since I last checked?**

Pl note that initially I will test these integrations and features  with my local Outlook, carry out coding in such way that in future i can connect to Microsoft exchange server also API and have these features with minimal/no code changes.

---

# 2. Critical Architectural Principle

### Outlook remains the system of record.

The application must **NOT create a duplicate local copy of the user's mailbox**.

Do **not** permanently store:

- Complete email bodies
- Complete email HTML
- Email attachments
- Duplicate copies of Outlook messages
- Duplicate copies of the entire mailbox

The original email and attachments remain in the user's **local Outlook/mail store**.

The local assistant should store only the **structured metadata and intelligence necessary to understand, search, track and retrieve the original Outlook item when required**.

The local repository is therefore a:

> **Working Memory / Intelligence Repository**

and NOT a second mailbox.

---

# 3. Local Outlook Only

The application is intended to work with the user's **locally installed Outlook application/mailbox on the user's Windows machine**.

Do not architect the system around:

- Microsoft Graph
- Cloud webhooks
- Remote mailbox polling
- Server-side email synchronization

unless explicitly introduced later as an optional capability.

The initial implementation should interact with the locally available Outlook/mail store through an appropriate supported local integration mechanism.

The exact Outlook integration mechanism should be selected based on the Outlook version/environment and should minimize impact on Outlook performance.

---

# 4. Outlook Monitoring

The application should continuously monitor:

- New incoming emails
- New outgoing/sent emails
- Replies
- Changes to existing conversations where detectable
- Relevant changes to existing tracked topics

### Recommended initial monitoring frequency

Use approximately **30–60 seconds** for lightweight detection, with **60 seconds as the default starting point**.

However:

**Do NOT perform a full mailbox scan every 60 seconds.**

The monitoring mechanism must be incremental.

Maintain a local synchronization checkpoint such as:

- Last successfully processed timestamp
- Last processed Outlook item identifier
- Relevant folder/checkpoint information
- Last successful synchronization state

The application should inspect only new/changed items since the previous checkpoint.

The monitoring interval should be configurable.

---

# 5. Never Block Outlook

The assistant must be designed so that its operation cannot make Outlook unusable.

The Outlook monitoring component should be lightweight.

Do not perform expensive AI inference directly inside the Outlook monitoring operation.

Use:

```text
Outlook
   ↓
Lightweight Detection
   ↓
Local Event Queue
   ↓
Asynchronous Processing
   ↓
AI Analysis
   ↓
Working Memory
```

The monitoring component should quickly identify relevant Outlook items and place them into a processing queue.

AI processing must happen asynchronously.

If AI processing becomes slow, Outlook monitoring must continue independently.

---

# 6. Large Email / Attachment Protection

A single huge email, very long conversation or large attachment must **never cause the entire assistant to become stuck**.

Implement:

- Configurable maximum processing size
- Processing timeouts
- Separate processing queues
- Worker isolation
- Retry limits
- Failure handling
- Maximum concurrency
- Large-attachment detection
- Unsupported-file handling

Do not automatically process every attachment.

For example:

```text
Small/normal email
    → normal processing

Large email
    → controlled asynchronous processing

Large attachment
    → metadata only initially

Unsupported attachment
    → metadata + reference only

Processing failure
    → record failure and continue processing other items
```

The user should be able to explicitly request processing of a skipped attachment later.

Temporary content fetched from Outlook for analysis should be discarded after processing unless explicitly required by an approved feature.

---

# 7. Email Understanding

Each new or changed email should be intelligently analysed.

Determine:

- What the email is about
- Whether it belongs to an existing topic
- Whether it represents a new topic
- Key points
- Important decisions
- Action requested
- Action expected from the user
- Action expected from other people
- Sender
- Relevant stakeholders
- Explicit deadlines
- Implied deadlines
- Commitments
- Dependencies
- Whether a response is required
- Whether it is informational
- Whether it is a follow-up
- Whether it changes an existing task or deadline
- Whether it completes an existing task

The AI should produce a concise structured representation rather than permanently storing the raw email.

---

# 8. Logical Topic vs Outlook Conversation

Do NOT assume that an Outlook conversation/thread is equivalent to a real-world topic.

Maintain two separate concepts:

### Outlook Conversation

The technical Outlook conversation/thread.

### Logical Topic

The real-world subject/workstream being tracked by the assistant.

One logical topic may contain multiple Outlook conversations.

One Outlook conversation may potentially contain multiple logical subjects.

Example:

```text
Outlook Thread A
       │
       ├── Email 1
       ├── Email 2
       └── Email 3
              │
              ▼
        Logical Topic
        "ABC Project"
              ▲
              │
       ┌──────┴──────┐
       │             │
Thread B         Phone Call
```

The assistant should maintain these relationships.

---

# 9. Topic Gist

For every meaningful logical topic, maintain a continuously updated **Topic Gist**.

The gist should contain:

- Topic name
- Executive summary
- Current status
- Key decisions
- Important developments
- Outstanding actions
- Tasks
- Task owners
- Relevant stakeholders
- Deadlines
- Commitments
- Dependencies
- Latest development
- Waiting-for items
- Related Outlook emails
- User-added events/notes
- Next expected action

The gist should be updated incrementally.

Do not regenerate the entire topic from scratch every time a new email arrives.

---

# 10. Incremental Topic Updating

When a new email arrives:

```text
New Email
   ↓
Candidate topic identification
   ↓
Existing topic?
   ├── YES
   │     ↓
   │   Extract changes
   │     ↓
   │   Update affected fields
   │
   └── NO
         ↓
      Create new topic
```

For an existing topic, identify only what has changed:

- New decision
- New deadline
- New task
- Completed task
- New stakeholder
- Changed status
- New commitment
- New dependency
- New response
- New waiting-for item

The system should preserve the previous state and maintain an event history.

---

# 11. Event Timeline

Every topic should have a chronological event timeline.

Example:

```text
02 Oct 09:15 — Email received
02 Oct 09:20 — Task created
02 Oct 11:30 — User replied
03 Oct 14:00 — User added phone-call update
03 Oct 14:05 — Deadline added: 07 Oct
05 Oct 10:00 — Reminder triggered
06 Oct 16:30 — Follow-up email received
07 Oct 09:00 — User marked completed
```

The **Topic Gist represents the current state**.

The **Event Timeline represents how that state evolved**.

---

# 12. Local Working Memory

Create a local structured database.

SQLite is the preferred initial implementation unless there is a strong technical reason to use another local database.

Suggested entities:

```text
emails_metadata
outlook_folders
outlook_threads
topics
topic_email_links
participants
tasks
deadlines
commitments
reminders
events
attachments_metadata
processing_queue
sync_state
audit_log
```

Again:

### emails_metadata must NOT contain the complete email body.

It should contain only metadata and derived intelligence required for retrieval and tracking.

Suggested fields:

```text
outlook_item_id
conversation_id
folder
sender
recipients
cc
subject
timestamp
direction
topic_id
gist
summary
importance
task_ids
deadline_ids
processing_status
processing_timestamp
outlook_reference
```

---

# 13. Retention Policy

The local working memory should have a configurable retention period.

Default:

### Six months

Do not retain the complete mailbox.

Recommended policy:

```text
Outlook
→ retains original email according to Outlook/mailbox policy

Local Working Memory
→ approximately 6 months

User-created important reminders/notes
→ user controlled
```

The six-month period should be configurable.

A retention/cleanup process should automatically remove obsolete working-memory records.

However, deleting a historical email-derived record should not accidentally delete the original Outlook email.

---

# 14. Search and Indexing

The local working memory must be highly searchable.

Use a hybrid approach.

### A. Relational indexes

For:

- Dates
- Deadlines
- Status
- Topics
- Stakeholders
- Tasks
- Commitments
- Direction
- Importance

### B. Full-text search

Use SQLite FTS5 or equivalent local full-text search.

Search should work across:

- Subject
- Gist
- Summary
- Topics
- Tasks
- Notes
- Events
- Stakeholders
- Decisions

### C. Semantic search

Use a local embedding model and vector index for semantic retrieval.

For example, the user should be able to search:

> "Find all discussions about the server capacity issue"

even if the exact phrase "server capacity issue" does not occur in the stored metadata.

Keep the semantic index local.

---

# 15. Incremental Index Updating

Do not rebuild the entire search/vector index whenever a new email arrives.

Only update the affected records.

For example:

```text
New email
   ↓
Determine topic
   ↓
Update topic
   ↓
Update affected FTS records
   ↓
Update affected embeddings
```

This keeps the system fast as the six-month working memory grows.

---

# 16. Retrieving Original Email

When the local agent needs to inspect the actual original email:

```text
Local Working Memory
       ↓
Find relevant Outlook item ID
       ↓
Retrieve original item from local Outlook
       ↓
Read/process only when required
```

The local repository should therefore preserve reliable Outlook references.

If an Outlook item is no longer accessible, the system should clearly indicate that the original content could not be retrieved rather than inventing information.

---

# 17. Task Extraction

Automatically create tasks when emails contain actionable requirements.

Each task should contain:

- Task description
- Related topic
- Source Outlook email
- Requestor
- Task owner
- Priority
- Created date
- Due date
- Dependencies
- Status
- Last update
- Related events
- Completion information

---

# 18. Distinguish Task Types

At minimum distinguish:

### My Actions

Things the user needs to do.

### Waiting For

Things the user has completed but another person must respond/do something.

### Delegated

Things another person is responsible for but the user may need to monitor.

### Commitments

Things the user has promised to do.

### Informational

No action required.

This distinction should be visible in the UI.

---

# 19. User Commitment Tracking

Explicitly track commitments made by the user.

Example:

> "I'll send the report by Friday."

This should create:

```text
Commitment:
Send report

Owner:
User

Deadline:
Friday

Status:
Pending
```

The assistant should track commitments even if the original email does not explicitly contain a task.

---

# 20. Automatic Deadline Monitoring

Continuously monitor deadlines.

Highlight:

- Overdue
- Due today
- Due within 3 days
- Due within 7 days
- Approaching deadlines with incomplete work
- Missed deadlines
- Deadlines dependent on another pending action
- Deadlines that have changed

The 7-day window should be configurable.

Example dashboard:

```text
Attention Required

🔴 2 overdue
🟠 2 due within 3 days
🟡 4 due within 7 days

3 people are awaiting your response

2 items are waiting for other stakeholders

1 commitment is due tomorrow
```

---

# 21. Automatic Task Completion Detection

The assistant should correlate new outgoing emails with existing tasks.

Example:

Original:

> "Please send the revised report by Friday."

Task:

> Send revised report — Friday

If the user sends the revised report, determine whether the action was completed.

However:

**Do not automatically close important/ambiguous tasks purely on weak AI inference.**

Use confidence levels:

```text
High confidence
→ Automatically complete where safe

Medium confidence
→ "Likely completed — confirm?"

Low confidence
→ Do not change status
```

---

# 22. Non-Email Completion

Not all work happens through Outlook.

The user may complete an action through:

- Phone call
- Physical letter
- Meeting
- Internal system
- Verbal communication
- In-person discussion
- Other channel

Provide:

### Mark as Completed

The user should be able to manually mark a task complete.

Optional fields:

- Completion date
- Method/channel
- Short note
- Supporting reference

This should create an event in the topic timeline.

---

# 23. Manual Updates to Existing Topics

Provide an easy way for the user to add non-email information to an existing topic.

Example:

The user opens an existing email/topic and enters:

> "Got a call from Sharma today. He asked me to send the revised report by Monday. Remind me Friday."

The assistant should automatically interpret:

```text
Event:
Phone call with Sharma

Action:
Send revised report

Deadline:
Monday

Reminder:
Friday

Status:
Pending
```

The user should not need to manually fill every field.

---

# 24. Manual Reminder Creation

Every topic/task should have:

### + Add Reminder

The user should be able to specify:

- Reminder date/time
- Reminder text
- Related topic
- Related task
- Optional notes
- Recurrence if required

Reminders can originate from:

- Email
- Phone call
- Meeting
- Letter
- Personal note
- Other communication

The reminder must remain linked to the appropriate topic/task.

---

# 25. Proactive Assistant Dashboard

The main UI should focus on work rather than merely displaying emails.

Suggested sections:

### Today

What needs attention today.

### Upcoming

Deadlines within the next 7 days.

### Overdue

Outstanding overdue tasks.

### Waiting For

People from whom the user is waiting.

### My Commitments

Things the user promised to do.

### Recent Changes

Topics that changed recently.

### Topics

All active logical topics.

### Reminders

Upcoming user-created reminders.

---

# 26. Catch-Up / Resume Monitoring

The application must maintain the last successfully processed timestamp/checkpoint.

Provide a prominent manual action:

### "Catch Up Since Last Run"

This is essential when the application has been stopped overnight, during weekends, during travel, after a crash, or for any other period.

Example:

```text
Last monitored:
01 Oct 2026, 6:00 PM

Current time:
02 Oct 2026, 10:00 AM

[ Catch Up Since Last Run ]
```

When triggered, the application should identify all relevant incoming/outgoing/changed Outlook items since the last successful checkpoint and process them.

It should then show something like:

```text
Catch-up completed

12 new emails
3 topic updates
2 new tasks
1 new deadline
1 new commitment
2 responses detected
```

The catch-up process must be:

- Incremental
- Checkpoint based
- Asynchronous
- Idempotent
- Safe against duplicate processing

It must not reprocess successfully processed items unnecessarily.

---

# 27. Startup Behaviour

When the application starts:

```text
Read last successful checkpoint
        ↓
Determine elapsed period
        ↓
Show:
"You were last monitored at 6:00 PM yesterday."
        ↓
Offer:
[ Catch Up Now ]
```

Optionally allow automatic catch-up on startup, but the user should have control over this behaviour.

---

# 28. End-of-Day / Stop Monitoring

The application should maintain an explicit concept of:

### Last Successfully Monitored Timestamp

For example:

> Monitoring stopped: 01 Oct, 6:00 PM

This timestamp must be persisted safely.

If the application shuts down unexpectedly, the checkpoint should represent the **last successfully processed item**, not merely the time the application was closed.

---

# 29. Notifications

Notifications should be meaningful rather than noisy.

Examples:

> **Deadline approaching:** Revised report is due tomorrow.

> **Waiting for you:** 3 people are awaiting responses.

> **Overdue:** Two tasks have crossed their deadlines.

> **New commitment detected:** You promised to send the report by Friday.

Allow the user to configure notification frequency and categories.

---

# 30. AI Confidence & Human Control

Every AI-derived important state should have a confidence level.

For example:

```text
Topic match: 94%
Deadline extraction: 98%
Task extraction: 91%
Completion inference: 73%
```

High-impact ambiguous actions should require user confirmation.

The assistant must never silently invent:

- Deadlines
- Commitments
- Completion
- Stakeholders
- Decisions

When uncertain, explicitly say:

> "I believe this means X. Please confirm."

---

# 31. Auditability

Maintain a local audit trail for important changes.

For every automatically created/changed task, deadline or topic status, record:

- What changed
- When
- Source Outlook item
- Previous state
- New state
- AI confidence
- Reason for change

Example:

```text
Task status changed:

Previous:
Pending

New:
Completed

Reason:
Outgoing email 14:32 contained the requested document.

Confidence:
96%
```

This should make the assistant explainable and debuggable.

---

# 32. Reliability

The system must handle:

- Outlook unavailable
- Outlook restarting
- Application restart
- Windows sleep/hibernation
- Network interruptions
- AI model unavailable
- Processing failures
- Corrupted/unsupported email content
- Huge emails
- Huge attachments
- Duplicate Outlook events
- Missed monitoring cycles
- Database errors

The monitoring service must continue independently from AI processing.

Failed processing should go into a retry/dead-letter mechanism rather than blocking the entire pipeline.

---

# 33. Idempotency

The same Outlook email must never create duplicate:

- Topics
- Tasks
- Deadlines
- Events
- Reminders

Every processed Outlook item should have a stable unique identifier.

Processing should be idempotent.

If the same email is encountered again during catch-up/reconciliation, the system should recognize that it has already been processed.

---

# 34. Privacy

This is a local personal assistant and should be designed for maximum privacy.

Prefer:

- Local processing
- Local database
- Local embeddings
- Local AI models where available
- No external upload of email content
- No cloud mailbox replication
- No unnecessary storage of email contents

If an external AI service is ever introduced, it must be an explicit configuration choice and must not happen silently.

---

# 35. Performance Requirements

The application should remain lightweight while Outlook is being used normally.

The system should:

- Minimize Outlook API/object-model calls
- Avoid full mailbox scans
- Use incremental detection
- Queue expensive operations
- Limit concurrent processing
- Rate-limit expensive AI operations
- Avoid duplicate processing
- Maintain local indexes incrementally
- Clean up expired working memory automatically

The assistant should degrade gracefully under heavy load.

For example:

```text
High workload
      ↓
Queue increases
      ↓
Outlook monitoring continues
      ↓
AI processing catches up asynchronously
```

The user should never experience Outlook becoming unusable because the assistant is processing emails.

---

# 36. Recommended Processing Architecture

Use separate logical components:

```text
┌─────────────────────────────┐
│       Local Outlook         │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ Outlook Monitoring Service  │
│ Lightweight / Incremental   │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│       Local Event Queue     │
└──────────────┬──────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
 Email Metadata     Processing Jobs
       │                │
       └───────┬────────┘
               ▼
┌─────────────────────────────┐
│       AI Processing         │
│ Topic / Task / Deadline     │
│ Commitment / Classification │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│     Local Working Memory    │
│          SQLite             │
└──────────────┬──────────────┘
               │
       ┌───────┼────────┐
       ▼       ▼        ▼
     FTS5   Vector     SQL
     Index   Index    Indexes
       │       │        │
       └───────┼────────┘
               ▼
┌─────────────────────────────┐
│       Local Agent / UI      │
└─────────────────────────────┘
```

---

# 37. Local Agent Retrieval

The local agent should use the working-memory repository as its first retrieval layer.

For a user query such as:

> "What is pending with the vendor regarding the H200 server?"

The agent should:

```text
User query
   ↓
Search local working memory
   ↓
Identify relevant topic/tasks/events
   ↓
Retrieve relevant Outlook message IDs
   ↓
Fetch original Outlook emails only if additional context is required
   ↓
Answer with traceable references
```

This avoids repeatedly searching Outlook for everything.

---

# 38. Six-Month Working Memory Should Be Optimized for Retrieval

The goal of retention is not simply storage.

The system should progressively build a compact representation of the user's work:

```text
Email metadata
      ↓
Topic
      ↓
Gist
      ↓
Tasks
      ↓
Deadlines
      ↓
Commitments
      ↓
Events
      ↓
Current status
```

The assistant should be able to understand an ongoing topic months later without having to permanently duplicate every email.

---

# 39. Example End-to-End Scenario

### Day 1

Email:

> "Please provide the revised proposal by Friday."

System creates:

```text
Topic:
Vendor Proposal

Task:
Provide revised proposal

Owner:
User

Deadline:
Friday

Status:
Pending
```

### Day 2

User replies:

> "I will send it by Thursday."

System updates:

```text
Commitment:
User promised delivery Thursday

Deadline:
Thursday
```

### Day 3

User receives a phone call.

They enter:

> "Vendor called. Asked me to include revised pricing. Remind me tomorrow."

System adds:

```text
Event:
Phone call

New requirement:
Include revised pricing

Reminder:
Tomorrow

Topic:
Vendor Proposal
```

### Day 4

User sends the revised proposal.

System detects the outgoing email and determines:

```text
Task:
Likely completed

Confidence:
High
```

It marks the task completed if configured to allow automatic completion.

The topic becomes:

```text
Vendor Proposal

Status:
Completed

Latest:
Revised proposal sent.

Outstanding:
Awaiting vendor response.
```

---

# 40. Key Design Philosophy

The application should not become another inbox.

It should become a **personal work-memory and action-management layer sitting on top of Outlook**.

The fundamental relationship should be:

```text
                    OUTLOOK
                Source of Truth
                       │
                       ▼
             Lightweight Metadata
                       │
                       ▼
              LOCAL WORKING MEMORY
                       │
       ┌───────────────┼────────────────┐
       ▼               ▼                ▼
     TOPICS          TASKS          DEADLINES
       │               │                │
       └───────────────┼────────────────┘
                       ▼
                 USER'S AGENT
                       │
                       ▼
            "What needs my attention?"
```

### Non-negotiable principles

1. **No duplicate local mailbox.**
2. **Do not permanently store raw email bodies.**
3. **Do not permanently store email attachments.**
4. **Outlook remains the authoritative source for original email content.**
5. **Local repository stores structured working memory and references.**
6. **Default working-memory retention: six months, configurable.**
7. **Use incremental Outlook monitoring rather than repeated full scans.**
8. **Default lightweight detection interval: approximately 60 seconds, configurable.**
9. **AI processing must be asynchronous.**
10. **A large/problematic email must never block the entire assistant.**
11. **Use SQLite + FTS5 + local semantic indexing initially.**
12. **Maintain logical topics separately from Outlook conversation threads.**
13. **Update topics incrementally rather than rebuilding them.**
14. **Maintain an event timeline for every important topic.**
15. **Track tasks, waiting-for items, delegated tasks and user commitments separately.**
16. **Support manual reminders and manual non-email events.**
17. **Support natural-language addition of phone calls, meetings, letters and other events.**
18. **Provide "Catch Up Since Last Run" based on a durable processing checkpoint.**
19. **Never lose track of activity that occurred while the application was stopped.**
20. **Use confidence-based automation and retain human control over ambiguous actions.**
21. **Maintain an auditable history of important AI-derived changes.**
22. **The database, not the LLM, is the system of record for tasks, deadlines and state.**
23. **The LLM is the interpretation/reasoning layer, not the permanent memory.**
24. **The assistant should proactively surface work requiring attention rather than merely summarize emails.**