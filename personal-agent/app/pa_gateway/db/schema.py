"""Database schema. See docs/DATABASE.md for the meaning of every table and column.

Rules:
  - Append new migrations; never edit a released one.
  - Each statement must end with ";\n" (migrations are split on it).
  - Timestamps are ISO-8601 UTC strings ("...Z"). JSON columns end in _json.
"""

V1 = """
CREATE TABLE settings (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE settings_history (
  id TEXT PRIMARY KEY,
  key TEXT NOT NULL,
  before_json TEXT,
  after_json TEXT,
  direction TEXT NOT NULL,
  ts TEXT NOT NULL
);
CREATE TABLE signin_history (
  id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  kind TEXT NOT NULL,
  success INTEGER NOT NULL,
  detail TEXT
);
CREATE TABLE secrets (
  id TEXT PRIMARY KEY,
  wrapped_key BLOB NOT NULL,
  blob BLOB NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  last_used_at TEXT,
  deleted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE secret_versions (
  id TEXT PRIMARY KEY,
  secret_id TEXT NOT NULL REFERENCES secrets(id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  wrapped_key BLOB NOT NULL,
  blob BLOB NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE secret_fingerprints (
  secret_id TEXT NOT NULL REFERENCES secrets(id) ON DELETE CASCADE,
  length INTEGER NOT NULL,
  fp BLOB NOT NULL
);
CREATE INDEX idx_secret_fp ON secret_fingerprints(fp);
CREATE TABLE secret_bindings (
  secret_id TEXT NOT NULL REFERENCES secrets(id) ON DELETE CASCADE,
  binding TEXT NOT NULL,
  PRIMARY KEY (secret_id, binding)
);
CREATE TABLE connectors (
  id TEXT PRIMARY KEY,
  enabled INTEGER NOT NULL DEFAULT 0,
  use_chat INTEGER NOT NULL DEFAULT 1,
  use_missions INTEGER NOT NULL DEFAULT 1,
  connected INTEGER NOT NULL DEFAULT 0,
  config_json TEXT NOT NULL DEFAULT '{}',
  scopes_json TEXT NOT NULL DEFAULT '[]',
  last_used_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE models (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  provider TEXT NOT NULL,
  endpoint TEXT,
  model_name TEXT,
  path TEXT,
  sha256 TEXT,
  size_bytes INTEGER,
  source TEXT,
  license TEXT,
  quantization TEXT,
  kind TEXT NOT NULL DEFAULT 'chat',
  api_key_secret_id TEXT,
  tested INTEGER NOT NULL DEFAULT 0,
  test_report_json TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE chats (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  hwm INTEGER NOT NULL DEFAULT 0,
  sources_json TEXT NOT NULL DEFAULT '[]',
  allow_tools INTEGER NOT NULL DEFAULT 1,
  archived INTEGER NOT NULL DEFAULT 0,
  deleted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE chat_messages (
  id TEXT PRIMARY KEY,
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  run_id TEXT,
  sources_json TEXT NOT NULL DEFAULT '[]',
  sensitivity INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE INDEX idx_chat_messages_chat ON chat_messages(chat_id, created_at);
CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  chat_id TEXT,
  task_id TEXT,
  mission_id TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  hwm INTEGER NOT NULL DEFAULT 0,
  sources_json TEXT NOT NULL DEFAULT '[]',
  tokens_in INTEGER NOT NULL DEFAULT 0,
  tokens_out INTEGER NOT NULL DEFAULT 0,
  tool_calls INTEGER NOT NULL DEFAULT 0,
  summary TEXT,
  archived INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_runs_started ON runs(started_at);
CREATE TABLE run_steps (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  status TEXT NOT NULL,
  detail_json TEXT NOT NULL DEFAULT '{}',
  started_at TEXT NOT NULL,
  ended_at TEXT,
  duration_ms INTEGER
);
CREATE INDEX idx_run_steps_run ON run_steps(run_id, seq);
CREATE TABLE session_events (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  role TEXT,
  content TEXT NOT NULL,
  source TEXT NOT NULL,
  trust TEXT NOT NULL,
  sensitivity INTEGER NOT NULL,
  meta_json TEXT NOT NULL DEFAULT '{}',
  prev_hash TEXT NOT NULL,
  hash TEXT NOT NULL,
  ts TEXT NOT NULL,
  UNIQUE(session_id, seq)
);
CREATE TABLE tasks (
  id TEXT PRIMARY KEY,
  run_id TEXT,
  session_id TEXT NOT NULL,
  chat_id TEXT,
  mission_id TEXT,
  parent_task_id TEXT,
  depth INTEGER NOT NULL DEFAULT 0,
  trigger_type TEXT NOT NULL,
  objective TEXT NOT NULL,
  state TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 5,
  allowed_tools_json TEXT NOT NULL DEFAULT '[]',
  budget_json TEXT NOT NULL DEFAULT '{}',
  usage_json TEXT NOT NULL DEFAULT '{}',
  hwm INTEGER NOT NULL DEFAULT 0,
  model_id TEXT,
  wait_reason TEXT,
  error TEXT,
  result TEXT,
  lease_owner TEXT,
  lease_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX idx_tasks_state ON tasks(state, priority, created_at);
CREATE TABLE task_transitions (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  ts TEXT NOT NULL
);
CREATE TABLE missions (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'mission',
  name TEXT NOT NULL,
  objective TEXT NOT NULL,
  schedule_json TEXT NOT NULL,
  timezone TEXT NOT NULL,
  allowed_tools_json TEXT NOT NULL DEFAULT '[]',
  allowed_connectors_json TEXT NOT NULL DEFAULT '[]',
  overnight INTEGER NOT NULL DEFAULT 1,
  budget_json TEXT NOT NULL DEFAULT '{}',
  model_id TEXT,
  output_format TEXT NOT NULL DEFAULT 'markdown',
  notification_level TEXT NOT NULL DEFAULT 'notify',
  missed_run_policy TEXT NOT NULL DEFAULT 'RUN_ONCE',
  resource_wait_minutes INTEGER NOT NULL DEFAULT 60,
  status TEXT NOT NULL,
  proposed_by TEXT NOT NULL DEFAULT 'user',
  last_run_at TEXT,
  next_run_at TEXT,
  last_task_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE approvals (
  id TEXT PRIMARY KEY,
  task_id TEXT,
  run_id TEXT,
  chat_id TEXT,
  mission_id TEXT,
  kind TEXT NOT NULL DEFAULT 'tool',
  tool TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  destination TEXT,
  sensitivity INTEGER NOT NULL,
  risk TEXT NOT NULL,
  reason TEXT NOT NULL,
  requires_password INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  decided_at TEXT,
  decision_ms INTEGER
);
CREATE INDEX idx_approvals_status ON approvals(status, created_at);
CREATE TABLE outbox (
  idem_key TEXT PRIMARY KEY,
  task_id TEXT,
  tool TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  state TEXT NOT NULL,
  result_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE files (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  folder TEXT NOT NULL DEFAULT '/',
  tags_json TEXT NOT NULL DEFAULT '[]',
  size_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  declared_type TEXT,
  sniffed_type TEXT,
  source TEXT NOT NULL,
  sensitivity INTEGER NOT NULL,
  status TEXT NOT NULL,
  status_reason TEXT,
  scan_json TEXT NOT NULL DEFAULT '{}',
  wrapped_key BLOB NOT NULL,
  text_content TEXT,
  hidden_json TEXT NOT NULL DEFAULT '[]',
  in_knowledge INTEGER NOT NULL DEFAULT 0,
  derived_from TEXT,
  created_at TEXT NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE memories (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  content TEXT NOT NULL,
  source TEXT NOT NULL,
  source_ref TEXT,
  provenance_json TEXT NOT NULL DEFAULT '[]',
  trust TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.5,
  importance TEXT NOT NULL DEFAULT 'normal',
  sensitivity INTEGER NOT NULL,
  status TEXT NOT NULL,
  embedding BLOB,
  created_at TEXT NOT NULL,
  last_verified_at TEXT,
  expires_at TEXT
);
CREATE TABLE skills (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  definition_json TEXT NOT NULL,
  status TEXT NOT NULL,
  lineage_json TEXT NOT NULL DEFAULT '[]',
  tainted INTEGER NOT NULL DEFAULT 0,
  review_json TEXT NOT NULL DEFAULT '{}',
  test_json TEXT NOT NULL DEFAULT '{}',
  mac TEXT NOT NULL,
  denials INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  last_used_at TEXT
);
CREATE TABLE reminders (
  id TEXT PRIMARY KEY,
  text TEXT NOT NULL,
  due_at TEXT NOT NULL,
  timezone TEXT NOT NULL,
  status TEXT NOT NULL,
  source TEXT NOT NULL,
  run_id TEXT,
  created_at TEXT NOT NULL,
  fired_at TEXT
);
CREATE TABLE notifications (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT,
  sensitivity INTEGER NOT NULL DEFAULT 0,
  screen TEXT,
  ref_id TEXT,
  read INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE TABLE budget_usage (
  day TEXT NOT NULL,
  key TEXT NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY (day, key)
);
CREATE TABLE home_events (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  severity TEXT NOT NULL,
  title TEXT NOT NULL,
  detail TEXT,
  ref_id TEXT,
  created_at TEXT NOT NULL,
  dismissed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE policy_history (
  id TEXT PRIMARY KEY,
  version TEXT NOT NULL,
  before_json TEXT,
  after_json TEXT NOT NULL,
  ts TEXT NOT NULL
);
CREATE VIRTUAL TABLE history_fts USING fts5(kind UNINDEXED, ref_id UNINDEXED, title, body, created_at UNINDEXED, tokenize='porter unicode61');
"""

V2 = """
ALTER TABLE chats ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chats ADD COLUMN folder TEXT NOT NULL DEFAULT '';
"""

V3 = """
ALTER TABLE missions ADD COLUMN template_id TEXT;
CREATE TABLE net_log (
  id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  component TEXT NOT NULL,
  method TEXT NOT NULL,
  scheme TEXT NOT NULL,
  host TEXT NOT NULL,
  port INTEGER,
  path TEXT NOT NULL,
  status INTEGER,
  outcome TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  bytes_out INTEGER NOT NULL DEFAULT 0,
  bytes_in INTEGER NOT NULL DEFAULT 0,
  duration_ms INTEGER NOT NULL DEFAULT 0,
  ip TEXT NOT NULL DEFAULT '',
  loopback INTEGER NOT NULL DEFAULT 0,
  purpose TEXT NOT NULL DEFAULT '',
  tool TEXT NOT NULL DEFAULT '',
  task_id TEXT,
  run_id TEXT
);
CREATE TABLE local_grants (
  id TEXT PRIMARY KEY,
  chat_id TEXT NOT NULL,
  path TEXT NOT NULL,
  name TEXT NOT NULL,
  ext TEXT NOT NULL,
  size INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  sensitivity INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  last_used_at TEXT
);
CREATE INDEX idx_local_grants_chat ON local_grants(chat_id);
CREATE INDEX idx_net_log_ts ON net_log(ts);
CREATE INDEX idx_net_log_host ON net_log(host, ts);
"""

V4 = """
ALTER TABLE local_grants ADD COLUMN scope TEXT NOT NULL DEFAULT 'file';
ALTER TABLE local_grants ADD COLUMN recursive INTEGER NOT NULL DEFAULT 0;
ALTER TABLE local_grants ADD COLUMN session_nonce TEXT NOT NULL DEFAULT '';
"""

V5 = """
CREATE TABLE file_meta (
  file_id TEXT PRIMARY KEY,
  title TEXT,
  doc_type TEXT,
  summary TEXT,
  keywords_json TEXT NOT NULL DEFAULT '[]',
  pii_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL DEFAULT 'PENDING',
  edited INTEGER NOT NULL DEFAULT 0,
  model TEXT,
  note TEXT,
  memory_id TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE memory_learn_state (
  chat_id TEXT PRIMARY KEY,
  last_msg_created_at TEXT NOT NULL
);
CREATE TABLE memory_forgotten (
  content_hash TEXT PRIMARY KEY,
  ts TEXT NOT NULL
);
"""

V6 = """
ALTER TABLE models ADD COLUMN context_length INTEGER;
ALTER TABLE models ADD COLUMN temperature REAL;
"""

MIGRATIONS: list[tuple[int, str]] = [
    (1, V1),
    (2, V2),
    (3, V3),
    (4, V4),
    (5, V5),
    (6, V6),
]
