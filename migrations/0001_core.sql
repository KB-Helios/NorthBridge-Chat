PRAGMA foreign_keys = ON;

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

INSERT INTO workspaces (id, name, created_at_ms, updated_at_ms)
VALUES ('00000000-0000-0000-0000-000000000001', 'Personal', 0, 0);

CREATE TABLE conversations (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    default_branch_id TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    deleted_at_ms INTEGER
) STRICT;

CREATE INDEX conversations_workspace_updated_idx
    ON conversations (workspace_id, updated_at_ms DESC, id DESC)
    WHERE deleted_at_ms IS NULL;

CREATE TABLE conversation_branches (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    parent_branch_id TEXT REFERENCES conversation_branches(id) ON DELETE SET NULL,
    forked_from_message_id TEXT,
    head_message_id TEXT,
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX conversation_branches_conversation_idx
    ON conversation_branches (conversation_id, created_at_ms, id);

CREATE TABLE messages (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    branch_id TEXT NOT NULL REFERENCES conversation_branches(id) ON DELETE CASCADE,
    parent_message_id TEXT REFERENCES messages(id) ON DELETE RESTRICT,
    role TEXT NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')),
    created_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX messages_branch_created_idx
    ON messages (branch_id, created_at_ms, id);

CREATE INDEX messages_parent_idx
    ON messages (parent_message_id)
    WHERE parent_message_id IS NOT NULL;

CREATE TABLE message_parts (
    id TEXT PRIMARY KEY NOT NULL,
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    kind TEXT NOT NULL CHECK (
        kind IN ('text', 'reasoning', 'image', 'audio', 'file', 'tool_call', 'tool_result', 'json')
    ),
    content TEXT NOT NULL,
    metadata_json TEXT CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
    UNIQUE (message_id, ordinal)
) STRICT;

CREATE VIRTUAL TABLE message_parts_fts USING fts5(
    part_id UNINDEXED,
    message_id UNINDEXED,
    conversation_id UNINDEXED,
    content,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER message_parts_fts_insert
AFTER INSERT ON message_parts
WHEN NEW.kind IN ('text', 'reasoning')
BEGIN
    INSERT INTO message_parts_fts (part_id, message_id, conversation_id, content)
    SELECT NEW.id, NEW.message_id, messages.conversation_id, NEW.content
    FROM messages
    WHERE messages.id = NEW.message_id;
END;

CREATE TRIGGER message_parts_fts_update
AFTER UPDATE OF content, kind ON message_parts
BEGIN
    DELETE FROM message_parts_fts WHERE part_id = OLD.id;
    INSERT INTO message_parts_fts (part_id, message_id, conversation_id, content)
    SELECT NEW.id, NEW.message_id, messages.conversation_id, NEW.content
    FROM messages
    WHERE messages.id = NEW.message_id
      AND NEW.kind IN ('text', 'reasoning');
END;

CREATE TRIGGER message_parts_fts_delete
AFTER DELETE ON message_parts
BEGIN
    DELETE FROM message_parts_fts WHERE part_id = OLD.id;
END;

CREATE TABLE generation_runs (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    branch_id TEXT NOT NULL REFERENCES conversation_branches(id) ON DELETE CASCADE,
    model_alias TEXT NOT NULL CHECK (length(trim(model_alias)) > 0),
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'waiting_for_approval', 'completed', 'failed', 'cancelled')
    ),
    last_sequence INTEGER NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX generation_runs_conversation_idx
    ON generation_runs (conversation_id, created_at_ms DESC, id DESC);

CREATE TABLE generation_events (
    generation_id TEXT NOT NULL REFERENCES generation_runs(id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_type TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
    timestamp_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    PRIMARY KEY (generation_id, sequence_number)
) STRICT, WITHOUT ROWID;

CREATE TABLE settings (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL CHECK (json_valid(value_json)),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (scope, key)
) STRICT, WITHOUT ROWID;
