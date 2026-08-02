CREATE TABLE split_chat_runs (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    source_branch_id TEXT NOT NULL REFERENCES conversation_branches(id) ON DELETE CASCADE,
    input_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE RESTRICT,
    client_request_id TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX split_chat_runs_client_request_idx
    ON split_chat_runs (conversation_id, client_request_id)
    WHERE client_request_id IS NOT NULL;

CREATE INDEX split_chat_runs_conversation_idx
    ON split_chat_runs (conversation_id, created_at_ms DESC, id DESC);

CREATE TABLE split_chat_lanes (
    split_chat_id TEXT NOT NULL REFERENCES split_chat_runs(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 3),
    branch_id TEXT NOT NULL UNIQUE REFERENCES conversation_branches(id) ON DELETE CASCADE,
    generation_id TEXT NOT NULL UNIQUE REFERENCES generation_runs(id) ON DELETE CASCADE,
    PRIMARY KEY (split_chat_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TRIGGER split_chat_runs_generation_updated
AFTER UPDATE OF status, updated_at_ms ON generation_runs
BEGIN
    UPDATE split_chat_runs
    SET updated_at_ms = NEW.updated_at_ms
    WHERE id = (
        SELECT split_chat_id
        FROM split_chat_lanes
        WHERE generation_id = NEW.id
    );
END;
