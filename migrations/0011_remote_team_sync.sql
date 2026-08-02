PRAGMA foreign_keys = ON;

CREATE TABLE team_users (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('owner', 'admin', 'member', 'viewer')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, email COLLATE NOCASE)
);

CREATE TABLE auth_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    user_id TEXT NOT NULL REFERENCES team_users(id) ON DELETE CASCADE,
    token_sha256 TEXT NOT NULL UNIQUE CHECK(length(token_sha256) = 64),
    expires_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER,
    last_used_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_auth_sessions_active
    ON auth_sessions(token_sha256, expires_at_ms) WHERE revoked_at_ms IS NULL;

CREATE TABLE team_policies (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    method TEXT NOT NULL,
    path_prefix TEXT NOT NULL,
    minimum_role TEXT NOT NULL CHECK(minimum_role IN ('owner', 'admin', 'member', 'viewer')),
    effect TEXT NOT NULL CHECK(effect IN ('allow', 'deny')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, name COLLATE NOCASE)
);

CREATE INDEX idx_team_policies_match
    ON team_policies(workspace_id, path_prefix, method);

CREATE TABLE audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE RESTRICT,
    principal TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT NOT NULL,
    outcome TEXT NOT NULL,
    request_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    occurred_at_ms INTEGER NOT NULL,
    previous_hash TEXT NOT NULL CHECK(length(previous_hash) = 64),
    event_hash TEXT NOT NULL UNIQUE CHECK(length(event_hash) = 64)
);

CREATE TABLE sync_records (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    deleted INTEGER NOT NULL DEFAULT 0 CHECK(deleted IN (0, 1)),
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(workspace_id, entity_type, entity_id)
);

CREATE TABLE sync_changes (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    deleted INTEGER NOT NULL DEFAULT 0 CHECK(deleted IN (0, 1)),
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    device_id TEXT NOT NULL,
    client_change_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(device_id, client_change_id)
);

CREATE INDEX idx_sync_changes_workspace_sequence
    ON sync_changes(workspace_id, sequence);
