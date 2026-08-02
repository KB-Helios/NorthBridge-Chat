PRAGMA foreign_keys = ON;

CREATE TABLE workflows (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL CHECK(version > 0),
    entry_node_id TEXT NOT NULL,
    nodes_json TEXT NOT NULL CHECK(json_valid(nodes_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, name COLLATE NOCASE)
);

CREATE TABLE workflow_runs (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    parent_run_id TEXT REFERENCES workflow_runs(id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'running', 'paused', 'waiting_for_approval', 'completed', 'failed',
        'cancelled', 'unknown_outcome'
    )),
    input_json TEXT NOT NULL CHECK(json_valid(input_json)),
    output_json TEXT CHECK(output_json IS NULL OR json_valid(output_json)),
    current_node_id TEXT,
    resume_cursor_json TEXT CHECK(resume_cursor_json IS NULL OR json_valid(resume_cursor_json)),
    last_error TEXT,
    pause_requested INTEGER NOT NULL DEFAULT 0 CHECK(pause_requested IN (0, 1)),
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_workflow_runs_workspace_status
    ON workflow_runs(workspace_id, status, updated_at_ms DESC);

CREATE TABLE workflow_node_runs (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    node_path TEXT NOT NULL,
    node_kind TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'running', 'waiting_for_approval', 'completed', 'failed', 'cancelled',
        'unknown_outcome'
    )),
    input_json TEXT NOT NULL CHECK(json_valid(input_json)),
    output_json TEXT CHECK(output_json IS NULL OR json_valid(output_json)),
    token_usage_json TEXT CHECK(token_usage_json IS NULL OR json_valid(token_usage_json)),
    tool_calls_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(tool_calls_json)),
    error TEXT,
    resume_cursor_json TEXT CHECK(resume_cursor_json IS NULL OR json_valid(resume_cursor_json)),
    artifact_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(artifact_ids_json)),
    side_effect_key TEXT,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    UNIQUE(workflow_run_id, node_path, attempt)
);

CREATE INDEX idx_workflow_node_runs_run
    ON workflow_node_runs(workflow_run_id, node_path, attempt DESC);

CREATE TABLE workflow_approvals (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
    node_run_id TEXT NOT NULL REFERENCES workflow_node_runs(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    prompt TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'approved', 'denied')),
    decided_by_user_id TEXT,
    decision_note TEXT,
    created_at_ms INTEGER NOT NULL,
    decided_at_ms INTEGER,
    UNIQUE(node_run_id)
);

CREATE INDEX idx_workflow_approvals_pending
    ON workflow_approvals(status, created_at_ms);

CREATE TABLE artifact_blobs (
    sha256 TEXT PRIMARY KEY NOT NULL CHECK(length(sha256) = 64),
    content BLOB NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('document', 'code', 'image', 'audio', 'video', 'data')),
    media_type TEXT NOT NULL,
    sha256 TEXT NOT NULL REFERENCES artifact_blobs(sha256) ON DELETE RESTRICT,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    source TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_artifacts_workspace_updated
    ON artifacts(workspace_id, updated_at_ms DESC);

CREATE TABLE credential_references (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    service TEXT NOT NULL,
    account TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, name COLLATE NOCASE),
    UNIQUE(service, account)
);

CREATE TABLE environments (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    variables_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(variables_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, name COLLATE NOCASE)
);

CREATE TABLE media_providers (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    modality TEXT NOT NULL CHECK(modality IN ('image', 'audio', 'video')),
    upstream_url TEXT NOT NULL,
    gateway_path TEXT NOT NULL UNIQUE,
    credential_id TEXT REFERENCES credential_references(id) ON DELETE SET NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE(workspace_id, name COLLATE NOCASE)
);

CREATE TABLE media_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    provider_id TEXT NOT NULL REFERENCES media_providers(id) ON DELETE RESTRICT,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    modality TEXT NOT NULL CHECK(modality IN ('image', 'audio', 'video')),
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'running', 'completed', 'failed', 'cancelled', 'unknown_outcome'
    )),
    prompt TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
    response_json TEXT CHECK(response_json IS NULL OR json_valid(response_json)),
    artifact_id TEXT REFERENCES artifacts(id) ON DELETE SET NULL,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_media_jobs_workspace_status
    ON media_jobs(workspace_id, status, updated_at_ms DESC);

CREATE TABLE package_operations (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    direction TEXT NOT NULL CHECK(direction IN ('import', 'export')),
    package_name TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256) = 64),
    status TEXT NOT NULL CHECK(status IN ('completed', 'failed')),
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
    error TEXT,
    created_at_ms INTEGER NOT NULL
);
