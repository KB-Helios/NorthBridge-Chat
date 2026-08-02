CREATE TABLE personas (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    system_prompt TEXT NOT NULL CHECK (length(trim(system_prompt)) > 0),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE INDEX personas_workspace_updated_idx
    ON personas (workspace_id, updated_at_ms DESC, id DESC);

CREATE TABLE skills (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    slug TEXT NOT NULL CHECK (
        length(slug) BETWEEN 1 AND 80 AND slug NOT GLOB '*[^a-z0-9_-]*'
    ),
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    instructions TEXT NOT NULL CHECK (length(trim(instructions)) > 0),
    permission_manifest_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(permission_manifest_json)),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, slug)
) STRICT;

CREATE INDEX skills_workspace_updated_idx
    ON skills (workspace_id, updated_at_ms DESC, id DESC);

CREATE TABLE agent_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    persona_id TEXT REFERENCES personas(id) ON DELETE SET NULL,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    model_alias TEXT NOT NULL CHECK (length(trim(model_alias)) BETWEEN 1 AND 160),
    instructions TEXT NOT NULL DEFAULT '',
    policy_mode TEXT NOT NULL DEFAULT 'side_effects' CHECK (
        policy_mode IN ('manual', 'side_effects', 'auto')
    ),
    max_turns INTEGER NOT NULL DEFAULT 12 CHECK (max_turns BETWEEN 1 AND 100),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE INDEX agent_profiles_workspace_updated_idx
    ON agent_profiles (workspace_id, updated_at_ms DESC, id DESC);

CREATE TABLE agent_skill_links (
    agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE CASCADE,
    skill_id TEXT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (agent_id, skill_id),
    UNIQUE (agent_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE agent_knowledge_links (
    agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE CASCADE,
    stack_id TEXT NOT NULL REFERENCES knowledge_stacks(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (agent_id, stack_id),
    UNIQUE (agent_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE mcp_servers (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (
        length(name) BETWEEN 1 AND 80 AND name NOT GLOB '*[^A-Za-z0-9_-]*'
    ),
    transport TEXT NOT NULL CHECK (
        transport IN ('stdio', 'streamable_http', 'sse', 'openapi')
    ),
    endpoint TEXT,
    command TEXT,
    args_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(args_json) AND json_type(args_json) = 'array'),
    env_refs_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(env_refs_json) AND json_type(env_refs_json) = 'object'),
    openapi_schema TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    config_revision INTEGER NOT NULL DEFAULT 1 CHECK (config_revision > 0),
    last_error TEXT,
    last_discovered_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (
        (transport = 'stdio' AND command IS NOT NULL AND endpoint IS NULL)
        OR (transport IN ('streamable_http', 'sse') AND endpoint IS NOT NULL AND command IS NULL)
        OR (transport = 'openapi' AND endpoint IS NOT NULL AND command IS NULL AND openapi_schema IS NOT NULL)
    ),
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE INDEX mcp_servers_enabled_idx
    ON mcp_servers (enabled, workspace_id, name);

CREATE TABLE mcp_tool_snapshots (
    server_id TEXT NOT NULL REFERENCES mcp_servers(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    description TEXT NOT NULL DEFAULT '',
    input_schema_json TEXT NOT NULL CHECK (json_valid(input_schema_json)),
    annotations_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(annotations_json)),
    read_only INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
    destructive INTEGER NOT NULL DEFAULT 0 CHECK (destructive IN (0, 1)),
    discovered_at_ms INTEGER NOT NULL,
    PRIMARY KEY (server_id, name)
) STRICT, WITHOUT ROWID;

CREATE TABLE agent_tool_allowlist (
    agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE CASCADE,
    server_id TEXT NOT NULL REFERENCES mcp_servers(id) ON DELETE CASCADE,
    tool_name TEXT NOT NULL,
    PRIMARY KEY (agent_id, server_id, tool_name)
) STRICT, WITHOUT ROWID;

CREATE TABLE agent_runs (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE RESTRICT,
    conversation_id TEXT REFERENCES conversations(id) ON DELETE SET NULL,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'waiting_for_approval', 'suspended', 'completed', 'failed', 'cancelled')
    ),
    prompt TEXT NOT NULL CHECK (length(trim(prompt)) > 0),
    rig_version TEXT NOT NULL CHECK (length(trim(rig_version)) > 0),
    checkpoint_json TEXT NOT NULL CHECK (json_valid(checkpoint_json)),
    checkpoint_generation INTEGER NOT NULL DEFAULT 0 CHECK (checkpoint_generation >= 0),
    current_turn INTEGER NOT NULL DEFAULT 0 CHECK (current_turn >= 0),
    output TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
) STRICT;

CREATE INDEX agent_runs_workspace_updated_idx
    ON agent_runs (workspace_id, updated_at_ms DESC, id DESC);
CREATE INDEX agent_runs_recovery_idx
    ON agent_runs (status, updated_at_ms)
    WHERE status IN ('queued', 'running', 'waiting_for_approval', 'suspended');

CREATE TABLE agent_run_events (
    run_id TEXT NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_type TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, sequence_number)
) STRICT, WITHOUT ROWID;

CREATE TABLE agent_tool_calls (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    server_id TEXT REFERENCES mcp_servers(id) ON DELETE SET NULL,
    rig_tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL CHECK (length(trim(tool_name)) > 0),
    arguments_json TEXT NOT NULL CHECK (json_valid(arguments_json)),
    status TEXT NOT NULL CHECK (
        status IN ('pending_approval', 'approved', 'running', 'completed', 'failed', 'denied', 'unknown_outcome', 'cancelled')
    ),
    read_only INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
    destructive INTEGER NOT NULL DEFAULT 0 CHECK (destructive IN (0, 1)),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    last_error TEXT,
    started_at_ms INTEGER,
    completed_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (run_id, rig_tool_call_id)
) STRICT;

CREATE INDEX agent_tool_calls_run_status_idx
    ON agent_tool_calls (run_id, status, created_at_ms);

CREATE TABLE agent_approvals (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    tool_call_id TEXT NOT NULL UNIQUE REFERENCES agent_tool_calls(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'denied', 'cancelled')),
    reason TEXT NOT NULL,
    decision_note TEXT,
    requested_at_ms INTEGER NOT NULL,
    decided_at_ms INTEGER
) STRICT;

CREATE INDEX agent_approvals_pending_idx
    ON agent_approvals (status, requested_at_ms)
    WHERE status = 'pending';

CREATE TABLE remote_agents (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    upstream_url TEXT NOT NULL CHECK (length(trim(upstream_url)) > 0),
    gateway_path TEXT NOT NULL UNIQUE CHECK (gateway_path GLOB '/a2a/*'),
    auth_environment_key TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    card_json TEXT CHECK (card_json IS NULL OR json_valid(card_json)),
    card_etag TEXT,
    last_discovered_at_ms INTEGER,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE TABLE remote_agent_tasks (
    id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    remote_agent_id TEXT NOT NULL REFERENCES remote_agents(id) ON DELETE RESTRICT,
    remote_task_id TEXT,
    context_id TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'submitted', 'working', 'input_required', 'completed', 'failed', 'cancelled')
    ),
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    response_json TEXT CHECK (response_json IS NULL OR json_valid(response_json)),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX remote_agent_tasks_status_idx
    ON remote_agent_tasks (status, updated_at_ms);

CREATE TABLE crews (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 120),
    description TEXT NOT NULL DEFAULT '',
    coordinator_agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE RESTRICT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (workspace_id, name COLLATE NOCASE)
) STRICT;

CREATE TABLE crew_members (
    crew_id TEXT NOT NULL REFERENCES crews(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    role TEXT NOT NULL CHECK (length(trim(role)) > 0),
    local_agent_id TEXT REFERENCES agent_profiles(id) ON DELETE RESTRICT,
    remote_agent_id TEXT REFERENCES remote_agents(id) ON DELETE RESTRICT,
    PRIMARY KEY (crew_id, ordinal),
    CHECK ((local_agent_id IS NOT NULL) != (remote_agent_id IS NOT NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE crew_runs (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    crew_id TEXT NOT NULL REFERENCES crews(id) ON DELETE RESTRICT,
    prompt TEXT NOT NULL CHECK (length(trim(prompt)) > 0),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued', 'running', 'suspended', 'completed', 'failed', 'cancelled')
    ),
    coordinator_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    output TEXT,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
) STRICT;

CREATE INDEX crew_runs_workspace_updated_idx
    ON crew_runs (workspace_id, updated_at_ms DESC, id DESC);
CREATE INDEX crew_runs_recovery_idx
    ON crew_runs (status, updated_at_ms)
    WHERE status IN ('queued', 'running', 'suspended');

CREATE TABLE crew_run_members (
    crew_run_id TEXT NOT NULL REFERENCES crew_runs(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    role TEXT NOT NULL CHECK (length(trim(role)) > 0),
    local_agent_id TEXT REFERENCES agent_profiles(id) ON DELETE RESTRICT,
    remote_agent_id TEXT REFERENCES remote_agents(id) ON DELETE RESTRICT,
    agent_run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    remote_task_id TEXT,
    remote_context_id TEXT,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued', 'running', 'completed', 'failed', 'unknown_outcome', 'cancelled')
    ),
    output TEXT,
    response_json TEXT CHECK (response_json IS NULL OR json_valid(response_json)),
    last_error TEXT,
    started_at_ms INTEGER,
    completed_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (crew_run_id, ordinal),
    CHECK ((local_agent_id IS NOT NULL) != (remote_agent_id IS NOT NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE shadow_subscriptions (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agent_profiles(id) ON DELETE CASCADE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    last_message_id TEXT REFERENCES messages(id) ON DELETE SET NULL,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (conversation_id, agent_id)
) STRICT;

CREATE TABLE shadow_suggestions (
    id TEXT PRIMARY KEY NOT NULL,
    subscription_id TEXT NOT NULL REFERENCES shadow_subscriptions(id) ON DELETE CASCADE,
    source_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES agent_runs(id) ON DELETE SET NULL,
    content TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'processing' CHECK (
        status IN ('processing', 'unread', 'accepted', 'dismissed', 'failed')
    ),
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (status IN ('processing', 'failed') OR length(trim(content)) > 0),
    UNIQUE (subscription_id, source_message_id)
) STRICT;

CREATE INDEX shadow_suggestions_processing_idx
    ON shadow_suggestions (status, updated_at_ms)
    WHERE status = 'processing';
