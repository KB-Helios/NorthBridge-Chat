use std::{collections::BTreeMap, str::FromStr};

use northbridge_domain::{
    AgentApproval, AgentProfile, AgentRun, AgentRunEvent, AgentRunStatus, AgentToolCall,
    AgentToolPermission, ApprovalStatus, Crew, CrewMember, CrewRun, CrewRunMember, CrewRunStatus,
    McpServer, McpToolSnapshot, McpTransport, Persona, RemoteAgent, ShadowSubscription,
    ShadowSuggestion, ShadowSuggestionStatus, Skill, ToolCallStatus, ToolPolicyMode,
};
use serde_json::Value;
use sqlx::{FromRow, QueryBuilder, Sqlite};
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, Clone)]
pub struct NewPersona {
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub system_prompt: String,
}

#[derive(Debug, Clone)]
pub struct NewSkill {
    pub workspace_id: Uuid,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub permission_manifest: Value,
}

#[derive(Debug, Clone)]
pub struct NewAgentProfile {
    pub workspace_id: Uuid,
    pub persona_id: Option<Uuid>,
    pub name: String,
    pub description: String,
    pub model_alias: String,
    pub instructions: String,
    pub policy_mode: ToolPolicyMode,
    pub max_turns: u32,
    pub skill_ids: Vec<Uuid>,
    pub knowledge_stack_ids: Vec<Uuid>,
    pub tool_allowlist: Vec<AgentToolPermission>,
}

#[derive(Debug, Clone)]
pub struct NewMcpServer {
    pub workspace_id: Uuid,
    pub name: String,
    pub transport: McpTransport,
    pub endpoint: Option<String>,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub environment_references: Value,
    pub openapi_schema: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewAgentRun {
    pub workspace_id: Uuid,
    pub agent_id: Uuid,
    pub conversation_id: Option<Uuid>,
    pub prompt: String,
    pub rig_version: String,
    pub checkpoint: Value,
}

#[derive(Debug, Clone, Copy)]
pub struct SaveAgentCheckpoint<'a> {
    pub run_id: Uuid,
    pub expected_generation: u64,
    pub status: AgentRunStatus,
    pub checkpoint: &'a Value,
    pub current_turn: u32,
    pub event_type: &'a str,
    pub payload: &'a Value,
}

#[derive(Debug, Clone, Copy)]
pub struct FinishCrewMember<'a> {
    pub crew_run_id: Uuid,
    pub ordinal: u32,
    pub output: Option<&'a str>,
    pub response: Option<&'a Value>,
    pub remote_task_id: Option<&'a str>,
    pub remote_context_id: Option<&'a str>,
    pub error: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct NewToolIntent {
    pub run_id: Uuid,
    pub server_id: Option<Uuid>,
    pub rig_tool_call_id: String,
    pub tool_name: String,
    pub arguments: Value,
    pub read_only: bool,
    pub destructive: bool,
    pub requires_approval: bool,
    pub approval_reason: String,
}

#[derive(Debug, Clone)]
pub struct NewRemoteAgent {
    pub workspace_id: Uuid,
    pub name: String,
    pub upstream_url: String,
    pub auth_environment_key: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct NewShadowSubscription {
    pub workspace_id: Uuid,
    pub conversation_id: Uuid,
    pub agent_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct NewCrew {
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub coordinator_agent_id: Uuid,
    pub members: Vec<CrewMember>,
}

#[derive(Debug, Clone)]
pub struct NewCrewRun {
    pub workspace_id: Uuid,
    pub crew_id: Uuid,
    pub prompt: String,
}

#[derive(Debug, FromRow)]
struct PersonaRow {
    id: String,
    workspace_id: String,
    name: String,
    description: String,
    system_prompt: String,
    version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct SkillRow {
    id: String,
    workspace_id: String,
    slug: String,
    name: String,
    description: String,
    instructions: String,
    permission_manifest_json: String,
    version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct AgentProfileRow {
    id: String,
    workspace_id: String,
    persona_id: Option<String>,
    name: String,
    description: String,
    model_alias: String,
    instructions: String,
    policy_mode: String,
    max_turns: i64,
    version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct McpServerRow {
    id: String,
    workspace_id: String,
    name: String,
    transport: String,
    endpoint: Option<String>,
    command: Option<String>,
    args_json: String,
    env_refs_json: String,
    openapi_schema: Option<String>,
    enabled: i64,
    config_revision: i64,
    last_error: Option<String>,
    last_discovered_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct McpToolRow {
    server_id: String,
    name: String,
    description: String,
    input_schema_json: String,
    annotations_json: String,
    read_only: i64,
    destructive: i64,
    discovered_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct AgentRunRow {
    id: String,
    workspace_id: String,
    agent_id: String,
    conversation_id: Option<String>,
    status: String,
    prompt: String,
    rig_version: String,
    checkpoint_json: String,
    checkpoint_generation: i64,
    current_turn: i64,
    output: Option<String>,
    cancel_requested: i64,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
}

#[derive(Debug, FromRow)]
struct AgentRunEventRow {
    run_id: String,
    sequence_number: i64,
    event_type: String,
    payload_json: String,
    created_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ToolCallRow {
    id: String,
    run_id: String,
    server_id: Option<String>,
    rig_tool_call_id: String,
    tool_name: String,
    arguments_json: String,
    status: String,
    read_only: i64,
    destructive: i64,
    result_json: Option<String>,
    last_error: Option<String>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ApprovalRow {
    id: String,
    run_id: String,
    tool_call_id: String,
    status: String,
    reason: String,
    decision_note: Option<String>,
    requested_at_ms: i64,
    decided_at_ms: Option<i64>,
}

#[derive(Debug, FromRow)]
struct RemoteAgentRow {
    id: String,
    workspace_id: String,
    name: String,
    upstream_url: String,
    gateway_path: String,
    auth_environment_key: Option<String>,
    enabled: i64,
    card_json: Option<String>,
    card_etag: Option<String>,
    last_discovered_at_ms: Option<i64>,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ShadowSubscriptionRow {
    id: String,
    workspace_id: String,
    conversation_id: String,
    agent_id: String,
    enabled: i64,
    last_message_id: Option<String>,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ShadowSuggestionRow {
    id: String,
    subscription_id: String,
    source_message_id: String,
    run_id: Option<String>,
    content: String,
    status: String,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct CrewRow {
    id: String,
    workspace_id: String,
    name: String,
    description: String,
    coordinator_agent_id: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct CrewMemberRow {
    ordinal: i64,
    role: String,
    local_agent_id: Option<String>,
    remote_agent_id: Option<String>,
}

#[derive(Debug, FromRow)]
struct CrewRunRow {
    id: String,
    workspace_id: String,
    crew_id: String,
    prompt: String,
    status: String,
    coordinator_run_id: Option<String>,
    output: Option<String>,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
}

#[derive(Debug, FromRow)]
struct CrewRunMemberRow {
    ordinal: i64,
    role: String,
    local_agent_id: Option<String>,
    remote_agent_id: Option<String>,
    agent_run_id: Option<String>,
    remote_task_id: Option<String>,
    remote_context_id: Option<String>,
    status: String,
    output: Option<String>,
    response_json: Option<String>,
    last_error: Option<String>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    updated_at_ms: i64,
}

impl Database {
    pub async fn create_persona(&self, input: NewPersona) -> Result<Persona> {
        validate_label("persona name", &input.name, 120)?;
        if input.system_prompt.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "persona system prompt cannot be empty".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO personas (
                 id, workspace_id, name, description, system_prompt, version, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(input.system_prompt.trim())
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("a persona with this name already exists"))?;
        self.persona(id).await
    }

    pub async fn persona(&self, id: Uuid) -> Result<Persona> {
        let row = sqlx::query_as::<_, PersonaRow>(
            "SELECT id, workspace_id, name, description, system_prompt, version, created_at_ms, updated_at_ms
             FROM personas WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "persona" })?;
        persona_from_row(row)
    }

    pub async fn list_personas(&self, workspace_id: Uuid) -> Result<Vec<Persona>> {
        let rows = sqlx::query_as::<_, PersonaRow>(
            "SELECT id, workspace_id, name, description, system_prompt, version, created_at_ms, updated_at_ms
             FROM personas WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(persona_from_row).collect()
    }

    pub async fn create_skill(&self, input: NewSkill) -> Result<Skill> {
        validate_slug(&input.slug)?;
        validate_label("skill name", &input.name, 120)?;
        if input.instructions.trim().is_empty() || !input.permission_manifest.is_object() {
            return Err(StorageError::InvalidInput(
                "skill instructions and an object permission manifest are required".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO skills (
                 id, workspace_id, slug, name, description, instructions,
                 permission_manifest_json, version, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.slug.trim())
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(input.instructions.trim())
        .bind(input.permission_manifest.to_string())
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("a skill with this slug already exists"))?;
        self.skill(id).await
    }

    pub async fn skill(&self, id: Uuid) -> Result<Skill> {
        let row = sqlx::query_as::<_, SkillRow>(
            "SELECT id, workspace_id, slug, name, description, instructions,
                    permission_manifest_json, version, created_at_ms, updated_at_ms
             FROM skills WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "skill" })?;
        skill_from_row(row)
    }

    pub async fn list_skills(&self, workspace_id: Uuid) -> Result<Vec<Skill>> {
        let rows = sqlx::query_as::<_, SkillRow>(
            "SELECT id, workspace_id, slug, name, description, instructions,
                    permission_manifest_json, version, created_at_ms, updated_at_ms
             FROM skills WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(skill_from_row).collect()
    }

    pub async fn create_agent_profile(&self, input: NewAgentProfile) -> Result<AgentProfile> {
        validate_label("agent name", &input.name, 120)?;
        validate_label("model alias", &input.model_alias, 160)?;
        if !(1..=100).contains(&input.max_turns) {
            return Err(StorageError::InvalidInput(
                "agent max turns must be between 1 and 100".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_profiles (
                 id, workspace_id, persona_id, name, description, model_alias, instructions,
                 policy_mode, max_turns, version, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.persona_id.map(|value| value.to_string()))
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(input.model_alias.trim())
        .bind(input.instructions.trim())
        .bind(input.policy_mode.as_str())
        .bind(i64::from(input.max_turns))
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(map_unique("an agent with this name already exists"))?;
        for (ordinal, skill_id) in input.skill_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO agent_skill_links (agent_id, skill_id, ordinal) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(skill_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await?;
        }
        for (ordinal, stack_id) in input.knowledge_stack_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO agent_knowledge_links (agent_id, stack_id, ordinal) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(stack_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await?;
        }
        for permission in &input.tool_allowlist {
            if permission.tool_name.trim().is_empty() {
                return Err(StorageError::InvalidInput(
                    "agent tool allowlist names cannot be empty".to_string(),
                ));
            }
            sqlx::query(
                "INSERT INTO agent_tool_allowlist (agent_id, server_id, tool_name) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(permission.server_id.to_string())
            .bind(permission.tool_name.trim())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.agent_profile(id).await
    }

    pub async fn agent_profile(&self, id: Uuid) -> Result<AgentProfile> {
        let row = sqlx::query_as::<_, AgentProfileRow>(
            "SELECT id, workspace_id, persona_id, name, description, model_alias, instructions,
                    policy_mode, max_turns, version, created_at_ms, updated_at_ms
             FROM agent_profiles WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "agent profile",
        })?;
        let skill_ids = load_link_ids(self, "agent_skill_links", "skill_id", id).await?;
        let knowledge_stack_ids =
            load_link_ids(self, "agent_knowledge_links", "stack_id", id).await?;
        let tool_allowlist = load_tool_permissions(self, id).await?;
        agent_profile_from_row(row, skill_ids, knowledge_stack_ids, tool_allowlist)
    }

    pub async fn list_agent_profiles(&self, workspace_id: Uuid) -> Result<Vec<AgentProfile>> {
        let rows = sqlx::query_as::<_, AgentProfileRow>(
            "SELECT id, workspace_id, persona_id, name, description, model_alias, instructions,
                    policy_mode, max_turns, version, created_at_ms, updated_at_ms
             FROM agent_profiles WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut profiles = Vec::with_capacity(rows.len());
        for row in rows {
            let id = parse_uuid("agent id", &row.id)?;
            let skill_ids = load_link_ids(self, "agent_skill_links", "skill_id", id).await?;
            let stack_ids = load_link_ids(self, "agent_knowledge_links", "stack_id", id).await?;
            let tool_allowlist = load_tool_permissions(self, id).await?;
            profiles.push(agent_profile_from_row(
                row,
                skill_ids,
                stack_ids,
                tool_allowlist,
            )?);
        }
        Ok(profiles)
    }

    pub async fn create_mcp_server(&self, input: NewMcpServer) -> Result<McpServer> {
        validate_mcp_server(&input)?;
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO mcp_servers (
                 id, workspace_id, name, transport, endpoint, command, args_json, env_refs_json,
                 openapi_schema, enabled, config_revision, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.transport.as_str())
        .bind(input.endpoint.as_deref())
        .bind(input.command.as_deref())
        .bind(serde_json::to_string(&input.args).map_err(json_input("MCP arguments"))?)
        .bind(input.environment_references.to_string())
        .bind(input.openapi_schema.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("an MCP server with this name already exists"))?;
        self.mcp_server(id).await
    }

    pub async fn upsert_mcp_server(&self, input: NewMcpServer) -> Result<McpServer> {
        validate_mcp_server(&input)?;
        let existing = sqlx::query_scalar::<_, String>(
            "SELECT id FROM mcp_servers WHERE workspace_id = ? AND name = ? COLLATE NOCASE",
        )
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .fetch_optional(self.pool())
        .await?;
        let id = existing
            .as_deref()
            .map(|value| parse_uuid("MCP server id", value))
            .transpose()?
            .unwrap_or_else(Uuid::now_v7);
        let now = now_ms();
        sqlx::query(
            "INSERT INTO mcp_servers (
                 id, workspace_id, name, transport, endpoint, command, args_json, env_refs_json,
                 openapi_schema, enabled, config_revision, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                 name = excluded.name, transport = excluded.transport, endpoint = excluded.endpoint,
                 command = excluded.command, args_json = excluded.args_json,
                 env_refs_json = excluded.env_refs_json, openapi_schema = excluded.openapi_schema,
                 enabled = excluded.enabled, config_revision = mcp_servers.config_revision + 1,
                 last_error = NULL, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.transport.as_str())
        .bind(input.endpoint.as_deref())
        .bind(input.command.as_deref())
        .bind(serde_json::to_string(&input.args).map_err(json_input("MCP arguments"))?)
        .bind(input.environment_references.to_string())
        .bind(input.openapi_schema.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("an MCP server with this name already exists"))?;
        self.mcp_server(id).await
    }

    pub async fn mcp_server(&self, id: Uuid) -> Result<McpServer> {
        let row = sqlx::query_as::<_, McpServerRow>(
            "SELECT id, workspace_id, name, transport, endpoint, command, args_json, env_refs_json,
                    openapi_schema, enabled, config_revision, last_error, last_discovered_at_ms,
                    created_at_ms, updated_at_ms FROM mcp_servers WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "MCP server",
        })?;
        mcp_server_from_row(row)
    }

    pub async fn list_mcp_servers(&self, workspace_id: Uuid) -> Result<Vec<McpServer>> {
        let rows = sqlx::query_as::<_, McpServerRow>(
            "SELECT id, workspace_id, name, transport, endpoint, command, args_json, env_refs_json,
                    openapi_schema, enabled, config_revision, last_error, last_discovered_at_ms,
                    created_at_ms, updated_at_ms FROM mcp_servers
             WHERE workspace_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(mcp_server_from_row).collect()
    }

    pub async fn set_mcp_server_error(&self, id: Uuid, error: &str) -> Result<()> {
        let now = now_ms();
        let result =
            sqlx::query("UPDATE mcp_servers SET last_error = ?, updated_at_ms = ? WHERE id = ?")
                .bind(error)
                .bind(now)
                .bind(id.to_string())
                .execute(self.pool())
                .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "MCP server",
            });
        }
        Ok(())
    }

    pub async fn replace_mcp_tool_snapshots(
        &self,
        server_id: Uuid,
        tools: &[McpToolSnapshot],
    ) -> Result<()> {
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query("DELETE FROM mcp_tool_snapshots WHERE server_id = ?")
            .bind(server_id.to_string())
            .execute(&mut *transaction)
            .await?;
        for tool in tools {
            if tool.server_id != server_id {
                return Err(StorageError::InvalidInput(
                    "MCP tool snapshot belongs to a different server".to_string(),
                ));
            }
            sqlx::query(
                "INSERT INTO mcp_tool_snapshots (
                     server_id, name, description, input_schema_json, annotations_json,
                     read_only, destructive, discovered_at_ms
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(server_id.to_string())
            .bind(tool.name.trim())
            .bind(tool.description.trim())
            .bind(tool.input_schema.to_string())
            .bind(tool.annotations.to_string())
            .bind(i64::from(tool.read_only))
            .bind(i64::from(tool.destructive))
            .bind(tool.discovered_at_ms)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE mcp_servers SET last_discovered_at_ms = ?, last_error = NULL, updated_at_ms = ? WHERE id = ?",
        )
        .bind(now)
        .bind(now)
        .bind(server_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn list_mcp_tools(&self, server_id: Uuid) -> Result<Vec<McpToolSnapshot>> {
        let rows = sqlx::query_as::<_, McpToolRow>(
            "SELECT server_id, name, description, input_schema_json, annotations_json,
                    read_only, destructive, discovered_at_ms
             FROM mcp_tool_snapshots WHERE server_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(server_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(mcp_tool_from_row).collect()
    }

    pub async fn list_agent_tools(&self, agent_id: Uuid) -> Result<Vec<McpToolSnapshot>> {
        let rows = sqlx::query_as::<_, McpToolRow>(
            "SELECT tool.server_id, tool.name, tool.description, tool.input_schema_json,
                    tool.annotations_json, tool.read_only, tool.destructive, tool.discovered_at_ms
             FROM agent_tool_allowlist allowed
             JOIN mcp_tool_snapshots tool
               ON tool.server_id = allowed.server_id AND tool.name = allowed.tool_name
             JOIN mcp_servers server ON server.id = tool.server_id
             WHERE allowed.agent_id = ? AND server.enabled = 1
             ORDER BY server.name COLLATE NOCASE, tool.name COLLATE NOCASE",
        )
        .bind(agent_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(mcp_tool_from_row).collect()
    }

    pub async fn create_agent_run(&self, input: NewAgentRun) -> Result<AgentRun> {
        if input.prompt.trim().is_empty() || input.rig_version.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "agent prompt and Rig version are required".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_runs (
                 id, workspace_id, agent_id, conversation_id, status, prompt, rig_version,
                 checkpoint_json, checkpoint_generation, current_turn, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, 0, 0, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.agent_id.to_string())
        .bind(input.conversation_id.map(|value| value.to_string()))
        .bind(input.prompt.trim())
        .bind(input.rig_version.trim())
        .bind(input.checkpoint.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        append_run_event_tx(
            &mut transaction,
            id,
            "run_created",
            &serde_json::json!({ "status": "queued" }),
            now,
        )
        .await?;
        transaction.commit().await?;
        self.agent_run(id).await
    }

    pub async fn agent_run(&self, id: Uuid) -> Result<AgentRun> {
        let row = load_agent_run_row(self, id).await?;
        agent_run_from_row(row)
    }

    pub async fn list_agent_runs(&self, workspace_id: Uuid, limit: u32) -> Result<Vec<AgentRun>> {
        let limit = limit.clamp(1, 200);
        let rows = sqlx::query_as::<_, AgentRunRow>(
            "SELECT id, workspace_id, agent_id, conversation_id, status, prompt, rig_version,
                    checkpoint_json, checkpoint_generation, current_turn, output,
                    cancel_requested, last_error, created_at_ms, updated_at_ms, completed_at_ms
             FROM agent_runs WHERE workspace_id = ?
             ORDER BY updated_at_ms DESC, id DESC LIMIT ?",
        )
        .bind(workspace_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(agent_run_from_row).collect()
    }

    pub async fn has_active_agent_runs(&self, workspace_id: Uuid) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_runs
                 WHERE workspace_id = ?
                   AND status NOT IN ('completed', 'failed', 'cancelled')
             )",
        )
        .bind(workspace_id.to_string())
        .fetch_one(self.pool())
        .await
        .map_err(Into::into)
    }

    pub async fn save_agent_checkpoint(&self, input: SaveAgentCheckpoint<'_>) -> Result<AgentRun> {
        if input.status.is_terminal() {
            return Err(StorageError::InvalidInput(
                "terminal runs must be completed through finish_agent_run".to_string(),
            ));
        }
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE agent_runs SET status = ?, checkpoint_json = ?,
                    checkpoint_generation = checkpoint_generation + 1, current_turn = ?,
                    updated_at_ms = ?
             WHERE id = ? AND checkpoint_generation = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(input.status.as_str())
        .bind(input.checkpoint.to_string())
        .bind(i64::from(input.current_turn))
        .bind(now)
        .bind(input.run_id.to_string())
        .bind(i64::try_from(input.expected_generation).map_err(|_| {
            StorageError::InvalidInput("checkpoint generation is too large".to_string())
        })?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "agent run checkpoint changed or is terminal".to_string(),
            ));
        }
        append_run_event_tx(
            &mut transaction,
            input.run_id,
            input.event_type,
            input.payload,
            now,
        )
        .await?;
        transaction.commit().await?;
        self.agent_run(input.run_id).await
    }

    pub async fn finish_agent_run(
        &self,
        run_id: Uuid,
        expected_generation: u64,
        status: AgentRunStatus,
        checkpoint: &Value,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<AgentRun> {
        if !status.is_terminal() {
            return Err(StorageError::InvalidInput(
                "finish_agent_run requires a terminal status".to_string(),
            ));
        }
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE agent_runs SET status = ?, checkpoint_json = ?,
                    checkpoint_generation = checkpoint_generation + 1, output = ?, last_error = ?,
                    completed_at_ms = ?, updated_at_ms = ?
             WHERE id = ? AND checkpoint_generation = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(status.as_str())
        .bind(checkpoint.to_string())
        .bind(output)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(run_id.to_string())
        .bind(i64::try_from(expected_generation).map_err(|_| {
            StorageError::InvalidInput("checkpoint generation is too large".to_string())
        })?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "agent run checkpoint changed or is terminal".to_string(),
            ));
        }
        append_run_event_tx(
            &mut transaction,
            run_id,
            "run_finished",
            &serde_json::json!({ "status": status, "error": error }),
            now,
        )
        .await?;
        transaction.commit().await?;
        self.agent_run(run_id).await
    }

    pub async fn request_agent_run_cancel(&self, run_id: Uuid) -> Result<bool> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE agent_runs SET cancel_requested = 1, updated_at_ms = ?
             WHERE id = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(now)
        .bind(run_id.to_string())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn append_agent_run_event(
        &self,
        run_id: Uuid,
        event_type: &str,
        payload: &Value,
    ) -> Result<AgentRunEvent> {
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let sequence =
            append_run_event_tx(&mut transaction, run_id, event_type, payload, now).await?;
        transaction.commit().await?;
        Ok(AgentRunEvent {
            run_id,
            sequence_number: sequence,
            event_type: event_type.to_string(),
            payload: payload.clone(),
            created_at_ms: now,
        })
    }

    pub async fn list_agent_run_events(
        &self,
        run_id: Uuid,
        after_sequence: u64,
    ) -> Result<Vec<AgentRunEvent>> {
        let rows = sqlx::query_as::<_, AgentRunEventRow>(
            "SELECT run_id, sequence_number, event_type, payload_json, created_at_ms
             FROM agent_run_events WHERE run_id = ? AND sequence_number > ?
             ORDER BY sequence_number LIMIT 1000",
        )
        .bind(run_id.to_string())
        .bind(i64::try_from(after_sequence).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(run_event_from_row).collect()
    }

    pub async fn record_tool_intent(
        &self,
        input: NewToolIntent,
    ) -> Result<(AgentToolCall, Option<AgentApproval>)> {
        if input.tool_name.trim().is_empty() || input.rig_tool_call_id.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "tool name and Rig tool-call id are required".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let approval_id = input.requires_approval.then(Uuid::now_v7);
        let now = now_ms();
        let status = if input.requires_approval {
            ToolCallStatus::PendingApproval
        } else {
            ToolCallStatus::Approved
        };
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_tool_calls (
                 id, run_id, server_id, rig_tool_call_id, tool_name, arguments_json, status,
                 read_only, destructive, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(run_id, rig_tool_call_id) DO NOTHING",
        )
        .bind(id.to_string())
        .bind(input.run_id.to_string())
        .bind(input.server_id.map(|value| value.to_string()))
        .bind(input.rig_tool_call_id.trim())
        .bind(input.tool_name.trim())
        .bind(input.arguments.to_string())
        .bind(status.as_str())
        .bind(i64::from(input.read_only))
        .bind(i64::from(input.destructive))
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let persisted_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_tool_calls WHERE run_id = ? AND rig_tool_call_id = ?",
        )
        .bind(input.run_id.to_string())
        .bind(input.rig_tool_call_id.trim())
        .fetch_one(&mut *transaction)
        .await?;
        let persisted_id = parse_uuid("tool call id", &persisted_id)?;
        if persisted_id == id
            && let Some(approval_id) = approval_id
        {
            sqlx::query(
                "INSERT INTO agent_approvals (
                     id, run_id, tool_call_id, status, reason, requested_at_ms
                 ) VALUES (?, ?, ?, 'pending', ?, ?)",
            )
            .bind(approval_id.to_string())
            .bind(input.run_id.to_string())
            .bind(id.to_string())
            .bind(input.approval_reason.trim())
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        append_run_event_tx(
            &mut transaction,
            input.run_id,
            "tool_intent_recorded",
            &serde_json::json!({
                "toolCallId": persisted_id,
                "toolName": input.tool_name,
                "requiresApproval": input.requires_approval,
            }),
            now,
        )
        .await?;
        transaction.commit().await?;
        let call = self.agent_tool_call(persisted_id).await?;
        let approval = self.approval_for_tool_call(persisted_id).await?;
        Ok((call, approval))
    }

    pub async fn agent_tool_call(&self, id: Uuid) -> Result<AgentToolCall> {
        let row = sqlx::query_as::<_, ToolCallRow>(
            "SELECT id, run_id, server_id, rig_tool_call_id, tool_name, arguments_json, status,
                    read_only, destructive, result_json, last_error, started_at_ms,
                    completed_at_ms, created_at_ms, updated_at_ms
             FROM agent_tool_calls WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "tool call",
        })?;
        tool_call_from_row(row)
    }

    pub async fn list_agent_tool_calls(&self, run_id: Uuid) -> Result<Vec<AgentToolCall>> {
        let rows = sqlx::query_as::<_, ToolCallRow>(
            "SELECT id, run_id, server_id, rig_tool_call_id, tool_name, arguments_json, status,
                    read_only, destructive, result_json, last_error, started_at_ms,
                    completed_at_ms, created_at_ms, updated_at_ms
             FROM agent_tool_calls WHERE run_id = ? ORDER BY created_at_ms, id",
        )
        .bind(run_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(tool_call_from_row).collect()
    }

    pub async fn approval_for_tool_call(
        &self,
        tool_call_id: Uuid,
    ) -> Result<Option<AgentApproval>> {
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, run_id, tool_call_id, status, reason, decision_note,
                    requested_at_ms, decided_at_ms
             FROM agent_approvals WHERE tool_call_id = ?",
        )
        .bind(tool_call_id.to_string())
        .fetch_optional(self.pool())
        .await?;
        row.map(approval_from_row).transpose()
    }

    pub async fn list_pending_approvals(&self, workspace_id: Uuid) -> Result<Vec<AgentApproval>> {
        let rows = sqlx::query_as::<_, ApprovalRow>(
            "SELECT approval.id, approval.run_id, approval.tool_call_id, approval.status,
                    approval.reason, approval.decision_note, approval.requested_at_ms,
                    approval.decided_at_ms
             FROM agent_approvals approval
             JOIN agent_runs run ON run.id = approval.run_id
             WHERE run.workspace_id = ? AND approval.status = 'pending'
             ORDER BY approval.requested_at_ms",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(approval_from_row).collect()
    }

    pub async fn decide_approval(
        &self,
        approval_id: Uuid,
        approve: bool,
        note: Option<&str>,
    ) -> Result<AgentApproval> {
        let now = now_ms();
        let approval_status = if approve { "approved" } else { "denied" };
        let tool_status = if approve { "approved" } else { "denied" };
        let mut transaction = self.begin_write().await?;
        let row = sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, run_id, tool_call_id, status, reason, decision_note,
                    requested_at_ms, decided_at_ms FROM agent_approvals WHERE id = ?",
        )
        .bind(approval_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StorageError::NotFound { entity: "approval" })?;
        if row.status != "pending" {
            return Err(StorageError::Conflict(
                "approval was already decided".to_string(),
            ));
        }
        sqlx::query(
            "UPDATE agent_approvals SET status = ?, decision_note = ?, decided_at_ms = ? WHERE id = ? AND status = 'pending'",
        )
        .bind(approval_status)
        .bind(note)
        .bind(now)
        .bind(approval_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE agent_tool_calls SET status = ?, updated_at_ms = ? WHERE id = ? AND status = 'pending_approval'",
        )
        .bind(tool_status)
        .bind(now)
        .bind(&row.tool_call_id)
        .execute(&mut *transaction)
        .await?;
        append_run_event_tx(
            &mut transaction,
            parse_uuid("approval run id", &row.run_id)?,
            "approval_decided",
            &serde_json::json!({ "approvalId": approval_id, "approved": approve, "note": note }),
            now,
        )
        .await?;
        transaction.commit().await?;
        let decided = sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, run_id, tool_call_id, status, reason, decision_note,
                    requested_at_ms, decided_at_ms FROM agent_approvals WHERE id = ?",
        )
        .bind(approval_id.to_string())
        .fetch_one(self.pool())
        .await?;
        approval_from_row(decided)
    }

    pub async fn claim_tool_call(&self, id: Uuid) -> Result<AgentToolCall> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE agent_tool_calls SET status = 'running', started_at_ms = ?, updated_at_ms = ?
             WHERE id = ? AND status = 'approved'",
        )
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "tool call is not approved or was already claimed".to_string(),
            ));
        }
        self.agent_tool_call(id).await
    }

    pub async fn finish_tool_call(
        &self,
        id: Uuid,
        result: std::result::Result<&Value, &str>,
    ) -> Result<AgentToolCall> {
        let now = now_ms();
        let (status, value, error) = match result {
            Ok(value) => ("completed", Some(value.to_string()), None),
            Err(error) => ("failed", None, Some(error)),
        };
        let update = sqlx::query(
            "UPDATE agent_tool_calls SET status = ?, result_json = ?, last_error = ?,
                    completed_at_ms = ?, updated_at_ms = ?
             WHERE id = ? AND status = 'running'",
        )
        .bind(status)
        .bind(value)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if update.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "tool call is not running".to_string(),
            ));
        }
        self.agent_tool_call(id).await
    }

    pub async fn recover_interrupted_agent_work(&self) -> Result<u64> {
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let calls = sqlx::query(
            "UPDATE agent_tool_calls SET status = 'unknown_outcome',
                    last_error = 'desktop core stopped while the tool call was running; manual reconciliation required',
                    updated_at_ms = ? WHERE status = 'running'",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let runs = sqlx::query(
            "UPDATE agent_runs SET status = 'suspended',
                    last_error = CASE WHEN last_error IS NULL THEN 'desktop core stopped during execution' ELSE last_error END,
                    updated_at_ms = ? WHERE status = 'running'",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(calls + runs)
    }

    pub async fn create_remote_agent(&self, input: NewRemoteAgent) -> Result<RemoteAgent> {
        validate_label("remote agent name", &input.name, 120)?;
        validate_remote_url(&input.upstream_url)?;
        let id = Uuid::now_v7();
        let gateway_path = format!("/a2a/{id}");
        let now = now_ms();
        sqlx::query(
            "INSERT INTO remote_agents (
                 id, workspace_id, name, upstream_url, gateway_path, auth_environment_key,
                 enabled, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.upstream_url.trim())
        .bind(&gateway_path)
        .bind(input.auth_environment_key.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("a remote agent with this name already exists"))?;
        self.remote_agent(id).await
    }

    pub async fn upsert_remote_agent(&self, input: NewRemoteAgent) -> Result<RemoteAgent> {
        validate_label("remote agent name", &input.name, 120)?;
        validate_remote_url(&input.upstream_url)?;
        let existing = sqlx::query_scalar::<_, String>(
            "SELECT id FROM remote_agents WHERE workspace_id = ? AND name = ? COLLATE NOCASE",
        )
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .fetch_optional(self.pool())
        .await?;
        let id = existing
            .as_deref()
            .map(|value| parse_uuid("remote agent id", value))
            .transpose()?
            .unwrap_or_else(Uuid::now_v7);
        let gateway_path = format!("/a2a/{id}");
        let now = now_ms();
        sqlx::query(
            "INSERT INTO remote_agents (
                 id, workspace_id, name, upstream_url, gateway_path, auth_environment_key,
                 enabled, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET name = excluded.name,
                 upstream_url = excluded.upstream_url, auth_environment_key = excluded.auth_environment_key,
                 enabled = excluded.enabled, last_error = NULL, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.upstream_url.trim())
        .bind(&gateway_path)
        .bind(input.auth_environment_key.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique("a remote agent with this name already exists"))?;
        self.remote_agent(id).await
    }

    pub async fn remote_agent(&self, id: Uuid) -> Result<RemoteAgent> {
        let row = sqlx::query_as::<_, RemoteAgentRow>(
            "SELECT id, workspace_id, name, upstream_url, gateway_path, auth_environment_key,
                    enabled, card_json, card_etag, last_discovered_at_ms, last_error,
                    created_at_ms, updated_at_ms FROM remote_agents WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "remote agent",
        })?;
        remote_agent_from_row(row)
    }

    pub async fn list_remote_agents(&self, workspace_id: Uuid) -> Result<Vec<RemoteAgent>> {
        let rows = sqlx::query_as::<_, RemoteAgentRow>(
            "SELECT id, workspace_id, name, upstream_url, gateway_path, auth_environment_key,
                    enabled, card_json, card_etag, last_discovered_at_ms, last_error,
                    created_at_ms, updated_at_ms FROM remote_agents
             WHERE workspace_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(remote_agent_from_row).collect()
    }

    pub async fn update_remote_agent_card(
        &self,
        id: Uuid,
        card: &Value,
        etag: Option<&str>,
    ) -> Result<RemoteAgent> {
        let now = now_ms();
        sqlx::query(
            "UPDATE remote_agents SET card_json = ?, card_etag = ?, last_discovered_at_ms = ?,
                    last_error = NULL, updated_at_ms = ? WHERE id = ?",
        )
        .bind(card.to_string())
        .bind(etag)
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        self.remote_agent(id).await
    }

    pub async fn create_shadow_subscription(
        &self,
        input: NewShadowSubscription,
    ) -> Result<ShadowSubscription> {
        let valid = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM conversations conversation, agent_profiles agent
                 WHERE conversation.id = ? AND conversation.workspace_id = ?
                   AND conversation.deleted_at_ms IS NULL
                   AND agent.id = ? AND agent.workspace_id = ?
             )",
        )
        .bind(input.conversation_id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.agent_id.to_string())
        .bind(input.workspace_id.to_string())
        .fetch_one(self.pool())
        .await?;
        if !valid {
            return Err(StorageError::NotFound {
                entity: "conversation or shadow agent",
            });
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO shadow_subscriptions (
                 id, workspace_id, conversation_id, agent_id, enabled, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.conversation_id.to_string())
        .bind(input.agent_id.to_string())
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(map_unique(
            "this conversation already has that shadow agent",
        ))?;
        self.shadow_subscription(id).await
    }

    pub async fn shadow_subscription(&self, id: Uuid) -> Result<ShadowSubscription> {
        let row = sqlx::query_as::<_, ShadowSubscriptionRow>(
            "SELECT id, workspace_id, conversation_id, agent_id, enabled, last_message_id,
                    last_error, created_at_ms, updated_at_ms
             FROM shadow_subscriptions WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "shadow subscription",
        })?;
        shadow_from_row(row)
    }

    pub async fn list_shadow_subscriptions(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<ShadowSubscription>> {
        let rows = sqlx::query_as::<_, ShadowSubscriptionRow>(
            "SELECT id, workspace_id, conversation_id, agent_id, enabled, last_message_id,
                    last_error, created_at_ms, updated_at_ms
             FROM shadow_subscriptions WHERE workspace_id = ? ORDER BY updated_at_ms DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(shadow_from_row).collect()
    }

    pub async fn list_shadow_subscriptions_for_conversation(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<ShadowSubscription>> {
        let rows = sqlx::query_as::<_, ShadowSubscriptionRow>(
            "SELECT id, workspace_id, conversation_id, agent_id, enabled, last_message_id,
                    last_error, created_at_ms, updated_at_ms
             FROM shadow_subscriptions
             WHERE conversation_id = ? AND enabled = 1
             ORDER BY created_at_ms, id",
        )
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(shadow_from_row).collect()
    }

    pub async fn reserve_shadow_suggestion(
        &self,
        subscription_id: Uuid,
        source_message_id: Uuid,
    ) -> Result<ShadowSuggestion> {
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let valid = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM shadow_subscriptions subscription
                 JOIN messages message ON message.conversation_id = subscription.conversation_id
                 WHERE subscription.id = ? AND subscription.enabled = 1 AND message.id = ?
             )",
        )
        .bind(subscription_id.to_string())
        .bind(source_message_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if !valid {
            return Err(StorageError::NotFound {
                entity: "enabled shadow subscription or source message",
            });
        }
        sqlx::query(
            "INSERT INTO shadow_suggestions (
                 id, subscription_id, source_message_id, content, status,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, '', 'processing', ?, ?)
             ON CONFLICT(subscription_id, source_message_id) DO NOTHING",
        )
        .bind(id.to_string())
        .bind(subscription_id.to_string())
        .bind(source_message_id.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM shadow_suggestions
             WHERE subscription_id = ? AND source_message_id = ?",
        )
        .bind(subscription_id.to_string())
        .bind(source_message_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE shadow_subscriptions
             SET last_message_id = ?, last_error = NULL, updated_at_ms = ? WHERE id = ?",
        )
        .bind(source_message_id.to_string())
        .bind(now)
        .bind(subscription_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.shadow_suggestion(parse_uuid("shadow suggestion id", &id)?)
            .await
    }

    pub async fn attach_shadow_suggestion_run(
        &self,
        suggestion_id: Uuid,
        run_id: Uuid,
    ) -> Result<ShadowSuggestion> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE shadow_suggestions SET run_id = COALESCE(run_id, ?), updated_at_ms = ?
             WHERE id = ? AND status = 'processing' AND (run_id IS NULL OR run_id = ?)",
        )
        .bind(run_id.to_string())
        .bind(now)
        .bind(suggestion_id.to_string())
        .bind(run_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "shadow suggestion already belongs to another run or is terminal".to_string(),
            ));
        }
        self.shadow_suggestion(suggestion_id).await
    }

    pub async fn complete_shadow_suggestion(
        &self,
        suggestion_id: Uuid,
        content: &str,
    ) -> Result<ShadowSuggestion> {
        if content.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "shadow suggestion content cannot be empty".to_string(),
            ));
        }
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE shadow_suggestions
             SET content = ?, status = 'unread', last_error = NULL, updated_at_ms = ?
             WHERE id = ? AND status = 'processing'",
        )
        .bind(content.trim())
        .bind(now)
        .bind(suggestion_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "shadow suggestion is no longer processing".to_string(),
            ));
        }
        self.shadow_suggestion(suggestion_id).await
    }

    pub async fn fail_shadow_suggestion(
        &self,
        suggestion_id: Uuid,
        error: &str,
    ) -> Result<ShadowSuggestion> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE shadow_suggestions SET status = 'failed', last_error = ?, updated_at_ms = ?
             WHERE id = ? AND status = 'processing'",
        )
        .bind(error)
        .bind(now)
        .bind(suggestion_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "shadow suggestion is no longer processing".to_string(),
            ));
        }
        self.shadow_suggestion(suggestion_id).await
    }

    pub async fn decide_shadow_suggestion(
        &self,
        suggestion_id: Uuid,
        accepted: bool,
    ) -> Result<ShadowSuggestion> {
        let now = now_ms();
        let status = if accepted { "accepted" } else { "dismissed" };
        let result = sqlx::query(
            "UPDATE shadow_suggestions SET status = ?, updated_at_ms = ?
             WHERE id = ? AND status = 'unread'",
        )
        .bind(status)
        .bind(now)
        .bind(suggestion_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "only unread shadow suggestions can be decided".to_string(),
            ));
        }
        self.shadow_suggestion(suggestion_id).await
    }

    pub async fn shadow_suggestion(&self, id: Uuid) -> Result<ShadowSuggestion> {
        let row = sqlx::query_as::<_, ShadowSuggestionRow>(
            "SELECT id, subscription_id, source_message_id, run_id, content, status,
                    last_error, created_at_ms, updated_at_ms
             FROM shadow_suggestions WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "shadow suggestion",
        })?;
        shadow_suggestion_from_row(row)
    }

    pub async fn list_shadow_suggestions(
        &self,
        subscription_id: Uuid,
    ) -> Result<Vec<ShadowSuggestion>> {
        let rows = sqlx::query_as::<_, ShadowSuggestionRow>(
            "SELECT id, subscription_id, source_message_id, run_id, content, status,
                    last_error, created_at_ms, updated_at_ms
             FROM shadow_suggestions WHERE subscription_id = ?
             ORDER BY created_at_ms DESC, id DESC LIMIT 200",
        )
        .bind(subscription_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(shadow_suggestion_from_row).collect()
    }

    pub async fn list_processing_shadow_suggestions(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<ShadowSuggestion>> {
        let rows = sqlx::query_as::<_, ShadowSuggestionRow>(
            "SELECT suggestion.id, suggestion.subscription_id, suggestion.source_message_id,
                    suggestion.run_id, suggestion.content, suggestion.status,
                    suggestion.last_error, suggestion.created_at_ms, suggestion.updated_at_ms
             FROM shadow_suggestions suggestion
             JOIN shadow_subscriptions subscription ON subscription.id = suggestion.subscription_id
             WHERE subscription.workspace_id = ? AND suggestion.status = 'processing'
             ORDER BY suggestion.created_at_ms, suggestion.id",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(shadow_suggestion_from_row).collect()
    }

    pub async fn create_crew(&self, input: NewCrew) -> Result<Crew> {
        validate_label("crew name", &input.name, 120)?;
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO crews (
                 id, workspace_id, name, description, coordinator_agent_id, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(input.coordinator_agent_id.to_string())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(map_unique("a crew with this name already exists"))?;
        for (index, member) in input.members.iter().enumerate() {
            if member.ordinal != u32::try_from(index).unwrap_or(u32::MAX)
                || member.role.trim().is_empty()
                || (member.local_agent_id.is_some() == member.remote_agent_id.is_some())
            {
                return Err(StorageError::InvalidInput(
                    "crew members require contiguous ordinals, a role, and exactly one local or remote agent"
                        .to_string(),
                ));
            }
            sqlx::query(
                "INSERT INTO crew_members (
                     crew_id, ordinal, role, local_agent_id, remote_agent_id
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(i64::from(member.ordinal))
            .bind(member.role.trim())
            .bind(member.local_agent_id.map(|value| value.to_string()))
            .bind(member.remote_agent_id.map(|value| value.to_string()))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.crew(id).await
    }

    pub async fn crew(&self, id: Uuid) -> Result<Crew> {
        let row = sqlx::query_as::<_, CrewRow>(
            "SELECT id, workspace_id, name, description, coordinator_agent_id,
                    created_at_ms, updated_at_ms FROM crews WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "crew" })?;
        crew_from_row(self, row).await
    }

    pub async fn list_crews(&self, workspace_id: Uuid) -> Result<Vec<Crew>> {
        let rows = sqlx::query_as::<_, CrewRow>(
            "SELECT id, workspace_id, name, description, coordinator_agent_id,
                    created_at_ms, updated_at_ms FROM crews
             WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut crews = Vec::with_capacity(rows.len());
        for row in rows {
            crews.push(crew_from_row(self, row).await?);
        }
        Ok(crews)
    }

    pub async fn create_crew_run(&self, input: NewCrewRun) -> Result<CrewRun> {
        if input.prompt.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "crew run prompt cannot be empty".to_string(),
            ));
        }
        let crew = self.crew(input.crew_id).await?;
        if crew.workspace_id != input.workspace_id {
            return Err(StorageError::NotFound { entity: "crew" });
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO crew_runs (
                 id, workspace_id, crew_id, prompt, status, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.crew_id.to_string())
        .bind(input.prompt.trim())
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        for member in &crew.members {
            sqlx::query(
                "INSERT INTO crew_run_members (
                     crew_run_id, ordinal, role, local_agent_id, remote_agent_id,
                     status, updated_at_ms
                 ) VALUES (?, ?, ?, ?, ?, 'queued', ?)",
            )
            .bind(id.to_string())
            .bind(i64::from(member.ordinal))
            .bind(&member.role)
            .bind(member.local_agent_id.map(|value| value.to_string()))
            .bind(member.remote_agent_id.map(|value| value.to_string()))
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        self.crew_run(id).await
    }

    pub async fn crew_run(&self, id: Uuid) -> Result<CrewRun> {
        let row = sqlx::query_as::<_, CrewRunRow>(
            "SELECT id, workspace_id, crew_id, prompt, status, coordinator_run_id,
                    output, last_error, created_at_ms, updated_at_ms, completed_at_ms
             FROM crew_runs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "crew run" })?;
        crew_run_from_row(self, row).await
    }

    pub async fn list_crew_runs(&self, workspace_id: Uuid, limit: u32) -> Result<Vec<CrewRun>> {
        let rows = sqlx::query_as::<_, CrewRunRow>(
            "SELECT id, workspace_id, crew_id, prompt, status, coordinator_run_id,
                    output, last_error, created_at_ms, updated_at_ms, completed_at_ms
             FROM crew_runs WHERE workspace_id = ?
             ORDER BY updated_at_ms DESC, id DESC LIMIT ?",
        )
        .bind(workspace_id.to_string())
        .bind(i64::from(limit.clamp(1, 200)))
        .fetch_all(self.pool())
        .await?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(crew_run_from_row(self, row).await?);
        }
        Ok(runs)
    }

    pub async fn has_active_crew_runs(&self, workspace_id: Uuid) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM crew_runs
                 WHERE workspace_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')
             )",
        )
        .bind(workspace_id.to_string())
        .fetch_one(self.pool())
        .await
        .map_err(Into::into)
    }

    pub async fn start_crew_run(&self, id: Uuid) -> Result<CrewRun> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE crew_runs SET status = 'running', last_error = NULL, updated_at_ms = ?
             WHERE id = ? AND status IN ('queued', 'suspended')",
        )
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "crew run is already active or terminal".to_string(),
            ));
        }
        self.crew_run(id).await
    }

    pub async fn start_local_crew_member(
        &self,
        crew_run_id: Uuid,
        ordinal: u32,
        agent_run_id: Uuid,
    ) -> Result<CrewRun> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE crew_run_members
             SET status = 'running', agent_run_id = COALESCE(agent_run_id, ?),
                 started_at_ms = COALESCE(started_at_ms, ?), updated_at_ms = ?
             WHERE crew_run_id = ? AND ordinal = ? AND local_agent_id IS NOT NULL
               AND status IN ('queued', 'running') AND (agent_run_id IS NULL OR agent_run_id = ?)",
        )
        .bind(agent_run_id.to_string())
        .bind(now)
        .bind(now)
        .bind(crew_run_id.to_string())
        .bind(i64::from(ordinal))
        .bind(agent_run_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "local crew member is already terminal or belongs to another run".to_string(),
            ));
        }
        self.crew_run(crew_run_id).await
    }

    pub async fn start_remote_crew_member(
        &self,
        crew_run_id: Uuid,
        ordinal: u32,
    ) -> Result<CrewRun> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE crew_run_members
             SET status = 'running', started_at_ms = COALESCE(started_at_ms, ?), updated_at_ms = ?
             WHERE crew_run_id = ? AND ordinal = ? AND remote_agent_id IS NOT NULL
               AND status = 'queued'",
        )
        .bind(now)
        .bind(now)
        .bind(crew_run_id.to_string())
        .bind(i64::from(ordinal))
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "remote crew member is already active or terminal".to_string(),
            ));
        }
        self.crew_run(crew_run_id).await
    }

    pub async fn finish_crew_member(&self, input: FinishCrewMember<'_>) -> Result<CrewRun> {
        let now = now_ms();
        let status = if input.error.is_some() {
            "failed"
        } else {
            "completed"
        };
        let result = sqlx::query(
            "UPDATE crew_run_members SET status = ?, output = ?, response_json = ?,
                    remote_task_id = ?, remote_context_id = ?, last_error = ?,
                    completed_at_ms = ?, updated_at_ms = ?
             WHERE crew_run_id = ? AND ordinal = ? AND status = 'running'",
        )
        .bind(status)
        .bind(input.output)
        .bind(input.response.map(Value::to_string))
        .bind(input.remote_task_id)
        .bind(input.remote_context_id)
        .bind(input.error)
        .bind(now)
        .bind(now)
        .bind(input.crew_run_id.to_string())
        .bind(i64::from(input.ordinal))
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "crew member is not running".to_string(),
            ));
        }
        self.crew_run(input.crew_run_id).await
    }

    pub async fn attach_crew_coordinator_run(
        &self,
        crew_run_id: Uuid,
        agent_run_id: Uuid,
    ) -> Result<CrewRun> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE crew_runs SET coordinator_run_id = COALESCE(coordinator_run_id, ?),
                    updated_at_ms = ?
             WHERE id = ? AND status = 'running'
               AND (coordinator_run_id IS NULL OR coordinator_run_id = ?)",
        )
        .bind(agent_run_id.to_string())
        .bind(now)
        .bind(crew_run_id.to_string())
        .bind(agent_run_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "crew coordinator is already attached or the run is terminal".to_string(),
            ));
        }
        self.crew_run(crew_run_id).await
    }

    pub async fn finish_crew_run(
        &self,
        id: Uuid,
        output: Option<&str>,
        error: Option<&str>,
    ) -> Result<CrewRun> {
        let now = now_ms();
        let status = if error.is_some() {
            "failed"
        } else {
            "completed"
        };
        let result = sqlx::query(
            "UPDATE crew_runs SET status = ?, output = ?, last_error = ?,
                    completed_at_ms = ?, updated_at_ms = ?
             WHERE id = ? AND status IN ('running', 'suspended')",
        )
        .bind(status)
        .bind(output)
        .bind(error)
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict("crew run is not active".to_string()));
        }
        self.crew_run(id).await
    }

    pub async fn recover_interrupted_crew_runs(&self) -> Result<u64> {
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let remote = sqlx::query(
            "UPDATE crew_run_members SET status = 'unknown_outcome',
                    last_error = COALESCE(last_error, 'desktop core stopped during remote A2A execution'),
                    updated_at_ms = ?
             WHERE status = 'running' AND remote_agent_id IS NOT NULL",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let runs = sqlx::query(
            "UPDATE crew_runs SET status = 'suspended',
                    last_error = CASE
                        WHEN EXISTS(
                            SELECT 1 FROM crew_run_members member
                            WHERE member.crew_run_id = crew_runs.id AND member.status = 'unknown_outcome'
                        ) THEN 'remote member outcome requires reconciliation'
                        ELSE last_error
                    END,
                    updated_at_ms = ? WHERE status = 'running'",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(remote + runs)
    }

    pub async fn list_recoverable_crew_runs(&self, workspace_id: Uuid) -> Result<Vec<CrewRun>> {
        let rows = sqlx::query_as::<_, CrewRunRow>(
            "SELECT id, workspace_id, crew_id, prompt, status, coordinator_run_id,
                    output, last_error, created_at_ms, updated_at_ms, completed_at_ms
             FROM crew_runs WHERE workspace_id = ? AND status IN ('queued', 'suspended')
             ORDER BY created_at_ms, id",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            runs.push(crew_run_from_row(self, row).await?);
        }
        Ok(runs)
    }
}

async fn load_agent_run_row(database: &Database, id: Uuid) -> Result<AgentRunRow> {
    sqlx::query_as::<_, AgentRunRow>(
        "SELECT id, workspace_id, agent_id, conversation_id, status, prompt, rig_version,
                checkpoint_json, checkpoint_generation, current_turn, output,
                cancel_requested, last_error, created_at_ms, updated_at_ms, completed_at_ms
         FROM agent_runs WHERE id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(database.pool())
    .await?
    .ok_or(StorageError::NotFound {
        entity: "agent run",
    })
}

async fn append_run_event_tx(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    run_id: Uuid,
    event_type: &str,
    payload: &Value,
    now: i64,
) -> Result<u64> {
    if event_type.trim().is_empty() {
        return Err(StorageError::InvalidInput(
            "agent event type cannot be empty".to_string(),
        ));
    }
    let sequence = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM agent_run_events WHERE run_id = ?",
    )
    .bind(run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO agent_run_events (run_id, sequence_number, event_type, payload_json, created_at_ms)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(run_id.to_string())
    .bind(sequence)
    .bind(event_type.trim())
    .bind(payload.to_string())
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    u64::try_from(sequence).map_err(|_| corrupt("agent event sequence", sequence))
}

async fn load_link_ids(
    database: &Database,
    table: &str,
    column: &str,
    agent_id: Uuid,
) -> Result<Vec<Uuid>> {
    let allowed = BTreeMap::from([
        ("agent_skill_links", "skill_id"),
        ("agent_knowledge_links", "stack_id"),
    ]);
    if allowed.get(table) != Some(&column) {
        return Err(StorageError::InvalidInput(
            "unsupported agent link lookup".to_string(),
        ));
    }
    let mut query = QueryBuilder::<Sqlite>::new("SELECT ");
    query.push(column).push(" FROM ").push(table);
    query
        .push(" WHERE agent_id = ")
        .push_bind(agent_id.to_string());
    query.push(" ORDER BY ordinal");
    let values = query
        .build_query_scalar::<String>()
        .fetch_all(database.pool())
        .await?;
    values
        .into_iter()
        .map(|value| parse_uuid("agent link id", &value))
        .collect()
}

async fn load_tool_permissions(
    database: &Database,
    agent_id: Uuid,
) -> Result<Vec<AgentToolPermission>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT server_id, tool_name FROM agent_tool_allowlist
         WHERE agent_id = ? ORDER BY server_id, tool_name",
    )
    .bind(agent_id.to_string())
    .fetch_all(database.pool())
    .await?;
    rows.into_iter()
        .map(|(server_id, tool_name)| {
            Ok(AgentToolPermission {
                server_id: parse_uuid("agent tool server id", &server_id)?,
                tool_name,
            })
        })
        .collect()
}

fn persona_from_row(row: PersonaRow) -> Result<Persona> {
    Ok(Persona {
        id: parse_uuid("persona id", &row.id)?,
        workspace_id: parse_uuid("persona workspace id", &row.workspace_id)?,
        name: row.name,
        description: row.description,
        system_prompt: row.system_prompt,
        version: to_u32("persona version", row.version)?,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn skill_from_row(row: SkillRow) -> Result<Skill> {
    Ok(Skill {
        id: parse_uuid("skill id", &row.id)?,
        workspace_id: parse_uuid("skill workspace id", &row.workspace_id)?,
        slug: row.slug,
        name: row.name,
        description: row.description,
        instructions: row.instructions,
        permission_manifest: parse_json(
            "skill permission manifest",
            &row.permission_manifest_json,
        )?,
        version: to_u32("skill version", row.version)?,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn agent_profile_from_row(
    row: AgentProfileRow,
    skill_ids: Vec<Uuid>,
    knowledge_stack_ids: Vec<Uuid>,
    tool_allowlist: Vec<AgentToolPermission>,
) -> Result<AgentProfile> {
    Ok(AgentProfile {
        id: parse_uuid("agent id", &row.id)?,
        workspace_id: parse_uuid("agent workspace id", &row.workspace_id)?,
        persona_id: parse_optional_uuid("agent persona id", row.persona_id)?,
        name: row.name,
        description: row.description,
        model_alias: row.model_alias,
        instructions: row.instructions,
        policy_mode: ToolPolicyMode::from_str(&row.policy_mode).map_err(|message| {
            StorageError::CorruptData {
                field: "agent policy mode",
                message,
            }
        })?,
        max_turns: to_u32("agent max turns", row.max_turns)?,
        version: to_u32("agent version", row.version)?,
        skill_ids,
        knowledge_stack_ids,
        tool_allowlist,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn mcp_server_from_row(row: McpServerRow) -> Result<McpServer> {
    Ok(McpServer {
        id: parse_uuid("MCP server id", &row.id)?,
        workspace_id: parse_uuid("MCP workspace id", &row.workspace_id)?,
        name: row.name,
        transport: McpTransport::from_str(&row.transport).map_err(|message| {
            StorageError::CorruptData {
                field: "MCP transport",
                message,
            }
        })?,
        endpoint: row.endpoint,
        command: row.command,
        args: serde_json::from_str(&row.args_json).map_err(|error| StorageError::CorruptData {
            field: "MCP arguments",
            message: error.to_string(),
        })?,
        environment_references: parse_json("MCP environment references", &row.env_refs_json)?,
        openapi_schema: row.openapi_schema,
        enabled: row.enabled != 0,
        config_revision: to_u64("MCP config revision", row.config_revision)?,
        last_error: row.last_error,
        last_discovered_at_ms: row.last_discovered_at_ms,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn mcp_tool_from_row(row: McpToolRow) -> Result<McpToolSnapshot> {
    Ok(McpToolSnapshot {
        server_id: parse_uuid("MCP tool server id", &row.server_id)?,
        name: row.name,
        description: row.description,
        input_schema: parse_json("MCP input schema", &row.input_schema_json)?,
        annotations: parse_json("MCP annotations", &row.annotations_json)?,
        read_only: row.read_only != 0,
        destructive: row.destructive != 0,
        discovered_at_ms: row.discovered_at_ms,
    })
}

fn agent_run_from_row(row: AgentRunRow) -> Result<AgentRun> {
    Ok(AgentRun {
        id: parse_uuid("agent run id", &row.id)?,
        workspace_id: parse_uuid("agent run workspace id", &row.workspace_id)?,
        agent_id: parse_uuid("agent run agent id", &row.agent_id)?,
        conversation_id: parse_optional_uuid("agent run conversation id", row.conversation_id)?,
        status: AgentRunStatus::from_str(&row.status).map_err(|message| {
            StorageError::CorruptData {
                field: "agent run status",
                message,
            }
        })?,
        prompt: row.prompt,
        rig_version: row.rig_version,
        checkpoint: parse_json("agent run checkpoint", &row.checkpoint_json)?,
        checkpoint_generation: to_u64("agent checkpoint generation", row.checkpoint_generation)?,
        current_turn: to_u32("agent current turn", row.current_turn)?,
        output: row.output,
        cancel_requested: row.cancel_requested != 0,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
        completed_at_ms: row.completed_at_ms,
    })
}

fn run_event_from_row(row: AgentRunEventRow) -> Result<AgentRunEvent> {
    Ok(AgentRunEvent {
        run_id: parse_uuid("agent event run id", &row.run_id)?,
        sequence_number: to_u64("agent event sequence", row.sequence_number)?,
        event_type: row.event_type,
        payload: parse_json("agent event payload", &row.payload_json)?,
        created_at_ms: row.created_at_ms,
    })
}

fn tool_call_from_row(row: ToolCallRow) -> Result<AgentToolCall> {
    Ok(AgentToolCall {
        id: parse_uuid("tool call id", &row.id)?,
        run_id: parse_uuid("tool call run id", &row.run_id)?,
        server_id: parse_optional_uuid("tool call server id", row.server_id)?,
        rig_tool_call_id: row.rig_tool_call_id,
        tool_name: row.tool_name,
        arguments: parse_json("tool arguments", &row.arguments_json)?,
        status: ToolCallStatus::from_str(&row.status).map_err(|message| {
            StorageError::CorruptData {
                field: "tool call status",
                message,
            }
        })?,
        read_only: row.read_only != 0,
        destructive: row.destructive != 0,
        result: row
            .result_json
            .map(|value| parse_json("tool result", &value))
            .transpose()?,
        last_error: row.last_error,
        started_at_ms: row.started_at_ms,
        completed_at_ms: row.completed_at_ms,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn approval_from_row(row: ApprovalRow) -> Result<AgentApproval> {
    Ok(AgentApproval {
        id: parse_uuid("approval id", &row.id)?,
        run_id: parse_uuid("approval run id", &row.run_id)?,
        tool_call_id: parse_uuid("approval tool call id", &row.tool_call_id)?,
        status: ApprovalStatus::from_str(&row.status).map_err(|message| {
            StorageError::CorruptData {
                field: "approval status",
                message,
            }
        })?,
        reason: row.reason,
        decision_note: row.decision_note,
        requested_at_ms: row.requested_at_ms,
        decided_at_ms: row.decided_at_ms,
    })
}

fn remote_agent_from_row(row: RemoteAgentRow) -> Result<RemoteAgent> {
    Ok(RemoteAgent {
        id: parse_uuid("remote agent id", &row.id)?,
        workspace_id: parse_uuid("remote agent workspace id", &row.workspace_id)?,
        name: row.name,
        upstream_url: row.upstream_url,
        gateway_path: row.gateway_path,
        auth_environment_key: row.auth_environment_key,
        enabled: row.enabled != 0,
        card: row
            .card_json
            .map(|value| parse_json("remote agent card", &value))
            .transpose()?,
        card_etag: row.card_etag,
        last_discovered_at_ms: row.last_discovered_at_ms,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn shadow_from_row(row: ShadowSubscriptionRow) -> Result<ShadowSubscription> {
    Ok(ShadowSubscription {
        id: parse_uuid("shadow subscription id", &row.id)?,
        workspace_id: parse_uuid("shadow workspace id", &row.workspace_id)?,
        conversation_id: parse_uuid("shadow conversation id", &row.conversation_id)?,
        agent_id: parse_uuid("shadow agent id", &row.agent_id)?,
        enabled: row.enabled != 0,
        last_message_id: parse_optional_uuid("shadow message id", row.last_message_id)?,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn shadow_suggestion_from_row(row: ShadowSuggestionRow) -> Result<ShadowSuggestion> {
    Ok(ShadowSuggestion {
        id: parse_uuid("shadow suggestion id", &row.id)?,
        subscription_id: parse_uuid("shadow suggestion subscription id", &row.subscription_id)?,
        source_message_id: parse_uuid(
            "shadow suggestion source message id",
            &row.source_message_id,
        )?,
        run_id: parse_optional_uuid("shadow suggestion run id", row.run_id)?,
        content: row.content,
        status: ShadowSuggestionStatus::from_str(&row.status)
            .map_err(|message| corrupt("shadow suggestion status", message))?,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

async fn crew_from_row(database: &Database, row: CrewRow) -> Result<Crew> {
    let id = parse_uuid("crew id", &row.id)?;
    let members = sqlx::query_as::<_, CrewMemberRow>(
        "SELECT ordinal, role, local_agent_id, remote_agent_id
         FROM crew_members WHERE crew_id = ? ORDER BY ordinal",
    )
    .bind(id.to_string())
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .map(|member| {
        Ok(CrewMember {
            ordinal: to_u32("crew member ordinal", member.ordinal)?,
            role: member.role,
            local_agent_id: parse_optional_uuid("crew local agent id", member.local_agent_id)?,
            remote_agent_id: parse_optional_uuid("crew remote agent id", member.remote_agent_id)?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(Crew {
        id,
        workspace_id: parse_uuid("crew workspace id", &row.workspace_id)?,
        name: row.name,
        description: row.description,
        coordinator_agent_id: parse_uuid("crew coordinator id", &row.coordinator_agent_id)?,
        members,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

async fn crew_run_from_row(database: &Database, row: CrewRunRow) -> Result<CrewRun> {
    let id = parse_uuid("crew run id", &row.id)?;
    let members = sqlx::query_as::<_, CrewRunMemberRow>(
        "SELECT ordinal, role, local_agent_id, remote_agent_id, agent_run_id,
                remote_task_id, remote_context_id, status, output, response_json,
                last_error, started_at_ms, completed_at_ms, updated_at_ms
         FROM crew_run_members WHERE crew_run_id = ? ORDER BY ordinal",
    )
    .bind(id.to_string())
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .map(|member| {
        Ok(CrewRunMember {
            ordinal: to_u32("crew run member ordinal", member.ordinal)?,
            role: member.role,
            local_agent_id: parse_optional_uuid("crew run local agent id", member.local_agent_id)?,
            remote_agent_id: parse_optional_uuid(
                "crew run remote agent id",
                member.remote_agent_id,
            )?,
            agent_run_id: parse_optional_uuid("crew member agent run id", member.agent_run_id)?,
            remote_task_id: member.remote_task_id,
            remote_context_id: member.remote_context_id,
            status: member.status,
            output: member.output,
            response: member
                .response_json
                .map(|value| parse_json("crew member response", &value))
                .transpose()?,
            last_error: member.last_error,
            started_at_ms: member.started_at_ms,
            completed_at_ms: member.completed_at_ms,
            updated_at_ms: member.updated_at_ms,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(CrewRun {
        id,
        workspace_id: parse_uuid("crew run workspace id", &row.workspace_id)?,
        crew_id: parse_uuid("crew run crew id", &row.crew_id)?,
        prompt: row.prompt,
        status: CrewRunStatus::from_str(&row.status)
            .map_err(|message| corrupt("crew run status", message))?,
        coordinator_run_id: parse_optional_uuid("crew coordinator run id", row.coordinator_run_id)?,
        output: row.output,
        last_error: row.last_error,
        members,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
        completed_at_ms: row.completed_at_ms,
    })
}

fn validate_label(field: &str, value: &str, maximum: usize) -> Result<()> {
    let length = value.trim().chars().count();
    if length == 0 || length > maximum {
        return Err(StorageError::InvalidInput(format!(
            "{field} must contain between 1 and {maximum} characters"
        )));
    }
    Ok(())
}

fn validate_slug(value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 80
        || !value.bytes().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || b"_-".contains(&character)
        })
    {
        return Err(StorageError::InvalidInput(
            "skill slug may contain only lowercase letters, digits, underscores, and hyphens"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_mcp_server(input: &NewMcpServer) -> Result<()> {
    validate_label("MCP server name", &input.name, 80)?;
    if !input.name.bytes().all(|character| {
        character.is_ascii_alphanumeric() || character == b'_' || character == b'-'
    }) {
        return Err(StorageError::InvalidInput(
            "MCP server name may contain only letters, digits, underscores, and hyphens"
                .to_string(),
        ));
    }
    if !input.environment_references.is_object() {
        return Err(StorageError::InvalidInput(
            "MCP environment references must be an object".to_string(),
        ));
    }
    match input.transport {
        McpTransport::Stdio
            if input
                .command
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && input.endpoint.is_none() => {}
        McpTransport::StreamableHttp | McpTransport::Sse
            if input.command.is_none()
                && input
                    .endpoint
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()) =>
        {
            validate_remote_url(input.endpoint.as_deref().unwrap_or_default())?;
        }
        McpTransport::Openapi
            if input.command.is_none()
                && input
                    .endpoint
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                && input
                    .openapi_schema
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()) =>
        {
            validate_remote_url(input.endpoint.as_deref().unwrap_or_default())?;
        }
        _ => {
            return Err(StorageError::InvalidInput(
                "MCP transport configuration is incomplete or contradictory".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_remote_url(value: &str) -> Result<()> {
    let url = url::Url::parse(value.trim())
        .map_err(|error| StorageError::InvalidInput(format!("invalid remote URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return Err(StorageError::InvalidInput(
            "remote URL must be absolute HTTP or HTTPS".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(StorageError::InvalidInput(
            "remote URLs cannot contain credentials or fragments".to_string(),
        ));
    }
    Ok(())
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn parse_optional_uuid(field: &'static str, value: Option<String>) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(field, &value)).transpose()
}

fn parse_json(field: &'static str, value: &str) -> Result<Value> {
    serde_json::from_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn to_u32(field: &'static str, value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| corrupt(field, value))
}

fn to_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| corrupt(field, value))
}

fn corrupt(field: &'static str, value: impl std::fmt::Display) -> StorageError {
    StorageError::CorruptData {
        field,
        message: format!("out-of-range value {value}"),
    }
}

fn json_input(field: &'static str) -> impl FnOnce(serde_json::Error) -> StorageError {
    move |error| StorageError::InvalidInput(format!("invalid {field}: {error}"))
}

fn map_unique(message: &'static str) -> impl FnOnce(sqlx::Error) -> StorageError {
    move |error| {
        if matches!(&error, sqlx::Error::Database(database) if database.is_unique_violation()) {
            StorageError::Conflict(message.to_string())
        } else {
            StorageError::Database(error)
        }
    }
}
