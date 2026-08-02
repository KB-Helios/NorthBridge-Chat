use std::collections::BTreeSet;

use northbridge_domain::{CredentialReference, WorkflowDefinition};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

use crate::{
    Database, NewAgentProfile, NewArtifact, NewCredentialReference, NewEnvironment, NewMcpServer,
    NewPersona, NewRemoteAgent, NewSkill, NewWorkflow, Result, StorageError, now_ms,
};

/// A package import write set. Dropping it before `commit` rolls back every imported entity.
pub struct PackageImportTransaction {
    inner: Transaction<'static, Sqlite>,
}

impl Database {
    pub async fn begin_package_import(&self) -> Result<PackageImportTransaction> {
        Ok(PackageImportTransaction {
            inner: self.begin_write().await?,
        })
    }
}

impl PackageImportTransaction {
    pub async fn commit(self) -> Result<()> {
        self.inner.commit().await?;
        Ok(())
    }

    pub async fn create_persona(&mut self, input: NewPersona) -> Result<Uuid> {
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
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "a persona with this name already exists"))?;
        Ok(id)
    }

    pub async fn create_skill(&mut self, input: NewSkill) -> Result<Uuid> {
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
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "a skill with this slug already exists"))?;
        Ok(id)
    }

    pub async fn create_mcp_server(&mut self, input: NewMcpServer) -> Result<Uuid> {
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
        .bind(json_string(&input.args, "MCP arguments")?)
        .bind(input.environment_references.to_string())
        .bind(input.openapi_schema.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "an MCP server with this name already exists"))?;
        Ok(id)
    }

    pub async fn create_remote_agent(&mut self, input: NewRemoteAgent) -> Result<Uuid> {
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
        .bind(gateway_path)
        .bind(input.auth_environment_key.as_deref())
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "a remote agent with this name already exists"))?;
        Ok(id)
    }

    pub async fn create_agent_profile(&mut self, input: NewAgentProfile) -> Result<Uuid> {
        validate_label("agent name", &input.name, 120)?;
        validate_label("model alias", &input.model_alias, 160)?;
        if !(1..=100).contains(&input.max_turns) {
            return Err(StorageError::InvalidInput(
                "agent max turns must be between 1 and 100".to_string(),
            ));
        }
        if input
            .tool_allowlist
            .iter()
            .any(|permission| permission.tool_name.trim().is_empty())
        {
            return Err(StorageError::InvalidInput(
                "agent tool allowlist names cannot be empty".to_string(),
            ));
        }

        let id = Uuid::now_v7();
        let now = now_ms();
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
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "an agent with this name already exists"))?;
        for (ordinal, skill_id) in input.skill_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO agent_skill_links (agent_id, skill_id, ordinal) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(skill_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .execute(&mut *self.inner)
            .await?;
        }
        for (ordinal, stack_id) in input.knowledge_stack_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO agent_knowledge_links (agent_id, stack_id, ordinal) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(stack_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .execute(&mut *self.inner)
            .await?;
        }
        for permission in &input.tool_allowlist {
            sqlx::query(
                "INSERT INTO agent_tool_allowlist (agent_id, server_id, tool_name) VALUES (?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(permission.server_id.to_string())
            .bind(permission.tool_name.trim())
            .execute(&mut *self.inner)
            .await?;
        }
        Ok(id)
    }

    pub async fn create_workflow(&mut self, input: NewWorkflow) -> Result<Uuid> {
        let id = Uuid::now_v7();
        let now = now_ms();
        let candidate = WorkflowDefinition {
            id,
            workspace_id: input.workspace_id,
            name: input.name.trim().to_string(),
            description: input.description.trim().to_string(),
            version: 1,
            entry_node_id: input.entry_node_id.trim().to_string(),
            nodes: input.nodes,
            created_at_ms: now,
            updated_at_ms: now,
        };
        candidate.validate().map_err(StorageError::InvalidInput)?;
        let nodes = json_string(&candidate.nodes, "workflow nodes")?;
        sqlx::query(
            "INSERT INTO workflows (
                 id, workspace_id, name, description, version, entry_node_id, nodes_json,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(candidate.workspace_id.to_string())
        .bind(&candidate.name)
        .bind(&candidate.description)
        .bind(i64::from(candidate.version))
        .bind(&candidate.entry_node_id)
        .bind(nodes)
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "a workflow with this name already exists"))?;
        Ok(id)
    }

    pub async fn create_credential_reference(
        &mut self,
        input: NewCredentialReference,
    ) -> Result<CredentialReference> {
        validate_label("credential name", &input.name, 120)?;
        let id = Uuid::now_v7();
        let service = "NorthBridge".to_string();
        let account = format!("{}:{id}", input.workspace_id);
        let now = now_ms();
        let credential = CredentialReference {
            id,
            workspace_id: input.workspace_id,
            name: input.name.trim().to_string(),
            service,
            account,
            created_at_ms: now,
            updated_at_ms: now,
        };
        sqlx::query(
            "INSERT INTO credential_references (
                 id, workspace_id, name, service, account, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(credential.workspace_id.to_string())
        .bind(&credential.name)
        .bind(&credential.service)
        .bind(&credential.account)
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "a credential with this name already exists"))?;
        Ok(credential)
    }

    pub async fn create_environment(&mut self, input: NewEnvironment) -> Result<Uuid> {
        validate_label("environment name", &input.name, 120)?;
        validate_environment_variables(&mut self.inner, input.workspace_id, &input.variables)
            .await?;
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO environments (
                 id, workspace_id, name, description, variables_json, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(json_string(&input.variables, "environment variables")?)
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await
        .map_err(|error| map_unique(error, "an environment with this name already exists"))?;
        Ok(id)
    }

    pub async fn create_artifact(&mut self, input: NewArtifact) -> Result<Uuid> {
        validate_label("artifact name", &input.name, 240)?;
        validate_media_type(&input.media_type)?;
        if input.content.len() > 64 * 1024 * 1024 {
            return Err(StorageError::InvalidInput(
                "artifact content cannot exceed 64 MiB".to_string(),
            ));
        }
        let sha256 = format!("{:x}", Sha256::digest(&input.content));
        let size_bytes = i64::try_from(input.content.len()).map_err(|_| {
            StorageError::InvalidInput("artifact content length cannot be represented".to_string())
        })?;
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO artifact_blobs (sha256, content, size_bytes, created_at_ms)
             VALUES (?, ?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(&sha256)
        .bind(input.content.as_slice())
        .bind(size_bytes)
        .bind(now)
        .execute(&mut *self.inner)
        .await?;
        sqlx::query(
            "INSERT INTO artifacts (
                 id, workspace_id, name, kind, media_type, sha256, size_bytes, source,
                 metadata_json, revision, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.kind.as_str())
        .bind(input.media_type.trim())
        .bind(&sha256)
        .bind(size_bytes)
        .bind(input.source.trim())
        .bind(json_string(&input.metadata, "artifact metadata")?)
        .bind(now)
        .bind(now)
        .execute(&mut *self.inner)
        .await?;
        Ok(id)
    }

    pub async fn record_package_operation(
        &mut self,
        workspace_id: Uuid,
        direction: &str,
        name: &str,
        manifest: &[u8],
        summary: &Value,
        error: Option<&str>,
    ) -> Result<Uuid> {
        if !matches!(direction, "import" | "export") {
            return Err(StorageError::InvalidInput(
                "package direction must be import or export".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let digest = format!("{:x}", Sha256::digest(manifest));
        sqlx::query(
            "INSERT INTO package_operations (
                 id, workspace_id, direction, package_name, manifest_sha256, status,
                 summary_json, error, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(workspace_id.to_string())
        .bind(direction)
        .bind(name.trim())
        .bind(digest)
        .bind(if error.is_some() {
            "failed"
        } else {
            "completed"
        })
        .bind(json_string(summary, "package operation summary")?)
        .bind(error)
        .bind(now_ms())
        .execute(&mut *self.inner)
        .await?;
        Ok(id)
    }
}

async fn validate_environment_variables(
    transaction: &mut Transaction<'static, Sqlite>,
    workspace_id: Uuid,
    variables: &[northbridge_domain::EnvironmentVariableRef],
) -> Result<()> {
    if variables.len() > 128 {
        return Err(StorageError::InvalidInput(
            "environment cannot contain more than 128 variables".to_string(),
        ));
    }
    let mut names = BTreeSet::new();
    for variable in variables {
        if !valid_environment_key(&variable.name) || !names.insert(variable.name.as_str()) {
            return Err(StorageError::InvalidInput(format!(
                "invalid or duplicate environment variable name: {}",
                variable.name
            )));
        }
        let credential_workspace = sqlx::query_scalar::<_, String>(
            "SELECT workspace_id FROM credential_references WHERE id = ?",
        )
        .bind(variable.credential_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(StorageError::NotFound {
            entity: "credential reference",
        })?;
        if credential_workspace != workspace_id.to_string() {
            return Err(StorageError::InvalidInput(
                "environment credential belongs to another workspace".to_string(),
            ));
        }
    }
    Ok(())
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
        northbridge_domain::McpTransport::Stdio
            if input
                .command
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                && input.endpoint.is_none() => {}
        northbridge_domain::McpTransport::StreamableHttp
        | northbridge_domain::McpTransport::Sse
            if input.command.is_none()
                && input
                    .endpoint
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty()) =>
        {
            validate_remote_url(input.endpoint.as_deref().unwrap_or_default())?;
        }
        northbridge_domain::McpTransport::Openapi
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

fn validate_media_type(value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'+' | b'-' | b'.'))
        || !value.contains('/')
    {
        return Err(StorageError::InvalidInput(
            "artifact media type is invalid".to_string(),
        ));
    }
    Ok(())
}

fn valid_environment_key(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn json_string<T: serde::Serialize + ?Sized>(value: &T, field: &'static str) -> Result<String> {
    serde_json::to_string(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn map_unique(error: sqlx::Error, message: &str) -> StorageError {
    if matches!(&error, sqlx::Error::Database(database) if database.is_unique_violation()) {
        StorageError::Conflict(message.to_string())
    } else {
        StorageError::Database(error)
    }
}
