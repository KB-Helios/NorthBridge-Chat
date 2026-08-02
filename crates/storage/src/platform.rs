use std::str::FromStr;

use northbridge_domain::{
    Artifact, ArtifactKind, CredentialReference, EnvironmentDefinition, EnvironmentVariableRef,
    MediaJob, MediaJobStatus, MediaModality, MediaProvider,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, Clone)]
pub struct NewArtifact {
    pub workspace_id: Uuid,
    pub name: String,
    pub kind: ArtifactKind,
    pub media_type: String,
    pub source: String,
    pub metadata: Value,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct NewCredentialReference {
    pub workspace_id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct NewEnvironment {
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub variables: Vec<EnvironmentVariableRef>,
}

#[derive(Debug, Clone)]
pub struct NewMediaProvider {
    pub workspace_id: Uuid,
    pub name: String,
    pub modality: MediaModality,
    pub upstream_url: String,
    pub credential_id: Option<Uuid>,
    pub enabled: bool,
}

impl Database {
    pub async fn create_artifact(&self, input: NewArtifact) -> Result<Artifact> {
        validate_label("artifact name", &input.name, 240)?;
        validate_media_type(&input.media_type)?;
        if input.content.len() > 64 * 1024 * 1024 {
            return Err(StorageError::InvalidInput(
                "artifact content cannot exceed 64 MiB".to_string(),
            ));
        }
        let sha256 = hex_digest(&input.content);
        let size_bytes = i64::try_from(input.content.len()).map_err(|_| {
            StorageError::InvalidInput("artifact content length cannot be represented".to_string())
        })?;
        let id = Uuid::now_v7();
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO artifact_blobs (sha256, content, size_bytes, created_at_ms)
             VALUES (?, ?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(&sha256)
        .bind(input.content)
        .bind(size_bytes)
        .bind(now)
        .execute(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.artifact(id).await
    }

    pub async fn revise_artifact(&self, id: Uuid, input: NewArtifact) -> Result<Artifact> {
        let existing = self.artifact(id).await?;
        if existing.workspace_id != input.workspace_id {
            return Err(StorageError::InvalidInput(
                "artifact revision cannot change workspace".to_string(),
            ));
        }
        validate_label("artifact name", &input.name, 240)?;
        validate_media_type(&input.media_type)?;
        if input.content.len() > 64 * 1024 * 1024 {
            return Err(StorageError::InvalidInput(
                "artifact content cannot exceed 64 MiB".to_string(),
            ));
        }
        let sha256 = hex_digest(&input.content);
        let size_bytes = i64::try_from(input.content.len()).map_err(|_| {
            StorageError::InvalidInput("artifact content length cannot be represented".to_string())
        })?;
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO artifact_blobs (sha256, content, size_bytes, created_at_ms)
             VALUES (?, ?, ?, ?) ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(&sha256)
        .bind(input.content)
        .bind(size_bytes)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE artifacts SET name = ?, kind = ?, media_type = ?, sha256 = ?, size_bytes = ?,
                    source = ?, metadata_json = ?, revision = revision + 1, updated_at_ms = ?
             WHERE id = ?",
        )
        .bind(input.name.trim())
        .bind(input.kind.as_str())
        .bind(input.media_type.trim())
        .bind(&sha256)
        .bind(size_bytes)
        .bind(input.source.trim())
        .bind(json_string(&input.metadata, "artifact metadata")?)
        .bind(now)
        .bind(id.to_string())
        .execute(&mut *transaction)
        .await?;
        delete_unreferenced_blob(&mut transaction, &existing.sha256).await?;
        transaction.commit().await?;
        self.artifact(id).await
    }

    pub async fn artifact(&self, id: Uuid) -> Result<Artifact> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, kind, media_type, sha256, size_bytes, source,
                    metadata_json, revision, created_at_ms, updated_at_ms
             FROM artifacts WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "artifact" })?;
        artifact_from_row(&row)
    }

    pub async fn artifact_content(&self, id: Uuid) -> Result<(Artifact, Vec<u8>)> {
        let artifact = self.artifact(id).await?;
        let content =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT content FROM artifact_blobs WHERE sha256 = ?")
                .bind(&artifact.sha256)
                .fetch_optional(self.pool())
                .await?
                .ok_or(StorageError::CorruptData {
                    field: "artifact blob",
                    message: "content-addressed blob is missing".to_string(),
                })?;
        if hex_digest(&content) != artifact.sha256 {
            return Err(StorageError::CorruptData {
                field: "artifact blob",
                message: "content hash does not match artifact metadata".to_string(),
            });
        }
        Ok((artifact, content))
    }

    pub async fn list_artifacts(&self, workspace_id: Uuid) -> Result<Vec<Artifact>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, kind, media_type, sha256, size_bytes, source,
                    metadata_json, revision, created_at_ms, updated_at_ms
             FROM artifacts WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(artifact_from_row).collect()
    }

    pub async fn delete_artifact(&self, id: Uuid) -> Result<bool> {
        let artifact = self.artifact(id).await?;
        let mut transaction = self.begin_write().await?;
        let changed = sqlx::query("DELETE FROM artifacts WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        delete_unreferenced_blob(&mut transaction, &artifact.sha256).await?;
        transaction.commit().await?;
        Ok(changed > 0)
    }

    pub async fn create_credential_reference(
        &self,
        input: NewCredentialReference,
    ) -> Result<CredentialReference> {
        validate_label("credential name", &input.name, 120)?;
        let id = Uuid::now_v7();
        let service = "NorthBridge".to_string();
        let account = format!("{}:{id}", input.workspace_id);
        let now = now_ms();
        sqlx::query(
            "INSERT INTO credential_references (
                 id, workspace_id, name, service, account, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(&service)
        .bind(&account)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "a credential with this name already exists"))?;
        self.credential_reference(id).await
    }

    pub async fn credential_reference(&self, id: Uuid) -> Result<CredentialReference> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, service, account, created_at_ms, updated_at_ms
             FROM credential_references WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "credential reference",
        })?;
        credential_from_row(&row)
    }

    pub async fn list_credential_references(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<CredentialReference>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, service, account, created_at_ms, updated_at_ms
             FROM credential_references WHERE workspace_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(credential_from_row).collect()
    }

    pub async fn delete_credential_reference(&self, id: Uuid) -> Result<bool> {
        Ok(
            sqlx::query("DELETE FROM credential_references WHERE id = ?")
                .bind(id.to_string())
                .execute(self.pool())
                .await?
                .rows_affected()
                > 0,
        )
    }

    pub async fn create_environment(&self, input: NewEnvironment) -> Result<EnvironmentDefinition> {
        validate_label("environment name", &input.name, 120)?;
        validate_environment_variables(self, input.workspace_id, &input.variables).await?;
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
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "an environment with this name already exists"))?;
        self.environment(id).await
    }

    pub async fn environment(&self, id: Uuid) -> Result<EnvironmentDefinition> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, description, variables_json, created_at_ms, updated_at_ms
             FROM environments WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "environment",
        })?;
        environment_from_row(&row)
    }

    pub async fn list_environments(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<EnvironmentDefinition>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, description, variables_json, created_at_ms, updated_at_ms
             FROM environments WHERE workspace_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(environment_from_row).collect()
    }

    pub async fn create_media_provider(&self, input: NewMediaProvider) -> Result<MediaProvider> {
        validate_label("media provider name", &input.name, 120)?;
        validate_remote_url(&input.upstream_url)?;
        if let Some(credential_id) = input.credential_id {
            let credential = self.credential_reference(credential_id).await?;
            if credential.workspace_id != input.workspace_id {
                return Err(StorageError::InvalidInput(
                    "media provider credential belongs to another workspace".to_string(),
                ));
            }
        }
        let id = Uuid::now_v7();
        let gateway_path = format!("/media/{id}");
        let now = now_ms();
        sqlx::query(
            "INSERT INTO media_providers (
                 id, workspace_id, name, modality, upstream_url, gateway_path, credential_id,
                 enabled, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.modality.as_str())
        .bind(input.upstream_url.trim())
        .bind(&gateway_path)
        .bind(input.credential_id.map(|id| id.to_string()))
        .bind(i64::from(input.enabled))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "a media provider with this name already exists"))?;
        self.media_provider(id).await
    }

    pub async fn media_provider(&self, id: Uuid) -> Result<MediaProvider> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, modality, upstream_url, gateway_path, credential_id,
                    enabled, created_at_ms, updated_at_ms FROM media_providers WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "media provider",
        })?;
        media_provider_from_row(&row)
    }

    pub async fn list_media_providers(&self, workspace_id: Uuid) -> Result<Vec<MediaProvider>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, modality, upstream_url, gateway_path, credential_id,
                    enabled, created_at_ms, updated_at_ms FROM media_providers
             WHERE workspace_id = ? ORDER BY name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(media_provider_from_row).collect()
    }

    pub async fn create_media_job(
        &self,
        provider_id: Uuid,
        workspace_id: Uuid,
        prompt: &str,
        request: &Value,
    ) -> Result<MediaJob> {
        let provider = self.media_provider(provider_id).await?;
        if provider.workspace_id != workspace_id || !provider.enabled {
            return Err(StorageError::InvalidInput(
                "media provider is disabled or belongs to another workspace".to_string(),
            ));
        }
        if prompt.trim().is_empty() || prompt.len() > 64 * 1024 {
            return Err(StorageError::InvalidInput(
                "media prompt must contain between 1 byte and 64 KiB".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO media_jobs (
                 id, provider_id, workspace_id, modality, status, prompt, request_json,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(provider_id.to_string())
        .bind(workspace_id.to_string())
        .bind(provider.modality.as_str())
        .bind(prompt.trim())
        .bind(json_string(request, "media request")?)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await?;
        self.media_job(id).await
    }

    pub async fn media_job(&self, id: Uuid) -> Result<MediaJob> {
        let row = sqlx::query(
            "SELECT id, provider_id, workspace_id, modality, status, prompt, request_json,
                    response_json, artifact_id, last_error, created_at_ms, updated_at_ms
             FROM media_jobs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "media job",
        })?;
        media_job_from_row(&row)
    }

    pub async fn list_media_jobs(&self, workspace_id: Uuid) -> Result<Vec<MediaJob>> {
        let rows = sqlx::query(
            "SELECT id, provider_id, workspace_id, modality, status, prompt, request_json,
                    response_json, artifact_id, last_error, created_at_ms, updated_at_ms
             FROM media_jobs WHERE workspace_id = ? ORDER BY updated_at_ms DESC LIMIT 200",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(media_job_from_row).collect()
    }

    pub async fn has_active_media_jobs(&self, workspace_id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM media_jobs WHERE workspace_id = ? AND status IN ('queued', 'running')
             )",
        )
        .bind(workspace_id.to_string())
        .fetch_one(self.pool())
        .await?
            != 0)
    }

    pub async fn set_media_job_state(
        &self,
        id: Uuid,
        status: MediaJobStatus,
        response: Option<&Value>,
        artifact_id: Option<Uuid>,
        error: Option<&str>,
    ) -> Result<MediaJob> {
        let changed = sqlx::query(
            "UPDATE media_jobs SET status = ?, response_json = ?, artifact_id = ?, last_error = ?,
                    updated_at_ms = ? WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(optional_json_string(response, "media response")?)
        .bind(artifact_id.map(|id| id.to_string()))
        .bind(error)
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(StorageError::NotFound {
                entity: "media job",
            });
        }
        self.media_job(id).await
    }

    pub async fn recover_interrupted_media_jobs(&self) -> Result<u64> {
        Ok(sqlx::query(
            "UPDATE media_jobs SET status = 'unknown_outcome',
                    last_error = 'desktop core stopped during provider execution; the request will not be retried automatically',
                    updated_at_ms = ? WHERE status = 'running'",
        )
        .bind(now_ms())
        .execute(self.pool())
        .await?
        .rows_affected())
    }

    pub async fn record_package_operation(
        &self,
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
        .bind(hex_digest(manifest))
        .bind(if error.is_some() {
            "failed"
        } else {
            "completed"
        })
        .bind(json_string(summary, "package operation summary")?)
        .bind(error)
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        Ok(id)
    }
}

async fn validate_environment_variables(
    database: &Database,
    workspace_id: Uuid,
    variables: &[EnvironmentVariableRef],
) -> Result<()> {
    if variables.len() > 128 {
        return Err(StorageError::InvalidInput(
            "environment cannot contain more than 128 variables".to_string(),
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for variable in variables {
        if !valid_environment_key(&variable.name) || !names.insert(variable.name.as_str()) {
            return Err(StorageError::InvalidInput(format!(
                "invalid or duplicate environment variable name: {}",
                variable.name
            )));
        }
        let credential = database
            .credential_reference(variable.credential_id)
            .await?;
        if credential.workspace_id != workspace_id {
            return Err(StorageError::InvalidInput(
                "environment credential belongs to another workspace".to_string(),
            ));
        }
    }
    Ok(())
}

async fn delete_unreferenced_blob(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    sha256: &str,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM artifact_blobs WHERE sha256 = ?
         AND NOT EXISTS (SELECT 1 FROM artifacts WHERE artifacts.sha256 = artifact_blobs.sha256)",
    )
    .bind(sha256)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn artifact_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Artifact> {
    Ok(Artifact {
        id: parse_uuid("artifact id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        kind: parse_enum("artifact kind", row.try_get("kind")?)?,
        media_type: row.try_get("media_type")?,
        sha256: row.try_get("sha256")?,
        size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?).map_err(|error| {
            StorageError::CorruptData {
                field: "artifact size",
                message: error.to_string(),
            }
        })?,
        source: row.try_get("source")?,
        metadata: parse_json("artifact metadata", row.try_get("metadata_json")?)?,
        revision: u32::try_from(row.try_get::<i64, _>("revision")?).map_err(|error| {
            StorageError::CorruptData {
                field: "artifact revision",
                message: error.to_string(),
            }
        })?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn credential_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<CredentialReference> {
    Ok(CredentialReference {
        id: parse_uuid("credential reference id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        service: row.try_get("service")?,
        account: row.try_get("account")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn environment_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<EnvironmentDefinition> {
    Ok(EnvironmentDefinition {
        id: parse_uuid("environment id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        variables: parse_json("environment variables", row.try_get("variables_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn media_provider_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<MediaProvider> {
    Ok(MediaProvider {
        id: parse_uuid("media provider id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        modality: parse_enum("media modality", row.try_get("modality")?)?,
        upstream_url: row.try_get("upstream_url")?,
        gateway_path: row.try_get("gateway_path")?,
        credential_id: parse_optional_uuid(
            "credential reference id",
            row.try_get("credential_id")?,
        )?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn media_job_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<MediaJob> {
    Ok(MediaJob {
        id: parse_uuid("media job id", row.try_get("id")?)?,
        provider_id: parse_uuid("media provider id", row.try_get("provider_id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        modality: parse_enum("media modality", row.try_get("modality")?)?,
        status: parse_enum("media job status", row.try_get("status")?)?,
        prompt: row.try_get("prompt")?,
        request: parse_json("media request", row.try_get("request_json")?)?,
        response: parse_optional_json("media response", row.try_get("response_json")?)?,
        artifact_id: parse_optional_uuid("artifact id", row.try_get("artifact_id")?)?,
        last_error: row.try_get("last_error")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn validate_label(field: &str, value: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.chars().count() > max {
        return Err(StorageError::InvalidInput(format!(
            "{field} must contain between 1 and {max} characters"
        )));
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

fn validate_remote_url(value: &str) -> Result<()> {
    let url = url::Url::parse(value.trim())
        .map_err(|error| StorageError::InvalidInput(format!("invalid provider URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(StorageError::InvalidInput(
            "provider URL must be absolute HTTP(S) without credentials, query, or fragment"
                .to_string(),
        ));
    }
    Ok(())
}

fn valid_environment_key(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn parse_uuid(field: &'static str, value: String) -> Result<Uuid> {
    Uuid::parse_str(&value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn parse_optional_uuid(field: &'static str, value: Option<String>) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(field, value)).transpose()
}

fn parse_enum<T: FromStr<Err = String>>(field: &'static str, value: String) -> Result<T> {
    value
        .parse()
        .map_err(|message| StorageError::CorruptData { field, message })
}

fn parse_json<T: serde::de::DeserializeOwned>(field: &'static str, value: String) -> Result<T> {
    serde_json::from_str(&value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn parse_optional_json<T: serde::de::DeserializeOwned>(
    field: &'static str,
    value: Option<String>,
) -> Result<Option<T>> {
    value.map(|value| parse_json(field, value)).transpose()
}

fn json_string<T: serde::Serialize + ?Sized>(value: &T, field: &'static str) -> Result<String> {
    serde_json::to_string(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn optional_json_string<T: serde::Serialize + ?Sized>(
    value: Option<&T>,
    field: &'static str,
) -> Result<Option<String>> {
    value.map(|value| json_string(value, field)).transpose()
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn map_unique(error: sqlx::Error, message: &str) -> StorageError {
    if matches!(&error, sqlx::Error::Database(database) if database.is_unique_violation()) {
        StorageError::Conflict(message.to_string())
    } else {
        StorageError::Database(error)
    }
}
