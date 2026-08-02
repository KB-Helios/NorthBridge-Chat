use std::str::FromStr;

use northbridge_domain::{AuditEvent, AuthSession, SyncChange, TeamPolicy, TeamRole, TeamUser};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, Clone)]
pub struct NewTeamUser {
    pub workspace_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub password_hash: String,
    pub role: TeamRole,
}

#[derive(Debug, Clone)]
pub struct NewTeamPolicy {
    pub workspace_id: Uuid,
    pub name: String,
    pub method: String,
    pub path_prefix: String,
    pub minimum_role: TeamRole,
    pub effect: String,
}

#[derive(Debug, Clone)]
pub struct NewSyncChange {
    pub workspace_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub base_version: u64,
    pub deleted: bool,
    pub payload: Value,
    pub device_id: Uuid,
    pub client_change_id: String,
}

impl Database {
    pub async fn count_team_users(&self, workspace_id: Uuid) -> Result<u64> {
        let count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM team_users WHERE workspace_id = ?")
                .bind(workspace_id.to_string())
                .fetch_one(self.pool())
                .await?;
        u64::try_from(count).map_err(|error| StorageError::CorruptData {
            field: "team user count",
            message: error.to_string(),
        })
    }

    pub async fn create_team_user(&self, input: NewTeamUser) -> Result<TeamUser> {
        validate_email(&input.email)?;
        validate_label("display name", &input.display_name, 120)?;
        if input.password_hash.len() < 32 || input.password_hash.len() > 1024 {
            return Err(StorageError::InvalidInput(
                "password hash is malformed".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO team_users (
                 id, workspace_id, email, display_name, password_hash, role, active,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.email.trim().to_ascii_lowercase())
        .bind(input.display_name.trim())
        .bind(input.password_hash)
        .bind(input.role.as_str())
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "a user with this email already exists"))?;
        self.team_user(id).await
    }

    pub async fn team_user(&self, id: Uuid) -> Result<TeamUser> {
        let row = sqlx::query(
            "SELECT id, workspace_id, email, display_name, role, active, created_at_ms, updated_at_ms
             FROM team_users WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "team user" })?;
        team_user_from_row(&row)
    }

    pub async fn team_user_with_password(&self, email: &str) -> Result<(TeamUser, String)> {
        let row = sqlx::query(
            "SELECT id, workspace_id, email, display_name, role, active, created_at_ms,
                    updated_at_ms, password_hash FROM team_users WHERE email = ? COLLATE NOCASE",
        )
        .bind(email.trim())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "team user",
        })?;
        let password_hash = row.try_get("password_hash")?;
        Ok((team_user_from_row(&row)?, password_hash))
    }

    pub async fn list_team_users(&self, workspace_id: Uuid) -> Result<Vec<TeamUser>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, email, display_name, role, active, created_at_ms, updated_at_ms
             FROM team_users WHERE workspace_id = ? ORDER BY email COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(team_user_from_row).collect()
    }

    pub async fn create_auth_session(
        &self,
        user_id: Uuid,
        token_sha256: &str,
        expires_at_ms: i64,
    ) -> Result<AuthSession> {
        if token_sha256.len() != 64 || !token_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(StorageError::InvalidInput(
                "session token hash must be SHA-256 hexadecimal".to_string(),
            ));
        }
        let user = self.team_user(user_id).await?;
        if !user.active {
            return Err(StorageError::Conflict("user is disabled".to_string()));
        }
        let now = now_ms();
        if expires_at_ms <= now {
            return Err(StorageError::InvalidInput(
                "session expiry must be in the future".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO auth_sessions (
                 id, user_id, token_sha256, expires_at_ms, last_used_at_ms, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(user_id.to_string())
        .bind(token_sha256.to_ascii_lowercase())
        .bind(expires_at_ms)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await?;
        Ok(AuthSession {
            id,
            user_id,
            expires_at_ms,
            created_at_ms: now,
        })
    }

    pub async fn authenticate_session_hash(&self, token_sha256: &str) -> Result<Option<TeamUser>> {
        let now = now_ms();
        let row = sqlx::query(
            "SELECT u.id, u.workspace_id, u.email, u.display_name, u.role, u.active,
                    u.created_at_ms, u.updated_at_ms, s.id AS session_id
             FROM auth_sessions s JOIN team_users u ON u.id = s.user_id
             WHERE s.token_sha256 = ? AND s.revoked_at_ms IS NULL AND s.expires_at_ms > ?
                   AND u.active = 1",
        )
        .bind(token_sha256.to_ascii_lowercase())
        .bind(now)
        .fetch_optional(self.pool())
        .await?;
        let Some(row) = row else { return Ok(None) };
        let session_id: String = row.try_get("session_id")?;
        sqlx::query("UPDATE auth_sessions SET last_used_at_ms = ? WHERE id = ?")
            .bind(now)
            .bind(session_id)
            .execute(self.pool())
            .await?;
        Ok(Some(team_user_from_row(&row)?))
    }

    pub async fn revoke_auth_session(&self, id: Uuid, user_id: Uuid) -> Result<bool> {
        Ok(sqlx::query(
            "UPDATE auth_sessions SET revoked_at_ms = ?
             WHERE id = ? AND user_id = ? AND revoked_at_ms IS NULL",
        )
        .bind(now_ms())
        .bind(id.to_string())
        .bind(user_id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected()
            > 0)
    }

    pub async fn create_team_policy(&self, input: NewTeamPolicy) -> Result<TeamPolicy> {
        validate_label("policy name", &input.name, 120)?;
        let method = input.method.trim().to_ascii_uppercase();
        if method != "*"
            && (method.is_empty()
                || !method
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte == b'-'))
        {
            return Err(StorageError::InvalidInput(
                "policy method must be * or an uppercase HTTP method".to_string(),
            ));
        }
        if !input.path_prefix.starts_with("/v1/")
            || input.path_prefix.contains("..")
            || input.path_prefix.len() > 512
        {
            return Err(StorageError::InvalidInput(
                "policy path prefix must be a safe /v1/ path".to_string(),
            ));
        }
        if !matches!(input.effect.as_str(), "allow" | "deny") {
            return Err(StorageError::InvalidInput(
                "policy effect must be allow or deny".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO team_policies (
                 id, workspace_id, name, method, path_prefix, minimum_role, effect,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(method)
        .bind(input.path_prefix.trim())
        .bind(input.minimum_role.as_str())
        .bind(input.effect)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "a team policy with this name already exists"))?;
        self.team_policy(id).await
    }

    pub async fn team_policy(&self, id: Uuid) -> Result<TeamPolicy> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, method, path_prefix, minimum_role, effect,
                    created_at_ms, updated_at_ms FROM team_policies WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "team policy",
        })?;
        team_policy_from_row(&row)
    }

    pub async fn list_team_policies(&self, workspace_id: Uuid) -> Result<Vec<TeamPolicy>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, method, path_prefix, minimum_role, effect,
                    created_at_ms, updated_at_ms FROM team_policies
             WHERE workspace_id = ? ORDER BY length(path_prefix) DESC, name COLLATE NOCASE",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(team_policy_from_row).collect()
    }

    pub async fn team_policy_allows(
        &self,
        workspace_id: Uuid,
        role: TeamRole,
        method: &str,
        path: &str,
    ) -> Result<bool> {
        let policies = self.list_team_policies(workspace_id).await?;
        for policy in policies {
            if (policy.method == "*" || policy.method.eq_ignore_ascii_case(method))
                && path.starts_with(&policy.path_prefix)
            {
                return Ok(policy.effect == "allow" && role.rank() >= policy.minimum_role.rank());
            }
        }
        let required = if path.starts_with("/v1/team/") || path.starts_with("/v1/audit/") {
            TeamRole::Admin
        } else if method.eq_ignore_ascii_case("GET") {
            TeamRole::Viewer
        } else {
            TeamRole::Member
        };
        Ok(role.rank() >= required.rank())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn append_audit_event(
        &self,
        workspace_id: Uuid,
        principal: &str,
        action: &str,
        resource: &str,
        outcome: &str,
        request_id: Option<&str>,
        metadata: &Value,
    ) -> Result<AuditEvent> {
        let now = now_ms();
        let metadata_json = canonical_json(metadata)?;
        let mut transaction = self.begin_write().await?;
        sqlx::query("UPDATE workspaces SET updated_at_ms = updated_at_ms WHERE id = ?")
            .bind(workspace_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let previous_hash = sqlx::query_scalar::<_, String>(
            "SELECT event_hash FROM audit_events ORDER BY sequence DESC LIMIT 1",
        )
        .fetch_optional(&mut *transaction)
        .await?
        .unwrap_or_else(|| "0".repeat(64));
        let canonical = serde_json::json!({
            "workspaceId": workspace_id,
            "principal": principal,
            "action": action,
            "resource": resource,
            "outcome": outcome,
            "requestId": request_id,
            "metadata": metadata,
            "occurredAtMs": now,
            "previousHash": previous_hash,
        });
        let event_hash = hex_digest(canonical_json(&canonical)?.as_bytes());
        let result = sqlx::query(
            "INSERT INTO audit_events (
                 workspace_id, principal, action, resource, outcome, request_id, metadata_json,
                 occurred_at_ms, previous_hash, event_hash
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(workspace_id.to_string())
        .bind(principal)
        .bind(action)
        .bind(resource)
        .bind(outcome)
        .bind(request_id)
        .bind(metadata_json)
        .bind(now)
        .bind(&previous_hash)
        .bind(&event_hash)
        .execute(&mut *transaction)
        .await?;
        let sequence = result.last_insert_rowid();
        transaction.commit().await?;
        Ok(AuditEvent {
            sequence,
            workspace_id,
            principal: principal.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            outcome: outcome.to_string(),
            request_id: request_id.map(str::to_string),
            metadata: metadata.clone(),
            occurred_at_ms: now,
            previous_hash,
            event_hash,
        })
    }

    pub async fn list_audit_events(
        &self,
        workspace_id: Uuid,
        after: i64,
        limit: u32,
    ) -> Result<Vec<AuditEvent>> {
        let rows = sqlx::query(
            "SELECT sequence, workspace_id, principal, action, resource, outcome, request_id,
                    metadata_json, occurred_at_ms, previous_hash, event_hash
             FROM audit_events WHERE workspace_id = ? AND sequence > ?
             ORDER BY sequence LIMIT ?",
        )
        .bind(workspace_id.to_string())
        .bind(after.max(0))
        .bind(i64::from(limit.clamp(1, 1000)))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(audit_event_from_row).collect()
    }

    pub async fn verify_audit_chain(&self) -> Result<bool> {
        let rows = sqlx::query(
            "SELECT sequence, workspace_id, principal, action, resource, outcome, request_id,
                    metadata_json, occurred_at_ms, previous_hash, event_hash
             FROM audit_events ORDER BY sequence",
        )
        .fetch_all(self.pool())
        .await?;
        let mut previous = "0".repeat(64);
        for row in rows {
            let event = audit_event_from_row(&row)?;
            if event.previous_hash != previous {
                return Ok(false);
            }
            let canonical = serde_json::json!({
                "workspaceId": event.workspace_id,
                "principal": event.principal,
                "action": event.action,
                "resource": event.resource,
                "outcome": event.outcome,
                "requestId": event.request_id,
                "metadata": event.metadata,
                "occurredAtMs": event.occurred_at_ms,
                "previousHash": event.previous_hash,
            });
            if hex_digest(canonical_json(&canonical)?.as_bytes()) != event.event_hash {
                return Ok(false);
            }
            previous = event.event_hash;
        }
        Ok(true)
    }

    pub async fn apply_sync_change(&self, input: NewSyncChange) -> Result<SyncChange> {
        validate_sync_name("entity type", &input.entity_type, 80)?;
        validate_sync_name("entity id", &input.entity_id, 240)?;
        validate_sync_name("client change id", &input.client_change_id, 160)?;
        let duplicate = sqlx::query(
            "SELECT sequence, workspace_id, entity_type, entity_id, version, deleted,
                    payload_json, device_id, client_change_id, created_at_ms
             FROM sync_changes WHERE device_id = ? AND client_change_id = ?",
        )
        .bind(input.device_id.to_string())
        .bind(&input.client_change_id)
        .fetch_optional(self.pool())
        .await?;
        if let Some(row) = duplicate {
            return sync_change_from_row(&row);
        }
        let mut transaction = self.begin_write().await?;
        sqlx::query("UPDATE workspaces SET updated_at_ms = updated_at_ms WHERE id = ?")
            .bind(input.workspace_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let current = sqlx::query_scalar::<_, i64>(
            "SELECT version FROM sync_records
             WHERE workspace_id = ? AND entity_type = ? AND entity_id = ?",
        )
        .bind(input.workspace_id.to_string())
        .bind(&input.entity_type)
        .bind(&input.entity_id)
        .fetch_optional(&mut *transaction)
        .await?
        .unwrap_or(0);
        let current = u64::try_from(current).map_err(|error| StorageError::CorruptData {
            field: "sync record version",
            message: error.to_string(),
        })?;
        if current != input.base_version {
            return Err(StorageError::Conflict(format!(
                "sync conflict for {}/{}: expected version {}, server has {current}",
                input.entity_type, input.entity_id, input.base_version
            )));
        }
        let version = current + 1;
        let version_i64 = i64::try_from(version)
            .map_err(|_| StorageError::Conflict("sync record version overflowed".to_string()))?;
        let payload = canonical_json(&input.payload)?;
        let now = now_ms();
        sqlx::query(
            "INSERT INTO sync_records (
                 workspace_id, entity_type, entity_id, version, deleted, payload_json, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(workspace_id, entity_type, entity_id) DO UPDATE SET
                 version = excluded.version, deleted = excluded.deleted,
                 payload_json = excluded.payload_json, updated_at_ms = excluded.updated_at_ms",
        )
        .bind(input.workspace_id.to_string())
        .bind(&input.entity_type)
        .bind(&input.entity_id)
        .bind(version_i64)
        .bind(i64::from(input.deleted))
        .bind(&payload)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let result = sqlx::query(
            "INSERT INTO sync_changes (
                 workspace_id, entity_type, entity_id, version, deleted, payload_json,
                 device_id, client_change_id, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.workspace_id.to_string())
        .bind(&input.entity_type)
        .bind(&input.entity_id)
        .bind(version_i64)
        .bind(i64::from(input.deleted))
        .bind(&payload)
        .bind(input.device_id.to_string())
        .bind(&input.client_change_id)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let sequence = result.last_insert_rowid();
        transaction.commit().await?;
        Ok(SyncChange {
            sequence,
            workspace_id: input.workspace_id,
            entity_type: input.entity_type,
            entity_id: input.entity_id,
            version,
            deleted: input.deleted,
            payload: input.payload,
            device_id: input.device_id,
            client_change_id: input.client_change_id,
            created_at_ms: now,
        })
    }

    pub async fn pull_sync_changes(
        &self,
        workspace_id: Uuid,
        after: i64,
        limit: u32,
    ) -> Result<Vec<SyncChange>> {
        let rows = sqlx::query(
            "SELECT sequence, workspace_id, entity_type, entity_id, version, deleted,
                    payload_json, device_id, client_change_id, created_at_ms
             FROM sync_changes WHERE workspace_id = ? AND sequence > ?
             ORDER BY sequence LIMIT ?",
        )
        .bind(workspace_id.to_string())
        .bind(after.max(0))
        .bind(i64::from(limit.clamp(1, 1000)))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(sync_change_from_row).collect()
    }
}

fn team_user_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TeamUser> {
    Ok(TeamUser {
        id: parse_uuid("team user id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        email: row.try_get("email")?,
        display_name: row.try_get("display_name")?,
        role: parse_enum("team role", row.try_get("role")?)?,
        active: row.try_get::<i64, _>("active")? != 0,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn team_policy_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<TeamPolicy> {
    Ok(TeamPolicy {
        id: parse_uuid("team policy id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        method: row.try_get("method")?,
        path_prefix: row.try_get("path_prefix")?,
        minimum_role: parse_enum("team policy role", row.try_get("minimum_role")?)?,
        effect: row.try_get("effect")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn audit_event_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<AuditEvent> {
    Ok(AuditEvent {
        sequence: row.try_get("sequence")?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        principal: row.try_get("principal")?,
        action: row.try_get("action")?,
        resource: row.try_get("resource")?,
        outcome: row.try_get("outcome")?,
        request_id: row.try_get("request_id")?,
        metadata: parse_json("audit metadata", row.try_get("metadata_json")?)?,
        occurred_at_ms: row.try_get("occurred_at_ms")?,
        previous_hash: row.try_get("previous_hash")?,
        event_hash: row.try_get("event_hash")?,
    })
}

fn sync_change_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SyncChange> {
    Ok(SyncChange {
        sequence: row.try_get("sequence")?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        entity_type: row.try_get("entity_type")?,
        entity_id: row.try_get("entity_id")?,
        version: u64::try_from(row.try_get::<i64, _>("version")?).map_err(|error| {
            StorageError::CorruptData {
                field: "sync change version",
                message: error.to_string(),
            }
        })?,
        deleted: row.try_get::<i64, _>("deleted")? != 0,
        payload: parse_json("sync change payload", row.try_get("payload_json")?)?,
        device_id: parse_uuid("sync device id", row.try_get("device_id")?)?,
        client_change_id: row.try_get("client_change_id")?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}

fn validate_email(value: &str) -> Result<()> {
    let value = value.trim();
    if value.len() < 3
        || value.len() > 320
        || value.chars().any(char::is_whitespace)
        || !value.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty() && domain.contains('.') && !domain.starts_with('.')
        })
    {
        return Err(StorageError::InvalidInput(
            "user email address is invalid".to_string(),
        ));
    }
    Ok(())
}

fn validate_label(field: &str, value: &str, max: usize) -> Result<()> {
    if value.trim().is_empty() || value.chars().count() > max {
        return Err(StorageError::InvalidInput(format!(
            "{field} must contain between 1 and {max} characters"
        )));
    }
    Ok(())
}

fn validate_sync_name(field: &str, value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(StorageError::InvalidInput(format!("invalid sync {field}")));
    }
    Ok(())
}

fn parse_uuid(field: &'static str, value: String) -> Result<Uuid> {
    Uuid::parse_str(&value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
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

fn canonical_json(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(|error| StorageError::CorruptData {
        field: "canonical JSON",
        message: error.to_string(),
    })
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
