use std::collections::HashSet;

use northbridge_domain::{GenerationRun, ModelSelection, SplitChatLane, SplitChatRun};
use sqlx::Row;
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

const MIN_LANES: usize = 2;
const MAX_LANES: usize = 4;

impl Database {
    pub async fn create_split_chat_idempotent(
        &self,
        conversation_id: Uuid,
        source_branch_id: Uuid,
        input_message_id: Uuid,
        selections: &[ModelSelection],
        client_request_id: Option<&str>,
    ) -> Result<SplitChatRun> {
        validate_selections(selections)?;
        let client_request_id = normalize_client_request_id(client_request_id)?;
        let mut transaction = self.begin_write().await?;

        if let Some(client_request_id) = client_request_id
            && let Some(existing_id) = sqlx::query_scalar::<_, String>(
                "SELECT id FROM split_chat_runs
                 WHERE conversation_id = ? AND client_request_id = ?",
            )
            .bind(conversation_id.to_string())
            .bind(client_request_id)
            .fetch_optional(&mut *transaction)
            .await?
        {
            transaction.commit().await?;
            let existing = self
                .get_split_chat(parse_uuid("split_chat_run.id", &existing_id)?)
                .await?;
            validate_idempotent_match(&existing, source_branch_id, input_message_id, selections)?;
            return Ok(existing);
        }

        let source_is_valid = sqlx::query_scalar::<_, bool>(
            "WITH RECURSIVE source_chain(id, parent_message_id, role) AS (
                 SELECT messages.id, messages.parent_message_id, messages.role
                 FROM conversation_branches AS branches
                 JOIN conversations ON conversations.id = branches.conversation_id
                 JOIN messages ON messages.id = branches.head_message_id
                 WHERE branches.id = ?
                   AND branches.conversation_id = ?
                   AND conversations.deleted_at_ms IS NULL
                 UNION ALL
                 SELECT parent.id, parent.parent_message_id, parent.role
                 FROM messages AS parent
                 JOIN source_chain AS child ON parent.id = child.parent_message_id
                 WHERE parent.conversation_id = ?
             )
             SELECT EXISTS(
                 SELECT 1 FROM source_chain WHERE id = ? AND role = 'user'
             )",
        )
        .bind(source_branch_id.to_string())
        .bind(conversation_id.to_string())
        .bind(conversation_id.to_string())
        .bind(input_message_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if !source_is_valid {
            return Err(StorageError::Conflict(
                "split chats must start from a user message on the source branch".to_string(),
            ));
        }

        let split_chat_id = Uuid::now_v7();
        let timestamp = now_ms();
        sqlx::query(
            "INSERT INTO split_chat_runs (
                id, conversation_id, source_branch_id, input_message_id,
                client_request_id, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(split_chat_id.to_string())
        .bind(conversation_id.to_string())
        .bind(source_branch_id.to_string())
        .bind(input_message_id.to_string())
        .bind(client_request_id)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;

        for (ordinal, selection) in selections.iter().enumerate() {
            let branch_id = Uuid::now_v7();
            let generation_id = Uuid::now_v7();
            let load_profile_json = serde_json::to_string(&selection.load_profile)
                .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
            let generation_request_id = format!("split:{split_chat_id}:{ordinal}");

            sqlx::query(
                "INSERT INTO conversation_branches (
                    id, conversation_id, parent_branch_id, forked_from_message_id,
                    head_message_id, created_at_ms
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(branch_id.to_string())
            .bind(conversation_id.to_string())
            .bind(source_branch_id.to_string())
            .bind(input_message_id.to_string())
            .bind(input_message_id.to_string())
            .bind(timestamp)
            .execute(&mut *transaction)
            .await?;

            sqlx::query(
                "INSERT INTO generation_runs (
                    id, conversation_id, branch_id, input_message_id, client_request_id,
                    model_alias, provider, resolved_model_id, load_profile_json, status,
                    last_sequence, created_at_ms, updated_at_ms
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?)",
            )
            .bind(generation_id.to_string())
            .bind(conversation_id.to_string())
            .bind(branch_id.to_string())
            .bind(input_message_id.to_string())
            .bind(generation_request_id)
            .bind(&selection.model_alias)
            .bind(selection.provider.as_str())
            .bind(&selection.model_id)
            .bind(load_profile_json)
            .bind(timestamp)
            .bind(timestamp)
            .execute(&mut *transaction)
            .await?;

            sqlx::query(
                "INSERT INTO split_chat_lanes (
                    split_chat_id, ordinal, branch_id, generation_id
                 ) VALUES (?, ?, ?, ?)",
            )
            .bind(split_chat_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .bind(branch_id.to_string())
            .bind(generation_id.to_string())
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;
        self.get_split_chat(split_chat_id).await
    }

    pub async fn get_split_chat(&self, id: Uuid) -> Result<SplitChatRun> {
        let row = sqlx::query(
            "SELECT id, conversation_id, source_branch_id, input_message_id,
                    client_request_id, created_at_ms, updated_at_ms
             FROM split_chat_runs
             WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "split chat",
        })?;
        let lane_rows = sqlx::query(
            "SELECT ordinal, branch_id, generation_id
             FROM split_chat_lanes
             WHERE split_chat_id = ?
             ORDER BY ordinal",
        )
        .bind(id.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut lanes = Vec::with_capacity(lane_rows.len());
        for lane in lane_rows {
            let ordinal: i64 = lane.try_get("ordinal")?;
            let branch_id: String = lane.try_get("branch_id")?;
            let generation_id: String = lane.try_get("generation_id")?;
            lanes.push(SplitChatLane {
                split_chat_id: id,
                ordinal: u32::try_from(ordinal).map_err(|error| StorageError::CorruptData {
                    field: "split_chat_lane.ordinal",
                    message: error.to_string(),
                })?,
                branch_id: parse_uuid("split_chat_lane.branch_id", &branch_id)?,
                generation: self
                    .get_generation_run(parse_uuid(
                        "split_chat_lane.generation_id",
                        &generation_id,
                    )?)
                    .await?,
            });
        }
        if !(MIN_LANES..=MAX_LANES).contains(&lanes.len()) {
            return Err(StorageError::CorruptData {
                field: "split_chat_run.lanes",
                message: format!("expected {MIN_LANES} to {MAX_LANES} lanes"),
            });
        }
        let id_value: String = row.try_get("id")?;
        let conversation_id: String = row.try_get("conversation_id")?;
        let source_branch_id: String = row.try_get("source_branch_id")?;
        let input_message_id: String = row.try_get("input_message_id")?;
        Ok(SplitChatRun {
            id: parse_uuid("split_chat_run.id", &id_value)?,
            conversation_id: parse_uuid("split_chat_run.conversation_id", &conversation_id)?,
            source_branch_id: parse_uuid("split_chat_run.source_branch_id", &source_branch_id)?,
            input_message_id: parse_uuid("split_chat_run.input_message_id", &input_message_id)?,
            client_request_id: row.try_get("client_request_id")?,
            created_at_ms: row.try_get("created_at_ms")?,
            updated_at_ms: row.try_get("updated_at_ms")?,
            lanes,
        })
    }

    pub async fn list_split_chats(&self, conversation_id: Uuid) -> Result<Vec<SplitChatRun>> {
        self.get_conversation(conversation_id).await?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM split_chat_runs
             WHERE conversation_id = ?
             ORDER BY created_at_ms DESC, id DESC",
        )
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?;
        let mut runs = Vec::with_capacity(ids.len());
        for id in ids {
            runs.push(
                self.get_split_chat(parse_uuid("split_chat_run.id", &id)?)
                    .await?,
            );
        }
        Ok(runs)
    }
}

fn validate_selections(selections: &[ModelSelection]) -> Result<()> {
    if !(MIN_LANES..=MAX_LANES).contains(&selections.len()) {
        return Err(StorageError::InvalidInput(format!(
            "split chats require between {MIN_LANES} and {MAX_LANES} model lanes"
        )));
    }
    let mut unique = HashSet::with_capacity(selections.len());
    for selection in selections {
        selection
            .load_profile
            .validate()
            .map_err(StorageError::InvalidInput)?;
        if selection.model_id.trim().is_empty() || selection.model_id.len() > 512 {
            return Err(StorageError::InvalidInput(
                "split-chat model IDs must contain between 1 and 512 bytes".to_string(),
            ));
        }
        if selection.model_alias.trim().is_empty() || selection.model_alias.len() > 200 {
            return Err(StorageError::InvalidInput(
                "split-chat model aliases must contain between 1 and 200 bytes".to_string(),
            ));
        }
        let key = serde_json::to_string(selection)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        if !unique.insert(key) {
            return Err(StorageError::InvalidInput(
                "split-chat model lanes must use distinct model and load-profile selections"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn normalize_client_request_id(value: Option<&str>) -> Result<Option<&str>> {
    let value = value.map(str::trim).filter(|value| !value.is_empty());
    if value.is_some_and(|value| value.len() > 200) {
        return Err(StorageError::InvalidInput(
            "client request ID cannot exceed 200 bytes".to_string(),
        ));
    }
    Ok(value)
}

fn validate_idempotent_match(
    existing: &SplitChatRun,
    source_branch_id: Uuid,
    input_message_id: Uuid,
    selections: &[ModelSelection],
) -> Result<()> {
    let stored = existing
        .lanes
        .iter()
        .map(|lane| generation_selection(&lane.generation))
        .collect::<Result<Vec<_>>>()?;
    if existing.source_branch_id != source_branch_id
        || existing.input_message_id != input_message_id
        || stored != selections
    {
        return Err(StorageError::Conflict(
            "client request ID was already used with different split-chat parameters".to_string(),
        ));
    }
    Ok(())
}

fn generation_selection(generation: &GenerationRun) -> Result<ModelSelection> {
    Ok(ModelSelection {
        provider: generation
            .provider
            .ok_or_else(|| StorageError::CorruptData {
                field: "split_chat_lane.generation.provider",
                message: "provider is missing".to_string(),
            })?,
        model_id: generation
            .model_id
            .clone()
            .ok_or_else(|| StorageError::CorruptData {
                field: "split_chat_lane.generation.model_id",
                message: "model ID is missing".to_string(),
            })?,
        model_alias: generation.model_alias.clone(),
        load_profile: generation
            .load_profile
            .clone()
            .ok_or_else(|| StorageError::CorruptData {
                field: "split_chat_lane.generation.load_profile",
                message: "load profile is missing".to_string(),
            })?,
    })
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}
