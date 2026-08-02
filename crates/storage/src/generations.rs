use northbridge_domain::{
    AiProvider, GenerationEvent, GenerationPerformanceSample, GenerationRun, GenerationStatus,
    Message, MessagePart, MessagePartKind, MessageRole, ModelLoadProfile, ModelSelection,
};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    Database, Result, StorageError, now_ms,
    rows::{GenerationEventRow, GenerationRunRow},
};

struct GenerationModelParameters<'a> {
    model_alias: &'a str,
    provider: Option<AiProvider>,
    model_id: Option<&'a str>,
    load_profile: Option<&'a ModelLoadProfile>,
    client_request_id: Option<&'a str>,
    knowledge_stack_ids: &'a [Uuid],
}

impl Database {
    pub async fn create_generation_run(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
        model_alias: &str,
    ) -> Result<GenerationRun> {
        self.create_generation_run_idempotent(conversation_id, branch_id, model_alias, None)
            .await
    }

    pub async fn create_generation_run_idempotent(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
        model_alias: &str,
        client_request_id: Option<&str>,
    ) -> Result<GenerationRun> {
        self.create_generation_run_with_model(
            conversation_id,
            branch_id,
            GenerationModelParameters {
                model_alias,
                provider: None,
                model_id: None,
                load_profile: None,
                client_request_id,
                knowledge_stack_ids: &[],
            },
        )
        .await
    }

    pub async fn create_generation_run_for_model_idempotent(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
        selection: &ModelSelection,
        client_request_id: Option<&str>,
    ) -> Result<GenerationRun> {
        self.create_generation_run_with_model(
            conversation_id,
            branch_id,
            GenerationModelParameters {
                model_alias: &selection.model_alias,
                provider: Some(selection.provider),
                model_id: Some(&selection.model_id),
                load_profile: Some(&selection.load_profile),
                client_request_id,
                knowledge_stack_ids: &[],
            },
        )
        .await
    }

    pub async fn create_generation_run_for_model_with_knowledge_idempotent(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
        selection: &ModelSelection,
        client_request_id: Option<&str>,
        knowledge_stack_ids: &[Uuid],
    ) -> Result<GenerationRun> {
        self.create_generation_run_with_model(
            conversation_id,
            branch_id,
            GenerationModelParameters {
                model_alias: &selection.model_alias,
                provider: Some(selection.provider),
                model_id: Some(&selection.model_id),
                load_profile: Some(&selection.load_profile),
                client_request_id,
                knowledge_stack_ids,
            },
        )
        .await
    }

    async fn create_generation_run_with_model(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
        parameters: GenerationModelParameters<'_>,
    ) -> Result<GenerationRun> {
        let GenerationModelParameters {
            model_alias,
            provider,
            model_id,
            load_profile,
            client_request_id,
            knowledge_stack_ids,
        } = parameters;
        let model_alias = model_alias.trim();
        if model_alias.is_empty() || model_alias.len() > 200 {
            return Err(StorageError::InvalidInput(
                "model alias must contain between 1 and 200 bytes".to_string(),
            ));
        }
        let client_request_id = client_request_id
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if client_request_id.is_some_and(|value| value.len() > 200) {
            return Err(StorageError::InvalidInput(
                "client request ID cannot exceed 200 bytes".to_string(),
            ));
        }
        let model_id = model_id.map(str::trim).filter(|value| !value.is_empty());
        if provider.is_some() != model_id.is_some() {
            return Err(StorageError::InvalidInput(
                "generation provider and model ID must be supplied together".to_string(),
            ));
        }
        if model_id.is_some_and(|value| value.len() > 512) {
            return Err(StorageError::InvalidInput(
                "resolved model ID cannot exceed 512 bytes".to_string(),
            ));
        }
        if provider.is_some() != load_profile.is_some() {
            return Err(StorageError::InvalidInput(
                "generation provider and load profile must be supplied together".to_string(),
            ));
        }
        if let Some(load_profile) = load_profile {
            load_profile
                .validate()
                .map_err(StorageError::InvalidInput)?;
        }
        if knowledge_stack_ids.len() > 8 {
            return Err(StorageError::InvalidInput(
                "a generation can use at most eight knowledge stacks".to_string(),
            ));
        }
        let mut unique_stack_ids = std::collections::BTreeSet::new();
        if !knowledge_stack_ids
            .iter()
            .all(|id| unique_stack_ids.insert(*id))
        {
            return Err(StorageError::InvalidInput(
                "knowledge stack selections must be unique".to_string(),
            ));
        }
        let load_profile_json = load_profile
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        let id = Uuid::now_v7();
        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        let insert_result = sqlx::query(
            "INSERT OR IGNORE INTO generation_runs (
                id, conversation_id, branch_id, input_message_id, client_request_id,
                model_alias, provider, resolved_model_id, load_profile_json, status, last_sequence,
                created_at_ms, updated_at_ms
             )
             SELECT ?, branches.conversation_id, branches.id, branches.head_message_id,
                    ?, ?, ?, ?, ?, 'queued', 0, ?, ?
             FROM conversation_branches AS branches
             JOIN conversations ON conversations.id = branches.conversation_id
             WHERE branches.id = ?
               AND branches.conversation_id = ?
               AND conversations.deleted_at_ms IS NULL",
        )
        .bind(id.to_string())
        .bind(client_request_id)
        .bind(model_alias)
        .bind(provider.map(AiProvider::as_str))
        .bind(model_id)
        .bind(load_profile_json.as_deref())
        .bind(timestamp)
        .bind(timestamp)
        .bind(branch_id.to_string())
        .bind(conversation_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let created = insert_result.rows_affected() > 0;
        let row = if let Some(client_request_id) = client_request_id {
            sqlx::query_as::<_, GenerationRunRow>(
                "SELECT id, conversation_id, branch_id, input_message_id, output_message_id,
                        client_request_id, model_alias, provider, resolved_model_id,
                        load_profile_json, status,
                        last_sequence, created_at_ms, updated_at_ms
                 FROM generation_runs
                 WHERE conversation_id = ? AND client_request_id = ?",
            )
            .bind(conversation_id.to_string())
            .bind(client_request_id)
            .fetch_optional(&mut *transaction)
            .await?
        } else {
            sqlx::query_as::<_, GenerationRunRow>(
                "SELECT id, conversation_id, branch_id, input_message_id, output_message_id,
                        client_request_id, model_alias, provider, resolved_model_id,
                        load_profile_json, status,
                        last_sequence, created_at_ms, updated_at_ms
                 FROM generation_runs WHERE id = ?",
            )
            .bind(id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
        }
        .ok_or(StorageError::NotFound { entity: "branch" })?;
        let run = GenerationRun::try_from(row)?;
        if created {
            let workspace_id = sqlx::query_scalar::<_, String>(
                "SELECT workspace_id FROM conversations WHERE id = ?",
            )
            .bind(conversation_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            for (ordinal, stack_id) in knowledge_stack_ids.iter().enumerate() {
                let stack_workspace = sqlx::query_scalar::<_, String>(
                    "SELECT workspace_id FROM knowledge_stacks WHERE id = ?",
                )
                .bind(stack_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(StorageError::NotFound {
                    entity: "knowledge stack",
                })?;
                if stack_workspace != workspace_id {
                    return Err(StorageError::InvalidInput(
                        "generation knowledge stacks must belong to the conversation workspace"
                            .to_string(),
                    ));
                }
                sqlx::query(
                    "INSERT INTO generation_knowledge_stacks (generation_id, stack_id, ordinal)
                     VALUES (?, ?, ?)",
                )
                .bind(run.id.to_string())
                .bind(stack_id.to_string())
                .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
                .execute(&mut *transaction)
                .await?;
            }
        } else {
            let stored_stack_ids = sqlx::query_scalar::<_, String>(
                "SELECT stack_id FROM generation_knowledge_stacks
                 WHERE generation_id = ? ORDER BY ordinal",
            )
            .bind(run.id.to_string())
            .fetch_all(&mut *transaction)
            .await?
            .into_iter()
            .map(|id| {
                Uuid::parse_str(&id).map_err(|error| StorageError::CorruptData {
                    field: "generation_knowledge_stack.stack_id",
                    message: error.to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
            if stored_stack_ids != knowledge_stack_ids {
                return Err(StorageError::Conflict(
                    "client request ID was already used with different knowledge stacks"
                        .to_string(),
                ));
            }
        }
        transaction.commit().await?;
        if run.branch_id != branch_id
            || run.model_alias != model_alias
            || run.provider != provider
            || run.model_id.as_deref() != model_id
            || run.load_profile.as_ref() != load_profile
        {
            return Err(StorageError::Conflict(
                "client request ID was already used with different generation parameters"
                    .to_string(),
            ));
        }
        Ok(run)
    }

    pub async fn get_generation_run(&self, id: Uuid) -> Result<GenerationRun> {
        let row = sqlx::query_as::<_, GenerationRunRow>(
            "SELECT id, conversation_id, branch_id, input_message_id, output_message_id,
                    client_request_id, model_alias, provider, resolved_model_id,
                    load_profile_json, status,
                    last_sequence, created_at_ms, updated_at_ms
             FROM generation_runs
             WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "generation",
        })?;
        row.try_into()
    }

    pub async fn list_active_generation_runs(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<GenerationRun>> {
        self.get_conversation(conversation_id).await?;
        sqlx::query_as::<_, GenerationRunRow>(
            "SELECT id, conversation_id, branch_id, input_message_id, output_message_id,
                    client_request_id, model_alias, provider, resolved_model_id,
                    load_profile_json, status,
                    last_sequence, created_at_ms, updated_at_ms
             FROM generation_runs
             WHERE conversation_id = ? AND status IN ('running', 'waiting_for_approval')
             ORDER BY created_at_ms, id",
        )
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(GenerationRun::try_from)
        .collect()
    }

    pub async fn set_generation_status(&self, id: Uuid, status: GenerationStatus) -> Result<()> {
        let result =
            sqlx::query("UPDATE generation_runs SET status = ?, updated_at_ms = ? WHERE id = ?")
                .bind(status.as_str())
                .bind(now_ms())
                .bind(id.to_string())
                .execute(self.pool())
                .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "generation",
            });
        }
        Ok(())
    }

    pub async fn start_generation_run(&self, id: Uuid) -> Result<GenerationEvent> {
        let timestamp = now_ms();
        let payload = json!({ "status": GenerationStatus::Running.as_str() });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        let mut transaction = self.begin_write().await?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "UPDATE generation_runs
             SET status = 'running', last_sequence = last_sequence + 1, updated_at_ms = ?
             WHERE id = ? AND status = 'queued'
             RETURNING last_sequence",
        )
        .bind(timestamp)
        .bind(id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let sequence = match sequence {
            Some(sequence) => sequence,
            None => {
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM generation_runs WHERE id = ?)")
                        .bind(id.to_string())
                        .fetch_one(&mut *transaction)
                        .await?;
                return Err(if exists {
                    StorageError::Conflict("generation is not queued".to_string())
                } else {
                    StorageError::NotFound {
                        entity: "generation",
                    }
                });
            }
        };
        insert_generation_event(
            &mut transaction,
            id,
            sequence,
            "started",
            timestamp,
            &payload_json,
        )
        .await?;
        transaction.commit().await?;
        Ok(GenerationEvent {
            generation_id: id,
            sequence_number: sequence,
            event_type: "started".to_string(),
            timestamp_ms: timestamp,
            payload,
        })
    }

    pub async fn finalize_generation(
        &self,
        id: Uuid,
        status: GenerationStatus,
        event_type: &str,
        payload: Value,
    ) -> Result<GenerationEvent> {
        if !matches!(
            status,
            GenerationStatus::Failed | GenerationStatus::Cancelled
        ) {
            return Err(StorageError::InvalidInput(
                "only failed or cancelled generations can be finalized without output".to_string(),
            ));
        }
        let event_type = validate_event(event_type, &payload)?;
        let payload_json = serde_json::to_string(&payload)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "UPDATE generation_runs
             SET status = ?, last_sequence = last_sequence + 1, updated_at_ms = ?
             WHERE id = ? AND status IN ('queued', 'running', 'waiting_for_approval')
             RETURNING last_sequence",
        )
        .bind(status.as_str())
        .bind(timestamp)
        .bind(id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let sequence = match sequence {
            Some(sequence) => sequence,
            None => {
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM generation_runs WHERE id = ?)")
                        .bind(id.to_string())
                        .fetch_one(&mut *transaction)
                        .await?;
                return Err(if exists {
                    StorageError::Conflict("generation is already terminal".to_string())
                } else {
                    StorageError::NotFound {
                        entity: "generation",
                    }
                });
            }
        };
        insert_generation_event(
            &mut transaction,
            id,
            sequence,
            event_type,
            timestamp,
            &payload_json,
        )
        .await?;
        transaction.commit().await?;
        Ok(GenerationEvent {
            generation_id: id,
            sequence_number: sequence,
            event_type: event_type.to_string(),
            timestamp_ms: timestamp,
            payload,
        })
    }

    pub async fn complete_generation_with_text(
        &self,
        id: Uuid,
        content: String,
    ) -> Result<(Message, GenerationEvent)> {
        self.finish_generation_with_text(
            id,
            content,
            GenerationStatus::Completed,
            "completed",
            false,
            None,
        )
        .await
    }

    pub async fn complete_generation_with_text_metadata(
        &self,
        id: Uuid,
        content: String,
        metadata: Value,
    ) -> Result<(Message, GenerationEvent)> {
        self.finish_generation_with_text(
            id,
            content,
            GenerationStatus::Completed,
            "completed",
            false,
            Some(metadata),
        )
        .await
    }

    pub async fn cancel_generation_with_text(
        &self,
        id: Uuid,
        content: String,
    ) -> Result<(Message, GenerationEvent)> {
        self.finish_generation_with_text(
            id,
            content,
            GenerationStatus::Cancelled,
            "cancelled",
            true,
            None,
        )
        .await
    }

    async fn finish_generation_with_text(
        &self,
        id: Uuid,
        content: String,
        terminal_status: GenerationStatus,
        event_type: &'static str,
        unfinished: bool,
        metadata: Option<Value>,
    ) -> Result<(Message, GenerationEvent)> {
        if !matches!(
            terminal_status,
            GenerationStatus::Completed | GenerationStatus::Cancelled
        ) {
            return Err(StorageError::InvalidInput(
                "text output requires a completed or cancelled terminal status".to_string(),
            ));
        }
        if content.is_empty() {
            return Err(StorageError::InvalidInput(
                "assistant output must not be empty".to_string(),
            ));
        }
        if content.len() > 4 * 1024 * 1024 {
            return Err(StorageError::InvalidInput(
                "assistant output cannot exceed 4 MiB".to_string(),
            ));
        }

        let mut transaction = self.begin_write().await?;
        let run = sqlx::query(
            "SELECT conversation_id, branch_id, input_message_id, status
             FROM generation_runs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StorageError::NotFound {
            entity: "generation",
        })?;
        let status: String = run.try_get("status")?;
        if status != GenerationStatus::Running.as_str() {
            return Err(StorageError::Conflict(
                "generation is not running".to_string(),
            ));
        }
        let conversation_id: String = run.try_get("conversation_id")?;
        let branch_id: String = run.try_get("branch_id")?;
        let input_message_id: Option<String> = run.try_get("input_message_id")?;
        let conversation_uuid =
            parse_generation_uuid("generation_run.conversation_id", &conversation_id)?;
        let branch_uuid = parse_generation_uuid("generation_run.branch_id", &branch_id)?;
        let parent_uuid = input_message_id
            .as_deref()
            .map(|value| parse_generation_uuid("generation_run.input_message_id", value))
            .transpose()?;

        let message_id = Uuid::now_v7();
        let part_id = Uuid::now_v7();
        let timestamp = now_ms();
        sqlx::query(
            "INSERT INTO messages (
                id, conversation_id, branch_id, parent_message_id, role, created_at_ms
             ) VALUES (?, ?, ?, ?, 'assistant', ?)",
        )
        .bind(message_id.to_string())
        .bind(&conversation_id)
        .bind(&branch_id)
        .bind(&input_message_id)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        let part_metadata = merge_part_metadata(metadata, unfinished)?;
        let part_metadata_json = part_metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        sqlx::query(
            "INSERT INTO message_parts (
                id, message_id, ordinal, kind, content, metadata_json
             ) VALUES (?, ?, 0, 'text', ?, ?)",
        )
        .bind(part_id.to_string())
        .bind(message_id.to_string())
        .bind(&content)
        .bind(part_metadata_json)
        .execute(&mut *transaction)
        .await?;
        let branch_update = sqlx::query(
            "UPDATE conversation_branches
             SET head_message_id = ?
             WHERE id = ? AND conversation_id = ? AND head_message_id IS ?",
        )
        .bind(message_id.to_string())
        .bind(&branch_id)
        .bind(&conversation_id)
        .bind(&input_message_id)
        .execute(&mut *transaction)
        .await?;
        if branch_update.rows_affected() != 1 {
            return Err(StorageError::Conflict(
                "conversation branch changed while generation was running".to_string(),
            ));
        }
        sqlx::query(
            "UPDATE conversations SET updated_at_ms = ?
             WHERE id = ? AND deleted_at_ms IS NULL",
        )
        .bind(timestamp)
        .bind(&conversation_id)
        .execute(&mut *transaction)
        .await?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "UPDATE generation_runs
             SET status = ?, output_message_id = ?,
                 last_sequence = last_sequence + 1, updated_at_ms = ?
             WHERE id = ? AND status = 'running'
             RETURNING last_sequence",
        )
        .bind(terminal_status.as_str())
        .bind(message_id.to_string())
        .bind(timestamp)
        .bind(id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StorageError::Conflict("generation is not running".to_string()))?;
        let payload = if unfinished {
            json!({
                "outputMessageId": message_id,
                "reason": "user_requested",
                "unfinished": true,
            })
        } else {
            json!({ "outputMessageId": message_id })
        };
        let payload_json = serde_json::to_string(&payload)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        insert_generation_event(
            &mut transaction,
            id,
            sequence,
            event_type,
            timestamp,
            &payload_json,
        )
        .await?;
        transaction.commit().await?;

        Ok((
            Message {
                id: message_id,
                conversation_id: conversation_uuid,
                branch_id: branch_uuid,
                parent_message_id: parent_uuid,
                role: MessageRole::Assistant,
                created_at_ms: timestamp,
                parts: vec![MessagePart {
                    id: part_id,
                    kind: MessagePartKind::Text,
                    content,
                    metadata: part_metadata,
                }],
            },
            GenerationEvent {
                generation_id: id,
                sequence_number: sequence,
                event_type: event_type.to_string(),
                timestamp_ms: timestamp,
                payload,
            },
        ))
    }

    pub async fn recover_interrupted_generations(&self) -> Result<Vec<GenerationEvent>> {
        let timestamp = now_ms();
        let payload = json!({
            "code": "core_restarted",
            "message": "generation was interrupted by a desktop-core restart"
        });
        let payload_json = serde_json::to_string(&payload)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
        let mut transaction = self.begin_write().await?;
        let rows = sqlx::query(
            "SELECT id, last_sequence
             FROM generation_runs
             WHERE status IN ('queued', 'running', 'waiting_for_approval')
             ORDER BY created_at_ms, id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let generation_id: String = row.try_get("id")?;
            let generation_uuid = parse_generation_uuid("generation_run.id", &generation_id)?;
            let last_sequence: i64 = row.try_get("last_sequence")?;
            let sequence =
                last_sequence
                    .checked_add(1)
                    .ok_or_else(|| StorageError::CorruptData {
                        field: "generation_run.last_sequence",
                        message: "sequence overflow".to_string(),
                    })?;
            let update = sqlx::query(
                "UPDATE generation_runs
                 SET status = 'failed', last_sequence = ?, updated_at_ms = ?
                 WHERE id = ? AND status IN ('queued', 'running', 'waiting_for_approval')",
            )
            .bind(sequence)
            .bind(timestamp)
            .bind(&generation_id)
            .execute(&mut *transaction)
            .await?;
            if update.rows_affected() != 1 {
                continue;
            }
            insert_generation_event(
                &mut transaction,
                generation_uuid,
                sequence,
                "failed",
                timestamp,
                &payload_json,
            )
            .await?;
            events.push(GenerationEvent {
                generation_id: generation_uuid,
                sequence_number: sequence,
                event_type: "failed".to_string(),
                timestamp_ms: timestamp,
                payload: payload.clone(),
            });
        }
        transaction.commit().await?;
        Ok(events)
    }

    pub async fn append_generation_event(
        &self,
        generation_id: Uuid,
        event_type: &str,
        payload: Value,
    ) -> Result<GenerationEvent> {
        let event_type = validate_event(event_type, &payload)?;
        let payload_json = serde_json::to_string(&payload)
            .map_err(|error| StorageError::InvalidInput(error.to_string()))?;

        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "UPDATE generation_runs
             SET last_sequence = last_sequence + 1, updated_at_ms = ?
             WHERE id = ?
             RETURNING last_sequence",
        )
        .bind(timestamp)
        .bind(generation_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StorageError::NotFound {
            entity: "generation",
        })?;
        sqlx::query(
            "INSERT INTO generation_events (
                generation_id, sequence_number, event_type, timestamp_ms, payload_json
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(generation_id.to_string())
        .bind(sequence)
        .bind(event_type)
        .bind(timestamp)
        .bind(payload_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(GenerationEvent {
            generation_id,
            sequence_number: sequence,
            event_type: event_type.to_string(),
            timestamp_ms: timestamp,
            payload,
        })
    }

    pub async fn list_generation_events(
        &self,
        generation_id: Uuid,
        after_sequence: i64,
        limit: u32,
    ) -> Result<Vec<GenerationEvent>> {
        if after_sequence < 0 {
            return Err(StorageError::InvalidInput(
                "event sequence cannot be negative".to_string(),
            ));
        }
        if !(1..=10_000).contains(&limit) {
            return Err(StorageError::InvalidInput(
                "event page size must be between 1 and 10000".to_string(),
            ));
        }
        let rows = sqlx::query_as::<_, GenerationEventRow>(
            "SELECT generation_id, sequence_number, event_type, timestamp_ms, payload_json
             FROM generation_events
             WHERE generation_id = ? AND sequence_number > ?
             ORDER BY sequence_number
             LIMIT ?",
        )
        .bind(generation_id.to_string())
        .bind(after_sequence)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(GenerationEvent::try_from).collect()
    }

    pub async fn generation_performance_samples(
        &self,
        limit: u32,
    ) -> Result<Vec<GenerationPerformanceSample>> {
        if !(1..=200).contains(&limit) {
            return Err(StorageError::InvalidInput(
                "performance sample limit must be between 1 and 200".to_string(),
            ));
        }
        let runs = sqlx::query_as::<_, GenerationRunRow>(
            "SELECT id, conversation_id, branch_id, input_message_id, output_message_id,
                    client_request_id, model_alias, provider, resolved_model_id,
                    load_profile_json, status, last_sequence, created_at_ms, updated_at_ms
             FROM generation_runs
             ORDER BY created_at_ms DESC, id DESC
             LIMIT ?",
        )
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(GenerationRun::try_from)
        .collect::<Result<Vec<_>>>()?;
        let mut samples = Vec::with_capacity(runs.len());
        for run in runs {
            let events = self.list_generation_events(run.id, 0, 10_000).await?;
            samples.push(performance_sample(run, &events));
        }
        Ok(samples)
    }
}

fn merge_part_metadata(mut metadata: Option<Value>, unfinished: bool) -> Result<Option<Value>> {
    if !unfinished {
        return Ok(metadata);
    }
    match &mut metadata {
        Some(Value::Object(object)) => {
            object.insert("unfinished".to_string(), Value::Bool(true));
            Ok(metadata)
        }
        Some(_) => Err(StorageError::InvalidInput(
            "assistant message metadata must be a JSON object".to_string(),
        )),
        None => Ok(Some(json!({ "unfinished": true }))),
    }
}

fn performance_sample(
    run: GenerationRun,
    events: &[GenerationEvent],
) -> GenerationPerformanceSample {
    let mut started_at_ms = None;
    let mut first_output_at_ms = None;
    let mut finished_at_ms = None;
    let mut input_characters = None;
    let mut output_characters = 0_i64;
    let mut prompt_tokens = None;
    let mut completion_tokens = None;
    let mut total_tokens = None;

    for event in events {
        match event.event_type.as_str() {
            "started" => started_at_ms.get_or_insert(event.timestamp_ms),
            "request_metrics" => {
                input_characters = event.payload.get("inputCharacters").and_then(Value::as_i64);
                continue;
            }
            "delta" | "reasoning_delta" => {
                first_output_at_ms.get_or_insert(event.timestamp_ms);
                if let Some(text) = event.payload.get("text").and_then(Value::as_str) {
                    output_characters = output_characters
                        .saturating_add(i64::try_from(text.chars().count()).unwrap_or(i64::MAX));
                }
                continue;
            }
            "usage" => {
                prompt_tokens = token_count(&event.payload, "prompt_tokens", "input_tokens");
                completion_tokens =
                    token_count(&event.payload, "completion_tokens", "output_tokens");
                total_tokens = event.payload.get("total_tokens").and_then(Value::as_i64);
                continue;
            }
            "completed" | "cancelled" | "failed" => {
                finished_at_ms = Some(event.timestamp_ms);
                continue;
            }
            _ => continue,
        };
    }

    let queue_duration_ms = duration_between(Some(run.created_at_ms), started_at_ms);
    let time_to_first_output_ms = duration_between(started_at_ms, first_output_at_ms);
    let total_duration_ms = duration_between(started_at_ms, finished_at_ms);
    let decode_tokens_per_second = match (
        completion_tokens,
        duration_between(first_output_at_ms, finished_at_ms),
    ) {
        (Some(tokens), Some(milliseconds)) if tokens >= 0 && milliseconds > 0 => {
            Some(tokens as f64 * 1000.0 / milliseconds as f64)
        }
        _ => None,
    };

    GenerationPerformanceSample {
        generation_id: run.id,
        provider: run.provider,
        model_id: run.model_id,
        load_profile: run.load_profile,
        status: run.status,
        queued_at_ms: run.created_at_ms,
        started_at_ms,
        first_output_at_ms,
        finished_at_ms,
        queue_duration_ms,
        time_to_first_output_ms,
        total_duration_ms,
        input_characters,
        output_characters,
        prompt_tokens,
        completion_tokens,
        total_tokens,
        decode_tokens_per_second,
    }
}

fn duration_between(start: Option<i64>, end: Option<i64>) -> Option<i64> {
    end.zip(start)
        .and_then(|(end, start)| end.checked_sub(start))
        .filter(|duration| *duration >= 0)
}

fn token_count(payload: &Value, primary: &str, alternate: &str) -> Option<i64> {
    payload
        .get(primary)
        .or_else(|| payload.get(alternate))
        .and_then(Value::as_i64)
}

fn validate_event<'a>(event_type: &'a str, payload: &Value) -> Result<&'a str> {
    let event_type = event_type.trim();
    if event_type.is_empty() || event_type.len() > 100 {
        return Err(StorageError::InvalidInput(
            "event type must contain between 1 and 100 bytes".to_string(),
        ));
    }
    let payload_bytes = serde_json::to_vec(payload)
        .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
    if payload_bytes.len() > 4 * 1024 * 1024 {
        return Err(StorageError::InvalidInput(
            "generation event payload cannot exceed 4 MiB".to_string(),
        ));
    }
    Ok(event_type)
}

async fn insert_generation_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    generation_id: Uuid,
    sequence: i64,
    event_type: &str,
    timestamp: i64,
    payload_json: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO generation_events (
            generation_id, sequence_number, event_type, timestamp_ms, payload_json
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(generation_id.to_string())
    .bind(sequence)
    .bind(event_type)
    .bind(timestamp)
    .bind(payload_json)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn parse_generation_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}
