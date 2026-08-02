use std::{collections::HashMap, str::FromStr};

use northbridge_domain::{
    Conversation, ConversationBranch, ConversationCursor, ConversationPage, Message, MessagePart,
    NewMessage,
};
use sqlx::{QueryBuilder, Row, Sqlite};
use uuid::Uuid;

use crate::{
    Database, Result, StorageError, now_ms,
    rows::{ConversationBranchRow, ConversationRow, MessagePartRow, MessageRow},
};

const CONVERSATION_COLUMNS: &str =
    "id, workspace_id, default_branch_id, title, created_at_ms, updated_at_ms";

impl Database {
    pub async fn create_conversation(
        &self,
        workspace_id: Uuid,
        title: &str,
    ) -> Result<Conversation> {
        let title = title.trim();
        if title.chars().count() > 500 {
            return Err(StorageError::InvalidInput(
                "conversation titles cannot exceed 500 characters".to_string(),
            ));
        }

        let conversation_id = Uuid::now_v7();
        let branch_id = Uuid::now_v7();
        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO conversations (
                id, workspace_id, default_branch_id, title, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(conversation_id.to_string())
        .bind(workspace_id.to_string())
        .bind(branch_id.to_string())
        .bind(title)
        .bind(timestamp)
        .bind(timestamp)
        .execute(&mut *transaction)
        .await
        .map_err(map_constraint("workspace"))?;
        sqlx::query(
            "INSERT INTO conversation_branches (
                id, conversation_id, created_at_ms
             ) VALUES (?, ?, ?)",
        )
        .bind(branch_id.to_string())
        .bind(conversation_id.to_string())
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(Conversation {
            id: conversation_id,
            workspace_id,
            default_branch_id: branch_id,
            title: title.to_string(),
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
        })
    }

    pub async fn get_conversation(&self, id: Uuid) -> Result<Conversation> {
        let sql = format!(
            "SELECT {CONVERSATION_COLUMNS}
             FROM conversations
             WHERE id = ? AND deleted_at_ms IS NULL"
        );
        let row = sqlx::query_as::<_, ConversationRow>(&sql)
            .bind(id.to_string())
            .fetch_optional(self.pool())
            .await?
            .ok_or(StorageError::NotFound {
                entity: "conversation",
            })?;
        row.try_into()
    }

    pub async fn list_conversations(
        &self,
        workspace_id: Uuid,
        cursor: Option<&ConversationCursor>,
        limit: u32,
    ) -> Result<ConversationPage> {
        if !(1..=100).contains(&limit) {
            return Err(StorageError::InvalidInput(
                "conversation page size must be between 1 and 100".to_string(),
            ));
        }

        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {CONVERSATION_COLUMNS}
             FROM conversations
             WHERE workspace_id = "
        ));
        query.push_bind(workspace_id.to_string());
        query.push(" AND deleted_at_ms IS NULL");
        if let Some(cursor) = cursor {
            query.push(" AND (updated_at_ms < ");
            query.push_bind(cursor.updated_at_ms);
            query.push(" OR (updated_at_ms = ");
            query.push_bind(cursor.updated_at_ms);
            query.push(" AND id < ");
            query.push_bind(cursor.id.to_string());
            query.push("))");
        }
        query.push(" ORDER BY updated_at_ms DESC, id DESC LIMIT ");
        query.push_bind(i64::from(limit) + 1);

        let rows = query
            .build_query_as::<ConversationRow>()
            .fetch_all(self.pool())
            .await?;
        let has_more = rows.len() > limit as usize;
        let mut items = rows
            .into_iter()
            .take(limit as usize)
            .map(Conversation::try_from)
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = has_more.then(|| {
            let last = items
                .last()
                .expect("a page with a continuation always contains an item");
            ConversationCursor {
                updated_at_ms: last.updated_at_ms,
                id: last.id,
            }
        });
        items.shrink_to_fit();
        Ok(ConversationPage { items, next_cursor })
    }

    pub async fn soft_delete_conversation(&self, id: Uuid) -> Result<()> {
        let timestamp = now_ms();
        let result = sqlx::query(
            "UPDATE conversations
             SET deleted_at_ms = ?, updated_at_ms = ?
             WHERE id = ? AND deleted_at_ms IS NULL",
        )
        .bind(timestamp)
        .bind(timestamp)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "conversation",
            });
        }
        Ok(())
    }

    pub async fn list_conversation_branches(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<ConversationBranch>> {
        self.get_conversation(conversation_id).await?;
        sqlx::query_as::<_, ConversationBranchRow>(
            "SELECT id, conversation_id, parent_branch_id, forked_from_message_id,
                    head_message_id, created_at_ms
             FROM conversation_branches
             WHERE conversation_id = ?
             ORDER BY created_at_ms, id",
        )
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(ConversationBranch::try_from)
        .collect()
    }

    pub async fn fork_conversation_branch(
        &self,
        conversation_id: Uuid,
        forked_from_message_id: Uuid,
        activate: bool,
    ) -> Result<ConversationBranch> {
        let branch_id = Uuid::now_v7();
        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        let source_branch_id = sqlx::query_scalar::<_, String>(
            "SELECT messages.branch_id
             FROM messages
             JOIN conversations ON conversations.id = messages.conversation_id
             WHERE messages.id = ? AND messages.conversation_id = ?
               AND conversations.deleted_at_ms IS NULL",
        )
        .bind(forked_from_message_id.to_string())
        .bind(conversation_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StorageError::NotFound { entity: "message" })?;
        sqlx::query(
            "INSERT INTO conversation_branches (
                id, conversation_id, parent_branch_id, forked_from_message_id,
                head_message_id, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(branch_id.to_string())
        .bind(conversation_id.to_string())
        .bind(&source_branch_id)
        .bind(forked_from_message_id.to_string())
        .bind(forked_from_message_id.to_string())
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;
        if activate {
            sqlx::query(
                "UPDATE conversations
                 SET default_branch_id = ?, updated_at_ms = ?
                 WHERE id = ? AND deleted_at_ms IS NULL",
            )
            .bind(branch_id.to_string())
            .bind(timestamp)
            .bind(conversation_id.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(ConversationBranch {
            id: branch_id,
            conversation_id,
            parent_branch_id: Some(Uuid::parse_str(&source_branch_id).map_err(|error| {
                StorageError::CorruptData {
                    field: "message.branch_id",
                    message: error.to_string(),
                }
            })?),
            forked_from_message_id: Some(forked_from_message_id),
            head_message_id: Some(forked_from_message_id),
            created_at_ms: timestamp,
        })
    }

    pub async fn activate_conversation_branch(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
    ) -> Result<Conversation> {
        let timestamp = now_ms();
        let result = sqlx::query(
            "UPDATE conversations
             SET default_branch_id = ?, updated_at_ms = ?
             WHERE id = ? AND deleted_at_ms IS NULL
               AND EXISTS(
                   SELECT 1 FROM conversation_branches
                   WHERE conversation_branches.id = ?
                     AND conversation_branches.conversation_id = conversations.id
               )",
        )
        .bind(branch_id.to_string())
        .bind(timestamp)
        .bind(conversation_id.to_string())
        .bind(branch_id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound { entity: "branch" });
        }
        self.get_conversation(conversation_id).await
    }

    pub async fn append_message(&self, new_message: NewMessage) -> Result<Message> {
        validate_message(&new_message)?;
        let message_id = Uuid::now_v7();
        let timestamp = now_ms();
        let mut transaction = self.begin_write().await?;
        let branch = sqlx::query(
            "SELECT branches.conversation_id, branches.head_message_id
             FROM conversation_branches AS branches
             JOIN conversations ON conversations.id = branches.conversation_id
             WHERE branches.id = ? AND conversations.deleted_at_ms IS NULL",
        )
        .bind(new_message.branch_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(StorageError::NotFound { entity: "branch" })?;
        let stored_conversation_id: String = branch.try_get("conversation_id")?;
        let stored_head: Option<String> = branch.try_get("head_message_id")?;
        if stored_conversation_id != new_message.conversation_id.to_string() {
            return Err(StorageError::Conflict(
                "branch does not belong to the conversation".to_string(),
            ));
        }
        let expected_parent = stored_head
            .as_deref()
            .map(Uuid::from_str)
            .transpose()
            .map_err(|error| StorageError::CorruptData {
                field: "conversation_branch.head_message_id",
                message: error.to_string(),
            })?;
        if expected_parent != new_message.parent_message_id {
            return Err(StorageError::Conflict(
                "parent message is not the current branch head".to_string(),
            ));
        }

        sqlx::query(
            "INSERT INTO messages (
                id, conversation_id, branch_id, parent_message_id, role, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(message_id.to_string())
        .bind(new_message.conversation_id.to_string())
        .bind(new_message.branch_id.to_string())
        .bind(new_message.parent_message_id.map(|id| id.to_string()))
        .bind(new_message.role.as_str())
        .bind(timestamp)
        .execute(&mut *transaction)
        .await?;

        let mut parts = Vec::with_capacity(new_message.parts.len());
        for (ordinal, new_part) in new_message.parts.into_iter().enumerate() {
            let part_id = Uuid::now_v7();
            let metadata_json = new_part
                .metadata
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| StorageError::InvalidInput(error.to_string()))?;
            sqlx::query(
                "INSERT INTO message_parts (
                    id, message_id, ordinal, kind, content, metadata_json
                 ) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(part_id.to_string())
            .bind(message_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .bind(new_part.kind.as_str())
            .bind(&new_part.content)
            .bind(metadata_json)
            .execute(&mut *transaction)
            .await?;
            parts.push(MessagePart {
                id: part_id,
                kind: new_part.kind,
                content: new_part.content,
                metadata: new_part.metadata,
            });
        }
        sqlx::query("UPDATE conversation_branches SET head_message_id = ? WHERE id = ?")
            .bind(message_id.to_string())
            .bind(new_message.branch_id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE conversations SET updated_at_ms = ? WHERE id = ? AND deleted_at_ms IS NULL",
        )
        .bind(timestamp)
        .bind(new_message.conversation_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(Message {
            id: message_id,
            conversation_id: new_message.conversation_id,
            branch_id: new_message.branch_id,
            parent_message_id: new_message.parent_message_id,
            role: new_message.role,
            created_at_ms: timestamp,
            parts,
        })
    }

    pub async fn list_messages(
        &self,
        conversation_id: Uuid,
        branch_id: Uuid,
    ) -> Result<Vec<Message>> {
        let head_message_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT head_message_id FROM conversation_branches
             WHERE id = ? AND conversation_id = ?",
        )
        .bind(branch_id.to_string())
        .bind(conversation_id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "branch" })?;
        let Some(head_message_id) = head_message_id else {
            return Ok(Vec::new());
        };
        let message_rows = sqlx::query_as::<_, MessageRow>(
            "WITH RECURSIVE message_chain(
                id, conversation_id, branch_id, parent_message_id, role, created_at_ms, depth
             ) AS (
                SELECT id, conversation_id, branch_id, parent_message_id, role, created_at_ms, 0
                FROM messages WHERE id = ? AND conversation_id = ?
                UNION ALL
                SELECT parent.id, parent.conversation_id, parent.branch_id,
                       parent.parent_message_id, parent.role, parent.created_at_ms, chain.depth + 1
                FROM messages AS parent
                JOIN message_chain AS chain ON parent.id = chain.parent_message_id
                WHERE parent.conversation_id = ?
             )
             SELECT id, conversation_id, branch_id, parent_message_id, role, created_at_ms
             FROM message_chain
             ORDER BY depth DESC",
        )
        .bind(head_message_id)
        .bind(conversation_id.to_string())
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?;
        self.hydrate_message_rows(message_rows).await
    }

    pub async fn list_all_conversation_messages(
        &self,
        conversation_id: Uuid,
    ) -> Result<Vec<Message>> {
        self.get_conversation(conversation_id).await?;
        let message_rows = sqlx::query_as::<_, MessageRow>(
            "SELECT id, conversation_id, branch_id, parent_message_id, role, created_at_ms
             FROM messages
             WHERE conversation_id = ?
             ORDER BY created_at_ms, id",
        )
        .bind(conversation_id.to_string())
        .fetch_all(self.pool())
        .await?;
        self.hydrate_message_rows(message_rows).await
    }

    pub async fn message(&self, id: Uuid) -> Result<Message> {
        let row = sqlx::query_as::<_, MessageRow>(
            "SELECT id, conversation_id, branch_id, parent_message_id, role, created_at_ms
             FROM messages WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "message" })?;
        self.hydrate_message_rows(vec![row])
            .await?
            .pop()
            .ok_or(StorageError::NotFound { entity: "message" })
    }

    async fn hydrate_message_rows(&self, message_rows: Vec<MessageRow>) -> Result<Vec<Message>> {
        if message_rows.is_empty() {
            return Ok(Vec::new());
        }

        let message_ids = message_rows
            .iter()
            .map(|row| row.id.clone())
            .collect::<Vec<_>>();
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT message_id, id, kind, content, metadata_json
             FROM message_parts
             WHERE message_id IN (",
        );
        let mut separated = query.separated(", ");
        for id in &message_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(") ORDER BY message_id, ordinal");
        let part_rows = query.build().fetch_all(self.pool()).await?;
        let mut parts_by_message = HashMap::<String, Vec<MessagePart>>::new();
        for row in part_rows {
            let message_id: String = row.try_get("message_id")?;
            let part = MessagePartRow {
                id: row.try_get("id")?,
                kind: row.try_get("kind")?,
                content: row.try_get("content")?,
                metadata_json: row.try_get("metadata_json")?,
            }
            .try_into()?;
            parts_by_message.entry(message_id).or_default().push(part);
        }

        message_rows
            .into_iter()
            .map(|row| {
                let parts = parts_by_message.remove(&row.id).unwrap_or_default();
                row.into_message(parts)
            })
            .collect()
    }
}

fn validate_message(message: &NewMessage) -> Result<()> {
    if message.parts.is_empty() {
        return Err(StorageError::InvalidInput(
            "messages must contain at least one part".to_string(),
        ));
    }
    if message.parts.len() > 128 {
        return Err(StorageError::InvalidInput(
            "messages cannot contain more than 128 parts".to_string(),
        ));
    }
    let content_bytes = message
        .parts
        .iter()
        .try_fold(0_usize, |total, part| total.checked_add(part.content.len()))
        .ok_or_else(|| StorageError::InvalidInput("message content is too large".to_string()))?;
    if content_bytes > 4 * 1024 * 1024 {
        return Err(StorageError::InvalidInput(
            "message content cannot exceed 4 MiB".to_string(),
        ));
    }
    Ok(())
}

fn map_constraint(entity: &'static str) -> impl FnOnce(sqlx::Error) -> StorageError + use<> {
    move |error| {
        let is_foreign_key_violation = error.as_database_error().is_some_and(|database_error| {
            database_error.kind() == sqlx::error::ErrorKind::ForeignKeyViolation
        });
        if is_foreign_key_violation {
            StorageError::NotFound { entity }
        } else {
            StorageError::Database(error)
        }
    }
}
