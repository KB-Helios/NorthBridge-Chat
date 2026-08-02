use std::str::FromStr;

use northbridge_domain::{
    AiProvider, Conversation, ConversationBranch, GenerationEvent, GenerationRun, GenerationStatus,
    Message, MessagePart, MessagePartKind, MessageRole, ModelLoadProfile,
};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

use crate::{Result, StorageError};

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn parse_optional_uuid(field: &'static str, value: Option<String>) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(field, &value)).transpose()
}

#[derive(Debug, FromRow)]
pub(crate) struct ConversationRow {
    id: String,
    workspace_id: String,
    default_branch_id: String,
    title: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl TryFrom<ConversationRow> for Conversation {
    type Error = StorageError;

    fn try_from(row: ConversationRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid("conversation.id", &row.id)?,
            workspace_id: parse_uuid("conversation.workspace_id", &row.workspace_id)?,
            default_branch_id: parse_uuid(
                "conversation.default_branch_id",
                &row.default_branch_id,
            )?,
            title: row.title,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct ConversationBranchRow {
    id: String,
    conversation_id: String,
    parent_branch_id: Option<String>,
    forked_from_message_id: Option<String>,
    head_message_id: Option<String>,
    created_at_ms: i64,
}

impl TryFrom<ConversationBranchRow> for ConversationBranch {
    type Error = StorageError;

    fn try_from(row: ConversationBranchRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid("conversation_branch.id", &row.id)?,
            conversation_id: parse_uuid(
                "conversation_branch.conversation_id",
                &row.conversation_id,
            )?,
            parent_branch_id: parse_optional_uuid(
                "conversation_branch.parent_branch_id",
                row.parent_branch_id,
            )?,
            forked_from_message_id: parse_optional_uuid(
                "conversation_branch.forked_from_message_id",
                row.forked_from_message_id,
            )?,
            head_message_id: parse_optional_uuid(
                "conversation_branch.head_message_id",
                row.head_message_id,
            )?,
            created_at_ms: row.created_at_ms,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct MessageRow {
    pub(crate) id: String,
    conversation_id: String,
    branch_id: String,
    parent_message_id: Option<String>,
    role: String,
    created_at_ms: i64,
}

impl MessageRow {
    pub(crate) fn into_message(self, parts: Vec<MessagePart>) -> Result<Message> {
        Ok(Message {
            id: parse_uuid("message.id", &self.id)?,
            conversation_id: parse_uuid("message.conversation_id", &self.conversation_id)?,
            branch_id: parse_uuid("message.branch_id", &self.branch_id)?,
            parent_message_id: parse_optional_uuid(
                "message.parent_message_id",
                self.parent_message_id,
            )?,
            role: MessageRole::from_str(&self.role).map_err(|message| {
                StorageError::CorruptData {
                    field: "message.role",
                    message,
                }
            })?,
            created_at_ms: self.created_at_ms,
            parts,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct MessagePartRow {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) content: String,
    pub(crate) metadata_json: Option<String>,
}

impl TryFrom<MessagePartRow> for MessagePart {
    type Error = StorageError;

    fn try_from(row: MessagePartRow) -> Result<Self> {
        let metadata = row
            .metadata_json
            .map(|value| {
                serde_json::from_str::<Value>(&value).map_err(|error| StorageError::CorruptData {
                    field: "message_part.metadata_json",
                    message: error.to_string(),
                })
            })
            .transpose()?;
        Ok(Self {
            id: parse_uuid("message_part.id", &row.id)?,
            kind: MessagePartKind::from_str(&row.kind).map_err(|message| {
                StorageError::CorruptData {
                    field: "message_part.kind",
                    message,
                }
            })?,
            content: row.content,
            metadata,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct GenerationRunRow {
    id: String,
    conversation_id: String,
    branch_id: String,
    input_message_id: Option<String>,
    output_message_id: Option<String>,
    client_request_id: Option<String>,
    model_alias: String,
    provider: Option<String>,
    resolved_model_id: Option<String>,
    load_profile_json: Option<String>,
    status: String,
    last_sequence: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl TryFrom<GenerationRunRow> for GenerationRun {
    type Error = StorageError;

    fn try_from(row: GenerationRunRow) -> Result<Self> {
        Ok(Self {
            id: parse_uuid("generation_run.id", &row.id)?,
            conversation_id: parse_uuid("generation_run.conversation_id", &row.conversation_id)?,
            branch_id: parse_uuid("generation_run.branch_id", &row.branch_id)?,
            input_message_id: parse_optional_uuid(
                "generation_run.input_message_id",
                row.input_message_id,
            )?,
            output_message_id: parse_optional_uuid(
                "generation_run.output_message_id",
                row.output_message_id,
            )?,
            client_request_id: row.client_request_id,
            model_alias: row.model_alias,
            provider: row
                .provider
                .map(|provider| {
                    AiProvider::from_str(&provider).map_err(|message| StorageError::CorruptData {
                        field: "generation_run.provider",
                        message,
                    })
                })
                .transpose()?,
            model_id: row.resolved_model_id,
            load_profile: row
                .load_profile_json
                .map(|profile| {
                    serde_json::from_str::<ModelLoadProfile>(&profile).map_err(|error| {
                        StorageError::CorruptData {
                            field: "generation_run.load_profile_json",
                            message: error.to_string(),
                        }
                    })
                })
                .transpose()?,
            status: GenerationStatus::from_str(&row.status).map_err(|message| {
                StorageError::CorruptData {
                    field: "generation_run.status",
                    message,
                }
            })?,
            last_sequence: row.last_sequence,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct GenerationEventRow {
    generation_id: String,
    sequence_number: i64,
    event_type: String,
    timestamp_ms: i64,
    payload_json: String,
}

impl TryFrom<GenerationEventRow> for GenerationEvent {
    type Error = StorageError;

    fn try_from(row: GenerationEventRow) -> Result<Self> {
        let payload =
            serde_json::from_str(&row.payload_json).map_err(|error| StorageError::CorruptData {
                field: "generation_event.payload_json",
                message: error.to_string(),
            })?;
        Ok(Self {
            generation_id: parse_uuid("generation_event.generation_id", &row.generation_id)?,
            sequence_number: row.sequence_number,
            event_type: row.event_type,
            timestamp_ms: row.timestamp_ms,
            payload,
        })
    }
}
