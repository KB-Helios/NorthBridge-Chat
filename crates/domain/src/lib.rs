use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

mod platform;

pub use platform::*;

pub const DEFAULT_WORKSPACE_ID: &str = "00000000-0000-0000-0000-000000000001";
pub const DESKTOP_SETTINGS_SCOPE: &str = "desktop";
pub const MODEL_SELECTION_SETTING_KEY: &str = "ai.model_selection";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AiProvider {
    LocalMistralRs,
}

impl AiProvider {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalMistralRs => "local_mistral_rs",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelQuantization {
    #[default]
    None,
    #[serde(rename = "q2k")]
    Q2K,
    #[serde(rename = "q3k")]
    Q3K,
    #[serde(rename = "q4k")]
    Q4K,
    #[serde(rename = "q5k")]
    Q5K,
    #[serde(rename = "q6k")]
    Q6K,
    #[serde(rename = "q8_0")]
    Q8Zero,
}

impl ModelQuantization {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Q2K => "q2k",
            Self::Q3K => "q3k",
            Self::Q4K => "q4k",
            Self::Q5K => "q5k",
            Self::Q6K => "q6k",
            Self::Q8Zero => "q8_0",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelTuningProfile {
    Quality,
    #[default]
    Balanced,
    Fast,
}

impl ModelTuningProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quality => "quality",
            Self::Balanced => "balanced",
            Self::Fast => "fast",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelLoadProfile {
    #[serde(default)]
    pub quantization: ModelQuantization,
    #[serde(default = "default_model_context_window")]
    pub max_sequence_length: u32,
    #[serde(default = "default_model_batch_size")]
    pub max_batch_size: u32,
    #[serde(default = "default_force_cpu")]
    pub force_cpu: bool,
}

impl Default for ModelLoadProfile {
    fn default() -> Self {
        Self {
            quantization: ModelQuantization::None,
            max_sequence_length: default_model_context_window(),
            max_batch_size: default_model_batch_size(),
            force_cpu: default_force_cpu(),
        }
    }
}

impl ModelLoadProfile {
    pub fn validate(&self) -> Result<(), String> {
        if !(128..=1_048_576).contains(&self.max_sequence_length) {
            return Err("maximum sequence length must be between 128 and 1,048,576".to_string());
        }
        if !(1..=1024).contains(&self.max_batch_size) {
            return Err("maximum batch size must be between 1 and 1,024".to_string());
        }
        Ok(())
    }
}

const fn default_model_context_window() -> u32 {
    8192
}

const fn default_model_batch_size() -> u32 {
    1
}

const fn default_force_cpu() -> bool {
    true
}

impl FromStr for AiProvider {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local_mistral_rs" => Ok(Self::LocalMistralRs),
            _ => Err(format!("unsupported AI provider: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelCatalogEntry {
    pub provider: AiProvider,
    pub model_id: String,
    pub model_alias: String,
    pub display_name: String,
    pub description: String,
    pub context_window: u32,
    pub recommended: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelection {
    pub provider: AiProvider,
    pub model_id: String,
    pub model_alias: String,
    #[serde(default)]
    pub load_profile: ModelLoadProfile,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManagedModelSource {
    HuggingFace,
    Imported,
}

impl ManagedModelSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HuggingFace => "hugging_face",
            Self::Imported => "imported",
        }
    }
}

impl FromStr for ManagedModelSource {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "hugging_face" => Ok(Self::HuggingFace),
            "imported" => Ok(Self::Imported),
            _ => Err(format!("unsupported managed model source: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManagedModelStatus {
    Queued,
    Downloading,
    Installed,
    Imported,
    Failed,
    Cancelled,
}

impl ManagedModelStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Installed => "installed",
            Self::Imported => "imported",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Installed | Self::Imported)
    }
}

impl FromStr for ManagedModelStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "downloading" => Ok(Self::Downloading),
            "installed" => Ok(Self::Installed),
            "imported" => Ok(Self::Imported),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported managed model status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedModel {
    pub id: Uuid,
    pub model_id: String,
    pub display_name: String,
    pub source: ManagedModelSource,
    pub source_reference: String,
    pub revision: Option<String>,
    pub local_path: String,
    pub status: ManagedModelStatus,
    pub bytes_downloaded: u64,
    pub total_bytes: Option<u64>,
    pub file_count: Option<u64>,
    pub manifest_sha256: Option<String>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub id: Uuid,
    pub name: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Conversation {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub default_branch_id: Uuid,
    pub title: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConversationCursor {
    pub updated_at_ms: i64,
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConversationPage {
    pub items: Vec<Conversation>,
    pub next_cursor: Option<ConversationCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConversationBranch {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub parent_branch_id: Option<Uuid>,
    pub forked_from_message_id: Option<Uuid>,
    pub head_message_id: Option<Uuid>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    System,
    User,
    Assistant,
    Tool,
}

impl MessageRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

impl fmt::Display for MessageRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MessageRole {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "system" => Ok(Self::System),
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            "tool" => Ok(Self::Tool),
            _ => Err(format!("unsupported message role: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessagePartKind {
    Text,
    Reasoning,
    Image,
    Audio,
    File,
    ToolCall,
    ToolResult,
    Json,
}

impl MessagePartKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Reasoning => "reasoning",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::File => "file",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::Json => "json",
        }
    }
}

impl FromStr for MessagePartKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "text" => Ok(Self::Text),
            "reasoning" => Ok(Self::Reasoning),
            "image" => Ok(Self::Image),
            "audio" => Ok(Self::Audio),
            "file" => Ok(Self::File),
            "tool_call" => Ok(Self::ToolCall),
            "tool_result" => Ok(Self::ToolResult),
            "json" => Ok(Self::Json),
            _ => Err(format!("unsupported message part kind: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MessagePart {
    pub id: Uuid,
    pub kind: MessagePartKind,
    pub content: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub branch_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub role: MessageRole,
    pub created_at_ms: i64,
    pub parts: Vec<MessagePart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NewMessagePart {
    pub kind: MessagePartKind,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NewMessage {
    pub conversation_id: Uuid,
    pub branch_id: Uuid,
    pub parent_message_id: Option<Uuid>,
    pub role: MessageRole,
    pub parts: Vec<NewMessagePart>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GenerationStatus {
    Queued,
    Running,
    WaitingForApproval,
    Completed,
    Failed,
    Cancelled,
}

impl GenerationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl FromStr for GenerationStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting_for_approval" => Ok(Self::WaitingForApproval),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported generation status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationRun {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub branch_id: Uuid,
    pub input_message_id: Option<Uuid>,
    pub output_message_id: Option<Uuid>,
    pub client_request_id: Option<String>,
    pub model_alias: String,
    pub provider: Option<AiProvider>,
    pub model_id: Option<String>,
    pub load_profile: Option<ModelLoadProfile>,
    pub status: GenerationStatus,
    pub last_sequence: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SplitChatLane {
    pub split_chat_id: Uuid,
    pub ordinal: u32,
    pub branch_id: Uuid,
    pub generation: GenerationRun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SplitChatRun {
    pub id: Uuid,
    pub conversation_id: Uuid,
    pub source_branch_id: Uuid,
    pub input_message_id: Uuid,
    pub client_request_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub lanes: Vec<SplitChatLane>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationEvent {
    pub generation_id: Uuid,
    pub sequence_number: i64,
    pub event_type: String,
    pub timestamp_ms: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationPerformanceSample {
    pub generation_id: Uuid,
    pub provider: Option<AiProvider>,
    pub model_id: Option<String>,
    pub load_profile: Option<ModelLoadProfile>,
    pub status: GenerationStatus,
    pub queued_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub first_output_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub queue_duration_ms: Option<i64>,
    pub time_to_first_output_ms: Option<i64>,
    pub total_duration_ms: Option<i64>,
    pub input_characters: Option<i64>,
    pub output_characters: i64,
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    pub decode_tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GenerationPerformanceDashboard {
    pub generated_at_ms: i64,
    pub completed_runs: usize,
    pub failed_runs: usize,
    pub cancelled_runs: usize,
    pub time_to_first_output_p50_ms: Option<i64>,
    pub time_to_first_output_p95_ms: Option<i64>,
    pub total_duration_p50_ms: Option<i64>,
    pub total_duration_p95_ms: Option<i64>,
    pub decode_tokens_per_second_p50: Option<f64>,
    pub samples: Vec<GenerationPerformanceSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    pub message_id: Uuid,
    pub conversation_id: Uuid,
    pub part_id: Uuid,
    pub content: String,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeSourceKind {
    File,
    Folder,
    Obsidian,
}

impl KnowledgeSourceKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Folder => "folder",
            Self::Obsidian => "obsidian",
        }
    }
}

impl FromStr for KnowledgeSourceKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "file" => Ok(Self::File),
            "folder" => Ok(Self::Folder),
            "obsidian" => Ok(Self::Obsidian),
            _ => Err(format!("unsupported knowledge source kind: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeDocumentStatus {
    Indexing,
    Ready,
    Failed,
}

impl KnowledgeDocumentStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Indexing => "indexing",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for KnowledgeDocumentStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "indexing" => Ok(Self::Indexing),
            "ready" => Ok(Self::Ready),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("unsupported knowledge document status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl IngestionJobStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for IngestionJobStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported ingestion job status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeWatchKind {
    Folder,
    Obsidian,
}

impl KnowledgeWatchKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Folder => "folder",
            Self::Obsidian => "obsidian",
        }
    }

    #[must_use]
    pub const fn source_kind(self) -> KnowledgeSourceKind {
        match self {
            Self::Folder => KnowledgeSourceKind::Folder,
            Self::Obsidian => KnowledgeSourceKind::Obsidian,
        }
    }
}

impl FromStr for KnowledgeWatchKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "folder" => Ok(Self::Folder),
            "obsidian" => Ok(Self::Obsidian),
            _ => Err(format!("unsupported knowledge watch kind: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeStack {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub embedding_model: String,
    pub embedding_dimensions: u32,
    pub chunk_size: u32,
    pub chunk_overlap: u32,
    pub chunker_hash: String,
    pub index_revision: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeDocument {
    pub id: Uuid,
    pub stack_id: Uuid,
    pub workspace_id: Uuid,
    pub source_kind: KnowledgeSourceKind,
    pub source_uri: String,
    pub title: String,
    pub mime_type: String,
    pub content_sha256: String,
    pub blob_sha256: String,
    pub byte_size: u64,
    pub status: KnowledgeDocumentStatus,
    pub last_error: Option<String>,
    pub source_modified_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DocumentChunk {
    pub dense_id: u64,
    pub id: Uuid,
    pub document_id: Uuid,
    pub stack_id: Uuid,
    pub workspace_id: Uuid,
    pub ordinal: u32,
    pub content: String,
    pub content_sha256: String,
    pub character_count: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub metadata: Value,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IngestionJob {
    pub id: Uuid,
    pub stack_id: Uuid,
    pub source_uri: String,
    pub source_kind: KnowledgeSourceKind,
    pub status: IngestionJobStatus,
    pub progress_current: u64,
    pub progress_total: Option<u64>,
    pub document_id: Option<Uuid>,
    pub cancel_requested: bool,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeWatchRoot {
    pub id: Uuid,
    pub stack_id: Uuid,
    pub workspace_id: Uuid,
    pub kind: KnowledgeWatchKind,
    pub path: String,
    pub recursive: bool,
    pub enabled: bool,
    pub last_scan_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeSearchHit {
    pub citation: String,
    pub dense_id: u64,
    pub chunk_id: Uuid,
    pub document_id: Uuid,
    pub stack_id: Uuid,
    pub title: String,
    pub source_uri: String,
    pub snippet: String,
    pub start_offset: u64,
    pub end_offset: u64,
    pub lexical_rank: Option<u32>,
    pub dense_rank: Option<u32>,
    pub fused_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeSearchResult {
    pub trace_id: Uuid,
    pub query: String,
    pub stack_ids: Vec<Uuid>,
    pub embedding_models: Vec<String>,
    pub lexical_candidate_count: u32,
    pub dense_candidate_count: u32,
    pub elapsed_ms: u64,
    pub hits: Vec<KnowledgeSearchHit>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolPolicyMode {
    Manual,
    #[default]
    SideEffects,
    Auto,
}

impl ToolPolicyMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::SideEffects => "side_effects",
            Self::Auto => "auto",
        }
    }
}

impl FromStr for ToolPolicyMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "manual" => Ok(Self::Manual),
            "side_effects" => Ok(Self::SideEffects),
            "auto" => Ok(Self::Auto),
            _ => Err(format!("unsupported tool policy mode: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRunStatus {
    Queued,
    Running,
    WaitingForApproval,
    Suspended,
    Completed,
    Failed,
    Cancelled,
}

impl AgentRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Suspended => "suspended",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl FromStr for AgentRunStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting_for_approval" => Ok(Self::WaitingForApproval),
            "suspended" => Ok(Self::Suspended),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported agent run status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpTransport {
    Stdio,
    StreamableHttp,
    Sse,
    Openapi,
}

impl McpTransport {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::StreamableHttp => "streamable_http",
            Self::Sse => "sse",
            Self::Openapi => "openapi",
        }
    }
}

impl FromStr for McpTransport {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "stdio" => Ok(Self::Stdio),
            "streamable_http" => Ok(Self::StreamableHttp),
            "sse" => Ok(Self::Sse),
            "openapi" => Ok(Self::Openapi),
            _ => Err(format!("unsupported MCP transport: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    PendingApproval,
    Approved,
    Running,
    Completed,
    Failed,
    Denied,
    UnknownOutcome,
    Cancelled,
}

impl ToolCallStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingApproval => "pending_approval",
            Self::Approved => "approved",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Denied => "denied",
            Self::UnknownOutcome => "unknown_outcome",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for ToolCallStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending_approval" => Ok(Self::PendingApproval),
            "approved" => Ok(Self::Approved),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "denied" => Ok(Self::Denied),
            "unknown_outcome" => Ok(Self::UnknownOutcome),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported tool call status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
    Cancelled,
}

impl ApprovalStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for ApprovalStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "denied" => Ok(Self::Denied),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported approval status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Persona {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub system_prompt: String,
    pub version: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Skill {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub permission_manifest: Value,
    pub version: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentProfile {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub persona_id: Option<Uuid>,
    pub name: String,
    pub description: String,
    pub model_alias: String,
    pub instructions: String,
    pub policy_mode: ToolPolicyMode,
    pub max_turns: u32,
    pub version: u32,
    pub skill_ids: Vec<Uuid>,
    pub knowledge_stack_ids: Vec<Uuid>,
    pub tool_allowlist: Vec<AgentToolPermission>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolPermission {
    pub server_id: Uuid,
    pub tool_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct McpServer {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub transport: McpTransport,
    pub endpoint: Option<String>,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub environment_references: Value,
    pub openapi_schema: Option<String>,
    pub enabled: bool,
    pub config_revision: u64,
    pub last_error: Option<String>,
    pub last_discovered_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct McpToolSnapshot {
    pub server_id: Uuid,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub annotations: Value,
    pub read_only: bool,
    pub destructive: bool,
    pub discovered_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRun {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub agent_id: Uuid,
    pub conversation_id: Option<Uuid>,
    pub status: AgentRunStatus,
    pub prompt: String,
    pub rig_version: String,
    pub checkpoint: Value,
    pub checkpoint_generation: u64,
    pub current_turn: u32,
    pub output: Option<String>,
    pub cancel_requested: bool,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRunEvent {
    pub run_id: Uuid,
    pub sequence_number: u64,
    pub event_type: String,
    pub payload: Value,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolCall {
    pub id: Uuid,
    pub run_id: Uuid,
    pub server_id: Option<Uuid>,
    pub rig_tool_call_id: String,
    pub tool_name: String,
    pub arguments: Value,
    pub status: ToolCallStatus,
    pub read_only: bool,
    pub destructive: bool,
    pub result: Option<Value>,
    pub last_error: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentApproval {
    pub id: Uuid,
    pub run_id: Uuid,
    pub tool_call_id: Uuid,
    pub status: ApprovalStatus,
    pub reason: String,
    pub decision_note: Option<String>,
    pub requested_at_ms: i64,
    pub decided_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteAgent {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub upstream_url: String,
    pub gateway_path: String,
    pub auth_environment_key: Option<String>,
    pub enabled: bool,
    pub card: Option<Value>,
    pub card_etag: Option<String>,
    pub last_discovered_at_ms: Option<i64>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Crew {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub coordinator_agent_id: Uuid,
    pub members: Vec<CrewMember>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CrewMember {
    pub ordinal: u32,
    pub role: String,
    pub local_agent_id: Option<Uuid>,
    pub remote_agent_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CrewRunStatus {
    Queued,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
}

impl CrewRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl FromStr for CrewRunStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "suspended" => Ok(Self::Suspended),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("unsupported crew run status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CrewRunMember {
    pub ordinal: u32,
    pub role: String,
    pub local_agent_id: Option<Uuid>,
    pub remote_agent_id: Option<Uuid>,
    pub agent_run_id: Option<Uuid>,
    pub remote_task_id: Option<String>,
    pub remote_context_id: Option<String>,
    pub status: String,
    pub output: Option<String>,
    pub response: Option<Value>,
    pub last_error: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CrewRun {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub crew_id: Uuid,
    pub prompt: String,
    pub status: CrewRunStatus,
    pub coordinator_run_id: Option<Uuid>,
    pub output: Option<String>,
    pub last_error: Option<String>,
    pub members: Vec<CrewRunMember>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowSubscription {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub conversation_id: Uuid,
    pub agent_id: Uuid,
    pub enabled: bool,
    pub last_message_id: Option<Uuid>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowSuggestionStatus {
    Processing,
    Unread,
    Accepted,
    Dismissed,
    Failed,
}

impl ShadowSuggestionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Processing => "processing",
            Self::Unread => "unread",
            Self::Accepted => "accepted",
            Self::Dismissed => "dismissed",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for ShadowSuggestionStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "processing" => Ok(Self::Processing),
            "unread" => Ok(Self::Unread),
            "accepted" => Ok(Self::Accepted),
            "dismissed" => Ok(Self::Dismissed),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("unsupported shadow suggestion status: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShadowSuggestion {
    pub id: Uuid,
    pub subscription_id: Uuid,
    pub source_message_id: Uuid,
    pub run_id: Option<Uuid>,
    pub content: String,
    pub status: ShadowSuggestionStatus,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HealthStatus {
    pub status: String,
    pub schema_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReadyEvent {
    pub event: String,
    pub address: String,
    pub pid: u32,
    pub schema_version: i64,
}

#[cfg(test)]
mod tests {
    use super::{ModelLoadProfile, ModelQuantization, ModelSelection};

    #[test]
    fn old_model_selections_receive_the_safe_default_load_profile() {
        let selection: ModelSelection = serde_json::from_str(
            r#"{
                "provider":"local_mistral_rs",
                "modelId":"org/model",
                "modelAlias":"fast-local"
            }"#,
        )
        .unwrap();
        assert_eq!(selection.load_profile, ModelLoadProfile::default());
    }

    #[test]
    fn quantization_uses_stable_cli_compatible_names() {
        assert_eq!(
            serde_json::to_string(&ModelQuantization::Q8Zero).unwrap(),
            "\"q8_0\""
        );
        assert_eq!(
            serde_json::to_string(&ModelQuantization::Q4K).unwrap(),
            "\"q4k\""
        );
    }
}
