use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{AgentProfile, McpServer, Persona, RemoteAgent, Skill};

#[must_use]
pub fn credential_environment_key(id: Uuid) -> String {
    format!("NORTHBRIDGE_CREDENTIAL_{}", id.simple()).to_ascii_uppercase()
}

macro_rules! string_enum {
    ($vis:vis enum $name:ident { $($variant:ident => $value:literal,)+ }) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        $vis enum $name { $($variant,)+ }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $value,)+ }
            }
        }

        impl std::str::FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(format!("unsupported {} value: {value}", stringify!($name))),
                }
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDefinition {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub version: u32,
    pub entry_node_id: String,
    pub nodes: Vec<WorkflowNode>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl WorkflowDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() || self.name.chars().count() > 120 {
            return Err("workflow name must contain between 1 and 120 characters".to_string());
        }
        validate_node_list(&self.nodes, &self.entry_node_id, 0)
    }
}

fn validate_node_list(nodes: &[WorkflowNode], entry: &str, depth: usize) -> Result<(), String> {
    if nodes.is_empty() || nodes.len() > 256 {
        return Err("a workflow graph must contain between 1 and 256 nodes".to_string());
    }
    if depth > 8 {
        return Err("workflow control-node nesting cannot exceed eight levels".to_string());
    }
    let mut ids = BTreeSet::new();
    for node in nodes {
        if node.id.trim().is_empty()
            || node.id.len() > 80
            || !node
                .id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!("invalid workflow node id: {}", node.id));
        }
        if !ids.insert(node.id.as_str()) {
            return Err(format!("duplicate workflow node id: {}", node.id));
        }
        match &node.kind {
            WorkflowNodeKind::Parallel { branches } => {
                if branches.len() < 2 || branches.len() > 16 {
                    return Err("parallel nodes require between 2 and 16 branches".to_string());
                }
                for branch in branches {
                    let branch_entry = branch.first().map(|node| node.id.as_str()).unwrap_or("");
                    validate_node_list(branch, branch_entry, depth + 1)?;
                }
            }
            WorkflowNodeKind::Loop {
                body,
                max_iterations,
                ..
            } => {
                if !(1..=100).contains(max_iterations) {
                    return Err("loop maxIterations must be between 1 and 100".to_string());
                }
                let body_entry = body.first().map(|node| node.id.as_str()).unwrap_or("");
                validate_node_list(body, body_entry, depth + 1)?;
            }
            _ => {}
        }
    }
    if !ids.contains(entry) {
        return Err(format!("workflow entry node does not exist: {entry}"));
    }
    for node in nodes {
        if let Some(next) = &node.next
            && !ids.contains(next.as_str())
        {
            return Err(format!(
                "node {} references missing next node {next}",
                node.id
            ));
        }
        if let WorkflowNodeKind::Condition {
            if_true, if_false, ..
        } = &node.kind
        {
            for target in [if_true, if_false].into_iter().flatten() {
                if !ids.contains(target.as_str()) {
                    return Err(format!(
                        "condition node {} references missing node {target}",
                        node.id
                    ));
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowNode {
    pub id: String,
    #[serde(flatten)]
    pub kind: WorkflowNodeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowNodeKind {
    Prompt {
        agent_id: Uuid,
        prompt: String,
    },
    Agent {
        agent_id: Uuid,
        prompt: String,
    },
    Tool {
        agent_id: Uuid,
        instruction: String,
    },
    Condition {
        pointer: String,
        #[serde(default)]
        equals: Option<Value>,
        #[serde(default)]
        if_true: Option<String>,
        #[serde(default)]
        if_false: Option<String>,
    },
    Parallel {
        branches: Vec<Vec<WorkflowNode>>,
    },
    Loop {
        body: Vec<WorkflowNode>,
        max_iterations: u32,
        #[serde(default)]
        until_pointer: Option<String>,
    },
    Approval {
        title: String,
        prompt: String,
    },
    Transform {
        operation: TransformOperation,
    },
    A2aTask {
        remote_agent_id: Uuid,
        prompt: String,
    },
    Subworkflow {
        workflow_id: Uuid,
        #[serde(default)]
        input: Value,
    },
}

impl WorkflowNodeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Prompt { .. } => "prompt",
            Self::Agent { .. } => "agent",
            Self::Tool { .. } => "tool",
            Self::Condition { .. } => "condition",
            Self::Parallel { .. } => "parallel",
            Self::Loop { .. } => "loop",
            Self::Approval { .. } => "approval",
            Self::Transform { .. } => "transform",
            Self::A2aTask { .. } => "a2a_task",
            Self::Subworkflow { .. } => "subworkflow",
        }
    }

    #[must_use]
    pub const fn can_have_ambiguous_external_effect(&self) -> bool {
        matches!(self, Self::Tool { .. } | Self::A2aTask { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum TransformOperation {
    Identity,
    Select {
        pointer: String,
    },
    Object {
        fields: serde_json::Map<String, Value>,
    },
    Template {
        template: String,
    },
}

string_enum! {
    pub enum WorkflowRunStatus {
        Queued => "queued",
        Running => "running",
        Paused => "paused",
        WaitingForApproval => "waiting_for_approval",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        UnknownOutcome => "unknown_outcome",
    }
}

impl WorkflowRunStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::UnknownOutcome
        )
    }
}

string_enum! {
    pub enum WorkflowNodeRunStatus {
        Queued => "queued",
        Running => "running",
        WaitingForApproval => "waiting_for_approval",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        UnknownOutcome => "unknown_outcome",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowRun {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub workspace_id: Uuid,
    pub parent_run_id: Option<Uuid>,
    pub status: WorkflowRunStatus,
    pub input: Value,
    pub output: Option<Value>,
    pub current_node_id: Option<String>,
    pub resume_cursor: Option<Value>,
    pub last_error: Option<String>,
    pub pause_requested: bool,
    pub cancel_requested: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowNodeRun {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub node_path: String,
    pub node_kind: String,
    pub attempt: u32,
    pub status: WorkflowNodeRunStatus,
    pub input: Value,
    pub output: Option<Value>,
    pub token_usage: Option<Value>,
    pub tool_calls: Vec<Value>,
    pub error: Option<String>,
    pub resume_cursor: Option<Value>,
    pub artifact_ids: Vec<Uuid>,
    pub side_effect_key: Option<String>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowApproval {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub node_run_id: Uuid,
    pub title: String,
    pub prompt: String,
    pub status: String,
    pub decided_by_user_id: Option<Uuid>,
    pub decision_note: Option<String>,
    pub created_at_ms: i64,
    pub decided_at_ms: Option<i64>,
}

string_enum! {
    pub enum ArtifactKind {
        Document => "document",
        Code => "code",
        Image => "image",
        Audio => "audio",
        Video => "video",
        Data => "data",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub kind: ArtifactKind,
    pub media_type: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub source: String,
    pub metadata: Value,
    pub revision: u32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentVariableRef {
    pub name: String,
    pub credential_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentDefinition {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub variables: Vec<EnvironmentVariableRef>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredentialReference {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub service: String,
    pub account: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

string_enum! {
    pub enum MediaModality {
        Image => "image",
        Audio => "audio",
        Video => "video",
    }
}

string_enum! {
    pub enum MediaJobStatus {
        Queued => "queued",
        Running => "running",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        UnknownOutcome => "unknown_outcome",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MediaProvider {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub modality: MediaModality,
    pub upstream_url: String,
    pub gateway_path: String,
    pub credential_id: Option<Uuid>,
    pub enabled: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MediaJob {
    pub id: Uuid,
    pub provider_id: Uuid,
    pub workspace_id: Uuid,
    pub modality: MediaModality,
    pub status: MediaJobStatus,
    pub prompt: String,
    pub request: Value,
    pub response: Option<Value>,
    pub artifact_id: Option<Uuid>,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NorthbridgePackage {
    pub schema_version: u32,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub personas: Vec<Persona>,
    #[serde(default)]
    pub skills: Vec<Skill>,
    #[serde(default)]
    pub agents: Vec<AgentProfile>,
    #[serde(default)]
    pub mcp_servers: Vec<McpServer>,
    #[serde(default)]
    pub remote_agents: Vec<RemoteAgent>,
    #[serde(default)]
    pub workflows: Vec<WorkflowDefinition>,
    #[serde(default)]
    pub environments: Vec<PackageEnvironment>,
    #[serde(default)]
    pub artifacts: Vec<PackageArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackageEnvironment {
    pub name: String,
    pub description: String,
    pub variables: Vec<PackageEnvironmentVariable>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PackageEnvironmentVariable {
    pub name: String,
    pub credential_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PackageArtifact {
    pub metadata: Artifact,
    pub content_base64: String,
}

string_enum! {
    pub enum TeamRole {
        Owner => "owner",
        Admin => "admin",
        Member => "member",
        Viewer => "viewer",
    }
}

impl TeamRole {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Owner => 4,
            Self::Admin => 3,
            Self::Member => 2,
            Self::Viewer => 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TeamUser {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub role: TeamRole,
    pub active: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub expires_at_ms: i64,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TeamPolicy {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub method: String,
    pub path_prefix: String,
    pub minimum_role: TeamRole,
    pub effect: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    pub sequence: i64,
    pub workspace_id: Uuid,
    pub principal: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub request_id: Option<String>,
    pub metadata: Value,
    pub occurred_at_ms: i64,
    pub previous_hash: String,
    pub event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SyncChange {
    pub sequence: i64,
    pub workspace_id: Uuid,
    pub entity_type: String,
    pub entity_id: String,
    pub version: u64,
    pub deleted: bool,
    pub payload: Value,
    pub device_id: Uuid,
    pub client_change_id: String,
    pub created_at_ms: i64,
}
