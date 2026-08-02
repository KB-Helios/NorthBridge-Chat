use std::str::FromStr;

use northbridge_domain::{
    WorkflowApproval, WorkflowDefinition, WorkflowNode, WorkflowNodeRun, WorkflowNodeRunStatus,
    WorkflowRun, WorkflowRunStatus,
};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, Clone)]
pub struct NewWorkflow {
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub entry_node_id: String,
    pub nodes: Vec<WorkflowNode>,
}

#[derive(Debug, Clone)]
pub struct NewWorkflowRun {
    pub workflow_id: Uuid,
    pub workspace_id: Uuid,
    pub parent_run_id: Option<Uuid>,
    pub input: Value,
}

impl Database {
    pub async fn create_workflow(&self, input: NewWorkflow) -> Result<WorkflowDefinition> {
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
        let nodes =
            serde_json::to_string(&candidate.nodes).map_err(corrupt_json("workflow nodes"))?;
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
        .execute(self.pool())
        .await
        .map_err(|error| map_unique(error, "a workflow with this name already exists"))?;
        Ok(candidate)
    }

    pub async fn workflow(&self, id: Uuid) -> Result<WorkflowDefinition> {
        let row = sqlx::query(
            "SELECT id, workspace_id, name, description, version, entry_node_id, nodes_json,
                    created_at_ms, updated_at_ms FROM workflows WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound { entity: "workflow" })?;
        workflow_from_row(&row)
    }

    pub async fn list_workflows(&self, workspace_id: Uuid) -> Result<Vec<WorkflowDefinition>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, name, description, version, entry_node_id, nodes_json,
                    created_at_ms, updated_at_ms FROM workflows
             WHERE workspace_id = ? ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(workflow_from_row).collect()
    }

    pub async fn create_workflow_run(&self, input: NewWorkflowRun) -> Result<WorkflowRun> {
        let workflow = self.workflow(input.workflow_id).await?;
        if workflow.workspace_id != input.workspace_id {
            return Err(StorageError::InvalidInput(
                "workflow and run must belong to the same workspace".to_string(),
            ));
        }
        if let Some(parent) = input.parent_run_id {
            let parent = self.workflow_run(parent).await?;
            if parent.workspace_id != input.workspace_id {
                return Err(StorageError::InvalidInput(
                    "parent workflow run belongs to another workspace".to_string(),
                ));
            }
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO workflow_runs (
                 id, workflow_id, workspace_id, parent_run_id, status, input_json,
                 current_node_id, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workflow_id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.parent_run_id.map(|id| id.to_string()))
        .bind(json_string(&input.input, "workflow input")?)
        .bind(&workflow.entry_node_id)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await?;
        self.workflow_run(id).await
    }

    pub async fn workflow_run(&self, id: Uuid) -> Result<WorkflowRun> {
        let row = sqlx::query(
            "SELECT id, workflow_id, workspace_id, parent_run_id, status, input_json, output_json,
                    current_node_id, resume_cursor_json, last_error, pause_requested,
                    cancel_requested, created_at_ms, updated_at_ms
             FROM workflow_runs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "workflow run",
        })?;
        workflow_run_from_row(&row)
    }

    pub async fn list_workflow_runs(
        &self,
        workspace_id: Uuid,
        limit: u32,
    ) -> Result<Vec<WorkflowRun>> {
        let rows = sqlx::query(
            "SELECT id, workflow_id, workspace_id, parent_run_id, status, input_json, output_json,
                    current_node_id, resume_cursor_json, last_error, pause_requested,
                    cancel_requested, created_at_ms, updated_at_ms
             FROM workflow_runs WHERE workspace_id = ?
             ORDER BY updated_at_ms DESC, id DESC LIMIT ?",
        )
        .bind(workspace_id.to_string())
        .bind(i64::from(limit.clamp(1, 500)))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(workflow_run_from_row).collect()
    }

    pub async fn has_active_workflow_runs(&self, workspace_id: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM workflow_runs
                 WHERE workspace_id = ? AND status IN ('queued', 'running', 'waiting_for_approval')
             )",
        )
        .bind(workspace_id.to_string())
        .fetch_one(self.pool())
        .await?
            != 0)
    }

    pub async fn set_workflow_run_state(
        &self,
        id: Uuid,
        status: WorkflowRunStatus,
        current_node_id: Option<&str>,
        output: Option<&Value>,
        resume_cursor: Option<&Value>,
        error: Option<&str>,
    ) -> Result<WorkflowRun> {
        let changed = sqlx::query(
            "UPDATE workflow_runs SET status = ?, current_node_id = ?, output_json = ?,
                    resume_cursor_json = ?, last_error = ?, updated_at_ms = ? WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(current_node_id)
        .bind(optional_json_string(output, "workflow output")?)
        .bind(optional_json_string(
            resume_cursor,
            "workflow resume cursor",
        )?)
        .bind(error)
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(StorageError::NotFound {
                entity: "workflow run",
            });
        }
        self.workflow_run(id).await
    }

    pub async fn request_workflow_pause(&self, id: Uuid, pause: bool) -> Result<WorkflowRun> {
        let changed = sqlx::query(
            "UPDATE workflow_runs SET pause_requested = ?, updated_at_ms = ?
             WHERE id = ? AND status NOT IN ('completed', 'cancelled')",
        )
        .bind(i64::from(pause))
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            let run = self.workflow_run(id).await?;
            if matches!(
                run.status,
                WorkflowRunStatus::Completed | WorkflowRunStatus::Cancelled
            ) {
                return Err(StorageError::Conflict(format!(
                    "workflow run cannot change pause state from {}",
                    run.status.as_str()
                )));
            }
        }
        self.workflow_run(id).await
    }

    pub async fn request_workflow_cancel(&self, id: Uuid) -> Result<WorkflowRun> {
        let run = self.workflow_run(id).await?;
        if run.status.is_terminal() {
            return Ok(run);
        }
        sqlx::query(
            "UPDATE workflow_runs SET cancel_requested = 1, updated_at_ms = ? WHERE id = ?",
        )
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        self.workflow_run(id).await
    }

    pub async fn create_workflow_node_run(
        &self,
        workflow_run_id: Uuid,
        node_path: &str,
        node_kind: &str,
        input: &Value,
        resume_cursor: Option<&Value>,
        side_effect_key: Option<&str>,
    ) -> Result<WorkflowNodeRun> {
        if node_path.is_empty() || node_path.len() > 512 {
            return Err(StorageError::InvalidInput(
                "workflow node path must contain between 1 and 512 bytes".to_string(),
            ));
        }
        let attempt = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(attempt), 0) + 1 FROM workflow_node_runs
             WHERE workflow_run_id = ? AND node_path = ?",
        )
        .bind(workflow_run_id.to_string())
        .bind(node_path)
        .fetch_one(self.pool())
        .await?;
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO workflow_node_runs (
                 id, workflow_run_id, node_path, node_kind, attempt, status, input_json,
                 resume_cursor_json, side_effect_key, started_at_ms
             ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(workflow_run_id.to_string())
        .bind(node_path)
        .bind(node_kind)
        .bind(attempt)
        .bind(json_string(input, "workflow node input")?)
        .bind(optional_json_string(
            resume_cursor,
            "workflow node resume cursor",
        )?)
        .bind(side_effect_key)
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        self.workflow_node_run(id).await
    }

    pub async fn workflow_node_run(&self, id: Uuid) -> Result<WorkflowNodeRun> {
        let row = sqlx::query(
            "SELECT id, workflow_run_id, node_path, node_kind, attempt, status, input_json,
                    output_json, token_usage_json, tool_calls_json, error, resume_cursor_json,
                    artifact_ids_json, side_effect_key, started_at_ms, finished_at_ms
             FROM workflow_node_runs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "workflow node run",
        })?;
        workflow_node_run_from_row(&row)
    }

    pub async fn checkpoint_workflow_node_run(
        &self,
        id: Uuid,
        resume_cursor: &Value,
    ) -> Result<WorkflowNodeRun> {
        let changed = sqlx::query(
            "UPDATE workflow_node_runs SET resume_cursor_json = ?
             WHERE id = ? AND status IN ('running', 'waiting_for_approval')",
        )
        .bind(json_string(resume_cursor, "workflow node resume cursor")?)
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(StorageError::Conflict(
                "workflow node run is no longer checkpointable".to_string(),
            ));
        }
        self.workflow_node_run(id).await
    }

    pub async fn latest_workflow_node_run(
        &self,
        workflow_run_id: Uuid,
        node_path: &str,
    ) -> Result<Option<WorkflowNodeRun>> {
        let row = sqlx::query(
            "SELECT id, workflow_run_id, node_path, node_kind, attempt, status, input_json,
                    output_json, token_usage_json, tool_calls_json, error, resume_cursor_json,
                    artifact_ids_json, side_effect_key, started_at_ms, finished_at_ms
             FROM workflow_node_runs WHERE workflow_run_id = ? AND node_path = ?
             ORDER BY attempt DESC LIMIT 1",
        )
        .bind(workflow_run_id.to_string())
        .bind(node_path)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(workflow_node_run_from_row).transpose()
    }

    pub async fn list_workflow_node_runs(&self, run_id: Uuid) -> Result<Vec<WorkflowNodeRun>> {
        let rows = sqlx::query(
            "SELECT id, workflow_run_id, node_path, node_kind, attempt, status, input_json,
                    output_json, token_usage_json, tool_calls_json, error, resume_cursor_json,
                    artifact_ids_json, side_effect_key, started_at_ms, finished_at_ms
             FROM workflow_node_runs WHERE workflow_run_id = ?
             ORDER BY started_at_ms, node_path, attempt",
        )
        .bind(run_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(workflow_node_run_from_row).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn finish_workflow_node_run(
        &self,
        id: Uuid,
        status: WorkflowNodeRunStatus,
        output: Option<&Value>,
        token_usage: Option<&Value>,
        tool_calls: &[Value],
        error: Option<&str>,
        resume_cursor: Option<&Value>,
        artifact_ids: &[Uuid],
    ) -> Result<WorkflowNodeRun> {
        let changed = sqlx::query(
            "UPDATE workflow_node_runs SET status = ?, output_json = ?, token_usage_json = ?,
                    tool_calls_json = ?, error = ?, resume_cursor_json = ?, artifact_ids_json = ?,
                    finished_at_ms = ? WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(optional_json_string(output, "workflow node output")?)
        .bind(optional_json_string(
            token_usage,
            "workflow node token usage",
        )?)
        .bind(json_string(tool_calls, "workflow node tool calls")?)
        .bind(error)
        .bind(optional_json_string(
            resume_cursor,
            "workflow node resume cursor",
        )?)
        .bind(json_string(artifact_ids, "workflow node artifact IDs")?)
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(StorageError::NotFound {
                entity: "workflow node run",
            });
        }
        self.workflow_node_run(id).await
    }

    pub async fn create_workflow_approval(
        &self,
        run_id: Uuid,
        node_run_id: Uuid,
        title: &str,
        prompt: &str,
    ) -> Result<WorkflowApproval> {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO workflow_approvals (
                 id, workflow_run_id, node_run_id, title, prompt, status, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        )
        .bind(id.to_string())
        .bind(run_id.to_string())
        .bind(node_run_id.to_string())
        .bind(title.trim())
        .bind(prompt.trim())
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        self.workflow_approval(id).await
    }

    pub async fn workflow_approval(&self, id: Uuid) -> Result<WorkflowApproval> {
        let row = sqlx::query(
            "SELECT id, workflow_run_id, node_run_id, title, prompt, status,
                    decided_by_user_id, decision_note, created_at_ms, decided_at_ms
             FROM workflow_approvals WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "workflow approval",
        })?;
        workflow_approval_from_row(&row)
    }

    pub async fn workflow_approval_for_node(
        &self,
        node_run_id: Uuid,
    ) -> Result<Option<WorkflowApproval>> {
        let row = sqlx::query(
            "SELECT id, workflow_run_id, node_run_id, title, prompt, status,
                    decided_by_user_id, decision_note, created_at_ms, decided_at_ms
             FROM workflow_approvals WHERE node_run_id = ?",
        )
        .bind(node_run_id.to_string())
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(workflow_approval_from_row).transpose()
    }

    pub async fn list_pending_workflow_approvals(
        &self,
        workspace_id: Uuid,
    ) -> Result<Vec<WorkflowApproval>> {
        let rows = sqlx::query(
            "SELECT a.id, a.workflow_run_id, a.node_run_id, a.title, a.prompt, a.status,
                    a.decided_by_user_id, a.decision_note, a.created_at_ms, a.decided_at_ms
             FROM workflow_approvals a JOIN workflow_runs r ON r.id = a.workflow_run_id
             WHERE r.workspace_id = ? AND a.status = 'pending' ORDER BY a.created_at_ms",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(workflow_approval_from_row).collect()
    }

    pub async fn decide_workflow_approval(
        &self,
        id: Uuid,
        approved: bool,
        user_id: Option<Uuid>,
        note: Option<&str>,
    ) -> Result<WorkflowApproval> {
        let changed = sqlx::query(
            "UPDATE workflow_approvals SET status = ?, decided_by_user_id = ?, decision_note = ?,
                    decided_at_ms = ? WHERE id = ? AND status = 'pending'",
        )
        .bind(if approved { "approved" } else { "denied" })
        .bind(user_id.map(|id| id.to_string()))
        .bind(note)
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?
        .rows_affected();
        if changed == 0 {
            let approval = self.workflow_approval(id).await?;
            return Err(StorageError::Conflict(format!(
                "workflow approval was already {}",
                approval.status
            )));
        }
        self.workflow_approval(id).await
    }

    pub async fn recover_interrupted_workflows(&self) -> Result<u64> {
        let now = now_ms();
        let mut transaction = self.begin_write().await?;
        let ambiguous = sqlx::query(
            "UPDATE workflow_node_runs SET status = 'unknown_outcome',
                    error = 'desktop core stopped during an externally observable operation; manual reconciliation is required',
                    finished_at_ms = ? WHERE status = 'running' AND node_kind IN ('tool', 'a2a_task')",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let resumable = sqlx::query(
            "UPDATE workflow_node_runs SET status = 'failed',
                    error = 'desktop core stopped during a resumable operation', finished_at_ms = ?
             WHERE status = 'running'",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        sqlx::query(
            "UPDATE workflow_runs SET status = CASE
                    WHEN EXISTS (
                        SELECT 1 FROM workflow_node_runs n
                        WHERE n.workflow_run_id = workflow_runs.id AND n.status = 'unknown_outcome'
                    ) THEN 'unknown_outcome' ELSE 'paused' END,
                    last_error = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM workflow_node_runs n
                            WHERE n.workflow_run_id = workflow_runs.id AND n.status = 'unknown_outcome'
                        ) THEN 'an external operation has an unknown outcome and will not be retried automatically'
                        ELSE 'desktop core stopped during workflow execution; execution can be resumed' END,
                    updated_at_ms = ?
             WHERE status IN ('running', 'queued')",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ambiguous + resumable)
    }
}

fn workflow_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowDefinition> {
    let nodes: Vec<WorkflowNode> = parse_json("workflow nodes", row.try_get("nodes_json")?)?;
    let workflow = WorkflowDefinition {
        id: parse_uuid("workflow id", row.try_get("id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        version: parse_u32("workflow version", row.try_get::<i64, _>("version")?)?,
        entry_node_id: row.try_get("entry_node_id")?,
        nodes,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    };
    workflow
        .validate()
        .map_err(|message| StorageError::CorruptData {
            field: "workflow definition",
            message,
        })?;
    Ok(workflow)
}

fn workflow_run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowRun> {
    Ok(WorkflowRun {
        id: parse_uuid("workflow run id", row.try_get("id")?)?,
        workflow_id: parse_uuid("workflow id", row.try_get("workflow_id")?)?,
        workspace_id: parse_uuid("workspace id", row.try_get("workspace_id")?)?,
        parent_run_id: parse_optional_uuid(
            "parent workflow run id",
            row.try_get("parent_run_id")?,
        )?,
        status: parse_enum("workflow run status", row.try_get("status")?)?,
        input: parse_json("workflow input", row.try_get("input_json")?)?,
        output: parse_optional_json("workflow output", row.try_get("output_json")?)?,
        current_node_id: row.try_get("current_node_id")?,
        resume_cursor: parse_optional_json(
            "workflow resume cursor",
            row.try_get("resume_cursor_json")?,
        )?,
        last_error: row.try_get("last_error")?,
        pause_requested: row.try_get::<i64, _>("pause_requested")? != 0,
        cancel_requested: row.try_get::<i64, _>("cancel_requested")? != 0,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn workflow_node_run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowNodeRun> {
    Ok(WorkflowNodeRun {
        id: parse_uuid("workflow node run id", row.try_get("id")?)?,
        workflow_run_id: parse_uuid("workflow run id", row.try_get("workflow_run_id")?)?,
        node_path: row.try_get("node_path")?,
        node_kind: row.try_get("node_kind")?,
        attempt: parse_u32("workflow node attempt", row.try_get::<i64, _>("attempt")?)?,
        status: parse_enum("workflow node status", row.try_get("status")?)?,
        input: parse_json("workflow node input", row.try_get("input_json")?)?,
        output: parse_optional_json("workflow node output", row.try_get("output_json")?)?,
        token_usage: parse_optional_json(
            "workflow node token usage",
            row.try_get("token_usage_json")?,
        )?,
        tool_calls: parse_json("workflow node tool calls", row.try_get("tool_calls_json")?)?,
        error: row.try_get("error")?,
        resume_cursor: parse_optional_json(
            "workflow node resume cursor",
            row.try_get("resume_cursor_json")?,
        )?,
        artifact_ids: parse_json(
            "workflow node artifact IDs",
            row.try_get("artifact_ids_json")?,
        )?,
        side_effect_key: row.try_get("side_effect_key")?,
        started_at_ms: row.try_get("started_at_ms")?,
        finished_at_ms: row.try_get("finished_at_ms")?,
    })
}

fn workflow_approval_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<WorkflowApproval> {
    Ok(WorkflowApproval {
        id: parse_uuid("workflow approval id", row.try_get("id")?)?,
        workflow_run_id: parse_uuid("workflow run id", row.try_get("workflow_run_id")?)?,
        node_run_id: parse_uuid("workflow node run id", row.try_get("node_run_id")?)?,
        title: row.try_get("title")?,
        prompt: row.try_get("prompt")?,
        status: row.try_get("status")?,
        decided_by_user_id: parse_optional_uuid(
            "workflow approval user id",
            row.try_get("decided_by_user_id")?,
        )?,
        decision_note: row.try_get("decision_note")?,
        created_at_ms: row.try_get("created_at_ms")?,
        decided_at_ms: row.try_get("decided_at_ms")?,
    })
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

fn parse_u32(field: &'static str, value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|error| StorageError::CorruptData {
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

fn corrupt_json(field: &'static str) -> impl FnOnce(serde_json::Error) -> StorageError {
    move |error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    }
}

fn map_unique(error: sqlx::Error, message: &str) -> StorageError {
    if matches!(&error, sqlx::Error::Database(database) if database.is_unique_violation()) {
        StorageError::Conflict(message.to_string())
    } else {
        StorageError::Database(error)
    }
}
