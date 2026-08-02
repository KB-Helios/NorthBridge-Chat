use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures_util::{FutureExt, future::BoxFuture};
use northbridge_agent_runtime::{AgentRuntime, AgentRuntimeError, StartAgentRun};
use northbridge_domain::{
    AgentRunStatus, TransformOperation, WorkflowDefinition, WorkflowNode, WorkflowNodeKind,
    WorkflowNodeRunStatus, WorkflowRun, WorkflowRunStatus,
};
use northbridge_storage::{Database, NewWorkflowRun, StorageError};
use serde_json::{Value, json};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::agents::AgentRuntimeRegistry;

const MAX_GRAPH_STEPS: usize = 10_000;
const MAX_TRANSFORM_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct WorkflowCoordinator {
    inner: Arc<Inner>,
}

struct Inner {
    database: Database,
    agents: AgentRuntimeRegistry,
    tasks: Mutex<BTreeMap<Uuid, TaskEntry>>,
    shutdown: CancellationToken,
}

struct TaskEntry {
    cancellation: CancellationToken,
    handle: JoinHandle<()>,
}

enum NodeOutcome {
    Completed { output: Value, next: Option<String> },
    WaitingForApproval,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WorkflowExecutionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Agent(#[from] AgentRuntimeError),
    #[error("invalid workflow: {0}")]
    Invalid(String),
    #[error("workflow execution was paused")]
    Paused,
    #[error("workflow execution was cancelled")]
    Cancelled,
    #[error("workflow execution has an unknown external outcome: {0}")]
    UnknownOutcome(String),
}

impl WorkflowCoordinator {
    pub(crate) fn new(database: Database, agents: AgentRuntimeRegistry) -> Self {
        Self {
            inner: Arc::new(Inner {
                database,
                agents,
                tasks: Mutex::new(BTreeMap::new()),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub(crate) async fn start(
        &self,
        workflow_id: Uuid,
        workspace_id: Uuid,
        input: Value,
        parent_run_id: Option<Uuid>,
    ) -> Result<WorkflowRun, WorkflowExecutionError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(WorkflowExecutionError::Invalid(
                "workflow runtime is shutting down".to_string(),
            ));
        }
        let run = self
            .inner
            .database
            .create_workflow_run(NewWorkflowRun {
                workflow_id,
                workspace_id,
                parent_run_id,
                input,
            })
            .await?;
        self.spawn(run.id).await?;
        Ok(run)
    }

    pub(crate) async fn resume(
        &self,
        run_id: Uuid,
        jump_to: Option<&str>,
    ) -> Result<WorkflowRun, WorkflowExecutionError> {
        let run = self.inner.database.workflow_run(run_id).await?;
        if run.status == WorkflowRunStatus::UnknownOutcome && jump_to.is_none() {
            return Err(WorkflowExecutionError::UnknownOutcome(
                "choose an explicit jump target after reconciling the external operation"
                    .to_string(),
            ));
        }
        if run.status == WorkflowRunStatus::Completed || run.status == WorkflowRunStatus::Cancelled
        {
            return Err(WorkflowExecutionError::Invalid(format!(
                "workflow run cannot resume from {}",
                run.status.as_str()
            )));
        }
        let workflow = self.inner.database.workflow(run.workflow_id).await?;
        let target = jump_to
            .map(str::to_string)
            .or(run.current_node_id.clone())
            .unwrap_or_else(|| workflow.entry_node_id.clone());
        if !workflow.nodes.iter().any(|node| node.id == target) {
            return Err(WorkflowExecutionError::Invalid(format!(
                "workflow jump target does not exist: {target}"
            )));
        }
        self.inner
            .database
            .request_workflow_pause(run_id, false)
            .await?;
        let run = self
            .inner
            .database
            .set_workflow_run_state(
                run_id,
                WorkflowRunStatus::Queued,
                Some(&target),
                run.output.as_ref(),
                run.resume_cursor.as_ref(),
                None,
            )
            .await?;
        self.spawn(run_id).await?;
        Ok(run)
    }

    pub(crate) async fn pause(&self, run_id: Uuid) -> Result<WorkflowRun, WorkflowExecutionError> {
        Ok(self
            .inner
            .database
            .request_workflow_pause(run_id, true)
            .await?)
    }

    pub(crate) async fn cancel(&self, run_id: Uuid) -> Result<WorkflowRun, WorkflowExecutionError> {
        let run = self.inner.database.request_workflow_cancel(run_id).await?;
        if let Some(entry) = self.inner.tasks.lock().await.get(&run_id) {
            entry.cancellation.cancel();
        }
        Ok(run)
    }

    pub(crate) async fn decide_approval(
        &self,
        approval_id: Uuid,
        approved: bool,
        user_id: Option<Uuid>,
        note: Option<&str>,
    ) -> Result<WorkflowRun, WorkflowExecutionError> {
        let approval = self
            .inner
            .database
            .decide_workflow_approval(approval_id, approved, user_id, note)
            .await?;
        if approved {
            let node_input = self
                .inner
                .database
                .workflow_node_run(approval.node_run_id)
                .await?
                .input;
            self.inner
                .database
                .finish_workflow_node_run(
                    approval.node_run_id,
                    WorkflowNodeRunStatus::Completed,
                    Some(&node_input),
                    None,
                    &[],
                    None,
                    None,
                    &[],
                )
                .await?;
            self.resume(approval.workflow_run_id, None).await
        } else {
            self.inner
                .database
                .finish_workflow_node_run(
                    approval.node_run_id,
                    WorkflowNodeRunStatus::Failed,
                    Some(&json!({ "approved": false, "note": note })),
                    None,
                    &[],
                    Some("workflow approval was denied"),
                    None,
                    &[],
                )
                .await?;
            Ok(self
                .inner
                .database
                .set_workflow_run_state(
                    approval.workflow_run_id,
                    WorkflowRunStatus::Failed,
                    None,
                    None,
                    None,
                    Some("workflow approval was denied"),
                )
                .await?)
        }
    }

    pub(crate) async fn recover(&self) -> Result<u64, WorkflowExecutionError> {
        let recovered = self.inner.database.recover_interrupted_workflows().await?;
        Ok(recovered)
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.shutdown.cancel();
        let entries = {
            let mut tasks = self.inner.tasks.lock().await;
            for entry in tasks.values() {
                entry.cancellation.cancel();
            }
            std::mem::take(&mut *tasks)
        };
        tokio::time::timeout(timeout, async move {
            for (_, entry) in entries {
                let _ = entry.handle.await;
            }
        })
        .await
        .is_ok()
    }

    async fn spawn(&self, run_id: Uuid) -> Result<(), WorkflowExecutionError> {
        let mut tasks = self.inner.tasks.lock().await;
        if tasks.contains_key(&run_id) {
            return Ok(());
        }
        let cancellation = self.inner.shutdown.child_token();
        let task_cancellation = cancellation.clone();
        let coordinator = self.clone();
        let handle = tokio::spawn(async move {
            let result = coordinator.execute_run(run_id, task_cancellation).await;
            if let Err(error) = result {
                coordinator.record_failure(run_id, &error).await;
            }
            coordinator.inner.tasks.lock().await.remove(&run_id);
        });
        tasks.insert(
            run_id,
            TaskEntry {
                cancellation,
                handle,
            },
        );
        Ok(())
    }

    async fn record_failure(&self, run_id: Uuid, error: &WorkflowExecutionError) {
        let (status, message) = match error {
            WorkflowExecutionError::Paused => return,
            WorkflowExecutionError::Cancelled => (WorkflowRunStatus::Cancelled, error.to_string()),
            WorkflowExecutionError::UnknownOutcome(message) => {
                (WorkflowRunStatus::UnknownOutcome, message.clone())
            }
            _ => (WorkflowRunStatus::Failed, error.to_string()),
        };
        if let Err(storage_error) = self
            .inner
            .database
            .set_workflow_run_state(run_id, status, None, None, None, Some(&message))
            .await
        {
            tracing::error!(%run_id, %storage_error, "failed to persist workflow failure");
        }
    }

    async fn execute_run(
        &self,
        run_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), WorkflowExecutionError> {
        let run = self.inner.database.workflow_run(run_id).await?;
        let workflow = self.inner.database.workflow(run.workflow_id).await?;
        workflow
            .validate()
            .map_err(WorkflowExecutionError::Invalid)?;
        let start = run
            .current_node_id
            .clone()
            .unwrap_or_else(|| workflow.entry_node_id.clone());
        self.inner
            .database
            .set_workflow_run_state(
                run_id,
                WorkflowRunStatus::Running,
                Some(&start),
                run.output.as_ref(),
                run.resume_cursor.as_ref(),
                None,
            )
            .await?;
        let context = run.output.clone().unwrap_or_else(|| run.input.clone());
        let output = self
            .execute_graph(
                run_id,
                &workflow,
                &workflow.nodes,
                start,
                "root".to_string(),
                context,
                cancellation,
                true,
            )
            .await?;
        self.inner
            .database
            .set_workflow_run_state(
                run_id,
                WorkflowRunStatus::Completed,
                None,
                Some(&output),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_graph<'a>(
        &'a self,
        run_id: Uuid,
        workflow: &'a WorkflowDefinition,
        nodes: &'a [WorkflowNode],
        entry: String,
        prefix: String,
        initial: Value,
        cancellation: CancellationToken,
        update_root_cursor: bool,
    ) -> BoxFuture<'a, Result<Value, WorkflowExecutionError>> {
        async move {
            let mut current = Some(entry);
            let mut context = initial;
            let mut steps = 0usize;
            while let Some(node_id) = current {
                steps += 1;
                if steps > MAX_GRAPH_STEPS {
                    return Err(WorkflowExecutionError::Invalid(
                        "workflow exceeded the maximum graph step count".to_string(),
                    ));
                }
                self.check_control(run_id, &cancellation).await?;
                let node = nodes
                    .iter()
                    .find(|node| node.id == node_id)
                    .ok_or_else(|| {
                        WorkflowExecutionError::Invalid(format!(
                            "workflow node disappeared during execution: {node_id}"
                        ))
                    })?;
                if update_root_cursor {
                    self.inner
                        .database
                        .set_workflow_run_state(
                            run_id,
                            WorkflowRunStatus::Running,
                            Some(&node.id),
                            Some(&context),
                            None,
                            None,
                        )
                        .await?;
                }
                let path = format!("{prefix}/{}", node.id);
                match self
                    .execute_node(
                        run_id,
                        workflow,
                        node,
                        &path,
                        context.clone(),
                        cancellation.clone(),
                    )
                    .await?
                {
                    NodeOutcome::Completed { output, next } => {
                        context = output;
                        current = next.or_else(|| node.next.clone());
                    }
                    NodeOutcome::WaitingForApproval => return Err(WorkflowExecutionError::Paused),
                }
            }
            Ok(context)
        }
        .boxed()
    }

    fn execute_node<'a>(
        &'a self,
        run_id: Uuid,
        workflow: &'a WorkflowDefinition,
        node: &'a WorkflowNode,
        path: &'a str,
        input: Value,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<NodeOutcome, WorkflowExecutionError>> {
        async move {
            if let Some(previous) = self
                .inner
                .database
                .latest_workflow_node_run(run_id, path)
                .await?
            {
                if previous.status == WorkflowNodeRunStatus::Completed {
                    let output = previous.output.unwrap_or(input);
                    let next = condition_next(node, &output);
                    return Ok(NodeOutcome::Completed {
                        output,
                        next,
                    });
                }
                if previous.status == WorkflowNodeRunStatus::WaitingForApproval {
                    let approval = self
                        .inner
                        .database
                        .workflow_approval_for_node(previous.id)
                        .await?
                        .ok_or_else(|| {
                            WorkflowExecutionError::Invalid(
                                "approval node is missing its durable approval".to_string(),
                            )
                        })?;
                    if approval.status == "pending" {
                        self.inner
                            .database
                            .set_workflow_run_state(
                                run_id,
                                WorkflowRunStatus::WaitingForApproval,
                                Some(&node.id),
                                Some(&input),
                                None,
                                None,
                            )
                            .await?;
                        return Ok(NodeOutcome::WaitingForApproval);
                    }
                }
                if previous.status == WorkflowNodeRunStatus::UnknownOutcome {
                    return Err(WorkflowExecutionError::UnknownOutcome(
                        previous.error.unwrap_or_else(|| {
                            "an external workflow operation has an unknown outcome".to_string()
                        }),
                    ));
                }
            }

            let side_effect_key = node
                .kind
                .can_have_ambiguous_external_effect()
                .then(|| format!("workflow:{run_id}:{path}"));
            let node_run = self
                .inner
                .database
                .create_workflow_node_run(
                    run_id,
                    path,
                    node.kind.as_str(),
                    &input,
                    None,
                    side_effect_key.as_deref(),
                )
                .await?;

            let result = match &node.kind {
                WorkflowNodeKind::Prompt { agent_id, prompt }
                | WorkflowNodeKind::Agent { agent_id, prompt } => {
                    self.execute_local_agent(
                        run_id,
                        node_run.id,
                        *agent_id,
                        &render_template(prompt, &input),
                        cancellation,
                    )
                    .await
                }
                WorkflowNodeKind::Tool {
                    agent_id,
                    instruction,
                } => {
                    let prompt = format!(
                        "Execute the explicitly configured workflow tool step. Use only your allowlisted tools and follow approval policy.\n\nInstruction:\n{}",
                        render_template(instruction, &input)
                    );
                    self.execute_local_agent(
                        run_id,
                        node_run.id,
                        *agent_id,
                        &prompt,
                        cancellation,
                    )
                    .await
                }
                WorkflowNodeKind::Condition {
                    pointer,
                    equals,
                    if_true,
                    if_false,
                } => {
                    let actual = input.pointer(pointer).cloned().unwrap_or(Value::Null);
                    let matched = equals.as_ref().map_or_else(|| truthy(&actual), |value| *value == actual);
                    Ok(NodeOutcome::Completed {
                        output: input.clone(),
                        next: if matched { if_true.clone() } else { if_false.clone() },
                    })
                }
                WorkflowNodeKind::Parallel { branches } => {
                    let futures = branches.iter().enumerate().map(|(index, branch)| {
                        let entry = branch.first().map(|node| node.id.as_str()).unwrap_or("");
                        let branch_prefix = format!("{path}/parallel-{index}");
                        self.execute_graph(
                            run_id,
                            workflow,
                            branch,
                            entry.to_string(),
                            branch_prefix,
                            input.clone(),
                            cancellation.clone(),
                            false,
                        )
                    });
                    let outputs = futures_util::future::join_all(futures)
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(NodeOutcome::Completed {
                        output: Value::Array(outputs),
                        next: None,
                    })
                }
                WorkflowNodeKind::Loop {
                    body,
                    max_iterations,
                    until_pointer,
                } => {
                    let mut output = input.clone();
                    for iteration in 0..*max_iterations {
                        self.check_control(run_id, &cancellation).await?;
                        let entry = body.first().map(|node| node.id.as_str()).unwrap_or("");
                        let iteration_prefix = format!("{path}/loop-{iteration}");
                        output = self
                            .execute_graph(
                                run_id,
                                workflow,
                                body,
                                entry.to_string(),
                                iteration_prefix,
                                output,
                                cancellation.clone(),
                                false,
                            )
                            .await?;
                        if until_pointer
                            .as_deref()
                            .and_then(|pointer| output.pointer(pointer))
                            .is_some_and(truthy)
                        {
                            break;
                        }
                    }
                    Ok(NodeOutcome::Completed { output, next: None })
                }
                WorkflowNodeKind::Approval { title, prompt } => {
                    self.inner
                        .database
                        .finish_workflow_node_run(
                            node_run.id,
                            WorkflowNodeRunStatus::WaitingForApproval,
                            None,
                            None,
                            &[],
                            None,
                            None,
                            &[],
                        )
                        .await?;
                    self.inner
                        .database
                        .create_workflow_approval(
                            run_id,
                            node_run.id,
                            title,
                            &render_template(prompt, &input),
                        )
                        .await?;
                    self.inner
                        .database
                        .set_workflow_run_state(
                            run_id,
                            WorkflowRunStatus::WaitingForApproval,
                            Some(&node.id),
                            Some(&input),
                            None,
                            None,
                        )
                        .await?;
                    return Ok(NodeOutcome::WaitingForApproval);
                }
                WorkflowNodeKind::Transform { operation } => {
                    let output = RestrictedTransformWorker::execute(operation.clone(), input.clone()).await?;
                    Ok(NodeOutcome::Completed { output, next: None })
                }
                WorkflowNodeKind::A2aTask {
                    remote_agent_id,
                    prompt,
                } => {
                    let runtime = self.required_agent_runtime()?;
                    let response = runtime
                        .run_remote_agent(*remote_agent_id, &render_template(prompt, &input))
                        .await?;
                    Ok(NodeOutcome::Completed {
                        output: json!({
                            "output": response.output,
                            "taskId": response.task_id,
                            "contextId": response.context_id,
                            "response": response.response,
                        }),
                        next: None,
                    })
                }
                WorkflowNodeKind::Subworkflow {
                    workflow_id,
                    input: child_input,
                } => {
                    let child = self
                        .inner
                        .database
                        .create_workflow_run(NewWorkflowRun {
                            workflow_id: *workflow_id,
                            workspace_id: workflow.workspace_id,
                            parent_run_id: Some(run_id),
                            input: merge_input(child_input, &input),
                        })
                        .await?;
                    self.execute_run(child.id, cancellation).await?;
                    let child = self.inner.database.workflow_run(child.id).await?;
                    Ok(NodeOutcome::Completed {
                        output: child.output.unwrap_or(Value::Null),
                        next: None,
                    })
                }
            };

            match result {
                Ok(NodeOutcome::Completed { output, next }) => {
                    self.inner
                        .database
                        .finish_workflow_node_run(
                            node_run.id,
                            WorkflowNodeRunStatus::Completed,
                            Some(&output),
                            None,
                            &[],
                            None,
                            None,
                            &[],
                        )
                        .await?;
                    Ok(NodeOutcome::Completed { output, next })
                }
                Ok(NodeOutcome::WaitingForApproval) => Ok(NodeOutcome::WaitingForApproval),
                Err(error) => {
                    let status = if matches!(error, WorkflowExecutionError::UnknownOutcome(_)) {
                        WorkflowNodeRunStatus::UnknownOutcome
                    } else if matches!(error, WorkflowExecutionError::Cancelled) {
                        WorkflowNodeRunStatus::Cancelled
                    } else {
                        WorkflowNodeRunStatus::Failed
                    };
                    let _ = self
                        .inner
                        .database
                        .finish_workflow_node_run(
                            node_run.id,
                            status,
                            None,
                            None,
                            &[],
                            Some(&error.to_string()),
                            None,
                            &[],
                        )
                        .await;
                    Err(error)
                }
            }
        }
        .boxed()
    }

    async fn execute_local_agent(
        &self,
        workflow_run_id: Uuid,
        node_run_id: Uuid,
        agent_id: Uuid,
        prompt: &str,
        cancellation: CancellationToken,
    ) -> Result<NodeOutcome, WorkflowExecutionError> {
        let runtime = self.required_agent_runtime()?;
        let node_run = self.inner.database.workflow_node_run(node_run_id).await?;
        let existing_agent_run = node_run
            .resume_cursor
            .as_ref()
            .and_then(|cursor| cursor.get("agentRunId"))
            .and_then(Value::as_str)
            .and_then(|id| Uuid::parse_str(id).ok());
        let agent_run = match existing_agent_run {
            Some(id) => runtime.resume_run(id).await?,
            None => {
                let workflow_run = self.inner.database.workflow_run(workflow_run_id).await?;
                runtime
                    .start_run(StartAgentRun {
                        workspace_id: workflow_run.workspace_id,
                        agent_id,
                        conversation_id: None,
                        prompt: prompt.to_string(),
                    })
                    .await?
            }
        };
        self.inner
            .database
            .checkpoint_workflow_node_run(node_run_id, &json!({ "agentRunId": agent_run.id }))
            .await?;
        loop {
            if cancellation.is_cancelled() {
                let _ = runtime.cancel_run(agent_run.id).await;
                return Err(WorkflowExecutionError::Cancelled);
            }
            let workflow_run = self.inner.database.workflow_run(workflow_run_id).await?;
            if workflow_run.cancel_requested {
                let _ = runtime.cancel_run(agent_run.id).await;
                return Err(WorkflowExecutionError::Cancelled);
            }
            let stored = self.inner.database.agent_run(agent_run.id).await?;
            match stored.status {
                AgentRunStatus::Completed => {
                    return Ok(NodeOutcome::Completed {
                        output: json!({ "text": stored.output.unwrap_or_default(), "agentRunId": stored.id }),
                        next: None,
                    });
                }
                AgentRunStatus::Failed | AgentRunStatus::Cancelled => {
                    return Err(WorkflowExecutionError::Invalid(
                        stored.last_error.unwrap_or_else(|| {
                            format!("agent run ended as {}", stored.status.as_str())
                        }),
                    ));
                }
                AgentRunStatus::WaitingForApproval => {
                    self.inner
                        .database
                        .set_workflow_run_state(
                            workflow_run_id,
                            WorkflowRunStatus::WaitingForApproval,
                            workflow_run.current_node_id.as_deref(),
                            workflow_run.output.as_ref(),
                            Some(&json!({ "agentRunId": stored.id })),
                            None,
                        )
                        .await?;
                }
                AgentRunStatus::Suspended => {
                    runtime.resume_run(stored.id).await?;
                }
                AgentRunStatus::Queued | AgentRunStatus::Running => {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn check_control(
        &self,
        run_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkflowExecutionError> {
        if cancellation.is_cancelled() || self.inner.shutdown.is_cancelled() {
            return Err(WorkflowExecutionError::Cancelled);
        }
        let run = self.inner.database.workflow_run(run_id).await?;
        if run.cancel_requested {
            return Err(WorkflowExecutionError::Cancelled);
        }
        if run.pause_requested {
            self.inner
                .database
                .set_workflow_run_state(
                    run_id,
                    WorkflowRunStatus::Paused,
                    run.current_node_id.as_deref(),
                    run.output.as_ref(),
                    run.resume_cursor.as_ref(),
                    None,
                )
                .await?;
            return Err(WorkflowExecutionError::Paused);
        }
        Ok(())
    }

    fn required_agent_runtime(
        &self,
    ) -> Result<northbridge_agent_runtime::RigAgentRuntime, WorkflowExecutionError> {
        self.inner.agents.current().ok_or_else(|| {
            WorkflowExecutionError::Invalid(
                "select a local model before running workflow agent nodes".to_string(),
            )
        })
    }
}

struct RestrictedTransformWorker;

impl RestrictedTransformWorker {
    async fn execute(
        operation: TransformOperation,
        input: Value,
    ) -> Result<Value, WorkflowExecutionError> {
        let input_size = serde_json::to_vec(&input)
            .map_err(|error| WorkflowExecutionError::Invalid(error.to_string()))?
            .len();
        if input_size > MAX_TRANSFORM_BYTES {
            return Err(WorkflowExecutionError::Invalid(
                "restricted transform input exceeded 4 MiB".to_string(),
            ));
        }
        tokio::task::spawn_blocking(move || match operation {
            TransformOperation::Identity => Ok(input),
            TransformOperation::Select { pointer } => {
                Ok(input.pointer(&pointer).cloned().unwrap_or(Value::Null))
            }
            TransformOperation::Object { fields } => Ok(Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| {
                        let value = value
                            .as_str()
                            .map(|value| Value::String(render_template(value, &input)))
                            .unwrap_or(value);
                        (key, value)
                    })
                    .collect(),
            )),
            TransformOperation::Template { template } => {
                Ok(Value::String(render_template(&template, &input)))
            }
        })
        .await
        .map_err(|error| {
            WorkflowExecutionError::Invalid(format!("transform worker failed: {error}"))
        })?
    }
}

fn condition_next(node: &WorkflowNode, output: &Value) -> Option<String> {
    if let WorkflowNodeKind::Condition {
        pointer,
        equals,
        if_true,
        if_false,
    } = &node.kind
    {
        let actual = output.pointer(pointer).cloned().unwrap_or(Value::Null);
        if equals
            .as_ref()
            .map_or_else(|| truthy(&actual), |value| *value == actual)
        {
            if_true.clone()
        } else {
            if_false.clone()
        }
    } else {
        None
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn render_template(template: &str, input: &Value) -> String {
    let mut output = template.replace("${input}", &input.to_string());
    let mut offset = 0;
    while let Some(start) = output[offset..].find("${/") {
        let start = offset + start;
        let Some(relative_end) = output[start..].find('}') else {
            break;
        };
        let end = start + relative_end;
        let pointer = &output[start + 2..end];
        let replacement = input
            .pointer(pointer)
            .map(|value| match value {
                Value::String(value) => value.clone(),
                value => value.to_string(),
            })
            .unwrap_or_default();
        output.replace_range(start..=end, &replacement);
        offset = start + replacement.len();
    }
    output
}

fn merge_input(template: &Value, parent: &Value) -> Value {
    match template {
        Value::Null => parent.clone(),
        Value::String(value) => Value::String(render_template(value, parent)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| merge_input(value, parent))
                .collect(),
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), merge_input(value, parent)))
                .collect(),
        ),
        value => value.clone(),
    }
}
