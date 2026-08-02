use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use northbridge_agent_runtime::{AgentRuntime, AgentRuntimeError, RigAgentRuntime, StartAgentRun};
use northbridge_domain::{
    AgentApproval, AgentProfile, AgentRun, AgentRunEvent, AgentRunStatus, AgentToolCall,
    AgentToolPermission, Crew, CrewMember, CrewRun, CrewRunStatus, DEFAULT_WORKSPACE_ID, McpServer,
    McpToolSnapshot, McpTransport, Message, MessagePartKind, MessageRole, Persona, RemoteAgent,
    ShadowSubscription, ShadowSuggestion, Skill, ToolPolicyMode,
};
use northbridge_storage::{
    FinishCrewMember, NewAgentProfile, NewCrew, NewCrewRun, NewMcpServer, NewPersona,
    NewRemoteAgent, NewShadowSubscription, NewSkill,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{error::ApiError, routes::ApiState};

#[derive(Clone, Default)]
pub(crate) struct AgentRuntimeRegistry {
    inner: Arc<RwLock<Option<RigAgentRuntime>>>,
}

impl AgentRuntimeRegistry {
    pub(crate) fn current(&self) -> Option<RigAgentRuntime> {
        read_runtime(&self.inner).clone()
    }

    pub(crate) fn replace(&self, runtime: RigAgentRuntime) -> Option<RigAgentRuntime> {
        write_runtime(&self.inner).replace(runtime)
    }

    pub(crate) fn take(&self) -> Option<RigAgentRuntime> {
        write_runtime(&self.inner).take()
    }
}

#[derive(Clone)]
pub(crate) struct ShadowCoordinator {
    inner: Arc<ShadowCoordinatorInner>,
}

struct ShadowCoordinatorInner {
    database: northbridge_storage::Database,
    agents: AgentRuntimeRegistry,
    tasks: Mutex<HashMap<Uuid, JoinHandle<()>>>,
    shutdown: CancellationToken,
}

impl ShadowCoordinator {
    pub(crate) fn new(
        database: northbridge_storage::Database,
        agents: AgentRuntimeRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(ShadowCoordinatorInner {
                database,
                agents,
                tasks: Mutex::new(HashMap::new()),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub(crate) async fn observe_message(&self, message: &Message) -> Result<usize, ApiError> {
        if message.role != MessageRole::User {
            return Ok(0);
        }
        let subscriptions = self
            .inner
            .database
            .list_shadow_subscriptions_for_conversation(message.conversation_id)
            .await?;
        let count = subscriptions.len();
        let runtime_available = self.inner.agents.current().is_some();
        for subscription in subscriptions {
            let suggestion = self
                .inner
                .database
                .reserve_shadow_suggestion(subscription.id, message.id)
                .await?;
            if runtime_available {
                self.spawn(suggestion.id).await;
            }
        }
        Ok(count)
    }

    pub(crate) async fn recover(&self, workspace_id: Uuid) -> Result<usize, ApiError> {
        if self.inner.agents.current().is_none() {
            return Ok(0);
        }
        let suggestions = self
            .inner
            .database
            .list_processing_shadow_suggestions(workspace_id)
            .await?;
        let count = suggestions.len();
        for suggestion in suggestions {
            self.spawn(suggestion.id).await;
        }
        Ok(count)
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.shutdown.cancel();
        let tasks = {
            let mut tasks = self.inner.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };
        tokio::time::timeout(timeout, async move {
            for (_, task) in tasks {
                let _ = task.await;
            }
        })
        .await
        .is_ok()
    }

    async fn spawn(&self, suggestion_id: Uuid) {
        let mut tasks = self.inner.tasks.lock().await;
        tasks.retain(|_, task| !task.is_finished());
        if tasks.contains_key(&suggestion_id) || self.inner.shutdown.is_cancelled() {
            return;
        }
        let coordinator = self.clone();
        let cancellation = self.inner.shutdown.child_token();
        tasks.insert(
            suggestion_id,
            tokio::spawn(async move {
                if let Err(error) = coordinator
                    .drive_suggestion(suggestion_id, &cancellation)
                    .await
                    && !cancellation.is_cancelled()
                {
                    let mut message = error.to_string();
                    truncate_utf8(&mut message, 4096);
                    let _ = coordinator
                        .inner
                        .database
                        .fail_shadow_suggestion(suggestion_id, &message)
                        .await;
                    tracing::error!(%suggestion_id, %error, "shadow suggestion failed");
                }
            }),
        );
    }

    async fn drive_suggestion(
        &self,
        suggestion_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<(), ApiError> {
        let suggestion = self.inner.database.shadow_suggestion(suggestion_id).await?;
        let subscription = self
            .inner
            .database
            .shadow_subscription(suggestion.subscription_id)
            .await?;
        let message = self
            .inner
            .database
            .message(suggestion.source_message_id)
            .await?;
        let runtime = self.inner.agents.current().ok_or_else(|| {
            ApiError::unavailable(
                "agent_runtime_unavailable",
                "a local model is required for Shadow Persona suggestions",
            )
        })?;
        let recovering = suggestion.run_id.is_some();
        let run = match suggestion.run_id {
            Some(run_id) => self.inner.database.agent_run(run_id).await?,
            None => {
                let run = runtime
                    .start_run(StartAgentRun {
                        workspace_id: subscription.workspace_id,
                        agent_id: subscription.agent_id,
                        conversation_id: Some(subscription.conversation_id),
                        prompt: shadow_prompt(&message),
                    })
                    .await?;
                self.inner
                    .database
                    .attach_shadow_suggestion_run(suggestion_id, run.id)
                    .await?;
                run
            }
        };
        if recovering && !run.status.is_terminal() {
            match runtime.resume_run(run.id).await {
                Ok(_) | Err(AgentRuntimeError::AlreadyActive(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        loop {
            let run = self.inner.database.agent_run(run.id).await?;
            match run.status {
                AgentRunStatus::Completed => {
                    let output = run.output.ok_or_else(|| {
                        ApiError::unavailable(
                            "shadow_output_missing",
                            "the Shadow Persona completed without a suggestion",
                        )
                    })?;
                    self.inner
                        .database
                        .complete_shadow_suggestion(suggestion_id, &output)
                        .await?;
                    return Ok(());
                }
                AgentRunStatus::Failed | AgentRunStatus::Cancelled => {
                    return Err(ApiError::unavailable(
                        "shadow_run_failed",
                        run.last_error.unwrap_or_else(|| {
                            format!("Shadow Persona run ended as {:?}", run.status)
                        }),
                    ));
                }
                AgentRunStatus::WaitingForApproval => {
                    return Err(ApiError::conflict(
                        "shadow_tool_approval_forbidden",
                        "Shadow Persona agents cannot request side-effecting tools",
                    ));
                }
                AgentRunStatus::Queued | AgentRunStatus::Running | AgentRunStatus::Suspended => {}
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct CrewCoordinator {
    inner: Arc<CrewCoordinatorInner>,
}

struct CrewCoordinatorInner {
    database: northbridge_storage::Database,
    agents: AgentRuntimeRegistry,
    tasks: Mutex<HashMap<Uuid, JoinHandle<()>>>,
    shutdown: CancellationToken,
}

impl CrewCoordinator {
    pub(crate) fn new(
        database: northbridge_storage::Database,
        agents: AgentRuntimeRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(CrewCoordinatorInner {
                database,
                agents,
                tasks: Mutex::new(HashMap::new()),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub(crate) async fn start(
        &self,
        workspace_id: Uuid,
        crew_id: Uuid,
        prompt: String,
    ) -> Result<CrewRun, ApiError> {
        if self.inner.agents.current().is_none() {
            return Err(ApiError::unavailable(
                "agent_runtime_unavailable",
                "select a local model before starting a Crew",
            ));
        }
        let run = self
            .inner
            .database
            .create_crew_run(NewCrewRun {
                workspace_id,
                crew_id,
                prompt,
            })
            .await?;
        self.spawn(run.id).await;
        Ok(run)
    }

    pub(crate) async fn recover(&self, workspace_id: Uuid) -> Result<usize, ApiError> {
        if self.inner.agents.current().is_none() {
            return Ok(0);
        }
        let runs = self
            .inner
            .database
            .list_recoverable_crew_runs(workspace_id)
            .await?;
        let count = runs.len();
        for run in runs {
            self.spawn(run.id).await;
        }
        Ok(count)
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.shutdown.cancel();
        let tasks = {
            let mut tasks = self.inner.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };
        tokio::time::timeout(timeout, async move {
            for (_, task) in tasks {
                let _ = task.await;
            }
        })
        .await
        .is_ok()
    }

    async fn spawn(&self, crew_run_id: Uuid) {
        let mut tasks = self.inner.tasks.lock().await;
        tasks.retain(|_, task| !task.is_finished());
        if tasks.contains_key(&crew_run_id) || self.inner.shutdown.is_cancelled() {
            return;
        }
        let coordinator = self.clone();
        let cancellation = self.inner.shutdown.child_token();
        tasks.insert(
            crew_run_id,
            tokio::spawn(async move {
                if let Err(error) = coordinator.drive(crew_run_id, &cancellation).await
                    && !cancellation.is_cancelled()
                {
                    let mut message = error.to_string();
                    truncate_utf8(&mut message, 4096);
                    let _ = coordinator
                        .inner
                        .database
                        .finish_crew_run(crew_run_id, None, Some(&message))
                        .await;
                    tracing::error!(%crew_run_id, %error, "Crew run failed");
                }
            }),
        );
    }

    async fn drive(
        &self,
        crew_run_id: Uuid,
        cancellation: &CancellationToken,
    ) -> Result<(), ApiError> {
        let mut crew_run = self.inner.database.crew_run(crew_run_id).await?;
        if crew_run.status.is_terminal() {
            return Ok(());
        }
        if crew_run
            .members
            .iter()
            .any(|member| member.status == "unknown_outcome")
        {
            self.inner
                .database
                .finish_crew_run(
                    crew_run_id,
                    None,
                    Some("a remote member outcome is unknown after restart; the call was not retried"),
                )
                .await?;
            return Ok(());
        }
        if crew_run.status != CrewRunStatus::Running {
            crew_run = self.inner.database.start_crew_run(crew_run_id).await?;
        }
        let crew = self.inner.database.crew(crew_run.crew_id).await?;
        let runtime = self.inner.agents.current().ok_or_else(|| {
            ApiError::unavailable(
                "agent_runtime_unavailable",
                "the local agent runtime is unavailable",
            )
        })?;
        let mut member_outputs = Vec::with_capacity(crew_run.members.len());
        for member in crew_run.members.clone() {
            if member.status == "completed" {
                member_outputs.push((member.role, member.output.unwrap_or_default()));
                continue;
            }
            if member.status == "failed" || member.status == "unknown_outcome" {
                return Err(ApiError::conflict(
                    "crew_member_failed",
                    member
                        .last_error
                        .unwrap_or_else(|| format!("Crew member {} failed", member.ordinal)),
                ));
            }
            let prompt = crew_member_prompt(&member.role, &crew_run.prompt);
            let output = if let Some(agent_id) = member.local_agent_id {
                let agent_run = match member.agent_run_id {
                    Some(run_id) => {
                        let stored = self.inner.database.agent_run(run_id).await?;
                        if !stored.status.is_terminal() {
                            match runtime.resume_run(run_id).await {
                                Ok(_) | Err(AgentRuntimeError::AlreadyActive(_)) => {}
                                Err(error) => return Err(error.into()),
                            }
                        }
                        stored
                    }
                    None => {
                        let started = runtime
                            .start_run(StartAgentRun {
                                workspace_id: crew_run.workspace_id,
                                agent_id,
                                conversation_id: None,
                                prompt,
                            })
                            .await?;
                        self.inner
                            .database
                            .start_local_crew_member(crew_run_id, member.ordinal, started.id)
                            .await?;
                        started
                    }
                };
                match wait_for_agent_output(
                    &self.inner.database,
                    &runtime,
                    agent_run.id,
                    cancellation,
                )
                .await
                {
                    Ok(output) => {
                        self.inner
                            .database
                            .finish_crew_member(FinishCrewMember {
                                crew_run_id,
                                ordinal: member.ordinal,
                                output: Some(&output),
                                response: None,
                                remote_task_id: None,
                                remote_context_id: None,
                                error: None,
                            })
                            .await?;
                        output
                    }
                    Err(error) => {
                        let message = error.to_string();
                        self.inner
                            .database
                            .finish_crew_member(FinishCrewMember {
                                crew_run_id,
                                ordinal: member.ordinal,
                                output: None,
                                response: None,
                                remote_task_id: None,
                                remote_context_id: None,
                                error: Some(&message),
                            })
                            .await?;
                        return Err(error);
                    }
                }
            } else if let Some(remote_agent_id) = member.remote_agent_id {
                self.inner
                    .database
                    .start_remote_crew_member(crew_run_id, member.ordinal)
                    .await?;
                let response = tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    response = runtime.run_remote_agent(remote_agent_id, &prompt) => response,
                };
                let response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        let message = error.to_string();
                        self.inner
                            .database
                            .finish_crew_member(FinishCrewMember {
                                crew_run_id,
                                ordinal: member.ordinal,
                                output: None,
                                response: None,
                                remote_task_id: None,
                                remote_context_id: None,
                                error: Some(&message),
                            })
                            .await?;
                        return Err(error.into());
                    }
                };
                self.inner
                    .database
                    .finish_crew_member(FinishCrewMember {
                        crew_run_id,
                        ordinal: member.ordinal,
                        output: Some(&response.output),
                        response: Some(&response.response),
                        remote_task_id: response.task_id.as_deref(),
                        remote_context_id: response.context_id.as_deref(),
                        error: None,
                    })
                    .await?;
                response.output
            } else {
                return Err(ApiError::invalid(
                    "Crew member has neither a local nor a remote agent",
                ));
            };
            member_outputs.push((member.role, output));
        }
        let synthesis_prompt = crew_coordinator_prompt(&crew_run.prompt, &member_outputs);
        let coordinator_run = match crew_run.coordinator_run_id {
            Some(run_id) => {
                let stored = self.inner.database.agent_run(run_id).await?;
                if !stored.status.is_terminal() {
                    match runtime.resume_run(run_id).await {
                        Ok(_) | Err(AgentRuntimeError::AlreadyActive(_)) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                stored
            }
            None => {
                let started = runtime
                    .start_run(StartAgentRun {
                        workspace_id: crew_run.workspace_id,
                        agent_id: crew.coordinator_agent_id,
                        conversation_id: None,
                        prompt: synthesis_prompt,
                    })
                    .await?;
                self.inner
                    .database
                    .attach_crew_coordinator_run(crew_run_id, started.id)
                    .await?;
                started
            }
        };
        let output = wait_for_agent_output(
            &self.inner.database,
            &runtime,
            coordinator_run.id,
            cancellation,
        )
        .await?;
        self.inner
            .database
            .finish_crew_run(crew_run_id, Some(&output), None)
            .await?;
        Ok(())
    }
}

async fn wait_for_agent_output(
    database: &northbridge_storage::Database,
    runtime: &RigAgentRuntime,
    run_id: Uuid,
    cancellation: &CancellationToken,
) -> Result<String, ApiError> {
    loop {
        let run = database.agent_run(run_id).await?;
        match run.status {
            AgentRunStatus::Completed => {
                return run.output.ok_or_else(|| {
                    ApiError::unavailable(
                        "agent_output_missing",
                        "an agent completed without text output",
                    )
                });
            }
            AgentRunStatus::Failed | AgentRunStatus::Cancelled => {
                return Err(ApiError::unavailable(
                    "agent_run_failed",
                    run.last_error
                        .unwrap_or_else(|| format!("agent run ended as {:?}", run.status)),
                ));
            }
            AgentRunStatus::Suspended => match runtime.resume_run(run.id).await {
                Ok(_) | Err(AgentRuntimeError::AlreadyActive(_)) => {}
                Err(error) => return Err(error.into()),
            },
            AgentRunStatus::Queued
            | AgentRunStatus::Running
            | AgentRunStatus::WaitingForApproval => {}
        }
        tokio::select! {
            () = cancellation.cancelled() => return Err(ApiError::unavailable(
                "crew_run_interrupted",
                "Crew execution was interrupted and will be recovered on restart",
            )),
            () = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

pub(crate) fn agent_routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/v1/agents/personas",
            get(list_personas).post(create_persona),
        )
        .route("/v1/agents/personas/{persona_id}", get(get_persona))
        .route("/v1/agents/skills", get(list_skills).post(create_skill))
        .route("/v1/agents/skills/{skill_id}", get(get_skill))
        .route("/v1/agents", get(list_agents).post(create_agent))
        .route("/v1/agents/{agent_id}", get(get_agent))
        .route(
            "/v1/agents/mcp/servers",
            get(list_mcp_servers).post(upsert_mcp_server),
        )
        .route("/v1/agents/mcp/servers/{server_id}", get(get_mcp_server))
        .route(
            "/v1/agents/mcp/servers/{server_id}/tools",
            get(list_mcp_tools),
        )
        .route("/v1/agents/mcp/sync", post(sync_agentgateway_protocols))
        .route("/v1/agents/runs", get(list_runs).post(start_run))
        .route("/v1/agents/runs/{run_id}", get(get_run))
        .route("/v1/agents/runs/{run_id}/events", get(list_run_events))
        .route("/v1/agents/runs/{run_id}/resume", post(resume_run))
        .route("/v1/agents/runs/{run_id}/cancel", post(cancel_run))
        .route("/v1/agents/approvals", get(list_pending_approvals))
        .route(
            "/v1/agents/approvals/{approval_id}/decision",
            post(decide_approval),
        )
        .route(
            "/v1/agents/remotes",
            get(list_remote_agents).post(upsert_remote_agent),
        )
        .route("/v1/agents/remotes/{remote_id}", get(get_remote_agent))
        .route(
            "/v1/agents/remotes/{remote_id}/discover",
            post(discover_remote_agent),
        )
        .route("/v1/agents/crews", get(list_crews).post(create_crew))
        .route("/v1/agents/crews/{crew_id}", get(get_crew))
        .route("/v1/agents/crew-runs", get(list_crew_runs))
        .route("/v1/agents/crew-runs/{crew_run_id}", get(get_crew_run))
        .route("/v1/agents/crews/{crew_id}/runs", post(start_crew_run))
        .route("/v1/agents/shadows", get(list_shadows).post(create_shadow))
        .route("/v1/agents/shadows/{shadow_id}", get(get_shadow))
        .route(
            "/v1/agents/shadows/{shadow_id}/suggestions",
            get(list_shadow_suggestions),
        )
        .route(
            "/v1/agents/shadow-suggestions/{suggestion_id}/decision",
            post(decide_shadow_suggestion),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatePersonaRequest {
    name: String,
    #[serde(default)]
    description: String,
    system_prompt: String,
}

async fn create_persona(
    State(state): State<ApiState>,
    Json(request): Json<CreatePersonaRequest>,
) -> Result<Json<Persona>, ApiError> {
    Ok(Json(
        state
            .database
            .create_persona(NewPersona {
                workspace_id: workspace_id()?,
                name: request.name,
                description: request.description,
                system_prompt: request.system_prompt,
            })
            .await?,
    ))
}

async fn list_personas(State(state): State<ApiState>) -> Result<Json<Vec<Persona>>, ApiError> {
    Ok(Json(state.database.list_personas(workspace_id()?).await?))
}

async fn get_persona(
    State(state): State<ApiState>,
    Path(persona_id): Path<Uuid>,
) -> Result<Json<Persona>, ApiError> {
    Ok(Json(state.database.persona(persona_id).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSkillRequest {
    slug: String,
    name: String,
    #[serde(default)]
    description: String,
    instructions: String,
    #[serde(default = "empty_object")]
    permission_manifest: Value,
}

async fn create_skill(
    State(state): State<ApiState>,
    Json(request): Json<CreateSkillRequest>,
) -> Result<Json<Skill>, ApiError> {
    Ok(Json(
        state
            .database
            .create_skill(NewSkill {
                workspace_id: workspace_id()?,
                slug: request.slug,
                name: request.name,
                description: request.description,
                instructions: request.instructions,
                permission_manifest: request.permission_manifest,
            })
            .await?,
    ))
}

async fn list_skills(State(state): State<ApiState>) -> Result<Json<Vec<Skill>>, ApiError> {
    Ok(Json(state.database.list_skills(workspace_id()?).await?))
}

async fn get_skill(
    State(state): State<ApiState>,
    Path(skill_id): Path<Uuid>,
) -> Result<Json<Skill>, ApiError> {
    Ok(Json(state.database.skill(skill_id).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAgentRequest {
    persona_id: Option<Uuid>,
    name: String,
    #[serde(default)]
    description: String,
    model_alias: String,
    #[serde(default)]
    instructions: String,
    #[serde(default)]
    policy_mode: ToolPolicyMode,
    #[serde(default = "default_max_turns")]
    max_turns: u32,
    #[serde(default)]
    skill_ids: Vec<Uuid>,
    #[serde(default)]
    knowledge_stack_ids: Vec<Uuid>,
    #[serde(default)]
    tool_allowlist: Vec<AgentToolPermission>,
}

async fn create_agent(
    State(state): State<ApiState>,
    Json(request): Json<CreateAgentRequest>,
) -> Result<Json<AgentProfile>, ApiError> {
    Ok(Json(
        state
            .database
            .create_agent_profile(NewAgentProfile {
                workspace_id: workspace_id()?,
                persona_id: request.persona_id,
                name: request.name,
                description: request.description,
                model_alias: request.model_alias,
                instructions: request.instructions,
                policy_mode: request.policy_mode,
                max_turns: request.max_turns,
                skill_ids: request.skill_ids,
                knowledge_stack_ids: request.knowledge_stack_ids,
                tool_allowlist: request.tool_allowlist,
            })
            .await?,
    ))
}

async fn list_agents(State(state): State<ApiState>) -> Result<Json<Vec<AgentProfile>>, ApiError> {
    Ok(Json(
        state.database.list_agent_profiles(workspace_id()?).await?,
    ))
}

async fn get_agent(
    State(state): State<ApiState>,
    Path(agent_id): Path<Uuid>,
) -> Result<Json<AgentProfile>, ApiError> {
    Ok(Json(state.database.agent_profile(agent_id).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpsertMcpServerRequest {
    name: String,
    transport: McpTransport,
    endpoint: Option<String>,
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "empty_object")]
    environment_references: Value,
    openapi_schema: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
}

async fn upsert_mcp_server(
    State(state): State<ApiState>,
    Json(request): Json<UpsertMcpServerRequest>,
) -> Result<Json<McpServer>, ApiError> {
    let server = state
        .database
        .upsert_mcp_server(NewMcpServer {
            workspace_id: workspace_id()?,
            name: request.name,
            transport: request.transport,
            endpoint: request.endpoint,
            command: request.command,
            args: request.args,
            environment_references: request.environment_references,
            openapi_schema: request.openapi_schema,
            enabled: request.enabled,
        })
        .await?;
    required_runtime(&state)?
        .synchronize_gateway_protocols(workspace_id()?)
        .await?;
    Ok(Json(state.database.mcp_server(server.id).await?))
}

async fn list_mcp_servers(State(state): State<ApiState>) -> Result<Json<Vec<McpServer>>, ApiError> {
    Ok(Json(
        state.database.list_mcp_servers(workspace_id()?).await?,
    ))
}

async fn get_mcp_server(
    State(state): State<ApiState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<McpServer>, ApiError> {
    Ok(Json(state.database.mcp_server(server_id).await?))
}

async fn list_mcp_tools(
    State(state): State<ApiState>,
    Path(server_id): Path<Uuid>,
) -> Result<Json<Vec<McpToolSnapshot>>, ApiError> {
    Ok(Json(state.database.list_mcp_tools(server_id).await?))
}

async fn sync_agentgateway_protocols(
    State(state): State<ApiState>,
) -> Result<Json<Vec<McpToolSnapshot>>, ApiError> {
    Ok(Json(
        required_runtime(&state)?
            .synchronize_gateway_protocols(workspace_id()?)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartRunRequest {
    agent_id: Uuid,
    conversation_id: Option<Uuid>,
    prompt: String,
}

async fn start_run(
    State(state): State<ApiState>,
    Json(request): Json<StartRunRequest>,
) -> Result<Json<AgentRun>, ApiError> {
    Ok(Json(
        required_runtime(&state)?
            .start_run(StartAgentRun {
                workspace_id: workspace_id()?,
                agent_id: request.agent_id,
                conversation_id: request.conversation_id,
                prompt: request.prompt,
            })
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunListQuery {
    limit: Option<u32>,
}

async fn list_runs(
    State(state): State<ApiState>,
    Query(query): Query<RunListQuery>,
) -> Result<Json<Vec<AgentRun>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_agent_runs(workspace_id()?, query.limit.unwrap_or(50))
            .await?,
    ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentRunDetail {
    run: AgentRun,
    tool_calls: Vec<AgentToolCall>,
    events: Vec<AgentRunEvent>,
}

async fn get_run(
    State(state): State<ApiState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<AgentRunDetail>, ApiError> {
    Ok(Json(AgentRunDetail {
        run: state.database.agent_run(run_id).await?,
        tool_calls: state.database.list_agent_tool_calls(run_id).await?,
        events: state.database.list_agent_run_events(run_id, 0).await?,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventQuery {
    after: Option<u64>,
}

async fn list_run_events(
    State(state): State<ApiState>,
    Path(run_id): Path<Uuid>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<AgentRunEvent>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_agent_run_events(run_id, query.after.unwrap_or(0))
            .await?,
    ))
}

async fn resume_run(
    State(state): State<ApiState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<AgentRun>, ApiError> {
    Ok(Json(required_runtime(&state)?.resume_run(run_id).await?))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelRunResponse {
    cancel_requested: bool,
}

async fn cancel_run(
    State(state): State<ApiState>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<CancelRunResponse>, ApiError> {
    Ok(Json(CancelRunResponse {
        cancel_requested: required_runtime(&state)?.cancel_run(run_id).await?,
    }))
}

async fn list_pending_approvals(
    State(state): State<ApiState>,
) -> Result<Json<Vec<AgentApproval>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_pending_approvals(workspace_id()?)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalDecisionRequest {
    approved: bool,
    note: Option<String>,
}

async fn decide_approval(
    State(state): State<ApiState>,
    Path(approval_id): Path<Uuid>,
    Json(request): Json<ApprovalDecisionRequest>,
) -> Result<Json<AgentRun>, ApiError> {
    Ok(Json(
        required_runtime(&state)?
            .decide_approval(approval_id, request.approved, request.note.as_deref())
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpsertRemoteAgentRequest {
    name: String,
    upstream_url: String,
    auth_environment_key: Option<String>,
    #[serde(default = "default_true")]
    enabled: bool,
}

async fn upsert_remote_agent(
    State(state): State<ApiState>,
    Json(request): Json<UpsertRemoteAgentRequest>,
) -> Result<Json<RemoteAgent>, ApiError> {
    let remote = state
        .database
        .upsert_remote_agent(NewRemoteAgent {
            workspace_id: workspace_id()?,
            name: request.name,
            upstream_url: request.upstream_url,
            auth_environment_key: request.auth_environment_key,
            enabled: request.enabled,
        })
        .await?;
    required_runtime(&state)?
        .synchronize_gateway_protocols(workspace_id()?)
        .await?;
    Ok(Json(remote))
}

async fn list_remote_agents(
    State(state): State<ApiState>,
) -> Result<Json<Vec<RemoteAgent>>, ApiError> {
    Ok(Json(
        state.database.list_remote_agents(workspace_id()?).await?,
    ))
}

async fn get_remote_agent(
    State(state): State<ApiState>,
    Path(remote_id): Path<Uuid>,
) -> Result<Json<RemoteAgent>, ApiError> {
    Ok(Json(state.database.remote_agent(remote_id).await?))
}

async fn discover_remote_agent(
    State(state): State<ApiState>,
    Path(remote_id): Path<Uuid>,
) -> Result<Json<RemoteAgent>, ApiError> {
    Ok(Json(
        required_runtime(&state)?
            .discover_remote_agent(remote_id)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateCrewRequest {
    name: String,
    #[serde(default)]
    description: String,
    coordinator_agent_id: Uuid,
    #[serde(default)]
    members: Vec<CrewMember>,
}

async fn create_crew(
    State(state): State<ApiState>,
    Json(request): Json<CreateCrewRequest>,
) -> Result<Json<Crew>, ApiError> {
    Ok(Json(
        state
            .database
            .create_crew(NewCrew {
                workspace_id: workspace_id()?,
                name: request.name,
                description: request.description,
                coordinator_agent_id: request.coordinator_agent_id,
                members: request.members,
            })
            .await?,
    ))
}

async fn list_crews(State(state): State<ApiState>) -> Result<Json<Vec<Crew>>, ApiError> {
    Ok(Json(state.database.list_crews(workspace_id()?).await?))
}

async fn get_crew(
    State(state): State<ApiState>,
    Path(crew_id): Path<Uuid>,
) -> Result<Json<Crew>, ApiError> {
    Ok(Json(state.database.crew(crew_id).await?))
}

#[derive(Debug, Deserialize)]
struct StartCrewRunRequest {
    prompt: String,
}

async fn start_crew_run(
    State(state): State<ApiState>,
    Path(crew_id): Path<Uuid>,
    Json(request): Json<StartCrewRunRequest>,
) -> Result<Json<CrewRun>, ApiError> {
    Ok(Json(
        state
            .crews
            .start(workspace_id()?, crew_id, request.prompt)
            .await?,
    ))
}

async fn list_crew_runs(State(state): State<ApiState>) -> Result<Json<Vec<CrewRun>>, ApiError> {
    Ok(Json(
        state.database.list_crew_runs(workspace_id()?, 100).await?,
    ))
}

async fn get_crew_run(
    State(state): State<ApiState>,
    Path(crew_run_id): Path<Uuid>,
) -> Result<Json<CrewRun>, ApiError> {
    Ok(Json(state.database.crew_run(crew_run_id).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateShadowRequest {
    conversation_id: Uuid,
    agent_id: Uuid,
}

async fn create_shadow(
    State(state): State<ApiState>,
    Json(request): Json<CreateShadowRequest>,
) -> Result<Json<ShadowSubscription>, ApiError> {
    let agent = state.database.agent_profile(request.agent_id).await?;
    if !agent.tool_allowlist.is_empty() {
        return Err(ApiError::invalid(
            "Shadow Persona agents must have an empty tool allowlist",
        ));
    }
    Ok(Json(
        state
            .database
            .create_shadow_subscription(NewShadowSubscription {
                workspace_id: workspace_id()?,
                conversation_id: request.conversation_id,
                agent_id: request.agent_id,
            })
            .await?,
    ))
}

async fn list_shadows(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ShadowSubscription>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_shadow_subscriptions(workspace_id()?)
            .await?,
    ))
}

async fn get_shadow(
    State(state): State<ApiState>,
    Path(shadow_id): Path<Uuid>,
) -> Result<Json<ShadowSubscription>, ApiError> {
    Ok(Json(state.database.shadow_subscription(shadow_id).await?))
}

async fn list_shadow_suggestions(
    State(state): State<ApiState>,
    Path(shadow_id): Path<Uuid>,
) -> Result<Json<Vec<ShadowSuggestion>>, ApiError> {
    state.database.shadow_subscription(shadow_id).await?;
    Ok(Json(
        state.database.list_shadow_suggestions(shadow_id).await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ShadowDecisionRequest {
    accepted: bool,
}

async fn decide_shadow_suggestion(
    State(state): State<ApiState>,
    Path(suggestion_id): Path<Uuid>,
    Json(request): Json<ShadowDecisionRequest>,
) -> Result<Json<ShadowSuggestion>, ApiError> {
    Ok(Json(
        state
            .database
            .decide_shadow_suggestion(suggestion_id, request.accepted)
            .await?,
    ))
}

fn required_runtime(state: &ApiState) -> Result<RigAgentRuntime, ApiError> {
    state.agents.current().ok_or_else(|| {
        ApiError::unavailable(
            "agent_runtime_unavailable",
            "select a local model before running agents or synchronizing tools",
        )
    })
}

fn workspace_id() -> Result<Uuid, ApiError> {
    Uuid::parse_str(DEFAULT_WORKSPACE_ID)
        .map_err(|_| ApiError::invalid("the built-in workspace ID is invalid"))
}

const fn default_true() -> bool {
    true
}

const fn default_max_turns() -> u32 {
    12
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn shadow_prompt(message: &Message) -> String {
    let content = message
        .parts
        .iter()
        .filter(|part| part.kind == MessagePartKind::Text)
        .map(|part| part.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let bounded = content.chars().take(16_000).collect::<String>();
    format!(
        "You are running as a silent Shadow Persona. Review the user's latest message and return one concise, actionable suggestion. Do not call tools and do not address the user directly.\n\nLatest message:\n{bounded}"
    )
}

fn crew_member_prompt(role: &str, prompt: &str) -> String {
    format!(
        "You are the {role} member of a coordinated Crew. Analyze the task from your specialty and return a concise contribution for the coordinator.\n\nTask:\n{prompt}"
    )
}

fn crew_coordinator_prompt(prompt: &str, outputs: &[(String, String)]) -> String {
    let contributions = outputs
        .iter()
        .map(|(role, output)| format!("## {role}\n{output}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    format!(
        "Synthesize the Crew's contributions into one coherent final answer. Resolve disagreements explicitly and answer the original task.\n\nOriginal task:\n{prompt}\n\nCrew contributions:\n{contributions}"
    )
}

fn read_runtime(
    runtime: &RwLock<Option<RigAgentRuntime>>,
) -> std::sync::RwLockReadGuard<'_, Option<RigAgentRuntime>> {
    runtime
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn truncate_utf8(value: &mut String, max_bytes: usize) {
    let mut boundary = value.len().min(max_bytes);
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
}

fn write_runtime(
    runtime: &RwLock<Option<RigAgentRuntime>>,
) -> std::sync::RwLockWriteGuard<'_, Option<RigAgentRuntime>> {
    runtime
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
