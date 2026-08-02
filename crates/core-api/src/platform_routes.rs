use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use northbridge_domain::{
    Artifact, ArtifactKind, EnvironmentDefinition, EnvironmentVariableRef, MediaJob, MediaModality,
    MediaProvider, NorthbridgePackage, PackageArtifact, PackageEnvironment,
    PackageEnvironmentVariable, WorkflowApproval, WorkflowDefinition, WorkflowNode,
    WorkflowNodeKind, WorkflowRun, credential_environment_key,
};
use northbridge_storage::{
    NewAgentProfile, NewArtifact, NewCredentialReference, NewEnvironment, NewMcpServer,
    NewMediaProvider, NewPersona, NewRemoteAgent, NewSkill, NewWorkflow,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    error::ApiError,
    platform::{delete_credential_secret, store_credential_secret},
    routes::ApiState,
};

pub(crate) fn platform_routes() -> Router<ApiState> {
    Router::new()
        .route("/v1/workflows", get(list_workflows).post(create_workflow))
        .route("/v1/workflows/{workflow_id}", get(get_workflow))
        .route("/v1/workflows/{workflow_id}/runs", post(start_workflow))
        .route("/v1/workflow-runs", get(list_workflow_runs))
        .route("/v1/workflow-runs/{run_id}", get(get_workflow_run))
        .route(
            "/v1/workflow-runs/{run_id}/nodes",
            get(list_workflow_node_runs),
        )
        .route("/v1/workflow-runs/{run_id}/pause", post(pause_workflow))
        .route("/v1/workflow-runs/{run_id}/resume", post(resume_workflow))
        .route("/v1/workflow-runs/{run_id}/retry", post(retry_workflow))
        .route("/v1/workflow-runs/{run_id}/jump", post(jump_workflow))
        .route("/v1/workflow-runs/{run_id}/cancel", post(cancel_workflow))
        .route("/v1/workflow-approvals", get(list_workflow_approvals))
        .route(
            "/v1/workflow-approvals/{approval_id}/decision",
            post(decide_workflow_approval),
        )
        .route("/v1/artifacts", get(list_artifacts).post(create_artifact))
        .route(
            "/v1/artifacts/{artifact_id}",
            get(get_artifact)
                .post(revise_artifact)
                .delete(delete_artifact),
        )
        .route(
            "/v1/artifacts/{artifact_id}/content",
            get(download_artifact),
        )
        .route(
            "/v1/environments",
            get(list_environments).post(create_environment),
        )
        .route(
            "/v1/credentials",
            get(list_credentials).post(create_credential),
        )
        .route(
            "/v1/credentials/{credential_id}",
            post(update_credential).delete(delete_credential),
        )
        .route(
            "/v1/media/providers",
            get(list_media_providers).post(create_media_provider),
        )
        .route("/v1/media/providers/sync", post(sync_media_providers))
        .route(
            "/v1/media/jobs",
            get(list_media_jobs).post(create_media_job),
        )
        .route("/v1/media/jobs/{job_id}", get(get_media_job))
        .route("/v1/media/jobs/{job_id}/cancel", post(cancel_media_job))
        .route("/v1/packages/export", post(export_package))
        .route("/v1/packages/import", post(import_package))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateWorkflowRequest {
    name: String,
    #[serde(default)]
    description: String,
    entry_node_id: String,
    nodes: Vec<WorkflowNode>,
}

async fn create_workflow(
    State(state): State<ApiState>,
    Json(request): Json<CreateWorkflowRequest>,
) -> Result<(StatusCode, Json<WorkflowDefinition>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .database
                .create_workflow(NewWorkflow {
                    workspace_id: workspace_id()?,
                    name: request.name,
                    description: request.description,
                    entry_node_id: request.entry_node_id,
                    nodes: request.nodes,
                })
                .await?,
        ),
    ))
}

async fn list_workflows(
    State(state): State<ApiState>,
) -> Result<Json<Vec<WorkflowDefinition>>, ApiError> {
    Ok(Json(state.database.list_workflows(workspace_id()?).await?))
}

async fn get_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowDefinition>, ApiError> {
    Ok(Json(state.database.workflow(id).await?))
}

#[derive(Debug, Deserialize)]
struct StartWorkflowRequest {
    #[serde(default)]
    input: Value,
}

async fn start_workflow(
    State(state): State<ApiState>,
    Path(workflow_id): Path<Uuid>,
    Json(request): Json<StartWorkflowRequest>,
) -> Result<(StatusCode, Json<WorkflowRun>), ApiError> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .workflows
                .start(workflow_id, workspace_id()?, request.input, None)
                .await
                .map_err(workflow_error)?,
        ),
    ))
}

async fn list_workflow_runs(
    State(state): State<ApiState>,
) -> Result<Json<Vec<WorkflowRun>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_workflow_runs(workspace_id()?, 200)
            .await?,
    ))
}

async fn get_workflow_run(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(state.database.workflow_run(id).await?))
}

async fn list_workflow_node_runs(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<northbridge_domain::WorkflowNodeRun>>, ApiError> {
    state.database.workflow_run(id).await?;
    Ok(Json(state.database.list_workflow_node_runs(id).await?))
}

async fn pause_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state.workflows.pause(id).await.map_err(workflow_error)?,
    ))
}

async fn resume_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state
            .workflows
            .resume(id, None)
            .await
            .map_err(workflow_error)?,
    ))
}

async fn retry_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state
            .workflows
            .resume(id, None)
            .await
            .map_err(workflow_error)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JumpWorkflowRequest {
    node_id: String,
}

async fn jump_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(request): Json<JumpWorkflowRequest>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state
            .workflows
            .resume(id, Some(&request.node_id))
            .await
            .map_err(workflow_error)?,
    ))
}

async fn cancel_workflow(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state.workflows.cancel(id).await.map_err(workflow_error)?,
    ))
}

async fn list_workflow_approvals(
    State(state): State<ApiState>,
) -> Result<Json<Vec<WorkflowApproval>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_pending_workflow_approvals(workspace_id()?)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
struct WorkflowApprovalDecisionRequest {
    approved: bool,
    note: Option<String>,
}

async fn decide_workflow_approval(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(request): Json<WorkflowApprovalDecisionRequest>,
) -> Result<Json<WorkflowRun>, ApiError> {
    Ok(Json(
        state
            .workflows
            .decide_approval(id, request.approved, None, request.note.as_deref())
            .await
            .map_err(workflow_error)?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactRequest {
    name: String,
    kind: ArtifactKind,
    media_type: String,
    #[serde(default = "default_artifact_source")]
    source: String,
    #[serde(default)]
    metadata: Value,
    content_base64: String,
}

async fn create_artifact(
    State(state): State<ApiState>,
    Json(request): Json<ArtifactRequest>,
) -> Result<(StatusCode, Json<Artifact>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .database
                .create_artifact(artifact_input(request)?)
                .await?,
        ),
    ))
}

async fn revise_artifact(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(request): Json<ArtifactRequest>,
) -> Result<Json<Artifact>, ApiError> {
    Ok(Json(
        state
            .database
            .revise_artifact(id, artifact_input(request)?)
            .await?,
    ))
}

fn artifact_input(request: ArtifactRequest) -> Result<NewArtifact, ApiError> {
    let content = STANDARD
        .decode(request.content_base64)
        .map_err(|error| ApiError::invalid(format!("artifact content is not base64: {error}")))?;
    Ok(NewArtifact {
        workspace_id: workspace_id()?,
        name: request.name,
        kind: request.kind,
        media_type: request.media_type,
        source: request.source,
        metadata: request.metadata,
        content,
    })
}

async fn list_artifacts(State(state): State<ApiState>) -> Result<Json<Vec<Artifact>>, ApiError> {
    Ok(Json(state.database.list_artifacts(workspace_id()?).await?))
}

async fn get_artifact(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Artifact>, ApiError> {
    Ok(Json(state.database.artifact(id).await?))
}

async fn download_artifact(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let (artifact, content) = state.database.artifact_content(id).await?;
    let content_type = HeaderValue::from_str(&artifact.media_type)
        .map_err(|_| ApiError::invalid("stored artifact media type is invalid"))?;
    let mut response = Response::new(Body::from(content));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", artifact.sha256))
            .map_err(|_| ApiError::invalid("stored artifact hash is invalid"))?,
    );
    Ok(response)
}

async fn delete_artifact(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.database.delete_artifact(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct CredentialRequest {
    name: String,
    secret: String,
}

async fn create_credential(
    State(state): State<ApiState>,
    Json(request): Json<CredentialRequest>,
) -> Result<(StatusCode, Json<northbridge_domain::CredentialReference>), ApiError> {
    let reference = state
        .database
        .create_credential_reference(NewCredentialReference {
            workspace_id: workspace_id()?,
            name: request.name,
        })
        .await?;
    let secret = request.secret;
    if let Err(error) = store_credential_secret(&reference, secret.clone()).await {
        let _ = state
            .database
            .delete_credential_reference(reference.id)
            .await;
        return Err(ApiError::unavailable(
            "credential_store_unavailable",
            error.to_string(),
        ));
    }
    state.ai_factory.set_environment(
        credential_environment_key(reference.id).into(),
        secret.into(),
    );
    Ok((StatusCode::CREATED, Json(reference)))
}

#[derive(Debug, Deserialize)]
struct UpdateCredentialRequest {
    secret: String,
}

async fn update_credential(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateCredentialRequest>,
) -> Result<Json<northbridge_domain::CredentialReference>, ApiError> {
    let reference = state.database.credential_reference(id).await?;
    let secret = request.secret;
    store_credential_secret(&reference, secret.clone())
        .await
        .map_err(|error| {
            ApiError::unavailable("credential_store_unavailable", error.to_string())
        })?;
    state.ai_factory.set_environment(
        credential_environment_key(reference.id).into(),
        secret.into(),
    );
    Ok(Json(reference))
}

async fn list_credentials(
    State(state): State<ApiState>,
) -> Result<Json<Vec<northbridge_domain::CredentialReference>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_credential_references(workspace_id()?)
            .await?,
    ))
}

async fn delete_credential(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let reference = state.database.credential_reference(id).await?;
    delete_credential_secret(&reference)
        .await
        .map_err(|error| {
            ApiError::unavailable("credential_store_unavailable", error.to_string())
        })?;
    state.database.delete_credential_reference(id).await?;
    state
        .ai_factory
        .remove_environment(&credential_environment_key(id).into());
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnvironmentRequest {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    variables: Vec<EnvironmentVariableRef>,
}

async fn create_environment(
    State(state): State<ApiState>,
    Json(request): Json<EnvironmentRequest>,
) -> Result<(StatusCode, Json<EnvironmentDefinition>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(
            state
                .database
                .create_environment(NewEnvironment {
                    workspace_id: workspace_id()?,
                    name: request.name,
                    description: request.description,
                    variables: request.variables,
                })
                .await?,
        ),
    ))
}

async fn list_environments(
    State(state): State<ApiState>,
) -> Result<Json<Vec<EnvironmentDefinition>>, ApiError> {
    Ok(Json(
        state.database.list_environments(workspace_id()?).await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MediaProviderRequest {
    name: String,
    modality: MediaModality,
    upstream_url: String,
    credential_id: Option<Uuid>,
    #[serde(default = "default_true")]
    enabled: bool,
}

async fn create_media_provider(
    State(state): State<ApiState>,
    Json(request): Json<MediaProviderRequest>,
) -> Result<(StatusCode, Json<MediaProvider>), ApiError> {
    let provider = state
        .database
        .create_media_provider(NewMediaProvider {
            workspace_id: workspace_id()?,
            name: request.name,
            modality: request.modality,
            upstream_url: request.upstream_url,
            credential_id: request.credential_id,
            enabled: request.enabled,
        })
        .await?;
    if let Err(error) = state.media.synchronize_gateway_routes().await
        && !matches!(error, crate::platform::MediaError::GatewayUnavailable)
    {
        return Err(ApiError::unavailable(
            "media_gateway_configuration_failed",
            error.to_string(),
        ));
    }
    Ok((StatusCode::CREATED, Json(provider)))
}

async fn list_media_providers(
    State(state): State<ApiState>,
) -> Result<Json<Vec<MediaProvider>>, ApiError> {
    Ok(Json(
        state.database.list_media_providers(workspace_id()?).await?,
    ))
}

async fn sync_media_providers(State(state): State<ApiState>) -> Result<StatusCode, ApiError> {
    state
        .media
        .synchronize_gateway_routes()
        .await
        .map_err(media_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MediaJobRequest {
    provider_id: Uuid,
    prompt: String,
    #[serde(default)]
    request: Value,
}

async fn create_media_job(
    State(state): State<ApiState>,
    Json(request): Json<MediaJobRequest>,
) -> Result<(StatusCode, Json<MediaJob>), ApiError> {
    let mut payload = request.request;
    if let Some(object) = payload.as_object_mut() {
        object
            .entry("prompt".to_string())
            .or_insert_with(|| Value::String(request.prompt.clone()));
    } else {
        payload = json!({ "prompt": request.prompt });
    }
    let job = state
        .database
        .create_media_job(
            request.provider_id,
            workspace_id()?,
            &request.prompt,
            &payload,
        )
        .await?;
    let job = state.media.start_job(job).await.map_err(media_error)?;
    Ok((StatusCode::ACCEPTED, Json(job)))
}

async fn list_media_jobs(State(state): State<ApiState>) -> Result<Json<Vec<MediaJob>>, ApiError> {
    Ok(Json(state.database.list_media_jobs(workspace_id()?).await?))
}

async fn get_media_job(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaJob>, ApiError> {
    Ok(Json(state.database.media_job(id).await?))
}

async fn cancel_media_job(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<MediaJob>, ApiError> {
    Ok(Json(state.media.cancel_job(id).await.map_err(media_error)?))
}

#[derive(Debug, Deserialize)]
struct ExportPackageRequest {
    name: String,
    #[serde(default)]
    description: String,
}

async fn export_package(
    State(state): State<ApiState>,
    Json(request): Json<ExportPackageRequest>,
) -> Result<Json<NorthbridgePackage>, ApiError> {
    let workspace_id = workspace_id()?;
    let credentials = state
        .database
        .list_credential_references(workspace_id)
        .await?
        .into_iter()
        .map(|credential| (credential.id, credential.name))
        .collect::<BTreeMap<_, _>>();
    let environments = state
        .database
        .list_environments(workspace_id)
        .await?
        .into_iter()
        .map(|environment| PackageEnvironment {
            name: environment.name,
            description: environment.description,
            variables: environment
                .variables
                .into_iter()
                .filter_map(|variable| {
                    credentials
                        .get(&variable.credential_id)
                        .map(|credential_name| PackageEnvironmentVariable {
                            name: variable.name,
                            credential_name: credential_name.clone(),
                        })
                })
                .collect(),
        })
        .collect();
    let mcp_servers = state
        .database
        .list_mcp_servers(workspace_id)
        .await?
        .into_iter()
        .map(|mut server| {
            server.environment_references = json!({});
            server
        })
        .collect();
    let mut artifacts = Vec::new();
    for metadata in state.database.list_artifacts(workspace_id).await? {
        let (_, content) = state.database.artifact_content(metadata.id).await?;
        artifacts.push(PackageArtifact {
            metadata,
            content_base64: STANDARD.encode(content),
        });
    }
    let package = NorthbridgePackage {
        schema_version: 1,
        name: request.name,
        description: request.description,
        personas: state.database.list_personas(workspace_id).await?,
        skills: state.database.list_skills(workspace_id).await?,
        agents: state.database.list_agent_profiles(workspace_id).await?,
        mcp_servers,
        remote_agents: state.database.list_remote_agents(workspace_id).await?,
        workflows: state.database.list_workflows(workspace_id).await?,
        environments,
        artifacts,
    };
    let bytes = serde_json::to_vec(&package)
        .map_err(|error| ApiError::invalid(format!("package serialization failed: {error}")))?;
    state
        .database
        .record_package_operation(
            workspace_id,
            "export",
            &package.name,
            &bytes,
            &json!({
                "personas": package.personas.len(),
                "skills": package.skills.len(),
                "agents": package.agents.len(),
                "mcpServers": package.mcp_servers.len(),
                "remoteAgents": package.remote_agents.len(),
                "workflows": package.workflows.len(),
                "environments": package.environments.len(),
                "artifacts": package.artifacts.len(),
            }),
            None,
        )
        .await?;
    Ok(Json(package))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportPackageResponse {
    operation_id: Uuid,
    personas: usize,
    skills: usize,
    agents: usize,
    workflows: usize,
    environments: usize,
    artifacts: usize,
    credentials_requiring_values: Vec<northbridge_domain::CredentialReference>,
}

async fn import_package(
    State(state): State<ApiState>,
    Json(package): Json<NorthbridgePackage>,
) -> Result<(StatusCode, Json<ImportPackageResponse>), ApiError> {
    validate_package(&package)?;
    let workspace_id = workspace_id()?;
    let bytes = serde_json::to_vec(&package)
        .map_err(|error| ApiError::invalid(format!("package serialization failed: {error}")))?;
    let mut persona_ids = BTreeMap::new();
    for persona in &package.personas {
        let created = state
            .database
            .create_persona(NewPersona {
                workspace_id,
                name: persona.name.clone(),
                description: persona.description.clone(),
                system_prompt: persona.system_prompt.clone(),
            })
            .await?;
        persona_ids.insert(persona.id, created.id);
    }
    let mut skill_ids = BTreeMap::new();
    for skill in &package.skills {
        let created = state
            .database
            .create_skill(NewSkill {
                workspace_id,
                slug: skill.slug.clone(),
                name: skill.name.clone(),
                description: skill.description.clone(),
                instructions: skill.instructions.clone(),
                permission_manifest: skill.permission_manifest.clone(),
            })
            .await?;
        skill_ids.insert(skill.id, created.id);
    }
    let mut agent_ids = BTreeMap::new();
    let mut mcp_server_ids = BTreeMap::new();
    for server in &package.mcp_servers {
        let created = state
            .database
            .upsert_mcp_server(NewMcpServer {
                workspace_id,
                name: server.name.clone(),
                transport: server.transport,
                endpoint: server.endpoint.clone(),
                command: server.command.clone(),
                args: server.args.clone(),
                environment_references: json!({}),
                openapi_schema: server.openapi_schema.clone(),
                enabled: server.enabled,
            })
            .await?;
        mcp_server_ids.insert(server.id, created.id);
    }
    let mut remote_agent_ids = BTreeMap::new();
    for remote in &package.remote_agents {
        let created = state
            .database
            .upsert_remote_agent(NewRemoteAgent {
                workspace_id,
                name: remote.name.clone(),
                upstream_url: remote.upstream_url.clone(),
                auth_environment_key: remote.auth_environment_key.clone(),
                enabled: remote.enabled,
            })
            .await?;
        remote_agent_ids.insert(remote.id, created.id);
    }
    for agent in &package.agents {
        let created = state
            .database
            .create_agent_profile(NewAgentProfile {
                workspace_id,
                persona_id: agent
                    .persona_id
                    .and_then(|id| persona_ids.get(&id).copied()),
                name: agent.name.clone(),
                description: agent.description.clone(),
                model_alias: agent.model_alias.clone(),
                instructions: agent.instructions.clone(),
                policy_mode: agent.policy_mode,
                max_turns: agent.max_turns,
                skill_ids: agent
                    .skill_ids
                    .iter()
                    .filter_map(|id| skill_ids.get(id).copied())
                    .collect(),
                knowledge_stack_ids: Vec::new(),
                tool_allowlist: agent
                    .tool_allowlist
                    .iter()
                    .filter_map(|permission| {
                        mcp_server_ids.get(&permission.server_id).map(|server_id| {
                            northbridge_domain::AgentToolPermission {
                                server_id: *server_id,
                                tool_name: permission.tool_name.clone(),
                            }
                        })
                    })
                    .collect(),
            })
            .await?;
        agent_ids.insert(agent.id, created.id);
    }
    let mut workflow_ids = BTreeMap::new();
    let mut remaining = package.workflows.clone();
    while !remaining.is_empty() {
        let position = remaining.iter().position(|workflow| {
            workflow_subworkflow_ids(&workflow.nodes)
                .iter()
                .all(|id| workflow_ids.contains_key(id))
        });
        let Some(position) = position else {
            return Err(ApiError::invalid(
                "package workflows contain a cyclic or missing subworkflow dependency",
            ));
        };
        let workflow = remaining.remove(position);
        let nodes =
            remap_workflow_nodes(workflow.nodes, &agent_ids, &remote_agent_ids, &workflow_ids)?;
        let created = state
            .database
            .create_workflow(NewWorkflow {
                workspace_id,
                name: workflow.name,
                description: workflow.description,
                entry_node_id: workflow.entry_node_id,
                nodes,
            })
            .await?;
        workflow_ids.insert(workflow.id, created.id);
    }
    let mut credential_ids = BTreeMap::new();
    let mut credentials_requiring_values = Vec::new();
    for environment in &package.environments {
        let mut variables = Vec::new();
        for variable in &environment.variables {
            let credential_key = variable.credential_name.to_ascii_lowercase();
            let credential_id = if let Some(id) = credential_ids.get(&credential_key) {
                *id
            } else {
                let credential = state
                    .database
                    .create_credential_reference(NewCredentialReference {
                        workspace_id,
                        name: variable.credential_name.clone(),
                    })
                    .await?;
                credential_ids.insert(credential_key, credential.id);
                credentials_requiring_values.push(credential.clone());
                credential.id
            };
            variables.push(EnvironmentVariableRef {
                name: variable.name.clone(),
                credential_id,
            });
        }
        state
            .database
            .create_environment(NewEnvironment {
                workspace_id,
                name: environment.name.clone(),
                description: environment.description.clone(),
                variables,
            })
            .await?;
    }
    for artifact in &package.artifacts {
        let content = STANDARD.decode(&artifact.content_base64).map_err(|error| {
            ApiError::invalid(format!("package artifact is not base64: {error}"))
        })?;
        state
            .database
            .create_artifact(NewArtifact {
                workspace_id,
                name: artifact.metadata.name.clone(),
                kind: artifact.metadata.kind,
                media_type: artifact.metadata.media_type.clone(),
                source: format!("package:{}", package.name),
                metadata: artifact.metadata.metadata.clone(),
                content,
            })
            .await?;
    }
    let summary = json!({
        "personas": persona_ids.len(),
        "skills": skill_ids.len(),
        "agents": agent_ids.len(),
        "mcpServers": mcp_server_ids.len(),
        "remoteAgents": remote_agent_ids.len(),
        "workflows": workflow_ids.len(),
        "environments": package.environments.len(),
        "artifacts": package.artifacts.len(),
        "credentialsRequiringValues": credentials_requiring_values.len(),
    });
    let operation_id = state
        .database
        .record_package_operation(
            workspace_id,
            "import",
            &package.name,
            &bytes,
            &summary,
            None,
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ImportPackageResponse {
            operation_id,
            personas: persona_ids.len(),
            skills: skill_ids.len(),
            agents: agent_ids.len(),
            workflows: workflow_ids.len(),
            environments: package.environments.len(),
            artifacts: package.artifacts.len(),
            credentials_requiring_values,
        }),
    ))
}

fn validate_package(package: &NorthbridgePackage) -> Result<(), ApiError> {
    if package.schema_version != 1 {
        return Err(ApiError::invalid("unsupported NorthBridge package schema"));
    }
    if package.name.trim().is_empty() || package.name.len() > 120 {
        return Err(ApiError::invalid(
            "package name must contain between 1 and 120 bytes",
        ));
    }
    let total = package.personas.len()
        + package.skills.len()
        + package.agents.len()
        + package.mcp_servers.len()
        + package.remote_agents.len()
        + package.workflows.len()
        + package.environments.len()
        + package.artifacts.len();
    if total > 2048 {
        return Err(ApiError::invalid("package contains too many records"));
    }
    let persona_ids = package
        .personas
        .iter()
        .map(|persona| persona.id)
        .collect::<BTreeSet<_>>();
    let skill_ids = package
        .skills
        .iter()
        .map(|skill| skill.id)
        .collect::<BTreeSet<_>>();
    let mcp_server_ids = package
        .mcp_servers
        .iter()
        .map(|server| server.id)
        .collect::<BTreeSet<_>>();
    let remote_agent_ids = package
        .remote_agents
        .iter()
        .map(|agent| agent.id)
        .collect::<BTreeSet<_>>();
    let agent_ids = package
        .agents
        .iter()
        .map(|agent| agent.id)
        .collect::<BTreeSet<_>>();
    let workflow_ids = package
        .workflows
        .iter()
        .map(|workflow| workflow.id)
        .collect::<BTreeSet<_>>();
    for agent in &package.agents {
        if agent
            .persona_id
            .is_some_and(|id| !persona_ids.contains(&id))
            || agent.skill_ids.iter().any(|id| !skill_ids.contains(id))
            || agent
                .tool_allowlist
                .iter()
                .any(|permission| !mcp_server_ids.contains(&permission.server_id))
        {
            return Err(ApiError::invalid(
                "package agent references an entity missing from the package",
            ));
        }
    }
    for workflow in &package.workflows {
        workflow
            .validate()
            .map_err(|error| ApiError::invalid(format!("invalid packaged workflow: {error}")))?;
        validate_workflow_package_refs(
            &workflow.nodes,
            &agent_ids,
            &remote_agent_ids,
            &workflow_ids,
        )?;
    }
    validate_package_workflow_dependencies(&package.workflows)?;
    let mut environment_names = BTreeSet::new();
    for environment in &package.environments {
        if environment.name.trim().is_empty()
            || environment.name.len() > 120
            || !environment_names.insert(environment.name.to_ascii_lowercase())
            || environment.variables.len() > 128
        {
            return Err(ApiError::invalid(
                "package environments have invalid or duplicate names or too many variables",
            ));
        }
        let mut variable_names = BTreeSet::new();
        for variable in &environment.variables {
            if !valid_environment_key(&variable.name)
                || !variable_names.insert(variable.name.as_str())
                || variable.credential_name.trim().is_empty()
                || variable.credential_name.len() > 120
            {
                return Err(ApiError::invalid(
                    "package environment contains an invalid variable or credential name",
                ));
            }
        }
    }
    for artifact in &package.artifacts {
        let content = STANDARD.decode(&artifact.content_base64).map_err(|error| {
            ApiError::invalid(format!("package artifact is not base64: {error}"))
        })?;
        if content.len() > 64 * 1024 * 1024 {
            return Err(ApiError::invalid(
                "package artifact content cannot exceed 64 MiB",
            ));
        }
        let digest = format!("{:x}", Sha256::digest(&content));
        if digest != artifact.metadata.sha256 {
            return Err(ApiError::invalid(
                "package artifact content does not match its SHA-256 metadata",
            ));
        }
    }
    Ok(())
}

fn validate_workflow_package_refs(
    nodes: &[WorkflowNode],
    agents: &BTreeSet<Uuid>,
    remote_agents: &BTreeSet<Uuid>,
    workflows: &BTreeSet<Uuid>,
) -> Result<(), ApiError> {
    for node in nodes {
        match &node.kind {
            WorkflowNodeKind::Prompt { agent_id, .. }
            | WorkflowNodeKind::Agent { agent_id, .. }
            | WorkflowNodeKind::Tool { agent_id, .. }
                if !agents.contains(agent_id) =>
            {
                return Err(ApiError::invalid(
                    "package workflow references an agent missing from the package",
                ));
            }
            WorkflowNodeKind::A2aTask {
                remote_agent_id, ..
            } if !remote_agents.contains(remote_agent_id) => {
                return Err(ApiError::invalid(
                    "package workflow references a remote agent missing from the package",
                ));
            }
            WorkflowNodeKind::Subworkflow { workflow_id, .. }
                if !workflows.contains(workflow_id) =>
            {
                return Err(ApiError::invalid(
                    "package workflow references a subworkflow missing from the package",
                ));
            }
            WorkflowNodeKind::Parallel { branches } => {
                for branch in branches {
                    validate_workflow_package_refs(branch, agents, remote_agents, workflows)?;
                }
            }
            WorkflowNodeKind::Loop { body, .. } => {
                validate_workflow_package_refs(body, agents, remote_agents, workflows)?;
            }
            WorkflowNodeKind::Prompt { .. }
            | WorkflowNodeKind::Agent { .. }
            | WorkflowNodeKind::Tool { .. }
            | WorkflowNodeKind::Condition { .. }
            | WorkflowNodeKind::Approval { .. }
            | WorkflowNodeKind::Transform { .. }
            | WorkflowNodeKind::A2aTask { .. }
            | WorkflowNodeKind::Subworkflow { .. } => {}
        }
    }
    Ok(())
}

fn validate_package_workflow_dependencies(
    workflows: &[WorkflowDefinition],
) -> Result<(), ApiError> {
    let mut unresolved = workflows
        .iter()
        .map(|workflow| {
            (
                workflow.id,
                workflow_subworkflow_ids(&workflow.nodes)
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut resolved = BTreeSet::new();
    while !unresolved.is_empty() {
        let ready = unresolved
            .iter()
            .filter(|(_, dependencies)| dependencies.is_subset(&resolved))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(ApiError::invalid(
                "package workflows contain a cyclic dependency",
            ));
        }
        for id in ready {
            unresolved.remove(&id);
            resolved.insert(id);
        }
    }
    Ok(())
}

fn valid_environment_key(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn workflow_subworkflow_ids(nodes: &[WorkflowNode]) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for node in nodes {
        match &node.kind {
            WorkflowNodeKind::Subworkflow { workflow_id, .. } => ids.push(*workflow_id),
            WorkflowNodeKind::Parallel { branches } => {
                for branch in branches {
                    ids.extend(workflow_subworkflow_ids(branch));
                }
            }
            WorkflowNodeKind::Loop { body, .. } => ids.extend(workflow_subworkflow_ids(body)),
            _ => {}
        }
    }
    ids
}

fn remap_workflow_nodes(
    nodes: Vec<WorkflowNode>,
    agents: &BTreeMap<Uuid, Uuid>,
    remote_agents: &BTreeMap<Uuid, Uuid>,
    workflows: &BTreeMap<Uuid, Uuid>,
) -> Result<Vec<WorkflowNode>, ApiError> {
    nodes
        .into_iter()
        .map(|mut node| {
            match &mut node.kind {
                WorkflowNodeKind::Prompt { agent_id, .. }
                | WorkflowNodeKind::Agent { agent_id, .. }
                | WorkflowNodeKind::Tool { agent_id, .. } => {
                    *agent_id = agents.get(agent_id).copied().ok_or_else(|| {
                        ApiError::invalid("workflow references an agent missing from the package")
                    })?;
                }
                WorkflowNodeKind::Parallel { branches } => {
                    for branch in branches {
                        *branch = remap_workflow_nodes(
                            std::mem::take(branch),
                            agents,
                            remote_agents,
                            workflows,
                        )?;
                    }
                }
                WorkflowNodeKind::Loop { body, .. } => {
                    *body = remap_workflow_nodes(
                        std::mem::take(body),
                        agents,
                        remote_agents,
                        workflows,
                    )?;
                }
                WorkflowNodeKind::Subworkflow { workflow_id, .. } => {
                    *workflow_id = workflows.get(workflow_id).copied().ok_or_else(|| {
                        ApiError::invalid("workflow references an unresolved subworkflow")
                    })?;
                }
                WorkflowNodeKind::A2aTask {
                    remote_agent_id, ..
                } => {
                    *remote_agent_id =
                        remote_agents.get(remote_agent_id).copied().ok_or_else(|| {
                            ApiError::invalid(
                                "workflow references a remote agent missing from the package",
                            )
                        })?;
                }
                WorkflowNodeKind::Condition { .. }
                | WorkflowNodeKind::Approval { .. }
                | WorkflowNodeKind::Transform { .. } => {}
            }
            Ok(node)
        })
        .collect()
}

fn workflow_error(error: crate::workflows::WorkflowExecutionError) -> ApiError {
    match error {
        crate::workflows::WorkflowExecutionError::Storage(error) => error.into(),
        crate::workflows::WorkflowExecutionError::UnknownOutcome(message) => {
            ApiError::conflict("workflow_unknown_outcome", message)
        }
        crate::workflows::WorkflowExecutionError::Invalid(message) => ApiError::invalid(message),
        crate::workflows::WorkflowExecutionError::Paused => {
            ApiError::conflict("workflow_paused", "workflow is paused")
        }
        crate::workflows::WorkflowExecutionError::Cancelled => {
            ApiError::conflict("workflow_cancelled", "workflow was cancelled")
        }
        crate::workflows::WorkflowExecutionError::Agent(error) => error.into(),
    }
}

pub(crate) fn media_error(error: crate::platform::MediaError) -> ApiError {
    match error {
        crate::platform::MediaError::Storage(error) => error.into(),
        crate::platform::MediaError::GatewayUnavailable => ApiError::unavailable(
            "media_gateway_unavailable",
            "select a local model before running media providers",
        ),
        crate::platform::MediaError::Cancelled => {
            ApiError::conflict("media_job_cancelled", "media job was cancelled")
        }
        error => ApiError::unavailable("media_provider_failure", error.to_string()),
    }
}

fn workspace_id() -> Result<Uuid, ApiError> {
    Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID)
        .map_err(|_| ApiError::invalid("the built-in workspace ID is invalid"))
}

fn default_artifact_source() -> String {
    "forge".to_string()
}

const fn default_true() -> bool {
    true
}
