use std::{
    collections::HashSet, convert::Infallible, ffi::OsString, path::PathBuf, time::Duration,
};

use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, Sse, sse::Event, sse::KeepAlive},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use northbridge_agent_runtime::{AgentRuntime, RigAgentRuntime, RigAgentRuntimeConfig};
use northbridge_ai_gateway::{LocalAiRuntimeStatus, MistralTuneProfile, MistralTuneRequest};
use northbridge_domain::{
    AiProvider, ConversationCursor, DEFAULT_WORKSPACE_ID, DESKTOP_SETTINGS_SCOPE, GenerationEvent,
    GenerationPerformanceDashboard, GenerationStatus, HealthStatus, IngestionJob,
    KnowledgeDocument, KnowledgeSearchResult, KnowledgeSourceKind, KnowledgeStack,
    KnowledgeWatchKind, KnowledgeWatchRoot, MODEL_SELECTION_SETTING_KEY, ManagedModel, MessageRole,
    ModelCatalogEntry, ModelLoadProfile, ModelSelection, ModelTuningProfile, NewMessage,
    NewMessagePart, SplitChatRun,
};
use northbridge_ingestion::chunker_hash;
use northbridge_model_manager::{DownloadModelRequest, ImportModelRequest, ModelManager};
use northbridge_storage::{Database, NewKnowledgeStack, now_ms};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    AiFactoryRegistry,
    agents::{AgentRuntimeRegistry, CrewCoordinator, ShadowCoordinator, agent_routes},
    auth::AuthToken,
    error::ApiError,
    events::GenerationEvents,
    generation::GenerationCoordinator,
    knowledge::KnowledgeRuntimeRegistry,
    platform::MediaCoordinator,
    platform_routes::platform_routes,
    runtime::{
        AiRuntimeRegistry, LOCAL_MODEL_ALIAS, gateway_quantization, gateway_runtime_profile,
        local_model_catalog, local_model_selection_with_profile,
    },
    workflows::WorkflowCoordinator,
};

const CONTROL_TOKEN_HEADER: &str = "x-northbridge-control-token";

#[derive(Clone)]
pub(crate) struct ApiState {
    pub(crate) database: Database,
    pub(crate) events: GenerationEvents,
    pub(crate) ai_runtime: AiRuntimeRegistry,
    pub(crate) ai_factory: AiFactoryRegistry,
    pub(crate) model_manager: Option<ModelManager>,
    pub(crate) generations: GenerationCoordinator,
    pub(crate) knowledge: KnowledgeRuntimeRegistry,
    pub(crate) agents: AgentRuntimeRegistry,
    pub(crate) shadows: ShadowCoordinator,
    pub(crate) crews: CrewCoordinator,
    pub(crate) workflows: WorkflowCoordinator,
    pub(crate) media: MediaCoordinator,
    pub(crate) control_token: AuthToken,
    pub(crate) shutdown: CancellationToken,
}

pub(crate) fn api_routes() -> Router<ApiState> {
    Router::new()
        .merge(agent_routes())
        .merge(platform_routes())
        .merge(crate::collaboration::collaboration_routes())
        .route("/v1/health", get(health))
        .route("/v1/ai/status", get(ai_status))
        .route("/v1/ai/chat/completions", post(ai_chat_completions))
        .route("/v1/hardware/diagnostics", get(hardware_diagnostics))
        .route("/v1/models/catalog", get(model_catalog))
        .route("/v1/models/tune", post(tune_model))
        .route("/v1/models/managed", get(managed_models))
        .route(
            "/v1/models/managed/{model_id}",
            get(managed_model).delete(remove_managed_model),
        )
        .route("/v1/models/downloads", post(start_model_download))
        .route(
            "/v1/models/downloads/{model_id}/cancel",
            post(cancel_model_download),
        )
        .route("/v1/models/imports", post(import_model))
        .route("/v1/performance/generations", get(generation_performance))
        .route(
            "/v1/models/selection",
            get(model_catalog).post(select_model).delete(unload_model),
        )
        .route("/v1/runtime/shutdown", post(request_shutdown))
        .route(
            "/v1/conversations",
            get(list_conversations).post(create_conversation),
        )
        .route(
            "/v1/conversations/{conversation_id}",
            get(get_conversation).delete(delete_conversation),
        )
        .route(
            "/v1/conversations/{conversation_id}/messages",
            get(list_messages).post(append_message),
        )
        .route(
            "/v1/conversations/{conversation_id}/branches",
            get(list_conversation_branches).post(fork_conversation_branch),
        )
        .route(
            "/v1/conversations/{conversation_id}/branches/{branch_id}/activate",
            post(activate_conversation_branch),
        )
        .route(
            "/v1/conversations/{conversation_id}/generations",
            post(create_generation),
        )
        .route(
            "/v1/conversations/{conversation_id}/generations/active",
            get(list_active_generations),
        )
        .route(
            "/v1/conversations/{conversation_id}/split-chats",
            get(list_split_chats).post(create_split_chat),
        )
        .route("/v1/split-chats/{split_chat_id}", get(get_split_chat))
        .route(
            "/v1/split-chats/{split_chat_id}/cancel",
            post(cancel_split_chat),
        )
        .route("/v1/generations/{generation_id}", get(get_generation))
        .route(
            "/v1/generations/{generation_id}/start",
            post(start_generation),
        )
        .route(
            "/v1/generations/{generation_id}/cancel",
            post(cancel_generation),
        )
        .route(
            "/v1/generations/{generation_id}/events",
            get(generation_events),
        )
        .route("/v1/search/messages", get(search_messages))
        .route(
            "/v1/knowledge/stacks",
            get(list_knowledge_stacks).post(create_knowledge_stack),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}",
            get(get_knowledge_stack).delete(delete_knowledge_stack),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/documents",
            get(list_knowledge_documents),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/documents/import",
            post(import_knowledge_documents),
        )
        .route(
            "/v1/knowledge/documents/{document_id}",
            get(get_knowledge_document).delete(delete_knowledge_document),
        )
        .route(
            "/v1/knowledge/documents/{document_id}/chunks",
            get(list_knowledge_chunks),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/search",
            post(search_knowledge_stack),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/jobs",
            get(list_ingestion_jobs),
        )
        .route("/v1/knowledge/jobs/{job_id}", get(get_ingestion_job))
        .route(
            "/v1/knowledge/jobs/{job_id}/cancel",
            post(cancel_ingestion_job),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/watch-roots",
            get(list_knowledge_watch_roots).post(create_knowledge_watch_root),
        )
        .route(
            "/v1/knowledge/watch-roots/{root_id}",
            get(get_knowledge_watch_root).delete(delete_knowledge_watch_root),
        )
        .route(
            "/v1/knowledge/watch-roots/{root_id}/rescan",
            post(rescan_knowledge_watch_root),
        )
        .route(
            "/v1/knowledge/stacks/{stack_id}/index",
            get(knowledge_index_status).post(rebuild_knowledge_index),
        )
}

async fn request_shutdown(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let candidate = headers
        .get(CONTROL_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    if !candidate.is_some_and(|candidate| state.control_token.matches(candidate)) {
        return Err(ApiError::unauthorized());
    }
    state.shutdown.cancel();
    Ok(StatusCode::ACCEPTED)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AiStatusResponse {
    configured: bool,
    selection: Option<ModelSelection>,
    runtime: Option<LocalAiRuntimeStatus>,
}

async fn ai_status(State(state): State<ApiState>) -> Json<AiStatusResponse> {
    let selected = state.ai_runtime.current();
    Json(AiStatusResponse {
        configured: selected.is_some(),
        selection: selected.as_ref().map(|selected| selected.selection.clone()),
        runtime: selected.map(|selected| selected.runtime.status()),
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelCatalogResponse {
    models: Vec<ModelCatalogEntry>,
    managed_models: Vec<ManagedModel>,
    selection: Option<ModelSelection>,
    runtime: Option<LocalAiRuntimeStatus>,
}

async fn model_catalog(
    State(state): State<ApiState>,
) -> Result<Json<ModelCatalogResponse>, ApiError> {
    Ok(Json(model_catalog_response(&state).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SelectModelRequest {
    model_id: String,
    #[serde(default)]
    load_profile: ModelLoadProfile,
}

async fn select_model(
    State(state): State<ApiState>,
    Json(request): Json<SelectModelRequest>,
) -> Result<Json<ModelCatalogResponse>, ApiError> {
    if state.generations.has_active().await {
        return Err(ApiError::conflict(
            "generation_active",
            "wait for active generations to finish before changing the local model",
        ));
    }
    let workspace_id = parse_default_workspace_id()?;
    if state.database.has_active_agent_runs(workspace_id).await? {
        return Err(ApiError::conflict(
            "agent_run_active",
            "finish or cancel active agent runs before changing the local model",
        ));
    }
    if state.database.has_active_crew_runs(workspace_id).await? {
        return Err(ApiError::conflict(
            "crew_run_active",
            "finish active Crew runs before changing the local model",
        ));
    }
    if state
        .database
        .has_active_workflow_runs(workspace_id)
        .await?
    {
        return Err(ApiError::conflict(
            "workflow_run_active",
            "finish or pause active workflows before changing the local model",
        ));
    }
    if state.database.has_active_media_jobs(workspace_id).await? {
        return Err(ApiError::conflict(
            "media_job_active",
            "finish or cancel active media jobs before changing the local model",
        ));
    }
    let selection =
        resolve_model_selection(&state, request.model_id.trim(), request.load_profile).await?;
    if state.ai_runtime.selection().as_ref() == Some(&selection) {
        return Ok(Json(model_catalog_response(&state).await?));
    }
    let factory = state.ai_factory.current().ok_or_else(|| {
        ApiError::unavailable(
            "local_ai_bundle_unavailable",
            "the verified local AI sidecar bundle is not available",
        )
    })?;
    let runtime_reference = resolve_runtime_reference(&state, &selection.model_id).await?;
    let runtime = factory
        .prepare_with_profile(
            runtime_reference,
            gateway_runtime_profile(&selection.load_profile),
        )
        .await?;
    let agent_runtime = RigAgentRuntime::new(RigAgentRuntimeConfig {
        database: state.database.clone(),
        ai_runtime: runtime.clone(),
        max_concurrent_runs: 4,
    })?;
    if let Err(error) = agent_runtime
        .synchronize_gateway_protocols(workspace_id)
        .await
    {
        let _ = agent_runtime.shutdown(Duration::from_secs(5)).await;
        let _ = runtime.shutdown().await;
        return Err(error.into());
    }
    let setting = serde_json::to_value(&selection).map_err(|error| {
        ApiError::invalid(format!("failed to serialize model selection: {error}"))
    })?;
    if let Err(error) = state
        .database
        .set_json_setting(
            DESKTOP_SETTINGS_SCOPE,
            MODEL_SELECTION_SETTING_KEY,
            &setting,
        )
        .await
    {
        let _ = agent_runtime.shutdown(Duration::from_secs(5)).await;
        let _ = runtime.shutdown().await;
        return Err(error.into());
    }
    if let Some(previous) = state.agents.replace(agent_runtime)
        && !previous.shutdown(Duration::from_secs(5)).await
    {
        tracing::warn!("previous agent runtime did not stop within the model-switch window");
    }
    let recovered_shadows = state.shadows.recover(workspace_id).await?;
    if recovered_shadows > 0 {
        tracing::info!(
            count = recovered_shadows,
            "resumed pending Shadow Persona suggestions"
        );
    }
    let recovered_crews = state.crews.recover(workspace_id).await?;
    if recovered_crews > 0 {
        tracing::info!(count = recovered_crews, "resumed pending Crew runs");
    }
    for previous in state.ai_runtime.replace(selection, runtime) {
        if let Err(error) = previous.runtime.shutdown().await {
            tracing::error!(%error, "failed to shut down a previously loaded local model");
        }
    }
    state
        .media
        .synchronize_gateway_routes()
        .await
        .map_err(super::platform_routes::media_error)?;
    Ok(Json(model_catalog_response(&state).await?))
}

async fn hardware_diagnostics(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let factory = state.ai_factory.current().ok_or_else(|| {
        ApiError::unavailable(
            "local_ai_bundle_unavailable",
            "the verified local AI sidecar bundle is not available",
        )
    })?;
    Ok(Json(camelize_json(factory.doctor().await?)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TuneModelRequest {
    model_id: String,
    #[serde(default)]
    load_profile: ModelLoadProfile,
    #[serde(default)]
    tuning_profile: ModelTuningProfile,
}

async fn tune_model(
    State(state): State<ApiState>,
    Json(request): Json<TuneModelRequest>,
) -> Result<Json<Value>, ApiError> {
    let selection =
        resolve_model_selection(&state, request.model_id.trim(), request.load_profile).await?;
    let factory = state.ai_factory.current().ok_or_else(|| {
        ApiError::unavailable(
            "local_ai_bundle_unavailable",
            "the verified local AI sidecar bundle is not available",
        )
    })?;
    let profile = &selection.load_profile;
    let runtime_reference = resolve_runtime_reference(&state, &selection.model_id).await?;
    Ok(Json(camelize_json(
        factory
            .tune(MistralTuneRequest {
                model_id: runtime_reference,
                quantization: gateway_quantization(profile.quantization),
                profile: match request.tuning_profile {
                    ModelTuningProfile::Quality => MistralTuneProfile::Quality,
                    ModelTuningProfile::Balanced => MistralTuneProfile::Balanced,
                    ModelTuningProfile::Fast => MistralTuneProfile::Fast,
                },
                force_cpu: profile.force_cpu,
                max_sequence_length: profile.max_sequence_length,
                max_batch_size: profile.max_batch_size,
            })
            .await?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenerationPerformanceQuery {
    limit: Option<u32>,
}

async fn generation_performance(
    State(state): State<ApiState>,
    Query(query): Query<GenerationPerformanceQuery>,
) -> Result<Json<GenerationPerformanceDashboard>, ApiError> {
    let samples = state
        .database
        .generation_performance_samples(query.limit.unwrap_or(50))
        .await?;
    let mut first_output = samples
        .iter()
        .filter_map(|sample| sample.time_to_first_output_ms)
        .collect::<Vec<_>>();
    let mut total_duration = samples
        .iter()
        .filter_map(|sample| sample.total_duration_ms)
        .collect::<Vec<_>>();
    let mut decode_throughput = samples
        .iter()
        .filter_map(|sample| sample.decode_tokens_per_second)
        .collect::<Vec<_>>();
    first_output.sort_unstable();
    total_duration.sort_unstable();
    decode_throughput.sort_by(f64::total_cmp);
    Ok(Json(GenerationPerformanceDashboard {
        generated_at_ms: now_ms(),
        completed_runs: samples
            .iter()
            .filter(|sample| sample.status == GenerationStatus::Completed)
            .count(),
        failed_runs: samples
            .iter()
            .filter(|sample| sample.status == GenerationStatus::Failed)
            .count(),
        cancelled_runs: samples
            .iter()
            .filter(|sample| sample.status == GenerationStatus::Cancelled)
            .count(),
        time_to_first_output_p50_ms: percentile(&first_output, 50),
        time_to_first_output_p95_ms: percentile(&first_output, 95),
        total_duration_p50_ms: percentile(&total_duration, 50),
        total_duration_p95_ms: percentile(&total_duration, 95),
        decode_tokens_per_second_p50: percentile_f64(&decode_throughput, 50),
        samples,
    }))
}

fn percentile(values: &[i64], percentile: usize) -> Option<i64> {
    percentile_index(values.len(), percentile).map(|index| values[index])
}

fn percentile_f64(values: &[f64], percentile: usize) -> Option<f64> {
    percentile_index(values.len(), percentile).map(|index| values[index])
}

fn percentile_index(length: usize, percentile: usize) -> Option<usize> {
    if length == 0 || !(1..=100).contains(&percentile) {
        return None;
    }
    Some((length.saturating_mul(percentile).div_ceil(100)).saturating_sub(1))
}

fn camelize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(camelize_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (camelize_key(&key), camelize_json(value)))
                .collect(),
        ),
        value => value,
    }
}

fn camelize_key(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut uppercase = false;
    for character in value.chars() {
        if character == '_' {
            uppercase = true;
        } else if uppercase {
            output.extend(character.to_uppercase());
            uppercase = false;
        } else {
            output.push(character);
        }
    }
    output
}

async fn unload_model(
    State(state): State<ApiState>,
) -> Result<Json<ModelCatalogResponse>, ApiError> {
    if state.generations.has_active().await {
        return Err(ApiError::conflict(
            "generation_active",
            "wait for active generations to finish before unloading the local model",
        ));
    }
    let workspace_id = parse_default_workspace_id()?;
    if state.database.has_active_agent_runs(workspace_id).await? {
        return Err(ApiError::conflict(
            "agent_run_active",
            "finish or cancel active agent runs before unloading the local model",
        ));
    }
    if state.database.has_active_crew_runs(workspace_id).await? {
        return Err(ApiError::conflict(
            "crew_run_active",
            "finish active Crew runs before unloading the local model",
        ));
    }
    if state
        .database
        .has_active_workflow_runs(workspace_id)
        .await?
    {
        return Err(ApiError::conflict(
            "workflow_run_active",
            "finish or pause active workflows before unloading the local model",
        ));
    }
    if state.database.has_active_media_jobs(workspace_id).await? {
        return Err(ApiError::conflict(
            "media_job_active",
            "finish or cancel active media jobs before unloading the local model",
        ));
    }
    state
        .database
        .delete_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY)
        .await?;
    if let Some(runtime) = state.agents.take()
        && !runtime.shutdown(Duration::from_secs(5)).await
    {
        tracing::warn!("agent runtime did not stop within the model-unload window");
    }
    for selected in state.ai_runtime.take_all() {
        selected.runtime.shutdown().await?;
    }
    Ok(Json(model_catalog_response(&state).await?))
}

async fn model_catalog_response(state: &ApiState) -> Result<ModelCatalogResponse, ApiError> {
    let selected = state.ai_runtime.current();
    let mut models = local_model_catalog();
    let managed_models = match &state.model_manager {
        Some(model_manager) => model_manager.list().await?,
        None => Vec::new(),
    };
    for managed in managed_models
        .iter()
        .filter(|managed| managed.status.is_ready())
    {
        if !models
            .iter()
            .any(|model| model.model_id == managed.model_id)
        {
            models.push(ModelCatalogEntry {
                provider: AiProvider::LocalMistralRs,
                model_id: managed.model_id.clone(),
                model_alias: LOCAL_MODEL_ALIAS.to_string(),
                display_name: managed.display_name.clone(),
                description: match managed.source {
                    northbridge_domain::ManagedModelSource::HuggingFace => {
                        "Downloaded and verified local Hugging Face model.".to_string()
                    }
                    northbridge_domain::ManagedModelSource::Imported => {
                        "Verified model imported from local storage.".to_string()
                    }
                },
                context_window: 8192,
                recommended: false,
            });
        }
    }
    if let Some(selection) = selected.as_ref().map(|selected| &selected.selection)
        && !models.iter().any(|model| {
            model.provider == selection.provider && model.model_id == selection.model_id
        })
    {
        models.push(ModelCatalogEntry {
            provider: selection.provider,
            model_id: selection.model_id.clone(),
            model_alias: selection.model_alias.clone(),
            display_name: selection.model_id.clone(),
            description: "Custom model configured through the desktop environment.".to_string(),
            context_window: 8192,
            recommended: false,
        });
    }
    Ok(ModelCatalogResponse {
        models,
        managed_models,
        selection: selected.as_ref().map(|selected| selected.selection.clone()),
        runtime: selected.map(|selected| selected.runtime.status()),
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartModelDownloadRequest {
    model_id: String,
    revision: Option<String>,
    display_name: Option<String>,
}

async fn start_model_download(
    State(state): State<ApiState>,
    Json(request): Json<StartModelDownloadRequest>,
) -> Result<(StatusCode, Json<ManagedModel>), ApiError> {
    let model = required_model_manager(&state)?
        .start_download(DownloadModelRequest {
            model_id: request.model_id,
            revision: request.revision,
            display_name: request.display_name,
        })
        .await?;
    Ok((StatusCode::ACCEPTED, Json(model)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImportModelBody {
    path: String,
    model_id: Option<String>,
    display_name: Option<String>,
}

async fn import_model(
    State(state): State<ApiState>,
    Json(request): Json<ImportModelBody>,
) -> Result<(StatusCode, Json<ManagedModel>), ApiError> {
    let model = required_model_manager(&state)?
        .import(ImportModelRequest {
            path: request.path.into(),
            model_id: request.model_id,
            display_name: request.display_name,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(model)))
}

async fn managed_models(
    State(state): State<ApiState>,
) -> Result<Json<Vec<ManagedModel>>, ApiError> {
    Ok(Json(required_model_manager(&state)?.list().await?))
}

async fn managed_model(
    State(state): State<ApiState>,
    Path(model_id): Path<Uuid>,
) -> Result<Json<ManagedModel>, ApiError> {
    Ok(Json(required_model_manager(&state)?.get(model_id).await?))
}

async fn cancel_model_download(
    State(state): State<ApiState>,
    Path(model_id): Path<Uuid>,
) -> Result<Json<ManagedModel>, ApiError> {
    Ok(Json(
        required_model_manager(&state)?
            .cancel_download(model_id)
            .await?,
    ))
}

async fn remove_managed_model(
    State(state): State<ApiState>,
    Path(model_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    if state.generations.has_active().await {
        return Err(ApiError::conflict(
            "generation_active",
            "wait for active generations to finish before removing a local model",
        ));
    }
    let manager = required_model_manager(&state)?;
    let model = manager.get(model_id).await?;
    if state.ai_runtime.uses_model(&model.model_id) {
        return Err(ApiError::conflict(
            "model_loaded",
            "unload this model before removing it",
        ));
    }
    manager.remove(model_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resolve_model_selection(
    state: &ApiState,
    model_id: &str,
    load_profile: ModelLoadProfile,
) -> Result<ModelSelection, ApiError> {
    if let Some(selection) = local_model_selection_with_profile(model_id, load_profile.clone()) {
        return Ok(selection);
    }
    load_profile.validate().map_err(ApiError::invalid)?;
    let model = required_model_manager(state)?
        .by_model_id(model_id)
        .await?
        .ok_or_else(|| ApiError::invalid("select a model published by the local model catalog"))?;
    if !model.status.is_ready() {
        return Err(ApiError::conflict(
            "model_not_ready",
            format!(
                "model {} is not ready; current state is {}",
                model.model_id,
                model.status.as_str()
            ),
        ));
    }
    Ok(ModelSelection {
        provider: AiProvider::LocalMistralRs,
        model_id: model.model_id,
        model_alias: LOCAL_MODEL_ALIAS.to_string(),
        load_profile,
    })
}

async fn resolve_runtime_reference(state: &ApiState, model_id: &str) -> Result<OsString, ApiError> {
    let path = match &state.model_manager {
        Some(model_manager) => model_manager.runtime_reference(model_id).await?,
        None => None,
    };
    Ok(path.map_or_else(|| OsString::from(model_id), PathBuf::into_os_string))
}

fn required_model_manager(state: &ApiState) -> Result<&ModelManager, ApiError> {
    state.model_manager.as_ref().ok_or_else(|| {
        ApiError::unavailable(
            "model_manager_unavailable",
            "the local model manager is unavailable",
        )
    })
}

async fn ai_chat_completions(
    State(state): State<ApiState>,
    Json(request): Json<Value>,
) -> Result<Response, ApiError> {
    let runtime = state.ai_runtime.current().ok_or_else(|| {
        ApiError::unavailable(
            "ai_not_configured",
            "select or install a local model before starting a chat",
        )
    })?;
    let stream = runtime.runtime.stream_chat(request).await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/event-stream")
        .header(http::header::CACHE_CONTROL, "no-store")
        .header("x-accel-buffering", "no")
        .body(Body::from_stream(stream))
        .map_err(|error| ApiError::invalid(format!("failed to create AI response: {error}")))
}

async fn health(State(state): State<ApiState>) -> Result<Json<HealthStatus>, ApiError> {
    state.database.ping().await?;
    let schema_version = state.database.schema_version().await?;
    Ok(Json(HealthStatus {
        status: "ok".to_string(),
        schema_version,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateConversationRequest {
    workspace_id: Option<Uuid>,
    title: Option<String>,
}

async fn create_conversation(
    State(state): State<ApiState>,
    Json(request): Json<CreateConversationRequest>,
) -> Result<(StatusCode, Json<northbridge_domain::Conversation>), ApiError> {
    let workspace_id = request
        .workspace_id
        .unwrap_or(parse_default_workspace_id()?);
    let conversation = state
        .database
        .create_conversation(workspace_id, request.title.as_deref().unwrap_or_default())
        .await?;
    Ok((StatusCode::CREATED, Json(conversation)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListConversationsQuery {
    workspace_id: Option<Uuid>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConversationPageResponse {
    items: Vec<northbridge_domain::Conversation>,
    next_cursor: Option<String>,
}

async fn list_conversations(
    State(state): State<ApiState>,
    Query(query): Query<ListConversationsQuery>,
) -> Result<Json<ConversationPageResponse>, ApiError> {
    let workspace_id = query.workspace_id.unwrap_or(parse_default_workspace_id()?);
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
    let page = state
        .database
        .list_conversations(workspace_id, cursor.as_ref(), query.limit.unwrap_or(50))
        .await?;
    let next_cursor = page.next_cursor.as_ref().map(encode_cursor).transpose()?;
    Ok(Json(ConversationPageResponse {
        items: page.items,
        next_cursor,
    }))
}

async fn get_conversation(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
) -> Result<Json<northbridge_domain::Conversation>, ApiError> {
    Ok(Json(
        state.database.get_conversation(conversation_id).await?,
    ))
}

async fn delete_conversation(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state
        .database
        .soft_delete_conversation(conversation_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AppendMessageRequest {
    branch_id: Uuid,
    parent_message_id: Option<Uuid>,
    role: MessageRole,
    parts: Vec<NewMessagePart>,
}

async fn append_message(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
    Json(request): Json<AppendMessageRequest>,
) -> Result<(StatusCode, Json<northbridge_domain::Message>), ApiError> {
    let message = state
        .database
        .append_message(NewMessage {
            conversation_id,
            branch_id: request.branch_id,
            parent_message_id: request.parent_message_id,
            role: request.role,
            parts: request.parts,
        })
        .await?;
    if let Err(error) = state.shadows.observe_message(&message).await {
        tracing::error!(?error, message_id = %message.id, "failed to schedule Shadow Persona suggestions");
    }
    Ok((StatusCode::CREATED, Json(message)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListMessagesQuery {
    branch_id: Option<Uuid>,
    #[serde(default)]
    all_branches: bool,
}

async fn list_messages(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
    Query(query): Query<ListMessagesQuery>,
) -> Result<Json<Vec<northbridge_domain::Message>>, ApiError> {
    if query.all_branches {
        return Ok(Json(
            state
                .database
                .list_all_conversation_messages(conversation_id)
                .await?,
        ));
    }
    let conversation = state.database.get_conversation(conversation_id).await?;
    let branch_id = query.branch_id.unwrap_or(conversation.default_branch_id);
    Ok(Json(
        state
            .database
            .list_messages(conversation_id, branch_id)
            .await?,
    ))
}

async fn list_conversation_branches(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
) -> Result<Json<Vec<northbridge_domain::ConversationBranch>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_conversation_branches(conversation_id)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForkConversationBranchRequest {
    forked_from_message_id: Uuid,
    #[serde(default)]
    activate: bool,
}

async fn fork_conversation_branch(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
    Json(request): Json<ForkConversationBranchRequest>,
) -> Result<(StatusCode, Json<northbridge_domain::ConversationBranch>), ApiError> {
    let branch = state
        .database
        .fork_conversation_branch(
            conversation_id,
            request.forked_from_message_id,
            request.activate,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(branch)))
}

async fn activate_conversation_branch(
    State(state): State<ApiState>,
    Path((conversation_id, branch_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<northbridge_domain::Conversation>, ApiError> {
    Ok(Json(
        state
            .database
            .activate_conversation_branch(conversation_id, branch_id)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateGenerationRequest {
    branch_id: Uuid,
    model_alias: String,
    client_request_id: Option<String>,
    #[serde(default)]
    knowledge_stack_ids: Vec<Uuid>,
}

async fn create_generation(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
    Json(request): Json<CreateGenerationRequest>,
) -> Result<(StatusCode, Json<northbridge_domain::GenerationRun>), ApiError> {
    let selection = state.ai_runtime.selection().ok_or_else(|| {
        ApiError::unavailable(
            "ai_not_configured",
            "select or install a local model before creating a generation",
        )
    })?;
    if request.model_alias != selection.model_alias {
        return Err(ApiError::invalid(format!(
            "model alias {:?} is not selected",
            request.model_alias
        )));
    }
    if !request.knowledge_stack_ids.is_empty() {
        required_knowledge(&state)?;
    }
    let run = state
        .database
        .create_generation_run_for_model_with_knowledge_idempotent(
            conversation_id,
            request.branch_id,
            &selection,
            request.client_request_id.as_deref(),
            &request.knowledge_stack_ids,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(run)))
}

async fn list_active_generations(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
) -> Result<Json<Vec<northbridge_domain::GenerationRun>>, ApiError> {
    Ok(Json(
        state
            .database
            .list_active_generation_runs(conversation_id)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SplitChatCandidateRequest {
    model_id: String,
    #[serde(default)]
    load_profile: ModelLoadProfile,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSplitChatRequest {
    source_branch_id: Uuid,
    input_message_id: Uuid,
    candidates: Vec<SplitChatCandidateRequest>,
    client_request_id: Option<String>,
}

async fn create_split_chat(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
    Json(request): Json<CreateSplitChatRequest>,
) -> Result<(StatusCode, Json<SplitChatRun>), ApiError> {
    if !(2..=4).contains(&request.candidates.len()) {
        return Err(ApiError::invalid(
            "split chats require between 2 and 4 model candidates",
        ));
    }
    let mut selections = Vec::with_capacity(request.candidates.len());
    let mut unique = HashSet::with_capacity(request.candidates.len());
    for candidate in request.candidates {
        let selection =
            resolve_model_selection(&state, candidate.model_id.trim(), candidate.load_profile)
                .await?;
        let key = serde_json::to_string(&selection)
            .map_err(|error| ApiError::invalid(format!("invalid model selection: {error}")))?;
        if !unique.insert(key) {
            return Err(ApiError::invalid(
                "split-chat candidates must use distinct models or load profiles",
            ));
        }
        selections.push(selection);
    }

    ensure_split_runtimes(&state, &selections).await?;
    let split = state
        .database
        .create_split_chat_idempotent(
            conversation_id,
            request.source_branch_id,
            request.input_message_id,
            &selections,
            request.client_request_id.as_deref(),
        )
        .await?;
    let mut started = Vec::with_capacity(split.lanes.len());
    for lane in &split.lanes {
        if lane.generation.status != GenerationStatus::Queued {
            continue;
        }
        if let Err(error) = state.generations.start(lane.generation.id).await {
            for generation_id in started {
                let _ = state.generations.cancel(generation_id).await;
            }
            for remaining in &split.lanes {
                if remaining.generation.status == GenerationStatus::Queued {
                    let _ = state.generations.cancel(remaining.generation.id).await;
                }
            }
            return Err(error);
        }
        started.push(lane.generation.id);
    }
    Ok((
        StatusCode::CREATED,
        Json(state.database.get_split_chat(split.id).await?),
    ))
}

async fn list_split_chats(
    State(state): State<ApiState>,
    Path(conversation_id): Path<Uuid>,
) -> Result<Json<Vec<SplitChatRun>>, ApiError> {
    Ok(Json(
        state.database.list_split_chats(conversation_id).await?,
    ))
}

async fn get_split_chat(
    State(state): State<ApiState>,
    Path(split_chat_id): Path<Uuid>,
) -> Result<Json<SplitChatRun>, ApiError> {
    Ok(Json(state.database.get_split_chat(split_chat_id).await?))
}

async fn cancel_split_chat(
    State(state): State<ApiState>,
    Path(split_chat_id): Path<Uuid>,
) -> Result<Json<SplitChatRun>, ApiError> {
    let split = state.database.get_split_chat(split_chat_id).await?;
    for lane in &split.lanes {
        if !lane.generation.status.is_terminal() {
            let _ = state.generations.cancel(lane.generation.id).await?;
        }
    }
    Ok(Json(state.database.get_split_chat(split_chat_id).await?))
}

async fn ensure_split_runtimes(
    state: &ApiState,
    selections: &[ModelSelection],
) -> Result<(), ApiError> {
    let missing = selections
        .iter()
        .filter(|selection| state.ai_runtime.for_selection(selection).is_none())
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let factory = state.ai_factory.current().ok_or_else(|| {
        ApiError::unavailable(
            "local_ai_bundle_unavailable",
            "the verified local AI sidecar bundle is not available",
        )
    })?;
    let mut prepared = Vec::with_capacity(missing.len());
    for selection in missing {
        let runtime_reference = resolve_runtime_reference(state, &selection.model_id).await?;
        match factory
            .prepare_with_profile(
                runtime_reference,
                gateway_runtime_profile(&selection.load_profile),
            )
            .await
        {
            Ok(runtime) => prepared.push((selection, runtime)),
            Err(error) => {
                for (_, runtime) in prepared {
                    let _ = runtime.shutdown().await;
                }
                return Err(error.into());
            }
        }
    }
    for (selection, runtime) in prepared {
        if let Some(redundant) = state.ai_runtime.add_pooled(selection, runtime) {
            let _ = redundant.shutdown().await;
        }
    }
    Ok(())
}

async fn get_generation(
    State(state): State<ApiState>,
    Path(generation_id): Path<Uuid>,
) -> Result<Json<northbridge_domain::GenerationRun>, ApiError> {
    Ok(Json(
        state.database.get_generation_run(generation_id).await?,
    ))
}

async fn start_generation(
    State(state): State<ApiState>,
    Path(generation_id): Path<Uuid>,
) -> Result<(StatusCode, Json<northbridge_domain::GenerationRun>), ApiError> {
    let run = state.generations.start(generation_id).await?;
    Ok((StatusCode::ACCEPTED, Json(run)))
}

async fn cancel_generation(
    State(state): State<ApiState>,
    Path(generation_id): Path<Uuid>,
) -> Result<Json<northbridge_domain::GenerationRun>, ApiError> {
    Ok(Json(state.generations.cancel(generation_id).await?))
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    after: Option<i64>,
}

async fn generation_events(
    State(state): State<ApiState>,
    Path(generation_id): Path<Uuid>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.database.get_generation_run(generation_id).await?;
    let header_sequence = headers
        .get("last-event-id")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| ApiError::invalid("Last-Event-ID must be ASCII"))?
                .parse::<i64>()
                .map_err(|_| ApiError::invalid("Last-Event-ID must be an integer"))
        })
        .transpose()?;
    let after_sequence = query
        .after
        .unwrap_or_default()
        .max(header_sequence.unwrap_or_default());
    if after_sequence < 0 {
        return Err(ApiError::invalid("event sequence cannot be negative"));
    }

    let mut receiver = state.events.subscribe(generation_id).await;
    let initial = state.events.replay(generation_id, after_sequence).await?;
    let event_service = state.events.clone();
    let event_stream = stream! {
        let mut last_sequence = after_sequence;
        for event in initial {
            last_sequence = event.sequence_number;
            yield Ok::<Event, Infallible>(sse_event(&event));
        }
        loop {
            match receiver.recv().await {
                Ok(event) if event.sequence_number > last_sequence => {
                    last_sequence = event.sequence_number;
                    yield Ok(sse_event(&event));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    match event_service.replay(generation_id, last_sequence).await {
                        Ok(events) => {
                            for event in events {
                                last_sequence = event.sequence_number;
                                yield Ok(sse_event(&event));
                            }
                        }
                        Err(error) => {
                            tracing::error!(%generation_id, %error, "failed to replay lagged SSE events");
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Ok(Sse::new(event_stream)
        .keep_alive(
            KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchMessagesQuery {
    q: String,
    workspace_id: Option<Uuid>,
    limit: Option<u32>,
}

async fn search_messages(
    State(state): State<ApiState>,
    Query(query): Query<SearchMessagesQuery>,
) -> Result<Json<Vec<northbridge_domain::SearchHit>>, ApiError> {
    let workspace_id = query.workspace_id.unwrap_or(parse_default_workspace_id()?);
    Ok(Json(
        state
            .database
            .search_messages(workspace_id, &query.q, query.limit.unwrap_or(20))
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateKnowledgeStackRequest {
    workspace_id: Option<Uuid>,
    name: String,
    #[serde(default)]
    description: String,
    embedding_model: Option<String>,
    embedding_dimensions: Option<u32>,
    chunk_size: Option<u32>,
    chunk_overlap: Option<u32>,
}

async fn create_knowledge_stack(
    State(state): State<ApiState>,
    Json(request): Json<CreateKnowledgeStackRequest>,
) -> Result<(StatusCode, Json<KnowledgeStack>), ApiError> {
    let services = required_knowledge(&state)?;
    let workspace_id = request
        .workspace_id
        .unwrap_or(parse_default_workspace_id()?);
    let chunk_size = request.chunk_size.unwrap_or(1200);
    let chunk_overlap = request.chunk_overlap.unwrap_or(180);
    let stack = state
        .database
        .create_knowledge_stack(NewKnowledgeStack {
            workspace_id,
            name: request.name,
            description: request.description,
            embedding_model: request
                .embedding_model
                .unwrap_or_else(|| services.config.default_embedding_model.clone()),
            embedding_dimensions: request
                .embedding_dimensions
                .unwrap_or(services.config.default_embedding_dimensions),
            chunk_size,
            chunk_overlap,
            chunker_hash: chunker_hash(chunk_size, chunk_overlap)?,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(stack)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KnowledgeWorkspaceQuery {
    workspace_id: Option<Uuid>,
}

async fn list_knowledge_stacks(
    State(state): State<ApiState>,
    Query(query): Query<KnowledgeWorkspaceQuery>,
) -> Result<Json<Vec<KnowledgeStack>>, ApiError> {
    let workspace_id = query.workspace_id.unwrap_or(parse_default_workspace_id()?);
    Ok(Json(
        state.database.list_knowledge_stacks(workspace_id).await?,
    ))
}

async fn get_knowledge_stack(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<KnowledgeStack>, ApiError> {
    Ok(Json(state.database.knowledge_stack(stack_id).await?))
}

async fn delete_knowledge_stack(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let services = required_knowledge(&state)?;
    let jobs = state.database.list_ingestion_jobs(stack_id).await?;
    if jobs.iter().any(|job| {
        matches!(
            job.status,
            northbridge_domain::IngestionJobStatus::Queued
                | northbridge_domain::IngestionJobStatus::Running
        )
    }) {
        return Err(ApiError::conflict(
            "ingestion_active",
            "cancel active ingestion jobs before deleting the knowledge stack",
        ));
    }
    for root in state
        .database
        .list_knowledge_watch_roots(Some(stack_id))
        .await?
    {
        services.watchers.remove_root(root.id).await?;
    }
    if !state.database.delete_knowledge_stack(stack_id).await? {
        return Err(northbridge_storage::StorageError::NotFound {
            entity: "knowledge stack",
        }
        .into());
    }
    services.retrieval.forget_stack(stack_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_knowledge_documents(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<Vec<KnowledgeDocument>>, ApiError> {
    state.database.knowledge_stack(stack_id).await?;
    Ok(Json(
        state.database.list_knowledge_documents(stack_id).await?,
    ))
}

async fn get_knowledge_document(
    State(state): State<ApiState>,
    Path(document_id): Path<Uuid>,
) -> Result<Json<KnowledgeDocument>, ApiError> {
    Ok(Json(state.database.knowledge_document(document_id).await?))
}

async fn list_knowledge_chunks(
    State(state): State<ApiState>,
    Path(document_id): Path<Uuid>,
) -> Result<Json<Vec<northbridge_domain::DocumentChunk>>, ApiError> {
    state.database.knowledge_document(document_id).await?;
    Ok(Json(
        state.database.list_document_chunks(document_id).await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ImportKnowledgeDocumentsRequest {
    paths: Vec<String>,
}

async fn import_knowledge_documents(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
    Json(request): Json<ImportKnowledgeDocumentsRequest>,
) -> Result<(StatusCode, Json<Vec<IngestionJob>>), ApiError> {
    if request.paths.is_empty() || request.paths.len() > 64 {
        return Err(ApiError::invalid(
            "select between one and 64 files for each ingestion request",
        ));
    }
    let services = required_knowledge(&state)?;
    state.database.knowledge_stack(stack_id).await?;
    let mut jobs = Vec::with_capacity(request.paths.len());
    for path in request.paths {
        jobs.push(
            services
                .ingestion
                .start_file(stack_id, PathBuf::from(path), KnowledgeSourceKind::File)
                .await?,
        );
    }
    Ok((StatusCode::ACCEPTED, Json(jobs)))
}

async fn delete_knowledge_document(
    State(state): State<ApiState>,
    Path(document_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let services = required_knowledge(&state)?;
    let document = state.database.knowledge_document(document_id).await?;
    state
        .database
        .delete_knowledge_document(document_id)
        .await?;
    services
        .retrieval
        .synchronize_stack(document.stack_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchKnowledgeRequest {
    query: String,
    workspace_id: Option<Uuid>,
    limit: Option<u32>,
}

async fn search_knowledge_stack(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
    Json(request): Json<SearchKnowledgeRequest>,
) -> Result<Json<KnowledgeSearchResult>, ApiError> {
    let services = required_knowledge(&state)?;
    let stack = state.database.knowledge_stack(stack_id).await?;
    let workspace_id = request.workspace_id.unwrap_or(stack.workspace_id);
    Ok(Json(
        services
            .retrieval
            .search(
                workspace_id,
                &[stack_id],
                &request.query,
                request.limit.unwrap_or(8),
                None,
            )
            .await?,
    ))
}

async fn list_ingestion_jobs(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<Vec<IngestionJob>>, ApiError> {
    state.database.knowledge_stack(stack_id).await?;
    Ok(Json(state.database.list_ingestion_jobs(stack_id).await?))
}

async fn get_ingestion_job(
    State(state): State<ApiState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<IngestionJob>, ApiError> {
    Ok(Json(state.database.ingestion_job(job_id).await?))
}

async fn cancel_ingestion_job(
    State(state): State<ApiState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<IngestionJob>, ApiError> {
    Ok(Json(
        required_knowledge(&state)?.ingestion.cancel(job_id).await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateKnowledgeWatchRootRequest {
    path: String,
    kind: KnowledgeWatchKind,
    #[serde(default = "default_true")]
    recursive: bool,
}

async fn create_knowledge_watch_root(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
    Json(request): Json<CreateKnowledgeWatchRootRequest>,
) -> Result<(StatusCode, Json<KnowledgeWatchRoot>), ApiError> {
    let services = required_knowledge(&state)?;
    let stack = state.database.knowledge_stack(stack_id).await?;
    let root = services
        .watchers
        .add_root(
            stack_id,
            stack.workspace_id,
            PathBuf::from(request.path),
            request.kind,
            request.recursive,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(root)))
}

async fn list_knowledge_watch_roots(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<Vec<KnowledgeWatchRoot>>, ApiError> {
    state.database.knowledge_stack(stack_id).await?;
    Ok(Json(
        state
            .database
            .list_knowledge_watch_roots(Some(stack_id))
            .await?,
    ))
}

async fn get_knowledge_watch_root(
    State(state): State<ApiState>,
    Path(root_id): Path<Uuid>,
) -> Result<Json<KnowledgeWatchRoot>, ApiError> {
    Ok(Json(state.database.knowledge_watch_root(root_id).await?))
}

async fn delete_knowledge_watch_root(
    State(state): State<ApiState>,
    Path(root_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    if !required_knowledge(&state)?
        .watchers
        .remove_root(root_id)
        .await?
    {
        return Err(northbridge_storage::StorageError::NotFound {
            entity: "knowledge watch root",
        }
        .into());
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn rescan_knowledge_watch_root(
    State(state): State<ApiState>,
    Path(root_id): Path<Uuid>,
) -> Result<Json<KnowledgeWatchRoot>, ApiError> {
    Ok(Json(
        required_knowledge(&state)?.watchers.rescan(root_id).await?,
    ))
}

async fn knowledge_index_status(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<northbridge_retrieval::DenseIndexStatus>, ApiError> {
    Ok(Json(
        required_knowledge(&state)?
            .retrieval
            .synchronize_stack(stack_id)
            .await?,
    ))
}

async fn rebuild_knowledge_index(
    State(state): State<ApiState>,
    Path(stack_id): Path<Uuid>,
) -> Result<Json<northbridge_retrieval::DenseIndexStatus>, ApiError> {
    Ok(Json(
        required_knowledge(&state)?
            .retrieval
            .rebuild_stack(stack_id)
            .await?,
    ))
}

fn required_knowledge(state: &ApiState) -> Result<crate::KnowledgeServices, ApiError> {
    state.knowledge.current().ok_or_else(|| {
        ApiError::unavailable(
            "knowledge_runtime_unavailable",
            "the local knowledge runtime is unavailable",
        )
    })
}

const fn default_true() -> bool {
    true
}

fn parse_default_workspace_id() -> Result<Uuid, ApiError> {
    Uuid::parse_str(DEFAULT_WORKSPACE_ID)
        .map_err(|_| ApiError::invalid("the built-in workspace ID is invalid"))
}

fn encode_cursor(cursor: &ConversationCursor) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(cursor)
        .map_err(|_| ApiError::invalid("conversation cursor could not be encoded"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(encoded: &str) -> Result<ConversationCursor, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError::invalid("conversation cursor is malformed"))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid("conversation cursor is malformed"))
}

fn sse_event(event: &GenerationEvent) -> Event {
    let data = serde_json::to_string(event).unwrap_or_else(|_| {
        serde_json::json!({
            "generationId": event.generation_id,
            "sequenceNumber": event.sequence_number,
            "eventType": "serialization_error",
            "timestampMs": event.timestamp_ms,
            "payload": Value::Null,
        })
        .to_string()
    });
    Event::default()
        .id(event.sequence_number.to_string())
        .event(event.event_type.clone())
        .data(data)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{camelize_json, percentile};

    #[test]
    fn normalizes_mistral_diagnostics_to_the_api_casing_contract() {
        let value = camelize_json(json!({
            "system": {
                "hf_cache_path": "cache",
                "build": { "flash_attn_v3": false }
            },
            "recommended_isq": "Q4K"
        }));
        assert_eq!(value["system"]["hfCachePath"], "cache");
        assert_eq!(value["system"]["build"]["flashAttnV3"], false);
        assert_eq!(value["recommendedIsq"], "Q4K");
    }

    #[test]
    fn computes_nearest_rank_dashboard_percentiles() {
        let values = [10, 20, 30, 40, 50];
        assert_eq!(percentile(&values, 50), Some(30));
        assert_eq!(percentile(&values, 95), Some(50));
        assert_eq!(percentile(&[], 50), None);
    }
}
