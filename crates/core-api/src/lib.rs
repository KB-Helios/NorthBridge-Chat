mod agents;
mod auth;
mod collaboration;
mod error;
mod events;
mod generation;
mod knowledge;
mod platform;
mod platform_routes;
mod routes;
mod runtime;
mod workflows;

use std::{
    ffi::OsString,
    sync::{Arc, RwLock},
    time::Duration,
};

use axum::{Router, body::Body, extract::DefaultBodyLimit, middleware};
use http::{HeaderName, HeaderValue, Method};
use northbridge_agent_runtime::AgentRuntime;
use northbridge_ai_gateway::{LocalAiRuntime, LocalAiRuntimeFactory};
use northbridge_domain::ModelSelection;
use northbridge_model_manager::ModelManager;
use northbridge_storage::Database;
use tokio_util::sync::CancellationToken;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    agents::{AgentRuntimeRegistry, CrewCoordinator, ShadowCoordinator},
    auth::{AuthState, AuthToken, require_bearer},
    error::{CoreApiConfigError, Result},
    events::GenerationEvents,
    generation::GenerationCoordinator,
    knowledge::KnowledgeRuntimeRegistry,
    platform::MediaCoordinator,
    routes::api_routes,
    runtime::AiRuntimeRegistry,
    workflows::WorkflowCoordinator,
};

pub use events::GenerationEvents as GenerationEventService;
pub use knowledge::{
    DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_MODEL, EMBEDDING_MODEL_ALIAS,
    KnowledgeRuntimeConfig, KnowledgeServices, KnowledgeStartupError,
};
pub use platform::{CredentialStoreError, load_credential_secret};
pub use runtime::{
    LOCAL_MODEL_ALIAS, custom_local_model_selection, gateway_runtime_profile, local_model_catalog,
    local_model_selection, local_model_selection_with_profile,
};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone, Default)]
pub(crate) struct AiFactoryRegistry {
    inner: Arc<RwLock<Option<LocalAiRuntimeFactory>>>,
}

impl AiFactoryRegistry {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Option<LocalAiRuntimeFactory>> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Option<LocalAiRuntimeFactory>> {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn current(&self) -> Option<LocalAiRuntimeFactory> {
        self.read().clone()
    }

    fn replace(&self, factory: LocalAiRuntimeFactory) {
        *self.write() = Some(factory);
    }

    pub(crate) fn set_environment(&self, key: OsString, value: OsString) {
        if let Some(factory) = self.write().as_mut() {
            factory.environment.insert(key, value);
        }
    }

    pub(crate) fn remove_environment(&self, key: &OsString) {
        if let Some(factory) = self.write().as_mut() {
            factory.environment.remove(key);
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreApiConfig {
    pub session_token: String,
    pub control_token: String,
    pub allowed_origins: Vec<String>,
    pub max_json_body_bytes: usize,
}

impl CoreApiConfig {
    #[must_use]
    pub fn desktop(session_token: String, control_token: String) -> Self {
        Self {
            session_token,
            control_token,
            allowed_origins: vec![
                "http://tauri.localhost".to_string(),
                "tauri://localhost".to_string(),
            ],
            max_json_body_bytes: 4 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct CoreApi {
    database: Database,
    events: GenerationEvents,
    auth_state: AuthState,
    control_token: AuthToken,
    shutdown: CancellationToken,
    allowed_origins: Vec<HeaderValue>,
    max_json_body_bytes: usize,
    ai_registry: AiRuntimeRegistry,
    ai_factory: AiFactoryRegistry,
    model_manager: Option<ModelManager>,
    generations: GenerationCoordinator,
    knowledge: KnowledgeRuntimeRegistry,
    agents: AgentRuntimeRegistry,
    shadows: ShadowCoordinator,
    crews: CrewCoordinator,
    workflows: WorkflowCoordinator,
    media: MediaCoordinator,
}

impl CoreApi {
    pub fn new(database: Database, config: CoreApiConfig) -> Result<Self> {
        if config.max_json_body_bytes == 0 || config.max_json_body_bytes > 64 * 1024 * 1024 {
            return Err(CoreApiConfigError::InvalidBodyLimit(
                config.max_json_body_bytes,
            ));
        }
        let allowed_origins = config
            .allowed_origins
            .iter()
            .map(|origin| {
                HeaderValue::from_str(origin)
                    .map_err(|_| CoreApiConfigError::InvalidOrigin(origin.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        if allowed_origins.is_empty() {
            return Err(CoreApiConfigError::MissingOrigins);
        }
        let auth_token = AuthToken::new(config.session_token, "session")?;
        let auth_state = AuthState::new(auth_token, database.clone());
        let control_token = AuthToken::new(config.control_token, "control")?;
        let events = GenerationEvents::new(database.clone());
        let ai_registry = AiRuntimeRegistry::default();
        let knowledge = KnowledgeRuntimeRegistry::default();
        let agents = AgentRuntimeRegistry::default();
        let shadows = ShadowCoordinator::new(database.clone(), agents.clone());
        let crews = CrewCoordinator::new(database.clone(), agents.clone());
        let workflows = WorkflowCoordinator::new(database.clone(), agents.clone());
        let media = MediaCoordinator::new(database.clone(), ai_registry.clone());
        let generations = GenerationCoordinator::new(
            database.clone(),
            events.clone(),
            ai_registry.clone(),
            knowledge.clone(),
        );
        Ok(Self {
            database,
            events,
            auth_state,
            control_token,
            shutdown: CancellationToken::new(),
            allowed_origins,
            max_json_body_bytes: config.max_json_body_bytes,
            ai_registry,
            ai_factory: AiFactoryRegistry::default(),
            model_manager: None,
            generations,
            knowledge,
            agents,
            shadows,
            crews,
            workflows,
            media,
        })
    }

    #[must_use]
    pub fn with_ai_factory(self, factory: LocalAiRuntimeFactory) -> Self {
        self.ai_factory.replace(factory);
        self
    }

    #[must_use]
    pub fn with_model_manager(mut self, model_manager: ModelManager) -> Self {
        self.model_manager = Some(model_manager);
        self
    }

    #[must_use]
    pub fn with_ai_runtime(self, selection: ModelSelection, runtime: LocalAiRuntime) -> Self {
        self.ai_registry.replace(selection, runtime);
        self
    }

    #[must_use]
    pub fn with_agent_runtime(self, runtime: northbridge_agent_runtime::RigAgentRuntime) -> Self {
        self.agents.replace(runtime);
        self
    }

    #[must_use]
    pub fn with_knowledge_services(self, services: KnowledgeServices) -> Self {
        self.knowledge.replace(services);
        self
    }

    #[must_use]
    pub fn generation_events(&self) -> GenerationEventService {
        self.events.clone()
    }

    #[must_use]
    pub fn shutdown_signal(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn cancel_active_generations(&self, timeout: Duration) -> bool {
        self.generations.cancel_all_and_wait(timeout).await
    }

    pub async fn shutdown_ai_runtime(
        &self,
    ) -> std::result::Result<(), northbridge_ai_gateway::GatewayError> {
        for selected in self.ai_registry.take_all() {
            selected.runtime.shutdown().await?;
        }
        Ok(())
    }

    pub async fn shutdown_agent_runtime(&self, timeout: Duration) -> bool {
        match self.agents.take() {
            Some(runtime) => runtime.shutdown(timeout).await,
            None => true,
        }
    }

    pub async fn recover_shadow_suggestions(&self) -> usize {
        let workspace_id = uuid::Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID)
            .expect("built-in workspace ID is valid");
        match self.shadows.recover(workspace_id).await {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(?error, "failed to recover Shadow Persona suggestions");
                0
            }
        }
    }

    pub async fn recover_crew_runs(&self) -> usize {
        let workspace_id = uuid::Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID)
            .expect("built-in workspace ID is valid");
        match self.crews.recover(workspace_id).await {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(?error, "failed to recover Crew runs");
                0
            }
        }
    }

    pub async fn shutdown_shadow_suggestions(&self, timeout: Duration) -> bool {
        self.shadows.shutdown(timeout).await
    }

    pub async fn shutdown_crew_runs(&self, timeout: Duration) -> bool {
        self.crews.shutdown(timeout).await
    }

    pub async fn recover_workflow_runs(&self) -> u64 {
        match self.workflows.recover().await {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(%error, "failed to recover workflow runs");
                0
            }
        }
    }

    pub async fn recover_media_jobs(&self) -> u64 {
        match self.media.recover().await {
            Ok(count) => count,
            Err(error) => {
                tracing::error!(%error, "failed to recover media jobs");
                0
            }
        }
    }

    pub async fn synchronize_media_routes(&self) {
        if let Err(error) = self.media.synchronize_gateway_routes().await
            && !matches!(error, platform::MediaError::GatewayUnavailable)
        {
            tracing::warn!(%error, "media gateway synchronization needs attention");
        }
    }

    pub async fn shutdown_workflow_runs(&self, timeout: Duration) -> bool {
        self.workflows.shutdown(timeout).await
    }

    pub async fn shutdown_media_jobs(&self, timeout: Duration) -> bool {
        self.media.shutdown(timeout).await
    }

    pub async fn shutdown_model_manager(&self, timeout: Duration) -> bool {
        match &self.model_manager {
            Some(model_manager) => model_manager.shutdown(timeout).await,
            None => true,
        }
    }

    pub async fn shutdown_knowledge(&self, timeout: Duration) -> bool {
        match self.knowledge.current() {
            Some(services) => services.shutdown(timeout).await,
            None => true,
        }
    }

    pub fn router(&self) -> Router {
        let state = routes::ApiState {
            database: self.database.clone(),
            events: self.events.clone(),
            ai_runtime: self.ai_registry.clone(),
            ai_factory: self.ai_factory.clone(),
            model_manager: self.model_manager.clone(),
            generations: self.generations.clone(),
            knowledge: self.knowledge.clone(),
            agents: self.agents.clone(),
            shadows: self.shadows.clone(),
            crews: self.crews.clone(),
            workflows: self.workflows.clone(),
            media: self.media.clone(),
            control_token: self.control_token.clone(),
            shutdown: self.shutdown.clone(),
        };
        let cors = CorsLayer::new()
            .allow_origin(AllowOrigin::list(self.allowed_origins.clone()))
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_headers([
                http::header::AUTHORIZATION,
                http::header::CONTENT_TYPE,
                HeaderName::from_static("last-event-id"),
            ])
            .expose_headers([REQUEST_ID_HEADER.clone()]);

        api_routes()
            .with_state(state)
            .route_layer(middleware::from_fn_with_state(
                self.auth_state.clone(),
                require_bearer,
            ))
            .layer(DefaultBodyLimit::max(self.max_json_body_bytes))
            .layer(cors)
            .layer(
                TraceLayer::new_for_http().make_span_with(|request: &http::Request<Body>| {
                    let request_id = request
                        .headers()
                        .get(&REQUEST_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing");
                    tracing::info_span!(
                        "http_request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id
                    )
                }),
            )
            .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER.clone()))
            .layer(SetRequestIdLayer::new(
                REQUEST_ID_HEADER.clone(),
                MakeRequestUuid,
            ))
    }
}

#[cfg(test)]
mod tests;
