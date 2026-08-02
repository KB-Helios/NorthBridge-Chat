mod config;

use std::{io::Write, time::Duration};

use anyhow::{Context, Result};
use config::Config;
use northbridge_agent_runtime::{RigAgentRuntime, RigAgentRuntimeConfig};
use northbridge_core_api::{
    CoreApi, CoreApiConfig, KnowledgeRuntimeConfig, KnowledgeServices,
    custom_local_model_selection, gateway_runtime_profile, load_credential_secret,
};
use northbridge_domain::{
    DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY, ModelSelection, ReadyEvent,
};
use northbridge_model_manager::{ModelManager, ModelManagerConfig};
use northbridge_storage::Database;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let config = Config::load()?;
    let database = Database::connect(&config.database_path)
        .await
        .with_context(|| {
            format!(
                "failed to open desktop database at {}",
                config.database_path.display()
            )
        })?;
    let data_directory = config
        .database_path
        .parent()
        .context("database path must have a parent directory")?
        .to_path_buf();
    let schema_version = database.schema_version().await?;
    let recovered = database.recover_interrupted_generations().await?;
    if !recovered.is_empty() {
        tracing::warn!(
            count = recovered.len(),
            "marked interrupted generations as failed after restart"
        );
    }
    let interrupted_ingestion = database.recover_interrupted_ingestion_jobs().await?;
    if interrupted_ingestion > 0 {
        tracing::warn!(
            count = interrupted_ingestion,
            "marked interrupted ingestion jobs as failed after restart"
        );
    }
    let interrupted_agent_work = database.recover_interrupted_agent_work().await?;
    if interrupted_agent_work > 0 {
        tracing::warn!(
            count = interrupted_agent_work,
            "suspended interrupted agent work for explicit recovery"
        );
    }
    let interrupted_crew_work = database.recover_interrupted_crew_runs().await?;
    if interrupted_crew_work > 0 {
        tracing::warn!(
            count = interrupted_crew_work,
            "suspended interrupted Crew work for explicit recovery"
        );
    }
    let mut ai_factory = config.ai_factory.clone();
    if let Some(factory) = &mut ai_factory {
        let workspace_id = uuid::Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID)
            .context("built-in workspace ID is invalid")?;
        for credential in database.list_credential_references(workspace_id).await? {
            match load_credential_secret(&credential).await {
                Ok(secret) => {
                    factory.environment.insert(
                        northbridge_domain::credential_environment_key(credential.id).into(),
                        secret.into(),
                    );
                }
                Err(error) => {
                    tracing::warn!(credential_id = %credential.id, %error, "credential requires attention before its provider can be used");
                }
            }
        }
    }
    let model_manager = ModelManager::open(
        database.clone(),
        ModelManagerConfig::local(config.models_directory.clone()),
    )
    .await
    .context("failed to initialize the local model manager")?;
    let knowledge = KnowledgeServices::open(
        database.clone(),
        KnowledgeRuntimeConfig::local(data_directory),
        ai_factory.clone(),
        Some(model_manager.clone()),
    )
    .await
    .context("failed to initialize the local knowledge runtime")?;
    let listener = TcpListener::bind(config.bind_address)
        .await
        .with_context(|| format!("failed to bind desktop core to {}", config.bind_address))?;
    let local_address = listener.local_addr()?;
    tracing::info!(
        server_mode = config.server_mode,
        tls_terminated = config.tls_terminated,
        "configured core network mode"
    );
    let persisted_selection = load_persisted_model_selection(&database).await?;
    let initial_selection = config
        .initial_model_id
        .map(|model_id| model_id.to_string_lossy().into_owned())
        .map(|model_id| {
            custom_local_model_selection(model_id)
                .context("NORTHBRIDGE_LOCAL_MODEL is not a valid model ID")
        })
        .transpose()?
        .or(persisted_selection);
    let ai_runtime = match (&ai_factory, &initial_selection) {
        (Some(factory), Some(selection)) => {
            let runtime_reference = model_manager
                .runtime_reference(&selection.model_id)
                .await
                .with_context(|| {
                    format!("failed to resolve selected model {}", selection.model_id)
                })?
                .map_or_else(
                    || selection.model_id.clone().into(),
                    std::path::PathBuf::into_os_string,
                );
            Some(
                factory
                    .prepare_with_profile(
                        runtime_reference,
                        gateway_runtime_profile(&selection.load_profile),
                    )
                    .await
                    .context("failed to prepare the selected local AI runtime")?,
            )
        }
        (None, Some(_)) => anyhow::bail!(
            "a model is selected but the verified local AI sidecar bundle is unavailable"
        ),
        (_, None) => None,
    };
    let agent_runtime = ai_runtime
        .as_ref()
        .map(|runtime| {
            RigAgentRuntime::new(RigAgentRuntimeConfig {
                database: database.clone(),
                ai_runtime: runtime.clone(),
                max_concurrent_runs: 4,
            })
        })
        .transpose()
        .context("failed to initialize the local agent runtime")?;
    if let Some(runtime) = &agent_runtime
        && let Err(error) = runtime
            .synchronize_gateway_protocols(
                uuid::Uuid::parse_str(northbridge_domain::DEFAULT_WORKSPACE_ID)
                    .context("built-in workspace ID is invalid")?,
            )
            .await
    {
        tracing::warn!(%error, "agentgateway protocol synchronization needs attention");
    }
    let mut api = CoreApi::new(
        database,
        CoreApiConfig {
            session_token: config.session_token,
            control_token: config.control_token,
            allowed_origins: config.allowed_origins,
            max_json_body_bytes: 4 * 1024 * 1024,
        },
    )?;
    if let Some(factory) = ai_factory {
        api = api.with_ai_factory(factory);
    }
    api = api.with_model_manager(model_manager);
    api = api.with_knowledge_services(knowledge);
    if let Some(runtime) = agent_runtime {
        api = api.with_agent_runtime(runtime);
    }
    if let (Some(selection), Some(runtime)) = (initial_selection, ai_runtime) {
        api = api.with_ai_runtime(selection, runtime);
    }
    let recovered_shadows = api.recover_shadow_suggestions().await;
    if recovered_shadows > 0 {
        tracing::info!(
            count = recovered_shadows,
            "resumed pending Shadow Persona suggestions"
        );
    }
    let recovered_crews = api.recover_crew_runs().await;
    if recovered_crews > 0 {
        tracing::info!(count = recovered_crews, "resumed pending Crew runs");
    }
    let recovered_workflows = api.recover_workflow_runs().await;
    if recovered_workflows > 0 {
        tracing::warn!(
            count = recovered_workflows,
            "recovered interrupted workflow nodes"
        );
    }
    let recovered_media = api.recover_media_jobs().await;
    if recovered_media > 0 {
        tracing::warn!(
            count = recovered_media,
            "marked interrupted media jobs as unknown outcome"
        );
    }
    api.synchronize_media_routes().await;
    let api_shutdown = api.shutdown_signal();

    emit_ready(&ReadyEvent {
        event: "ready".to_string(),
        address: format!("http://{local_address}"),
        pid: std::process::id(),
        schema_version,
    })?;
    tracing::info!(address = %local_address, schema_version, "desktop core ready");

    let server_result = axum::serve(listener, api.router())
        .with_graceful_shutdown(shutdown_signal(api_shutdown))
        .await;
    let generations_stopped = api.cancel_active_generations(Duration::from_secs(5)).await;
    if !generations_stopped {
        tracing::warn!("active generations did not stop within the graceful shutdown window");
    }
    if !api
        .shutdown_shadow_suggestions(Duration::from_secs(5))
        .await
    {
        tracing::warn!(
            "Shadow Persona suggestions did not stop within the graceful shutdown window"
        );
    }
    if !api.shutdown_crew_runs(Duration::from_secs(5)).await {
        tracing::warn!("Crew runs did not stop within the graceful shutdown window");
    }
    if !api.shutdown_workflow_runs(Duration::from_secs(5)).await {
        tracing::warn!("workflow runs did not stop within the graceful shutdown window");
    }
    if !api.shutdown_media_jobs(Duration::from_secs(5)).await {
        tracing::warn!("media jobs did not stop within the graceful shutdown window");
    }
    if !api.shutdown_agent_runtime(Duration::from_secs(10)).await {
        tracing::warn!("agent runs did not stop within the graceful shutdown window");
    }
    if !api.shutdown_knowledge(Duration::from_secs(10)).await {
        tracing::warn!("knowledge ingestion or embedding runtimes did not stop cleanly");
    }
    if let Err(error) = api.shutdown_ai_runtime().await {
        tracing::error!(%error, "failed to cleanly shut down the local AI runtime");
    }
    if !api.shutdown_model_manager(Duration::from_secs(5)).await {
        tracing::warn!("active model downloads did not stop within the graceful shutdown window");
    }
    server_result.context("desktop core server failed")
}

async fn load_persisted_model_selection(database: &Database) -> Result<Option<ModelSelection>> {
    database
        .get_json_setting(DESKTOP_SETTINGS_SCOPE, MODEL_SELECTION_SETTING_KEY)
        .await?
        .map(|value| {
            serde_json::from_value(value)
                .context("persisted desktop model selection is invalid or incompatible")
        })
        .transpose()
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("northbridge=info,tower_http=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .compact()
        .init();
}

fn emit_ready(event: &ReadyEvent) -> Result<()> {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, event)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

async fn shutdown_signal(api_shutdown: tokio_util::sync::CancellationToken) {
    tokio::select! {
        () = api_shutdown.cancelled() => {
            tracing::info!("desktop core shutdown requested by native shell");
        }
        signal = tokio::signal::ctrl_c() => {
            if let Err(error) = signal {
                tracing::error!(%error, "failed to install shutdown signal handler");
            }
        }
    }
}
