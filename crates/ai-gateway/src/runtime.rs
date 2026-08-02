use std::{
    collections::BTreeMap, ffi::OsString, net::TcpListener, path::PathBuf, sync::Arc,
    time::Duration,
};

use northbridge_sidecar_supervisor::{
    HttpMethod, HttpProbe, RestartPolicy, ShutdownRequest, SidecarLogEntry, SidecarSnapshot,
    SidecarSpec, SidecarSupervisor, verify_executable_integrity,
};
use serde::Serialize;
use serde_json::Value;
use tokio::{process::Command, sync::Mutex, time::timeout};

use crate::{
    AgentgatewayPlan, ConfigArtifact, GatewayA2aRoute, GatewayByteStream, GatewayClient,
    GatewayCredential, GatewayError, GatewayHttpRoute, GatewayMcpTarget, IntegrityPolicy,
    ModelAlias, Result, remove_config, restore_config_atomically, update_config_atomically,
    validate_config_with_binary, write_new_config,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentgatewaySecurityProfile {
    NorthbridgeLoopbackPatched,
    UnpatchedDevelopment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MistralQuantization {
    Q2K,
    Q3K,
    Q4K,
    Q5K,
    Q6K,
    Q8Zero,
}

impl MistralQuantization {
    #[must_use]
    pub const fn as_cli_arg(self) -> &'static str {
        match self {
            Self::Q2K => "q2k",
            Self::Q3K => "q3k",
            Self::Q4K => "q4k",
            Self::Q5K => "q5k",
            Self::Q6K => "q6k",
            Self::Q8Zero => "q8_0",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MistralTuneProfile {
    Quality,
    Balanced,
    Fast,
}

impl MistralTuneProfile {
    #[must_use]
    pub const fn as_cli_arg(self) -> &'static str {
        match self {
            Self::Quality => "quality",
            Self::Balanced => "balanced",
            Self::Fast => "fast",
        }
    }
}

#[derive(Debug, Clone)]
pub struct MistralRuntimeProfile {
    pub quantization: Option<MistralQuantization>,
    pub force_cpu: bool,
    pub max_sequence_length: u32,
    pub max_batch_size: u32,
}

#[derive(Debug, Clone)]
pub struct MistralTuneRequest {
    pub model_id: OsString,
    pub quantization: Option<MistralQuantization>,
    pub profile: MistralTuneProfile,
    pub force_cpu: bool,
    pub max_sequence_length: u32,
    pub max_batch_size: u32,
}

#[derive(Debug, Clone)]
pub struct MistralLaunch {
    pub model_id: OsString,
    pub quantization: Option<MistralQuantization>,
    pub force_cpu: bool,
    pub max_sequence_length: u32,
    pub max_batch_size: u32,
    pub hf_cache: Option<PathBuf>,
}

impl MistralLaunch {
    pub fn validate(&self) -> Result<()> {
        if self.model_id.is_empty() {
            return Err(GatewayError::InvalidConfig(
                "mistral.rs model ID or local path must not be empty".to_string(),
            ));
        }
        if !(128..=1_048_576).contains(&self.max_sequence_length) {
            return Err(GatewayError::InvalidConfig(
                "mistral.rs maximum sequence length must be between 128 and 1,048,576".to_string(),
            ));
        }
        if !(1..=1024).contains(&self.max_batch_size) {
            return Err(GatewayError::InvalidConfig(
                "mistral.rs maximum batch size must be between 1 and 1,024".to_string(),
            ));
        }
        if self
            .hf_cache
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(GatewayError::InvalidConfig(
                "mistral.rs Hugging Face cache path must be absolute".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LocalAiRuntimeOptions {
    pub runtime_directory: PathBuf,
    pub agentgateway_executable: PathBuf,
    pub agentgateway_integrity: IntegrityPolicy,
    pub agentgateway_security_profile: AgentgatewaySecurityProfile,
    pub mistral_executable: PathBuf,
    pub mistral_integrity: IntegrityPolicy,
    pub model_alias: ModelAlias,
    pub mistral: MistralLaunch,
    pub environment: BTreeMap<OsString, OsString>,
    pub allow_unverified_development: bool,
}

#[derive(Debug, Clone)]
pub struct LocalAiRuntimeFactory {
    pub runtime_directory: PathBuf,
    pub agentgateway_executable: PathBuf,
    pub agentgateway_integrity: IntegrityPolicy,
    pub agentgateway_security_profile: AgentgatewaySecurityProfile,
    pub mistral_executable: PathBuf,
    pub mistral_integrity: IntegrityPolicy,
    pub model_alias: ModelAlias,
    pub force_cpu: bool,
    pub max_sequence_length: u32,
    pub max_batch_size: u32,
    pub hf_cache: Option<PathBuf>,
    pub environment: BTreeMap<OsString, OsString>,
    pub allow_unverified_development: bool,
}

impl LocalAiRuntimeFactory {
    pub async fn prepare(&self, model_id: impl Into<OsString>) -> Result<LocalAiRuntime> {
        self.prepare_with_profile(
            model_id,
            MistralRuntimeProfile {
                quantization: None,
                force_cpu: self.force_cpu,
                max_sequence_length: self.max_sequence_length,
                max_batch_size: self.max_batch_size,
            },
        )
        .await
    }

    pub async fn prepare_with_profile(
        &self,
        model_id: impl Into<OsString>,
        profile: MistralRuntimeProfile,
    ) -> Result<LocalAiRuntime> {
        LocalAiRuntime::prepare(LocalAiRuntimeOptions {
            runtime_directory: self.runtime_directory.clone(),
            agentgateway_executable: self.agentgateway_executable.clone(),
            agentgateway_integrity: self.agentgateway_integrity.clone(),
            agentgateway_security_profile: self.agentgateway_security_profile,
            mistral_executable: self.mistral_executable.clone(),
            mistral_integrity: self.mistral_integrity.clone(),
            model_alias: self.model_alias.clone(),
            mistral: MistralLaunch {
                model_id: model_id.into(),
                quantization: profile.quantization,
                force_cpu: profile.force_cpu,
                max_sequence_length: profile.max_sequence_length,
                max_batch_size: profile.max_batch_size,
                hf_cache: self.hf_cache.clone(),
            },
            environment: self.environment.clone(),
            allow_unverified_development: self.allow_unverified_development,
        })
        .await
    }

    pub async fn doctor(&self) -> Result<Value> {
        self.run_mistral_json(
            ["doctor", "--json", "--token-source", "none"].map(OsString::from),
            Duration::from_secs(60),
        )
        .await
    }

    pub async fn tune(&self, request: MistralTuneRequest) -> Result<Value> {
        let launch = MistralLaunch {
            model_id: request.model_id.clone(),
            quantization: request.quantization,
            force_cpu: request.force_cpu,
            max_sequence_length: request.max_sequence_length,
            max_batch_size: request.max_batch_size,
            hf_cache: self.hf_cache.clone(),
        };
        launch.validate()?;
        let mut args = vec![
            OsString::from("tune"),
            OsString::from("--profile"),
            OsString::from(request.profile.as_cli_arg()),
            OsString::from("--json"),
            OsString::from("--token-source"),
            OsString::from("none"),
            OsString::from("auto"),
            OsString::from("--model-id"),
            request.model_id,
            OsString::from("--max-seq-len"),
            OsString::from(request.max_sequence_length.to_string()),
            OsString::from("--max-batch-size"),
            OsString::from(request.max_batch_size.to_string()),
        ];
        if request.force_cpu {
            args.push(OsString::from("--cpu"));
        }
        if let Some(quantization) = request.quantization {
            args.push(OsString::from("--quant"));
            args.push(OsString::from(quantization.as_cli_arg()));
        }
        if let Some(cache) = &self.hf_cache {
            args.push(OsString::from("--hf-cache"));
            args.push(hf_hub_cache(cache).into_os_string());
        }
        self.run_mistral_json(args, Duration::from_secs(2 * 60))
            .await
    }

    async fn run_mistral_json(
        &self,
        args: impl IntoIterator<Item = OsString>,
        command_timeout: Duration,
    ) -> Result<Value> {
        verify_executable_integrity(
            "mistral.rs",
            &self.mistral_executable,
            &self.mistral_integrity,
        )
        .await
        .map_err(|error| GatewayError::Sidecar(error.to_string()))?;
        tokio::fs::create_dir_all(&self.runtime_directory)
            .await
            .map_err(|error| {
                GatewayError::Io(format!(
                    "failed to create AI runtime directory {}: {error}",
                    self.runtime_directory.display()
                ))
            })?;
        let mut command = Command::new(&self.mistral_executable);
        command
            .args(args)
            .envs(&self.environment)
            .current_dir(&self.runtime_directory)
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
        }
        if let Some(cache) = &self.hf_cache {
            command
                .env("HF_HOME", cache)
                .env("HF_HUB_CACHE", hf_hub_cache(cache));
        }
        let output = timeout(command_timeout, command.output())
            .await
            .map_err(|_| {
                GatewayError::Sidecar(format!(
                    "mistral.rs diagnostics exceeded {} seconds",
                    command_timeout.as_secs()
                ))
            })?
            .map_err(|error| {
                GatewayError::Io(format!("failed to execute mistral.rs diagnostics: {error}"))
            })?;
        if !output.status.success() {
            return Err(GatewayError::Sidecar(format!(
                "mistral.rs diagnostics exited with {}: {}",
                output.status,
                bounded_output(&output.stderr)
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(|error| {
            GatewayError::Sidecar(format!(
                "mistral.rs diagnostics returned invalid JSON: {error}"
            ))
        })
    }
}

impl LocalAiRuntimeOptions {
    fn validate(&self) -> Result<()> {
        if !self.runtime_directory.is_absolute() {
            return Err(GatewayError::InvalidConfig(
                "AI runtime directory must be absolute".to_string(),
            ));
        }
        for (name, path) in [
            ("agentgateway", &self.agentgateway_executable),
            ("mistral.rs", &self.mistral_executable),
        ] {
            if !path.is_absolute() {
                return Err(GatewayError::InvalidConfig(format!(
                    "{name} executable path must be absolute: {}",
                    path.display()
                )));
            }
        }
        if !self.allow_unverified_development {
            if matches!(
                self.agentgateway_integrity,
                IntegrityPolicy::SkipForDevelopment
            ) || matches!(self.mistral_integrity, IntegrityPolicy::SkipForDevelopment)
            {
                return Err(GatewayError::InvalidConfig(
                    "production AI runtime requires SHA-256 verification for every sidecar"
                        .to_string(),
                ));
            }
            if self.agentgateway_security_profile
                != AgentgatewaySecurityProfile::NorthbridgeLoopbackPatched
            {
                return Err(GatewayError::InvalidConfig(
                    "production AI runtime refuses unpatched agentgateway because 1.4.0 binds data-plane gateways to all interfaces"
                        .to_string(),
                ));
            }
        }
        self.model_alias.validate()?;
        self.mistral.validate()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalAiRuntimeStatus {
    pub alias: String,
    pub gateway: SidecarSnapshot,
    pub inference: SidecarSnapshot,
    pub gateway_logs: Vec<SidecarLogEntry>,
    pub inference_logs: Vec<SidecarLogEntry>,
}

#[derive(Debug, Clone)]
pub struct AgentgatewayAccess {
    pub base_url: url::Url,
    pub credential: GatewayCredential,
    pub model_alias: String,
}

#[derive(Clone)]
pub struct LocalAiRuntime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    alias: String,
    gateway: SidecarSupervisor,
    inference: SidecarSupervisor,
    client: GatewayClient,
    access: AgentgatewayAccess,
    config: ConfigArtifact,
    plan: Mutex<AgentgatewayPlan>,
    agentgateway_executable: PathBuf,
    start_lock: Mutex<()>,
}

impl LocalAiRuntime {
    pub async fn prepare(options: LocalAiRuntimeOptions) -> Result<Self> {
        options.validate()?;
        if let Some(cache) = &options.mistral.hf_cache {
            tokio::fs::create_dir_all(hf_hub_cache(cache))
                .await
                .map_err(|error| {
                    GatewayError::Io(format!(
                        "failed to create Hugging Face cache {}: {error}",
                        cache.display()
                    ))
                })?;
        }
        verify_runtime_artifacts(&options).await?;
        verify_agentgateway_security_profile(&options).await?;
        let reservations = PortReservations::new(4)?;
        let ports = reservations.ports();
        let inference_base_url = format!("http://127.0.0.1:{}/v1", ports[3])
            .parse()
            .expect("loopback URL is valid");
        let credential = GatewayCredential::generate();
        let plan = AgentgatewayPlan {
            gateway_port: ports[0],
            admin_port: ports[1],
            readiness_port: ports[2],
            inference_base_url,
            credential: credential.clone(),
            aliases: vec![options.model_alias.clone()],
            mcp_targets: Vec::new(),
            a2a_routes: Vec::new(),
            http_routes: Vec::new(),
        };
        let config = write_new_config(&options.runtime_directory, &plan).await?;
        if let Err(error) = verify_executable_integrity(
            "agentgateway",
            &options.agentgateway_executable,
            &options.agentgateway_integrity,
        )
        .await
        .map_err(|error| GatewayError::Sidecar(error.to_string()))
        {
            let _ = remove_config(&config).await;
            return Err(error);
        }
        if let Err(error) = validate_config_with_binary(
            &options.agentgateway_executable,
            &config,
            &[credential.expose_secret().to_string()],
        )
        .await
        {
            let _ = remove_config(&config).await;
            return Err(error);
        }

        let gateway = SidecarSupervisor::spawn(gateway_spec(&options, &plan, &config, &credential))
            .map_err(|error| GatewayError::Sidecar(error.to_string()));
        let inference = SidecarSupervisor::spawn(inference_spec(&options, ports[3]))
            .map_err(|error| GatewayError::Sidecar(error.to_string()));
        let (gateway, inference) = match (gateway, inference) {
            (Ok(gateway), Ok(inference)) => (gateway, inference),
            (gateway, inference) => {
                let _ = remove_config(&config).await;
                return Err(gateway
                    .err()
                    .or_else(|| inference.err())
                    .expect("one error"));
            }
        };
        let client = GatewayClient::new(
            plan.gateway_base_url(),
            credential.clone(),
            [options.model_alias.name.clone()],
        )?;
        let access = AgentgatewayAccess {
            base_url: plan.gateway_base_url(),
            credential,
            model_alias: options.model_alias.name.clone(),
        };
        drop(reservations);

        Ok(Self {
            inner: Arc::new(RuntimeInner {
                alias: options.model_alias.name,
                gateway,
                inference,
                client,
                access,
                config,
                plan: Mutex::new(plan),
                agentgateway_executable: options.agentgateway_executable,
                start_lock: Mutex::new(()),
            }),
        })
    }

    pub async fn ensure_ready(&self) -> Result<LocalAiRuntimeStatus> {
        let _guard = self.inner.start_lock.lock().await;
        if let Err(error) = self.inner.inference.ensure_started().await {
            return Err(sidecar_failure(&self.inner.inference, error));
        }
        if let Err(error) = self.inner.gateway.ensure_started().await {
            let _ = self.inner.inference.stop().await;
            return Err(sidecar_failure(&self.inner.gateway, error));
        }
        let models = match self.inner.client.list_models().await {
            Ok(models) => models,
            Err(error) => {
                let _ = self.inner.gateway.stop().await;
                let _ = self.inner.inference.stop().await;
                return Err(error);
            }
        };
        let alias_visible = models
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|models| {
                models
                    .iter()
                    .any(|model| model.get("id").and_then(Value::as_str) == Some(&self.inner.alias))
            });
        if !alias_visible {
            let _ = self.inner.gateway.stop().await;
            let _ = self.inner.inference.stop().await;
            return Err(GatewayError::Validation(format!(
                "agentgateway did not publish configured alias {}",
                self.inner.alias
            )));
        }
        Ok(self.status())
    }

    pub async fn stream_chat(&self, request: Value) -> Result<GatewayByteStream> {
        self.ensure_ready().await?;
        self.inner.client.stream_chat(request).await
    }

    pub async fn embeddings(&self, inputs: &[String]) -> Result<crate::GatewayEmbeddingResponse> {
        self.ensure_ready().await?;
        self.inner
            .client
            .embeddings(&self.inner.alias, inputs)
            .await
    }

    #[must_use]
    pub fn agentgateway_access(&self) -> AgentgatewayAccess {
        self.inner.access.clone()
    }

    pub async fn reconfigure_protocols(
        &self,
        mcp_targets: Vec<GatewayMcpTarget>,
        a2a_routes: Vec<GatewayA2aRoute>,
    ) -> Result<()> {
        let _guard = self.inner.start_lock.lock().await;
        let mut plan_guard = self.inner.plan.lock().await;
        let mut candidate = plan_guard.clone();
        candidate.mcp_targets = mcp_targets;
        candidate.a2a_routes = a2a_routes;
        candidate.validate()?;
        let gateway_running = self.inner.gateway.snapshot().pid.is_some();
        let reload_started_at_ms = epoch_millis();
        let old_contents = update_config_atomically(
            &self.inner.agentgateway_executable,
            &self.inner.config,
            &candidate,
            &[self.inner.access.credential.expose_secret().to_string()],
        )
        .await?;

        if gateway_running
            && let Err(error) = self.observe_gateway_reload(reload_started_at_ms).await
        {
            restore_config_atomically(&self.inner.config, &old_contents).await?;
            let _ = self.observe_gateway_reload(epoch_millis()).await;
            return Err(error);
        }
        *plan_guard = candidate;
        Ok(())
    }

    pub async fn reconfigure_http_routes(&self, http_routes: Vec<GatewayHttpRoute>) -> Result<()> {
        let _guard = self.inner.start_lock.lock().await;
        let mut plan_guard = self.inner.plan.lock().await;
        let mut candidate = plan_guard.clone();
        candidate.http_routes = http_routes;
        candidate.validate()?;
        let gateway_running = self.inner.gateway.snapshot().pid.is_some();
        let reload_started_at_ms = epoch_millis();
        let old_contents = update_config_atomically(
            &self.inner.agentgateway_executable,
            &self.inner.config,
            &candidate,
            &[self.inner.access.credential.expose_secret().to_string()],
        )
        .await?;

        if gateway_running
            && let Err(error) = self.observe_gateway_reload(reload_started_at_ms).await
        {
            restore_config_atomically(&self.inner.config, &old_contents).await?;
            let _ = self.observe_gateway_reload(epoch_millis()).await;
            return Err(error);
        }
        *plan_guard = candidate;
        Ok(())
    }

    async fn observe_gateway_reload(&self, started_at_ms: u64) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut reload_observed = false;
        loop {
            if self.inner.gateway.snapshot().pid.is_none() {
                return Err(sidecar_failure(
                    &self.inner.gateway,
                    "agentgateway stopped while reloading configuration",
                ));
            }
            reload_observed |= self.inner.gateway.logs().iter().any(|entry| {
                entry.timestamp_ms >= started_at_ms
                    && entry.message.to_ascii_lowercase().contains("reloading")
            });
            if reload_observed && self.inner.client.list_models().await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(GatewayError::Validation(
                    "agentgateway did not confirm a healthy configuration reload".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[must_use]
    pub fn status(&self) -> LocalAiRuntimeStatus {
        LocalAiRuntimeStatus {
            alias: self.inner.alias.clone(),
            gateway: self.inner.gateway.snapshot(),
            inference: self.inner.inference.snapshot(),
            gateway_logs: self.inner.gateway.logs(),
            inference_logs: self.inner.inference.logs(),
        }
    }

    pub async fn shutdown(&self) -> Result<LocalAiRuntimeStatus> {
        let (gateway_result, inference_result) = tokio::join!(
            self.inner.gateway.shutdown(),
            self.inner.inference.shutdown()
        );
        let cleanup_result = remove_config(&self.inner.config).await;
        gateway_result.map_err(|error| GatewayError::Sidecar(error.to_string()))?;
        inference_result.map_err(|error| GatewayError::Sidecar(error.to_string()))?;
        cleanup_result?;
        Ok(self.status())
    }
}

async fn verify_runtime_artifacts(options: &LocalAiRuntimeOptions) -> Result<()> {
    for (name, executable, integrity) in [
        (
            "agentgateway",
            options.agentgateway_executable.as_path(),
            &options.agentgateway_integrity,
        ),
        (
            "mistral.rs",
            options.mistral_executable.as_path(),
            &options.mistral_integrity,
        ),
    ] {
        verify_executable_integrity(name, executable, integrity)
            .await
            .map_err(|error| GatewayError::Sidecar(error.to_string()))?;
    }
    Ok(())
}

async fn verify_agentgateway_security_profile(options: &LocalAiRuntimeOptions) -> Result<()> {
    if options.agentgateway_security_profile == AgentgatewaySecurityProfile::UnpatchedDevelopment {
        return Ok(());
    }

    let mut command = Command::new(&options.agentgateway_executable);
    command.arg("-V").kill_on_drop(true);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    }
    let output = timeout(Duration::from_secs(10), command.output())
        .await
        .map_err(|_| {
            GatewayError::Validation("agentgateway security marker probe timed out".to_string())
        })?
        .map_err(|error| {
            GatewayError::Validation(format!(
                "failed to run agentgateway security marker probe: {error}"
            ))
        })?;
    let marker = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !output.status.success() || marker != "northbridge-loopback-v1.4.0" {
        return Err(GatewayError::Validation(format!(
            "agentgateway lacks the required NorthBridge loopback marker (got {marker:?})"
        )));
    }
    Ok(())
}

fn gateway_spec(
    options: &LocalAiRuntimeOptions,
    plan: &AgentgatewayPlan,
    config: &ConfigArtifact,
    credential: &GatewayCredential,
) -> SidecarSpec {
    SidecarSpec {
        name: "agentgateway".to_string(),
        executable: options.agentgateway_executable.clone(),
        args: vec![OsString::from("-f"), config.path.clone().into_os_string()],
        env: options.environment.clone(),
        working_directory: Some(options.runtime_directory.clone()),
        readiness: HttpProbe::loopback(plan.readiness_url(), 200)
            .expect("generated readiness URL is loopback"),
        shutdown: Some(
            ShutdownRequest::loopback(plan.shutdown_url(), HttpMethod::Post)
                .expect("generated shutdown URL is loopback"),
        ),
        startup_timeout: Duration::from_secs(30),
        probe_interval: Duration::from_millis(100),
        request_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(10),
        restart: RestartPolicy::default(),
        integrity: options.agentgateway_integrity.clone(),
        log_capacity: 512,
        redactions: vec![credential.expose_secret().to_string()],
    }
}

fn inference_spec(options: &LocalAiRuntimeOptions, port: u16) -> SidecarSpec {
    let mut environment = options.environment.clone();
    if let Some(cache) = &options.mistral.hf_cache {
        environment.insert(OsString::from("HF_HOME"), cache.clone().into_os_string());
        environment.insert(
            OsString::from("HF_HUB_CACHE"),
            hf_hub_cache(cache).into_os_string(),
        );
    }
    SidecarSpec {
        name: "mistral.rs".to_string(),
        executable: options.mistral_executable.clone(),
        args: mistral_args(&options.mistral, port),
        env: environment,
        working_directory: Some(options.runtime_directory.clone()),
        readiness: HttpProbe::loopback(
            format!("http://127.0.0.1:{port}/v1/models")
                .parse()
                .expect("loopback URL is valid"),
            200,
        )
        .expect("generated inference URL is loopback"),
        shutdown: None,
        startup_timeout: Duration::from_secs(15 * 60),
        probe_interval: Duration::from_millis(250),
        request_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(5),
        restart: RestartPolicy {
            max_restarts: 2,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
        },
        integrity: options.mistral_integrity.clone(),
        log_capacity: 2048,
        redactions: Vec::new(),
    }
}

fn mistral_args(launch: &MistralLaunch, port: u16) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("serve"),
        OsString::from("--host"),
        OsString::from("127.0.0.1"),
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--no-ui"),
        OsString::from("--disable-access-log"),
        OsString::from("--disable-metrics"),
        OsString::from("--max-seq-len"),
        OsString::from(launch.max_sequence_length.to_string()),
        OsString::from("--max-batch-size"),
        OsString::from(launch.max_batch_size.to_string()),
        OsString::from("--model-id"),
        launch.model_id.clone(),
    ];
    if launch.force_cpu {
        args.push(OsString::from("--cpu"));
    }
    if let Some(quantization) = launch.quantization {
        args.push(OsString::from("--isq"));
        args.push(OsString::from(quantization.as_cli_arg()));
    }
    if let Some(cache) = &launch.hf_cache {
        args.push(OsString::from("--hf-cache"));
        args.push(hf_hub_cache(cache).into_os_string());
    }
    args
}

fn hf_hub_cache(cache_home: &std::path::Path) -> PathBuf {
    cache_home.join("hub")
}

fn sidecar_failure(supervisor: &SidecarSupervisor, error: impl std::fmt::Display) -> GatewayError {
    let logs = supervisor.logs();
    let tail = logs
        .iter()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|entry| {
            let message = entry.message.chars().take(512).collect::<String>();
            format!("{:?}: {message}", entry.stream)
        })
        .collect::<Vec<_>>()
        .join(" | ");
    if tail.is_empty() {
        GatewayError::Sidecar(error.to_string())
    } else {
        GatewayError::Sidecar(format!("{error}; recent redacted logs: {tail}"))
    }
}

fn bounded_output(bytes: &[u8]) -> String {
    const MAX_DIAGNOSTIC_BYTES: usize = 4096;
    let length = bytes.len().min(MAX_DIAGNOSTIC_BYTES);
    let mut output = String::from_utf8_lossy(&bytes[..length]).trim().to_string();
    if bytes.len() > MAX_DIAGNOSTIC_BYTES {
        output.push_str("...");
    }
    output
}

fn epoch_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

struct PortReservations {
    listeners: Vec<TcpListener>,
}

impl PortReservations {
    fn new(count: usize) -> Result<Self> {
        let listeners = (0..count)
            .map(|_| {
                TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
                    GatewayError::Io(format!("failed to reserve loopback sidecar port: {error}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { listeners })
    }

    fn ports(&self) -> Vec<u16> {
        self.listeners
            .iter()
            .map(|listener| listener.local_addr().expect("bound listener").port())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, ffi::OsString};

    use super::{MistralLaunch, PortReservations, hf_hub_cache, inference_spec, mistral_args};
    use crate::{AgentgatewaySecurityProfile, IntegrityPolicy, ModelAlias};

    #[test]
    fn reserves_distinct_dynamic_loopback_ports() {
        let reservations = PortReservations::new(8).unwrap();
        let ports = reservations.ports();
        assert!(ports.iter().all(|port| *port > 0));
        assert_eq!(ports.iter().copied().collect::<BTreeSet<_>>().len(), 8);
    }

    #[test]
    fn mistral_launch_is_loopback_and_never_enables_unsafe_agent_tools() {
        let launch = MistralLaunch {
            model_id: OsString::from("local-model"),
            quantization: Some(super::MistralQuantization::Q4K),
            force_cpu: true,
            max_sequence_length: 4096,
            max_batch_size: 1,
            hf_cache: None,
        };
        let args = mistral_args(&launch, 43210);
        let args = args
            .iter()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(args.windows(2).any(|pair| pair == ["--host", "127.0.0.1"]));
        assert!(args.contains(&"--no-ui".into()));
        assert!(args.contains(&"--cpu".into()));
        assert!(args.windows(2).any(|pair| pair == ["--isq", "q4k"]));
        assert!(!args.iter().any(|arg| {
            matches!(
                arg.as_ref(),
                "--agent" | "--enable-code-execution" | "--enable-shell" | "--mcp-port"
            )
        }));
    }

    #[test]
    fn mistral_uses_an_isolated_hugging_face_home_and_hub_cache() {
        let cache_home = std::env::temp_dir().join("northbridge-hf-home");
        let options = super::LocalAiRuntimeOptions {
            runtime_directory: std::env::temp_dir().join("northbridge-runtime"),
            agentgateway_executable: std::env::current_exe().unwrap(),
            agentgateway_integrity: IntegrityPolicy::SkipForDevelopment,
            agentgateway_security_profile: AgentgatewaySecurityProfile::UnpatchedDevelopment,
            mistral_executable: std::env::current_exe().unwrap(),
            mistral_integrity: IntegrityPolicy::SkipForDevelopment,
            model_alias: ModelAlias::local_chat("fast-local").unwrap(),
            mistral: MistralLaunch {
                model_id: OsString::from("org/model"),
                quantization: None,
                force_cpu: true,
                max_sequence_length: 8192,
                max_batch_size: 1,
                hf_cache: Some(cache_home.clone()),
            },
            environment: Default::default(),
            allow_unverified_development: true,
        };

        let spec = inference_spec(&options, 43210);
        assert_eq!(
            spec.env.get(&OsString::from("HF_HOME")),
            Some(&cache_home.clone().into_os_string())
        );
        assert_eq!(
            spec.env.get(&OsString::from("HF_HUB_CACHE")),
            Some(&hf_hub_cache(&cache_home).into_os_string())
        );
        assert!(spec.args.windows(2).any(|pair| {
            pair[0] == "--hf-cache" && pair[1] == hf_hub_cache(&cache_home).into_os_string()
        }));
    }
}
