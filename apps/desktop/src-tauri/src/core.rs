use std::{
    net::IpAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use northbridge_domain::ReadyEvent;
use serde::Serialize;
use tauri::{AppHandle, Manager};
use tauri_plugin_shell::{
    ShellExt,
    process::{CommandChild, CommandEvent},
};
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;

const CORE_READY_TIMEOUT: Duration = Duration::from_secs(20);
const CORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const CORE_SHUTDOWN_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_TOKEN_HEADER: &str = "x-northbridge-control-token";
const AGENTGATEWAY_SHA256: Option<&str> = option_env!("NORTHBRIDGE_BUNDLED_AGENTGATEWAY_SHA256");
const AGENTGATEWAY_PROFILE: Option<&str> = option_env!("NORTHBRIDGE_BUNDLED_AGENTGATEWAY_PROFILE");
const MISTRALRS_SHA256: Option<&str> = option_env!("NORTHBRIDGE_BUNDLED_MISTRALRS_SHA256");

#[derive(Debug, Clone)]
struct CoreConnection {
    base_url: String,
    session_token: String,
    control_token: String,
    pid: u32,
    schema_version: i64,
}

#[derive(Debug, Clone)]
enum InternalState {
    NotStarted,
    Starting {
        pid: u32,
    },
    Ready(CoreConnection),
    Failed {
        message: String,
    },
    Stopped {
        code: Option<i32>,
        signal: Option<i32>,
    },
}

#[derive(Clone)]
pub(crate) struct CoreRuntime {
    inner: Arc<CoreRuntimeInner>,
}

struct CoreRuntimeInner {
    state: watch::Sender<InternalState>,
    child: Mutex<Option<CommandChild>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopBootstrap {
    pub(crate) base_url: String,
    pub(crate) session_token: String,
    pub(crate) pid: u32,
    pub(crate) schema_version: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoreStatus {
    pub(crate) state: &'static str,
    pub(crate) pid: Option<u32>,
    pub(crate) base_url: Option<String>,
    pub(crate) schema_version: Option<i64>,
    pub(crate) error: Option<String>,
    pub(crate) exit_code: Option<i32>,
    pub(crate) signal: Option<i32>,
}

impl CoreRuntime {
    pub(crate) fn new() -> Self {
        let (state, _) = watch::channel(InternalState::NotStarted);
        Self {
            inner: Arc::new(CoreRuntimeInner {
                state,
                child: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn launch(&self, app: &AppHandle) -> Result<(), String> {
        if !matches!(*self.inner.state.borrow(), InternalState::NotStarted) {
            return Err("desktop-core has already been launched".to_string());
        }
        let database_path = database_path(app)?;
        let session_token = generate_secret_token();
        let control_token = generate_secret_token();
        let mut command = app
            .shell()
            .sidecar("desktop-core")
            .map_err(|error| format!("failed to resolve desktop-core sidecar: {error}"))?
            .env("NORTHBRIDGE_SESSION_TOKEN", &session_token)
            .env("NORTHBRIDGE_CONTROL_TOKEN", &control_token)
            .arg("--database")
            .arg(&database_path);
        if let Some(bundle) = ai_bundle(app)? {
            command = command
                .env("NORTHBRIDGE_AGENTGATEWAY_PATH", bundle.agentgateway)
                .env(
                    "NORTHBRIDGE_AGENTGATEWAY_SHA256",
                    bundle.agentgateway_sha256,
                )
                .env(
                    "NORTHBRIDGE_AGENTGATEWAY_PROFILE",
                    bundle.agentgateway_profile,
                )
                .env("NORTHBRIDGE_MISTRALRS_PATH", bundle.mistralrs)
                .env("NORTHBRIDGE_MISTRALRS_SHA256", bundle.mistralrs_sha256)
                .env("NORTHBRIDGE_AI_RUNTIME_DIRECTORY", bundle.runtime_directory)
                .env("NORTHBRIDGE_HF_CACHE", bundle.hf_cache);
        }
        let (receiver, child) = command
            .spawn()
            .map_err(|error| format!("failed to launch desktop-core: {error}"))?;
        let pid = child.pid();
        *lock_child(&self.inner.child) = Some(child);
        self.inner
            .state
            .send_replace(InternalState::Starting { pid });

        let runtime = self.clone();
        tauri::async_runtime::spawn(async move {
            runtime
                .consume_events(receiver, session_token, control_token, pid)
                .await;
        });
        Ok(())
    }

    pub(crate) async fn bootstrap(&self) -> Result<DesktopBootstrap, String> {
        let mut receiver = self.inner.state.subscribe();
        let deadline = tokio::time::sleep(CORE_READY_TIMEOUT);
        tokio::pin!(deadline);

        loop {
            let current = receiver.borrow().clone();
            match current {
                InternalState::Ready(connection) => {
                    return Ok(DesktopBootstrap {
                        base_url: connection.base_url,
                        session_token: connection.session_token,
                        pid: connection.pid,
                        schema_version: connection.schema_version,
                    });
                }
                InternalState::Failed { message } => return Err(message),
                InternalState::Stopped { code, signal } => {
                    return Err(format!(
                        "desktop-core stopped before becoming ready (code: {code:?}, signal: {signal:?})"
                    ));
                }
                InternalState::NotStarted | InternalState::Starting { .. } => {}
            }

            tokio::select! {
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err("desktop-core state channel closed".to_string());
                    }
                }
                () = &mut deadline => {
                    return Err(format!(
                        "desktop-core did not become ready within {} seconds",
                        CORE_READY_TIMEOUT.as_secs()
                    ));
                }
            }
        }
    }

    pub(crate) fn status(&self) -> CoreStatus {
        match self.inner.state.borrow().clone() {
            InternalState::NotStarted => status("not_started"),
            InternalState::Starting { pid } => CoreStatus {
                pid: Some(pid),
                ..status("starting")
            },
            InternalState::Ready(connection) => CoreStatus {
                pid: Some(connection.pid),
                base_url: Some(connection.base_url),
                schema_version: Some(connection.schema_version),
                ..status("ready")
            },
            InternalState::Failed { message } => CoreStatus {
                error: Some(message),
                ..status("failed")
            },
            InternalState::Stopped { code, signal } => CoreStatus {
                exit_code: code,
                signal,
                ..status("stopped")
            },
        }
    }

    pub(crate) async fn shutdown(&self) {
        if let Err(error) = self.request_graceful_shutdown().await {
            tracing::warn!(%error, "graceful desktop-core shutdown failed; forcing termination");
            self.force_shutdown();
        }
    }

    pub(crate) fn force_shutdown(&self) {
        if let Some(child) = lock_child(&self.inner.child).take()
            && let Err(error) = child.kill()
        {
            tracing::warn!(%error, "failed to terminate desktop-core");
        }
    }

    async fn request_graceful_shutdown(&self) -> Result<(), String> {
        let connection = match self.inner.state.borrow().clone() {
            InternalState::Ready(connection) => connection,
            InternalState::NotStarted | InternalState::Stopped { .. } => return Ok(()),
            InternalState::Starting { .. } => {
                return Err("desktop-core was still starting during shutdown".to_string());
            }
            InternalState::Failed { message } => {
                return Err(format!("desktop-core was already failed: {message}"));
            }
        };
        let mut state = self.inner.state.subscribe();
        let endpoint = Url::parse(&connection.base_url)
            .and_then(|url| url.join("v1/runtime/shutdown"))
            .map_err(|error| {
                format!("failed to resolve desktop-core shutdown endpoint: {error}")
            })?;
        let client = reqwest::Client::builder()
            .timeout(CORE_SHUTDOWN_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| format!("failed to create desktop-core shutdown client: {error}"))?;
        let response = client
            .post(endpoint)
            .bearer_auth(&connection.session_token)
            .header(CONTROL_TOKEN_HEADER, &connection.control_token)
            .send()
            .await
            .map_err(|error| format!("desktop-core shutdown request failed: {error}"))?;
        if response.status() != reqwest::StatusCode::ACCEPTED {
            return Err(format!(
                "desktop-core rejected graceful shutdown with {}",
                response.status()
            ));
        }

        let deadline = tokio::time::sleep(CORE_SHUTDOWN_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            match state.borrow().clone() {
                InternalState::Stopped { .. } => return Ok(()),
                InternalState::Failed { message } => {
                    return Err(format!("desktop-core failed during shutdown: {message}"));
                }
                InternalState::NotStarted
                | InternalState::Starting { .. }
                | InternalState::Ready(_) => {}
            }
            tokio::select! {
                changed = state.changed() => {
                    if changed.is_err() {
                        return Err("desktop-core state channel closed during shutdown".to_string());
                    }
                }
                () = &mut deadline => {
                    return Err(format!(
                        "desktop-core did not stop within {} seconds",
                        CORE_SHUTDOWN_TIMEOUT.as_secs()
                    ));
                }
            }
        }
    }

    async fn consume_events(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<CommandEvent>,
        session_token: String,
        control_token: String,
        expected_pid: u32,
    ) {
        let mut ready_seen = false;
        while let Some(event) = receiver.recv().await {
            match event {
                CommandEvent::Stdout(bytes) if !ready_seen => {
                    match parse_ready(&bytes, expected_pid) {
                        Ok(ready) => {
                            ready_seen = true;
                            self.inner
                                .state
                                .send_replace(InternalState::Ready(CoreConnection {
                                    base_url: ready.address,
                                    session_token: session_token.clone(),
                                    control_token: control_token.clone(),
                                    pid: ready.pid,
                                    schema_version: ready.schema_version,
                                }));
                        }
                        Err(message) => {
                            self.inner
                                .state
                                .send_replace(InternalState::Failed { message });
                            self.force_shutdown();
                        }
                    }
                }
                CommandEvent::Stdout(bytes) => {
                    tracing::debug!(output = %String::from_utf8_lossy(&bytes), "desktop-core stdout");
                }
                CommandEvent::Stderr(bytes) => {
                    tracing::info!(output = %String::from_utf8_lossy(&bytes), "desktop-core");
                }
                CommandEvent::Error(message) => {
                    self.inner
                        .state
                        .send_replace(InternalState::Failed { message });
                    self.force_shutdown();
                }
                CommandEvent::Terminated(payload) => {
                    lock_child(&self.inner.child).take();
                    if !matches!(*self.inner.state.borrow(), InternalState::Failed { .. }) {
                        self.inner.state.send_replace(InternalState::Stopped {
                            code: payload.code,
                            signal: payload.signal,
                        });
                    }
                    break;
                }
                _ => {}
            }
        }
    }
}

fn database_path(app: &AppHandle) -> Result<PathBuf, String> {
    Ok(data_directory(app)?.join("northbridge.sqlite"))
}

fn data_directory(app: &AppHandle) -> Result<PathBuf, String> {
    let data_directory = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("failed to resolve app data directory: {error}"))?;
    std::fs::create_dir_all(&data_directory)
        .map_err(|error| format!("failed to create app data directory: {error}"))?;
    Ok(data_directory)
}

struct AiBundle {
    agentgateway: PathBuf,
    agentgateway_sha256: &'static str,
    agentgateway_profile: &'static str,
    mistralrs: PathBuf,
    mistralrs_sha256: &'static str,
    runtime_directory: PathBuf,
    hf_cache: PathBuf,
}

fn ai_bundle(app: &AppHandle) -> Result<Option<AiBundle>, String> {
    let (Some(agentgateway_sha256), Some(agentgateway_profile), Some(mistralrs_sha256)) =
        (AGENTGATEWAY_SHA256, AGENTGATEWAY_PROFILE, MISTRALRS_SHA256)
    else {
        return Ok(None);
    };
    let executable_directory = std::env::current_exe()
        .map_err(|error| format!("failed to resolve desktop executable: {error}"))?
        .parent()
        .map(std::path::Path::to_path_buf)
        .ok_or_else(|| "desktop executable does not have a parent directory".to_string())?;
    let agentgateway = sidecar_sibling(&executable_directory, "agentgateway");
    let mistralrs = sidecar_sibling(&executable_directory, "mistralrs");
    for (name, path) in [("agentgateway", &agentgateway), ("mistral.rs", &mistralrs)] {
        if !path.is_file() {
            return Err(format!(
                "verified {name} bundle is missing at {}",
                path.display()
            ));
        }
    }
    let data_directory = data_directory(app)?;
    let runtime_directory = data_directory.join("runtime");
    let hf_cache = data_directory.join("models").join("huggingface");
    for directory in [&runtime_directory, &hf_cache] {
        std::fs::create_dir_all(directory).map_err(|error| {
            format!(
                "failed to create local AI directory {}: {error}",
                directory.display()
            )
        })?;
    }

    Ok(Some(AiBundle {
        agentgateway,
        agentgateway_sha256,
        agentgateway_profile,
        mistralrs,
        mistralrs_sha256,
        runtime_directory,
        hf_cache,
    }))
}

fn sidecar_sibling(directory: &std::path::Path, name: &str) -> PathBuf {
    let extension = std::env::consts::EXE_EXTENSION;
    if extension.is_empty() {
        directory.join(name)
    } else {
        directory.join(format!("{name}.{extension}"))
    }
}

fn generate_secret_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn parse_ready(bytes: &[u8], expected_pid: u32) -> Result<ReadyEvent, String> {
    let ready: ReadyEvent = serde_json::from_slice(bytes)
        .map_err(|error| format!("desktop-core emitted an invalid ready event: {error}"))?;
    if ready.event != "ready" {
        return Err(format!(
            "desktop-core emitted an unexpected startup event: {}",
            ready.event
        ));
    }
    if ready.pid != expected_pid {
        return Err(format!(
            "desktop-core ready event PID {} did not match child PID {expected_pid}",
            ready.pid
        ));
    }
    if ready.schema_version <= 0 {
        return Err("desktop-core reported an invalid schema version".to_string());
    }
    let address = Url::parse(&ready.address)
        .map_err(|error| format!("desktop-core reported an invalid address: {error}"))?;
    let ip = address
        .host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .ok_or_else(|| "desktop-core address must use an IP loopback host".to_string())?;
    if address.scheme() != "http" || !ip.is_loopback() || address.port().is_none() {
        return Err("desktop-core address must be an explicit loopback HTTP endpoint".to_string());
    }
    if address.username() != ""
        || address.password().is_some()
        || address.query().is_some()
        || address.fragment().is_some()
        || address.path() != "/"
    {
        return Err("desktop-core address contains unsupported URL components".to_string());
    }
    Ok(ready)
}

fn status(state: &'static str) -> CoreStatus {
    CoreStatus {
        state,
        pid: None,
        base_url: None,
        schema_version: None,
        error: None,
        exit_code: None,
        signal: None,
    }
}

fn lock_child(
    mutex: &Mutex<Option<CommandChild>>,
) -> std::sync::MutexGuard<'_, Option<CommandChild>> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use northbridge_domain::ReadyEvent;

    use super::{generate_secret_token, parse_ready};

    #[test]
    fn session_tokens_are_high_entropy_and_unique() {
        let first = generate_secret_token();
        let second = generate_secret_token();
        assert_eq!(first.len(), 64);
        assert_eq!(second.len(), 64);
        assert_ne!(first, second);
    }

    #[test]
    fn accepts_only_the_expected_loopback_ready_event() {
        let ready = ReadyEvent {
            event: "ready".to_string(),
            address: "http://127.0.0.1:49152".to_string(),
            pid: 42,
            schema_version: 1,
        };
        let bytes = serde_json::to_vec(&ready).unwrap();
        assert!(parse_ready(&bytes, 42).is_ok());

        let remote = ReadyEvent {
            address: "http://192.168.1.20:49152".to_string(),
            ..ready.clone()
        };
        assert!(parse_ready(&serde_json::to_vec(&remote).unwrap(), 42).is_err());
        assert!(parse_ready(&bytes, 41).is_err());
    }
}
