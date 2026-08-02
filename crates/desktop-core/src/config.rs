use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use northbridge_ai_gateway::{
    AgentgatewaySecurityProfile, IntegrityPolicy, LocalAiRuntimeFactory, ModelAlias,
};
use northbridge_core_api::LOCAL_MODEL_ALIAS;

const DEFAULT_BIND: &str = "127.0.0.1:0";
const DEFAULT_DATABASE: &str = "northbridge.sqlite";

pub(crate) struct Config {
    pub(crate) database_path: PathBuf,
    pub(crate) models_directory: PathBuf,
    pub(crate) bind_address: SocketAddr,
    pub(crate) session_token: String,
    pub(crate) control_token: String,
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) server_mode: bool,
    pub(crate) tls_terminated: bool,
    pub(crate) ai_factory: Option<LocalAiRuntimeFactory>,
    pub(crate) initial_model_id: Option<OsString>,
}

impl Config {
    pub(crate) fn load() -> Result<Self> {
        let mut database_path = env::var_os("NORTHBRIDGE_DATABASE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATABASE));
        let mut bind = env::var("NORTHBRIDGE_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_string());
        let mut allowed_origins = env::var("NORTHBRIDGE_ALLOWED_ORIGINS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut server_mode = parse_flag("NORTHBRIDGE_SERVER_MODE")?;
        let mut tls_terminated = parse_flag("NORTHBRIDGE_TLS_TERMINATED")?;

        let mut arguments = env::args_os().skip(1);
        while let Some(argument) = arguments.next() {
            let argument = argument.to_string_lossy();
            match argument.as_ref() {
                "--database" => {
                    database_path = PathBuf::from(
                        arguments
                            .next()
                            .context("--database requires a filesystem path")?,
                    );
                }
                "--bind" => {
                    bind = arguments
                        .next()
                        .context("--bind requires an IP address and port")?
                        .to_string_lossy()
                        .into_owned();
                }
                "--allowed-origin" => {
                    allowed_origins.push(
                        arguments
                            .next()
                            .context("--allowed-origin requires an origin")?
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
                "--server" => server_mode = true,
                "--tls-terminated" => tls_terminated = true,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                value => bail!("unsupported argument: {value}"),
            }
        }

        database_path = absolute_path(database_path)?;
        let data_directory = database_path
            .parent()
            .context("database path must have a parent directory")?;
        let models_directory = optional_absolute_path(
            "NORTHBRIDGE_MODELS_DIRECTORY",
            data_directory.join("models"),
        )?;
        let bind_address = SocketAddr::from_str(&bind)
            .with_context(|| format!("invalid NORTHBRIDGE_BIND address: {bind}"))?;
        ensure_bind_allowed(bind_address.ip(), server_mode, tls_terminated)?;
        let session_token = env::var("NORTHBRIDGE_SESSION_TOKEN")
            .context("NORTHBRIDGE_SESSION_TOKEN is required")?;
        let control_token = env::var("NORTHBRIDGE_CONTROL_TOKEN")
            .context("NORTHBRIDGE_CONTROL_TOKEN is required")?;
        if allowed_origins.is_empty() && server_mode {
            bail!("server mode requires an explicit NORTHBRIDGE_ALLOWED_ORIGINS allowlist");
        }
        if allowed_origins.is_empty() {
            allowed_origins = vec![
                "http://tauri.localhost".to_string(),
                "tauri://localhost".to_string(),
            ];
        }
        let initial_model_id = env::var_os("NORTHBRIDGE_LOCAL_MODEL");
        if initial_model_id
            .as_ref()
            .is_some_and(|model_id| model_id.as_os_str().is_empty())
        {
            bail!("NORTHBRIDGE_LOCAL_MODEL must not be empty when present");
        }
        let ai_factory = load_ai_factory(&database_path, initial_model_id.is_some())?;

        Ok(Self {
            database_path,
            models_directory,
            bind_address,
            session_token,
            control_token,
            allowed_origins,
            server_mode,
            tls_terminated,
            ai_factory,
            initial_model_id,
        })
    }
}

fn load_ai_factory(
    database_path: &std::path::Path,
    model_requested: bool,
) -> Result<Option<LocalAiRuntimeFactory>> {
    let bundle_present = [
        "NORTHBRIDGE_AGENTGATEWAY_PATH",
        "NORTHBRIDGE_AGENTGATEWAY_SHA256",
        "NORTHBRIDGE_AGENTGATEWAY_PROFILE",
        "NORTHBRIDGE_MISTRALRS_PATH",
        "NORTHBRIDGE_MISTRALRS_SHA256",
    ]
    .iter()
    .any(|name| env::var_os(name).is_some());
    if !bundle_present {
        if model_requested {
            bail!("NORTHBRIDGE_LOCAL_MODEL requires the verified local AI sidecar bundle");
        }
        return Ok(None);
    }

    let data_directory = database_path
        .parent()
        .context("database path must have a parent directory")?;
    let runtime_directory = optional_absolute_path(
        "NORTHBRIDGE_AI_RUNTIME_DIRECTORY",
        data_directory.join("runtime"),
    )?;
    let hf_cache = optional_absolute_path(
        "NORTHBRIDGE_HF_CACHE",
        data_directory.join("models").join("huggingface"),
    )?;
    let agentgateway_executable = required_absolute_path("NORTHBRIDGE_AGENTGATEWAY_PATH")?;
    let mistral_executable = required_absolute_path("NORTHBRIDGE_MISTRALRS_PATH")?;
    let agentgateway_integrity = required_integrity("NORTHBRIDGE_AGENTGATEWAY_SHA256")?;
    let mistral_integrity = required_integrity("NORTHBRIDGE_MISTRALRS_SHA256")?;
    let agentgateway_security_profile = match env::var("NORTHBRIDGE_AGENTGATEWAY_PROFILE")
        .context("NORTHBRIDGE_AGENTGATEWAY_PROFILE is required when local AI is configured")?
        .as_str()
    {
        "northbridge-loopback-v1" => AgentgatewaySecurityProfile::NorthbridgeLoopbackPatched,
        value => bail!("unsupported agentgateway security profile: {value}"),
    };
    Ok(Some(LocalAiRuntimeFactory {
        runtime_directory,
        agentgateway_executable,
        agentgateway_integrity,
        agentgateway_security_profile,
        mistral_executable,
        mistral_integrity,
        model_alias: ModelAlias::local_chat(LOCAL_MODEL_ALIAS)?,
        force_cpu: true,
        max_sequence_length: 8192,
        max_batch_size: 1,
        hf_cache: Some(hf_cache),
        environment: BTreeMap::new(),
        allow_unverified_development: false,
    }))
}

fn required_integrity(name: &str) -> Result<IntegrityPolicy> {
    let value = env::var(name)
        .with_context(|| format!("{name} is required when local AI is configured"))?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{name} must contain exactly 64 hexadecimal characters");
    }
    Ok(IntegrityPolicy::Sha256(value.to_ascii_lowercase()))
}

fn required_absolute_path(name: &str) -> Result<PathBuf> {
    let value = env::var_os(name)
        .with_context(|| format!("{name} is required when local AI is configured"))?;
    absolute_path(PathBuf::from(value)).with_context(|| format!("invalid {name}"))
}

fn optional_absolute_path(name: &str, default: PathBuf) -> Result<PathBuf> {
    let value = env::var_os(name).map(PathBuf::from).unwrap_or(default);
    absolute_path(value).with_context(|| format!("invalid {name}"))
}

fn absolute_path(path: PathBuf) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("filesystem path must not be empty");
    }
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(env::current_dir()
        .context("failed to resolve current working directory")?
        .join(path))
}

fn ensure_bind_allowed(ip: IpAddr, server_mode: bool, tls_terminated: bool) -> Result<()> {
    if ip.is_loopback() {
        return Ok(());
    }
    if !server_mode {
        bail!("desktop-core refuses a non-loopback bind outside explicit server mode: {ip}");
    }
    if !tls_terminated {
        bail!(
            "server mode requires NORTHBRIDGE_TLS_TERMINATED=true and a trusted TLS reverse proxy"
        );
    }
    Ok(())
}

fn parse_flag(name: &str) -> Result<bool> {
    match env::var(name).ok().as_deref() {
        None | Some("") | Some("0" | "false" | "FALSE" | "no" | "NO") => Ok(false),
        Some("1" | "true" | "TRUE" | "yes" | "YES") => Ok(true),
        Some(value) => bail!("{name} must be true or false, got {value}"),
    }
}

fn print_help() {
    println!(
        "desktop-core\n\n\
         Environment:\n\
           NORTHBRIDGE_SESSION_TOKEN   Required high-entropy bearer token\n\
           NORTHBRIDGE_CONTROL_TOKEN   Required native-shell shutdown token\n\
           NORTHBRIDGE_DATABASE_PATH   SQLite path (default: northbridge.sqlite)\n\
           NORTHBRIDGE_MODELS_DIRECTORY Managed model root (default: database directory/models)\n\
           NORTHBRIDGE_BIND            Loopback listener (default: 127.0.0.1:0)\n\
           NORTHBRIDGE_ALLOWED_ORIGINS Comma-separated WebView origins\n\n\
           NORTHBRIDGE_SERVER_MODE     Permit an authenticated non-loopback headless bind\n\
           NORTHBRIDGE_TLS_TERMINATED  Assert a trusted TLS reverse proxy terminates HTTPS\n\n\
           NORTHBRIDGE_LOCAL_MODEL     Optional Hugging Face model ID or local path\n\
           NORTHBRIDGE_AGENTGATEWAY_PATH / _SHA256 / _PROFILE\n\
                                      Required verified gateway bundle metadata\n\
           NORTHBRIDGE_MISTRALRS_PATH / _SHA256\n\
                                      Required verified inference bundle metadata\n\n\
         Options:\n\
           --database PATH\n\
           --bind ADDRESS\n\
           --allowed-origin ORIGIN (repeatable)"
    );
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{absolute_path, ensure_bind_allowed};

    #[test]
    fn accepts_loopback_addresses_only() {
        assert!(ensure_bind_allowed("127.0.0.1".parse().unwrap(), false, false).is_ok());
        assert!(ensure_bind_allowed("::1".parse().unwrap(), false, false).is_ok());
        assert!(ensure_bind_allowed("0.0.0.0".parse().unwrap(), false, true).is_err());
        assert!(ensure_bind_allowed("0.0.0.0".parse().unwrap(), true, false).is_err());
        assert!(ensure_bind_allowed("0.0.0.0".parse().unwrap(), true, true).is_ok());
    }

    #[test]
    fn resolves_relative_paths_without_requiring_them_to_exist() {
        let path = absolute_path(PathBuf::from("future.sqlite")).unwrap();
        assert!(path.is_absolute());
        assert!(path.ends_with("future.sqlite"));
    }
}
