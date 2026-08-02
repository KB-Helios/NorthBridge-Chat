use std::{collections::BTreeMap, net::IpAddr, path::PathBuf, time::Duration};

use serde::Serialize;
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};
use url::{Host, Url};
use uuid::Uuid;

use crate::{GatewayCredential, GatewayError, ModelAlias, Result};

#[derive(Debug, Clone)]
pub struct AgentgatewayPlan {
    pub gateway_port: u16,
    pub admin_port: u16,
    pub readiness_port: u16,
    pub inference_base_url: Url,
    pub credential: GatewayCredential,
    pub aliases: Vec<ModelAlias>,
    pub mcp_targets: Vec<GatewayMcpTarget>,
    pub a2a_routes: Vec<GatewayA2aRoute>,
    pub http_routes: Vec<GatewayHttpRoute>,
}

#[derive(Debug, Clone)]
pub enum GatewayMcpTarget {
    Stdio {
        name: String,
        command: PathBuf,
        args: Vec<String>,
        environment: BTreeMap<String, String>,
    },
    StreamableHttp {
        name: String,
        endpoint: Url,
    },
    Sse {
        name: String,
        endpoint: Url,
    },
    OpenApi {
        name: String,
        endpoint: Url,
        schema: GatewaySchemaSource,
    },
}

#[derive(Debug, Clone)]
pub enum GatewaySchemaSource {
    File(PathBuf),
    Inline(String),
    Remote(Url),
}

#[derive(Debug, Clone)]
pub struct GatewayA2aRoute {
    pub name: String,
    pub gateway_path: String,
    pub upstream: Url,
    pub auth_environment_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GatewayHttpRoute {
    pub name: String,
    pub gateway_path: String,
    pub upstream: Url,
    pub auth_environment_key: Option<String>,
}

impl AgentgatewayPlan {
    pub fn validate(&self) -> Result<()> {
        let ports = [self.gateway_port, self.admin_port, self.readiness_port];
        if ports.contains(&0) {
            return Err(GatewayError::InvalidConfig(
                "agentgateway ports must be allocated before rendering configuration".to_string(),
            ));
        }
        if ports[0] == ports[1] || ports[0] == ports[2] || ports[1] == ports[2] {
            return Err(GatewayError::InvalidConfig(
                "agentgateway gateway, admin, and readiness ports must be distinct".to_string(),
            ));
        }
        ensure_loopback_inference_url(&self.inference_base_url)?;
        if self.aliases.is_empty() {
            return Err(GatewayError::InvalidConfig(
                "agentgateway requires at least one model alias".to_string(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for alias in &self.aliases {
            alias.validate()?;
            if !names.insert(&alias.name) {
                return Err(GatewayError::InvalidConfig(format!(
                    "duplicate model alias {}",
                    alias.name
                )));
            }
        }
        let mut mcp_names = std::collections::BTreeSet::new();
        for target in &self.mcp_targets {
            let name = target.name();
            validate_protocol_name("MCP target", name)?;
            if !mcp_names.insert(name) {
                return Err(GatewayError::InvalidConfig(format!(
                    "duplicate MCP target {name}"
                )));
            }
            target.validate()?;
        }
        let mut route_names = std::collections::BTreeSet::new();
        let mut route_paths = std::collections::BTreeSet::new();
        for route in &self.a2a_routes {
            validate_protocol_name("A2A route", &route.name)?;
            validate_remote_http_url("A2A upstream", &route.upstream)?;
            if !route.gateway_path.starts_with("/a2a/")
                || route.gateway_path.ends_with('/')
                || route.gateway_path.contains("..")
                || route.gateway_path.contains('?')
                || route.gateway_path.contains('#')
            {
                return Err(GatewayError::InvalidConfig(format!(
                    "A2A route {} must use an assigned /a2a/<id> gateway path",
                    route.name
                )));
            }
            if !route_names.insert(route.name.as_str())
                || !route_paths.insert(route.gateway_path.as_str())
            {
                return Err(GatewayError::InvalidConfig(
                    "A2A route names and gateway paths must be unique".to_string(),
                ));
            }
            if let Some(key) = &route.auth_environment_key
                && !valid_environment_key(key)
            {
                return Err(GatewayError::InvalidConfig(format!(
                    "A2A route {} has an invalid authentication environment key",
                    route.name
                )));
            }
        }
        for route in &self.http_routes {
            validate_protocol_name("HTTP route", &route.name)?;
            validate_remote_http_url("HTTP upstream", &route.upstream)?;
            if !route.gateway_path.starts_with("/media/")
                || route.gateway_path.ends_with('/')
                || route.gateway_path.contains("..")
                || route.gateway_path.contains('?')
                || route.gateway_path.contains('#')
            {
                return Err(GatewayError::InvalidConfig(format!(
                    "HTTP route {} must use an assigned /media/<id> gateway path",
                    route.name
                )));
            }
            if !route_names.insert(route.name.as_str())
                || !route_paths.insert(route.gateway_path.as_str())
            {
                return Err(GatewayError::InvalidConfig(
                    "gateway route names and paths must be unique".to_string(),
                ));
            }
            if let Some(key) = &route.auth_environment_key
                && !valid_environment_key(key)
            {
                return Err(GatewayError::InvalidConfig(format!(
                    "HTTP route {} has an invalid authentication environment key",
                    route.name
                )));
            }
        }
        Ok(())
    }

    pub fn render_yaml(&self) -> Result<String> {
        self.validate()?;
        let gateway = Gateway {
            port: self.gateway_port,
            api_key: ApiKeyPolicy {
                mode: "strict",
                keys: vec![ApiKey {
                    key: self.credential.expose_secret(),
                    metadata: ApiKeyMetadata {
                        owner: "northbridge-desktop-core",
                    },
                }],
            },
        };
        let models = self
            .aliases
            .iter()
            .map(|alias| LlmModel {
                name: &alias.name,
                provider: CustomProvider {
                    custom: CustomProviderFormats {
                        formats: alias
                            .apis
                            .iter()
                            .map(|api| ApiFormat { kind: *api })
                            .collect(),
                    },
                },
                params: ModelParams {
                    model: &alias.upstream_model,
                    base_url: self.inference_base_url.as_str(),
                },
            })
            .collect();
        let mut routes = self
            .a2a_routes
            .iter()
            .map(render_a2a_route)
            .collect::<Result<Vec<_>>>()?;
        routes.extend(
            self.http_routes
                .iter()
                .map(render_http_route)
                .collect::<Result<Vec<_>>>()?,
        );
        let mcp = (!self.mcp_targets.is_empty()).then(|| McpConfig {
            gateways: vec!["default"],
            targets: self.mcp_targets.iter().map(render_mcp_target).collect(),
            prefix_mode: "always",
            failure_mode: "failClosed",
        });
        let config = RootConfig {
            config: StaticConfig {
                enable_ipv6: false,
                admin_addr: format!("127.0.0.1:{}", self.admin_port),
                stats_addr: "off",
                readiness_addr: format!("127.0.0.1:{}", self.readiness_port),
            },
            gateways: BTreeMap::from([("default", gateway)]),
            llm: LlmConfig {
                gateways: vec!["default"],
                models,
            },
            routes,
            mcp,
        };
        let yaml = serde_yaml::to_string(&config).map_err(|error| {
            GatewayError::InvalidConfig(format!("failed to render agentgateway YAML: {error}"))
        })?;
        Ok(format!(
            "# Generated by NorthBridge for pinned agentgateway 1.4.0.\n# Contains a per-launch credential; do not copy or log.\n{yaml}"
        ))
    }

    #[must_use]
    pub fn gateway_base_url(&self) -> Url {
        format!("http://127.0.0.1:{}/", self.gateway_port)
            .parse()
            .expect("validated loopback URL")
    }

    #[must_use]
    pub fn readiness_url(&self) -> Url {
        format!("http://127.0.0.1:{}/healthz/ready", self.readiness_port)
            .parse()
            .expect("validated loopback URL")
    }

    #[must_use]
    pub fn shutdown_url(&self) -> Url {
        format!("http://127.0.0.1:{}/quitquitquit", self.admin_port)
            .parse()
            .expect("validated loopback URL")
    }
}

#[derive(Debug, Clone)]
pub struct ConfigArtifact {
    pub path: PathBuf,
    pub session_id: Uuid,
}

pub async fn write_new_config(
    directory: impl Into<PathBuf>,
    plan: &AgentgatewayPlan,
) -> Result<ConfigArtifact> {
    let directory = directory.into();
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| {
            GatewayError::Io(format!(
                "failed to create agentgateway runtime directory {}: {error}",
                directory.display()
            ))
        })?;
    let session_id = Uuid::new_v4();
    let path = directory.join(format!("agentgateway-{session_id}.yaml"));
    let temporary = directory.join(format!(".agentgateway-{session_id}.yaml.tmp"));
    let contents = plan.render_yaml()?;
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| {
            GatewayError::Io(format!(
                "failed to create temporary gateway config {}: {error}",
                temporary.display()
            ))
        })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|error| {
                GatewayError::Io(format!(
                    "failed to restrict gateway config permissions: {error}"
                ))
            })?;
    }

    if let Err(error) = file.write_all(contents.as_bytes()).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(GatewayError::Io(format!(
            "failed to write gateway config: {error}"
        )));
    }
    if let Err(error) = file.sync_all().await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(GatewayError::Io(format!(
            "failed to sync gateway config: {error}"
        )));
    }
    drop(file);
    tokio::fs::rename(&temporary, &path)
        .await
        .map_err(|error| {
            GatewayError::Io(format!(
                "failed to publish gateway config {}: {error}",
                path.display()
            ))
        })?;
    Ok(ConfigArtifact { path, session_id })
}

pub async fn remove_config(artifact: &ConfigArtifact) -> Result<()> {
    match tokio::fs::remove_file(&artifact.path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(GatewayError::Io(format!(
            "failed to remove gateway config {}: {error}",
            artifact.path.display()
        ))),
    }
}

pub async fn update_config_atomically(
    executable: &std::path::Path,
    artifact: &ConfigArtifact,
    plan: &AgentgatewayPlan,
    redactions: &[String],
) -> Result<String> {
    let old_contents = tokio::fs::read_to_string(&artifact.path)
        .await
        .map_err(|error| {
            GatewayError::Io(format!(
                "failed to read active gateway config {}: {error}",
                artifact.path.display()
            ))
        })?;
    let candidate = artifact.path.with_file_name(format!(
        ".agentgateway-{}-update-{}.yaml.tmp",
        artifact.session_id,
        Uuid::new_v4()
    ));
    write_restricted_file(&candidate, &plan.render_yaml()?).await?;
    let candidate_artifact = ConfigArtifact {
        path: candidate.clone(),
        session_id: artifact.session_id,
    };
    if let Err(error) =
        validate_config_with_binary(executable, &candidate_artifact, redactions).await
    {
        let _ = tokio::fs::remove_file(&candidate).await;
        return Err(error);
    }
    if let Err(error) = atomic_replace(candidate.clone(), artifact.path.clone()).await {
        let _ = tokio::fs::remove_file(&candidate).await;
        return Err(error);
    }
    Ok(old_contents)
}

pub async fn restore_config_atomically(artifact: &ConfigArtifact, contents: &str) -> Result<()> {
    let candidate = artifact.path.with_file_name(format!(
        ".agentgateway-{}-rollback-{}.yaml.tmp",
        artifact.session_id,
        Uuid::new_v4()
    ));
    write_restricted_file(&candidate, contents).await?;
    if let Err(error) = atomic_replace(candidate.clone(), artifact.path.clone()).await {
        let _ = tokio::fs::remove_file(&candidate).await;
        return Err(error);
    }
    Ok(())
}

async fn write_restricted_file(path: &std::path::Path, contents: &str) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
        .map_err(|error| {
            GatewayError::Io(format!(
                "failed to create staged gateway config {}: {error}",
                path.display()
            ))
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|error| {
                GatewayError::Io(format!("failed to restrict gateway config: {error}"))
            })?;
    }
    if let Err(error) = file.write_all(contents.as_bytes()).await {
        let _ = tokio::fs::remove_file(path).await;
        return Err(GatewayError::Io(format!(
            "failed to write staged gateway config: {error}"
        )));
    }
    if let Err(error) = file.sync_all().await {
        let _ = tokio::fs::remove_file(path).await;
        return Err(GatewayError::Io(format!(
            "failed to sync staged gateway config: {error}"
        )));
    }
    Ok(())
}

async fn atomic_replace(source: PathBuf, destination: PathBuf) -> Result<()> {
    #[cfg(windows)]
    {
        tokio::task::spawn_blocking(move || atomic_replace_windows(&source, &destination))
            .await
            .map_err(|error| {
                GatewayError::Io(format!("gateway config replace task failed: {error}"))
            })??;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        tokio::fs::rename(&source, &destination)
            .await
            .map_err(|error| {
                GatewayError::Io(format!(
                    "failed to atomically replace gateway config {}: {error}",
                    destination.display()
                ))
            })
    }
}

#[cfg(windows)]
fn atomic_replace_windows(source: &std::path::Path, destination: &std::path::Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{REPLACEFILE_WRITE_THROUGH, ReplaceFileW};

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both paths are owned, NUL-terminated UTF-16 buffers that remain
    // alive for the duration of the synchronous Windows API call.
    let replaced = unsafe {
        ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            std::ptr::null(),
            REPLACEFILE_WRITE_THROUGH,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if replaced == 0 {
        return Err(GatewayError::Io(format!(
            "failed to atomically replace gateway config: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

pub async fn validate_config_with_binary(
    executable: &std::path::Path,
    artifact: &ConfigArtifact,
    redactions: &[String],
) -> Result<()> {
    if !executable.is_absolute() {
        return Err(GatewayError::InvalidConfig(format!(
            "agentgateway executable must be absolute: {}",
            executable.display()
        )));
    }
    let mut command = Command::new(executable);
    command
        .arg("-f")
        .arg(&artifact.path)
        .arg("--validate-only")
        .kill_on_drop(true);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.as_std_mut().creation_flags(CREATE_NO_WINDOW);
    }

    let output = timeout(Duration::from_secs(15), command.output())
        .await
        .map_err(|_| GatewayError::Validation("validation timed out".to_string()))?
        .map_err(|error| {
            GatewayError::Validation(format!("failed to launch validator: {error}"))
        })?;
    if output.status.success() {
        return Ok(());
    }
    let mut detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if detail.is_empty() {
        detail = String::from_utf8_lossy(&output.stdout).trim().to_string();
    }
    for secret in redactions.iter().filter(|secret| !secret.is_empty()) {
        detail = detail.replace(secret, "[REDACTED]");
    }
    if detail.len() > 4096 {
        detail.truncate(4096);
        detail.push('…');
    }
    Err(GatewayError::Validation(detail))
}

fn ensure_loopback_inference_url(url: &Url) -> Result<()> {
    if url.scheme() != "http" || url.port().is_none() {
        return Err(GatewayError::InvalidConfig(format!(
            "local inference base URL must use HTTP and an explicit port: {url}"
        )));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GatewayError::InvalidConfig(
            "local inference base URL must not contain credentials, query, or fragment".to_string(),
        ));
    }
    let ip = match url.host() {
        Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
        _ => {
            return Err(GatewayError::InvalidConfig(format!(
                "local inference base URL must use a loopback IP literal: {url}"
            )));
        }
    };
    if !ip.is_loopback() || url.path().trim_end_matches('/') != "/v1" {
        return Err(GatewayError::InvalidConfig(format!(
            "local inference base URL must target loopback /v1: {url}"
        )));
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RootConfig<'a> {
    config: StaticConfig<'a>,
    gateways: BTreeMap<&'a str, Gateway<'a>>,
    llm: LlmConfig<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    routes: Vec<RouteYaml>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mcp: Option<McpConfig<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StaticConfig<'a> {
    enable_ipv6: bool,
    admin_addr: String,
    stats_addr: &'a str,
    readiness_addr: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Gateway<'a> {
    port: u16,
    api_key: ApiKeyPolicy<'a>,
}

#[derive(Serialize)]
struct ApiKeyPolicy<'a> {
    mode: &'a str,
    keys: Vec<ApiKey<'a>>,
}

#[derive(Serialize)]
struct ApiKey<'a> {
    key: &'a str,
    metadata: ApiKeyMetadata<'a>,
}

#[derive(Serialize)]
struct ApiKeyMetadata<'a> {
    owner: &'a str,
}

#[derive(Serialize)]
struct LlmConfig<'a> {
    gateways: Vec<&'a str>,
    models: Vec<LlmModel<'a>>,
}

#[derive(Serialize)]
struct LlmModel<'a> {
    name: &'a str,
    provider: CustomProvider,
    params: ModelParams<'a>,
}

#[derive(Serialize)]
struct CustomProvider {
    custom: CustomProviderFormats,
}

#[derive(Serialize)]
struct CustomProviderFormats {
    formats: Vec<ApiFormat>,
}

#[derive(Serialize)]
struct ApiFormat {
    #[serde(rename = "type")]
    kind: crate::ModelApi,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ModelParams<'a> {
    model: &'a str,
    base_url: &'a str,
}

impl GatewayMcpTarget {
    fn name(&self) -> &str {
        match self {
            Self::Stdio { name, .. }
            | Self::StreamableHttp { name, .. }
            | Self::Sse { name, .. }
            | Self::OpenApi { name, .. } => name,
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Stdio {
                command,
                args,
                environment,
                ..
            } => {
                if !command.is_absolute() {
                    return Err(GatewayError::InvalidConfig(format!(
                        "MCP stdio executable must be absolute: {}",
                        command.display()
                    )));
                }
                if args.iter().any(|value| value.contains('\0'))
                    || environment.iter().any(|(key, value)| {
                        key.is_empty()
                            || key.contains('\0')
                            || key.contains('=')
                            || value.contains('\0')
                    })
                {
                    return Err(GatewayError::InvalidConfig(
                        "MCP stdio arguments or environment are invalid".to_string(),
                    ));
                }
            }
            Self::StreamableHttp { endpoint, .. } | Self::Sse { endpoint, .. } => {
                validate_remote_http_url("MCP endpoint", endpoint)?;
            }
            Self::OpenApi {
                endpoint, schema, ..
            } => {
                validate_remote_http_url("OpenAPI MCP endpoint", endpoint)?;
                schema.validate()?;
            }
        }
        Ok(())
    }
}

impl GatewaySchemaSource {
    fn validate(&self) -> Result<()> {
        match self {
            Self::File(path) if !path.is_absolute() => Err(GatewayError::InvalidConfig(format!(
                "OpenAPI schema path must be absolute: {}",
                path.display()
            ))),
            Self::Inline(value) if value.trim().is_empty() => Err(GatewayError::InvalidConfig(
                "inline OpenAPI schema cannot be empty".to_string(),
            )),
            Self::Remote(url) => validate_remote_http_url("OpenAPI schema URL", url),
            Self::File(_) | Self::Inline(_) => Ok(()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpConfig<'a> {
    gateways: Vec<&'a str>,
    targets: Vec<McpTargetYaml>,
    prefix_mode: &'a str,
    failure_mode: &'a str,
}

#[derive(Serialize)]
struct McpTargetYaml {
    name: String,
    #[serde(flatten)]
    spec: McpTargetSpecYaml,
}

#[derive(Serialize)]
#[serde(untagged)]
enum McpTargetSpecYaml {
    Stdio { stdio: McpStdioYaml },
    Mcp { mcp: McpEndpointYaml },
    Sse { sse: McpEndpointYaml },
    OpenApi { openapi: McpOpenApiYaml },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpStdioYaml {
    cmd: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    args: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: BTreeMap<String, String>,
    clear_env: bool,
}

#[derive(Serialize)]
struct McpEndpointYaml {
    host: String,
}

#[derive(Serialize)]
struct McpOpenApiYaml {
    host: String,
    schema: GatewaySchemaYaml,
}

#[derive(Serialize)]
#[serde(untagged)]
enum GatewaySchemaYaml {
    File { file: String },
    Inline(String),
    Remote { url: String },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RouteYaml {
    name: String,
    gateways: Vec<&'static str>,
    matches: Vec<RouteMatchYaml>,
    policies: RoutePoliciesYaml,
    backends: Vec<RouteBackendYaml>,
}

#[derive(Serialize)]
struct RouteMatchYaml {
    path: RoutePathMatchYaml,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RoutePathMatchYaml {
    path_prefix: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RoutePoliciesYaml {
    #[serde(skip_serializing_if = "Option::is_none")]
    a2a: Option<EmptyYaml>,
    url_rewrite: UrlRewriteYaml,
    request_header_modifier: RequestHeaderModifierYaml,
}

#[derive(Serialize)]
struct RequestHeaderModifierYaml {
    remove: Vec<&'static str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    set: BTreeMap<&'static str, String>,
}

#[derive(Serialize)]
struct UrlRewriteYaml {
    path: PathRewriteYaml,
}

#[derive(Serialize)]
struct PathRewriteYaml {
    prefix: String,
}

#[derive(Serialize)]
struct RouteBackendYaml {
    host: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    policies: Option<RouteBackendPoliciesYaml>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RouteBackendPoliciesYaml {
    #[serde(rename = "backendTLS")]
    backend_tls: EmptyYaml,
}

#[derive(Serialize)]
struct EmptyYaml {}

fn render_mcp_target(target: &GatewayMcpTarget) -> McpTargetYaml {
    match target {
        GatewayMcpTarget::Stdio {
            name,
            command,
            args,
            environment,
        } => McpTargetYaml {
            name: name.clone(),
            spec: McpTargetSpecYaml::Stdio {
                stdio: McpStdioYaml {
                    cmd: command.to_string_lossy().into_owned(),
                    args: args.clone(),
                    env: environment.clone(),
                    clear_env: true,
                },
            },
        },
        GatewayMcpTarget::StreamableHttp { name, endpoint } => McpTargetYaml {
            name: name.clone(),
            spec: McpTargetSpecYaml::Mcp {
                mcp: McpEndpointYaml {
                    host: endpoint.to_string(),
                },
            },
        },
        GatewayMcpTarget::Sse { name, endpoint } => McpTargetYaml {
            name: name.clone(),
            spec: McpTargetSpecYaml::Sse {
                sse: McpEndpointYaml {
                    host: endpoint.to_string(),
                },
            },
        },
        GatewayMcpTarget::OpenApi {
            name,
            endpoint,
            schema,
        } => McpTargetYaml {
            name: name.clone(),
            spec: McpTargetSpecYaml::OpenApi {
                openapi: McpOpenApiYaml {
                    host: endpoint.to_string(),
                    schema: match schema {
                        GatewaySchemaSource::File(path) => GatewaySchemaYaml::File {
                            file: path.to_string_lossy().into_owned(),
                        },
                        GatewaySchemaSource::Inline(value) => {
                            GatewaySchemaYaml::Inline(value.clone())
                        }
                        GatewaySchemaSource::Remote(url) => GatewaySchemaYaml::Remote {
                            url: url.to_string(),
                        },
                    },
                },
            },
        },
    }
}

fn render_a2a_route(route: &GatewayA2aRoute) -> Result<RouteYaml> {
    let host = backend_host(&route.upstream)?;
    let upstream_path = route.upstream.path().trim_end_matches('/');
    Ok(RouteYaml {
        name: route.name.clone(),
        gateways: vec!["default"],
        matches: vec![RouteMatchYaml {
            path: RoutePathMatchYaml {
                path_prefix: route.gateway_path.clone(),
            },
        }],
        policies: RoutePoliciesYaml {
            a2a: Some(EmptyYaml {}),
            url_rewrite: UrlRewriteYaml {
                path: PathRewriteYaml {
                    prefix: if upstream_path.is_empty() {
                        "/".to_string()
                    } else {
                        upstream_path.to_string()
                    },
                },
            },
            request_header_modifier: RequestHeaderModifierYaml {
                remove: vec!["authorization"],
                set: route
                    .auth_environment_key
                    .as_ref()
                    .map(|key| BTreeMap::from([("authorization", format!("Bearer ${{{key}}}"))]))
                    .unwrap_or_default(),
            },
        },
        backends: vec![RouteBackendYaml {
            host,
            policies: (route.upstream.scheme() == "https").then_some(RouteBackendPoliciesYaml {
                backend_tls: EmptyYaml {},
            }),
        }],
    })
}

fn render_http_route(route: &GatewayHttpRoute) -> Result<RouteYaml> {
    let host = backend_host(&route.upstream)?;
    let upstream_path = route.upstream.path().trim_end_matches('/');
    Ok(RouteYaml {
        name: route.name.clone(),
        gateways: vec!["default"],
        matches: vec![RouteMatchYaml {
            path: RoutePathMatchYaml {
                path_prefix: route.gateway_path.clone(),
            },
        }],
        policies: RoutePoliciesYaml {
            a2a: None,
            url_rewrite: UrlRewriteYaml {
                path: PathRewriteYaml {
                    prefix: if upstream_path.is_empty() {
                        "/".to_string()
                    } else {
                        upstream_path.to_string()
                    },
                },
            },
            request_header_modifier: RequestHeaderModifierYaml {
                remove: vec!["authorization"],
                set: route
                    .auth_environment_key
                    .as_ref()
                    .map(|key| BTreeMap::from([("authorization", format!("Bearer ${{{key}}}"))]))
                    .unwrap_or_default(),
            },
        },
        backends: vec![RouteBackendYaml {
            host,
            policies: (route.upstream.scheme() == "https").then_some(RouteBackendPoliciesYaml {
                backend_tls: EmptyYaml {},
            }),
        }],
    })
}

fn validate_protocol_name(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 80
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(GatewayError::InvalidConfig(format!(
            "{kind} name may contain only letters, digits, underscores, and hyphens"
        )));
    }
    Ok(())
}

fn valid_environment_key(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn validate_remote_http_url(kind: &str, url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GatewayError::InvalidConfig(format!(
            "{kind} must be an absolute HTTP(S) URL without credentials, query, or fragment"
        )));
    }
    Ok(())
}

fn backend_host(url: &Url) -> Result<String> {
    let host = match url.host() {
        Some(Host::Ipv4(value)) => value.to_string(),
        Some(Host::Ipv6(value)) => format!("[{value}]"),
        Some(Host::Domain(value)) => value.to_string(),
        None => {
            return Err(GatewayError::InvalidConfig(
                "backend URL omitted a host".to_string(),
            ));
        }
    };
    let port = url.port_or_known_default().ok_or_else(|| {
        GatewayError::InvalidConfig("backend URL omitted a usable port".to_string())
    })?;
    Ok(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{AgentgatewayPlan, restore_config_atomically, write_new_config};
    use crate::{GatewayCredential, ModelAlias, ModelApi};

    fn plan() -> AgentgatewayPlan {
        AgentgatewayPlan {
            gateway_port: 41001,
            admin_port: 41002,
            readiness_port: 41003,
            inference_base_url: "http://127.0.0.1:41004/v1".parse().unwrap(),
            credential: GatewayCredential::parse(
                "nb-test-0000000000000000000000000000000000000000".to_string(),
            )
            .unwrap(),
            aliases: vec![ModelAlias {
                name: "fast-local".to_string(),
                upstream_model: "default".to_string(),
                apis: BTreeSet::from([
                    ModelApi::Completions,
                    ModelApi::Responses,
                    ModelApi::Embeddings,
                ]),
            }],
            mcp_targets: Vec::new(),
            a2a_routes: Vec::new(),
            http_routes: Vec::new(),
        }
    }

    #[test]
    fn renders_pinned_agentgateway_schema_without_direct_provider_access() {
        let rendered = plan().render_yaml().unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(&rendered).unwrap();
        assert_eq!(value["config"]["enableIpv6"], false);
        assert_eq!(value["gateways"]["default"]["port"], 41001);
        assert_eq!(value["gateways"]["default"]["apiKey"]["mode"], "strict");
        assert_eq!(value["llm"]["gateways"][0], "default");
        assert_eq!(value["llm"]["models"][0]["name"], "fast-local");
        assert_eq!(
            value["llm"]["models"][0]["params"]["baseUrl"],
            "http://127.0.0.1:41004/v1"
        );
    }

    #[tokio::test]
    async fn writes_a_new_atomic_per_session_config() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = write_new_config(directory.path(), &plan()).await.unwrap();
        assert!(artifact.path.is_file());
        assert!(artifact.path.starts_with(directory.path()));
        let contents = tokio::fs::read_to_string(&artifact.path).await.unwrap();
        assert!(contents.contains("do not copy or log"));
        assert!(contents.contains("fast-local"));
    }

    #[tokio::test]
    async fn atomically_restores_the_watched_config_path() {
        let directory = tempfile::tempdir().unwrap();
        let artifact = write_new_config(directory.path(), &plan()).await.unwrap();
        let original = tokio::fs::read_to_string(&artifact.path).await.unwrap();
        restore_config_atomically(&artifact, "replacement")
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&artifact.path).await.unwrap(),
            "replacement"
        );
        restore_config_atomically(&artifact, &original)
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read_to_string(&artifact.path).await.unwrap(),
            original
        );
    }

    #[test]
    fn rejects_non_loopback_inference_and_duplicate_ports() {
        let mut invalid = plan();
        invalid.inference_base_url = "http://0.0.0.0:41004/v1".parse().unwrap();
        assert!(invalid.validate().is_err());

        let mut invalid = plan();
        invalid.admin_port = invalid.gateway_port;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn renders_mcp_and_a2a_only_on_the_authenticated_gateway() {
        let mut plan = plan();
        plan.mcp_targets
            .push(super::GatewayMcpTarget::StreamableHttp {
                name: "docs".to_string(),
                endpoint: "https://mcp.example.test/mcp".parse().unwrap(),
            });
        plan.a2a_routes.push(super::GatewayA2aRoute {
            name: "researcher".to_string(),
            gateway_path: "/a2a/018f0000-0000-7000-8000-000000000001".to_string(),
            upstream: "https://agents.example.test/base".parse().unwrap(),
            auth_environment_key: Some("REMOTE_AGENT_TOKEN".to_string()),
        });
        plan.http_routes.push(super::GatewayHttpRoute {
            name: "image-provider".to_string(),
            gateway_path: "/media/018f0000-0000-7000-8000-000000000002".to_string(),
            upstream: "https://media.example.test/v1/images/generations"
                .parse()
                .unwrap(),
            auth_environment_key: Some("MEDIA_PROVIDER_TOKEN".to_string()),
        });
        let value: serde_yaml::Value = serde_yaml::from_str(&plan.render_yaml().unwrap()).unwrap();
        assert_eq!(value["mcp"]["gateways"][0], "default");
        assert_eq!(value["mcp"]["prefixMode"], "always");
        assert_eq!(value["mcp"]["targets"][0]["name"], "docs");
        assert_eq!(value["routes"][0]["gateways"][0], "default");
        assert!(value["routes"][0]["policies"]["a2a"].is_mapping());
        assert_eq!(
            value["routes"][0]["policies"]["urlRewrite"]["path"]["prefix"],
            "/base"
        );
        assert_eq!(
            value["routes"][0]["policies"]["requestHeaderModifier"]["remove"][0],
            "authorization"
        );
        assert_eq!(
            value["routes"][0]["policies"]["requestHeaderModifier"]["set"]["authorization"],
            "Bearer ${REMOTE_AGENT_TOKEN}"
        );
        assert!(value["routes"][0]["backends"][0]["policies"]["backendTLS"].is_mapping());
        assert!(value["routes"][1]["policies"]["a2a"].is_null());
        assert_eq!(
            value["routes"][1]["policies"]["urlRewrite"]["path"]["prefix"],
            "/v1/images/generations"
        );
        assert_eq!(
            value["routes"][1]["policies"]["requestHeaderModifier"]["remove"][0],
            "authorization"
        );
        assert_eq!(
            value["routes"][1]["policies"]["requestHeaderModifier"]["set"]["authorization"],
            "Bearer ${MEDIA_PROVIDER_TOKEN}"
        );
    }
}
