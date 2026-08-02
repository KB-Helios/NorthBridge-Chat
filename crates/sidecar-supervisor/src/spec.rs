use std::{collections::BTreeMap, ffi::OsString, net::IpAddr, path::PathBuf, time::Duration};

use url::{Host, Url};

use crate::error::{Result, SidecarError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Debug, Clone)]
pub struct HttpProbe {
    pub url: Url,
    pub expected_status: u16,
}

impl HttpProbe {
    pub fn loopback(url: Url, expected_status: u16) -> Result<Self> {
        ensure_loopback_http_url(&url)?;
        if !(100..=599).contains(&expected_status) {
            return Err(SidecarError::InvalidSpec(format!(
                "invalid expected HTTP status {expected_status}"
            )));
        }
        Ok(Self {
            url,
            expected_status,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ShutdownRequest {
    pub url: Url,
    pub method: HttpMethod,
}

impl ShutdownRequest {
    pub fn loopback(url: Url, method: HttpMethod) -> Result<Self> {
        ensure_loopback_http_url(&url)?;
        Ok(Self { url, method })
    }
}

#[derive(Debug, Clone)]
pub struct RestartPolicy {
    pub max_restarts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityPolicy {
    Sha256(String),
    SkipForDevelopment,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 3,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SidecarSpec {
    pub name: String,
    pub executable: PathBuf,
    pub args: Vec<OsString>,
    pub env: BTreeMap<OsString, OsString>,
    pub working_directory: Option<PathBuf>,
    pub readiness: HttpProbe,
    pub shutdown: Option<ShutdownRequest>,
    pub startup_timeout: Duration,
    pub probe_interval: Duration,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub restart: RestartPolicy,
    pub integrity: IntegrityPolicy,
    pub log_capacity: usize,
    pub redactions: Vec<String>,
}

impl SidecarSpec {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(SidecarError::InvalidSpec(
                "sidecar name must not be empty".to_string(),
            ));
        }
        if !self.executable.is_absolute() {
            return Err(SidecarError::InvalidSpec(format!(
                "sidecar executable must be an absolute path: {}",
                self.executable.display()
            )));
        }
        ensure_loopback_http_url(&self.readiness.url)?;
        if let Some(shutdown) = &self.shutdown {
            ensure_loopback_http_url(&shutdown.url)?;
        }
        for (label, duration) in [
            ("startup timeout", self.startup_timeout),
            ("probe interval", self.probe_interval),
            ("request timeout", self.request_timeout),
            ("shutdown timeout", self.shutdown_timeout),
        ] {
            if duration.is_zero() {
                return Err(SidecarError::InvalidSpec(format!(
                    "{label} must be greater than zero"
                )));
            }
        }
        if self.restart.initial_delay.is_zero() || self.restart.max_delay.is_zero() {
            return Err(SidecarError::InvalidSpec(
                "restart delays must be greater than zero".to_string(),
            ));
        }
        if self.restart.initial_delay > self.restart.max_delay {
            return Err(SidecarError::InvalidSpec(
                "initial restart delay must not exceed maximum delay".to_string(),
            ));
        }
        if self.log_capacity == 0 {
            return Err(SidecarError::InvalidSpec(
                "log capacity must be greater than zero".to_string(),
            ));
        }
        if let IntegrityPolicy::Sha256(hash) = &self.integrity {
            decode_sha256(hash)?;
        }
        Ok(())
    }
}

pub(crate) fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SidecarError::InvalidSpec(
            "expected SHA-256 must contain exactly 64 hexadecimal characters".to_string(),
        ));
    }

    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16).map_err(|error| {
            SidecarError::InvalidSpec(format!("invalid expected SHA-256: {error}"))
        })?;
    }
    Ok(output)
}

fn ensure_loopback_http_url(url: &Url) -> Result<()> {
    if url.scheme() != "http" {
        return Err(SidecarError::InvalidSpec(format!(
            "sidecar control URL must use http on loopback: {url}"
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(SidecarError::InvalidSpec(
            "sidecar control URL must not contain credentials".to_string(),
        ));
    }
    if url.port().is_none() {
        return Err(SidecarError::InvalidSpec(format!(
            "sidecar control URL must use an explicit port: {url}"
        )));
    }

    let ip = match url.host() {
        Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
        _ => {
            return Err(SidecarError::InvalidSpec(format!(
                "sidecar control URL must use a loopback IP literal: {url}"
            )));
        }
    };
    if !ip.is_loopback() {
        return Err(SidecarError::InvalidSpec(format!(
            "sidecar control URL is not loopback: {url}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{HttpProbe, decode_sha256};

    #[test]
    fn accepts_only_loopback_ip_literals() {
        assert!(HttpProbe::loopback("http://127.0.0.1:8080/health".parse().unwrap(), 200).is_ok());
        assert!(HttpProbe::loopback("http://[::1]:8080/health".parse().unwrap(), 200).is_ok());
        assert!(HttpProbe::loopback("http://0.0.0.0:8080/health".parse().unwrap(), 200).is_err());
        assert!(HttpProbe::loopback("http://localhost:8080/health".parse().unwrap(), 200).is_err());
    }

    #[test]
    fn parses_sha256_case_insensitively() {
        let value = "F60AC4318C0352A18C2419842FE1CC1FDCA0521500848260A3F03A2F98D4AC87";
        let decoded = decode_sha256(value).unwrap();
        assert_eq!(decoded[0], 0xf6);
        assert_eq!(decoded[31], 0x87);
        assert!(decode_sha256("not-a-hash").is_err());
    }
}
