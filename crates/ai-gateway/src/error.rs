use thiserror::Error;

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("invalid AI gateway configuration: {0}")]
    InvalidConfig(String),
    #[error("AI gateway I/O failure: {0}")]
    Io(String),
    #[error("agentgateway rejected its generated configuration: {0}")]
    Validation(String),
    #[error("invalid AI request: {0}")]
    InvalidRequest(String),
    #[error("AI gateway request failed: {0}")]
    Transport(String),
    #[error("AI sidecar failure: {0}")]
    Sidecar(String),
    #[error("AI gateway returned HTTP {status}: {message}")]
    Upstream { status: u16, message: String },
}

pub type Result<T> = std::result::Result<T, GatewayError>;
