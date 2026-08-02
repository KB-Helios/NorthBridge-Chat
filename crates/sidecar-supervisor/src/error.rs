use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum SidecarError {
    #[error("invalid sidecar specification: {0}")]
    InvalidSpec(String),
    #[error("sidecar lifecycle failure: {0}")]
    Lifecycle(String),
    #[error("sidecar supervisor is no longer available")]
    SupervisorUnavailable,
}

pub type Result<T> = std::result::Result<T, SidecarError>;
