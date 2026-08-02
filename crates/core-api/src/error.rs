use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use northbridge_agent_runtime::AgentRuntimeError;
use northbridge_ai_gateway::GatewayError;
use northbridge_ingestion::IngestionError;
use northbridge_model_manager::ModelManagerError;
use northbridge_retrieval::RetrievalError;
use northbridge_storage::StorageError;
use serde::Serialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CoreApiConfigError>;

#[derive(Debug, Error)]
pub enum CoreApiConfigError {
    #[error("{0} token must contain at least 32 bytes")]
    WeakToken(&'static str),
    #[error("{0} token cannot contain whitespace")]
    InvalidToken(&'static str),
    #[error("at least one allowed WebView origin is required")]
    MissingOrigins,
    #[error("invalid WebView origin: {0}")]
    InvalidOrigin(String),
    #[error("JSON body limit must be between 1 byte and 64 MiB, got {0}")]
    InvalidBodyLimit(usize),
}

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }

    pub(crate) fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "a valid desktop session token is required".to_string(),
        }
    }

    pub(crate) fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: "the authenticated principal is not allowed to perform this operation"
                .to_string(),
        }
    }

    pub(crate) fn unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code,
            message: message.into(),
        }
    }

    pub(crate) fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<GatewayError> for ApiError {
    fn from(error: GatewayError) -> Self {
        match error {
            GatewayError::InvalidRequest(message) => Self::invalid(message),
            error => {
                tracing::error!(error = %error, "AI gateway operation failed");
                Self {
                    status: StatusCode::BAD_GATEWAY,
                    code: "ai_gateway_failure",
                    message: "the local AI runtime could not complete the request".to_string(),
                }
            }
        }
    }
}

impl From<AgentRuntimeError> for ApiError {
    fn from(error: AgentRuntimeError) -> Self {
        match error {
            AgentRuntimeError::Storage(error) => error.into(),
            AgentRuntimeError::AlreadyActive(id) => Self::conflict(
                "agent_run_active",
                format!("agent run {id} is already active"),
            ),
            AgentRuntimeError::NotResumable(status) => Self::conflict(
                "agent_run_not_resumable",
                format!("agent run cannot resume from {status:?}"),
            ),
            AgentRuntimeError::IncompatibleCheckpoint { .. } => Self::conflict(
                "agent_checkpoint_incompatible",
                "the saved agent checkpoint was created by an incompatible runtime",
            ),
            AgentRuntimeError::ShuttingDown => Self::unavailable(
                "agent_runtime_shutting_down",
                "the local agent runtime is shutting down",
            ),
            error => {
                tracing::error!(%error, "agent runtime operation failed");
                Self {
                    status: StatusCode::BAD_GATEWAY,
                    code: "agent_runtime_failure",
                    message: "the local agent runtime could not complete the request".to_string(),
                }
            }
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound { entity } => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: format!("{entity} was not found"),
            },
            StorageError::Conflict(message) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict",
                message,
            },
            StorageError::InvalidInput(message) => Self::invalid(message),
            error => {
                tracing::error!(error = %error, "core API storage failure");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "internal_error",
                    message: "the local core could not complete the request".to_string(),
                }
            }
        }
    }
}

impl From<ModelManagerError> for ApiError {
    fn from(error: ModelManagerError) -> Self {
        match error {
            ModelManagerError::InvalidInput(message) => Self::invalid(message),
            ModelManagerError::Conflict(message) => {
                Self::conflict("model_operation_conflict", message)
            }
            ModelManagerError::NotFound => Self {
                status: StatusCode::NOT_FOUND,
                code: "not_found",
                message: "managed model was not found".to_string(),
            },
            ModelManagerError::InsufficientDiskSpace {
                required_bytes,
                available_bytes,
            } => Self::conflict(
                "insufficient_disk_space",
                format!(
                    "the model requires {required_bytes} bytes including safety margin, but only {available_bytes} bytes are available"
                ),
            ),
            ModelManagerError::Storage(error) => error.into(),
            error => {
                tracing::error!(error = %error, "model manager operation failed");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "model_manager_failure",
                    message: "the local model manager could not complete the request".to_string(),
                }
            }
        }
    }
}

impl From<RetrievalError> for ApiError {
    fn from(error: RetrievalError) -> Self {
        match error {
            RetrievalError::Storage(error) => error.into(),
            RetrievalError::InvalidInput(message) => Self::invalid(message),
            RetrievalError::Embedding(message) => {
                tracing::error!(%message, "knowledge embedding operation failed");
                Self {
                    status: StatusCode::BAD_GATEWAY,
                    code: "embedding_failure",
                    message: "the configured local embedding model could not complete the request"
                        .to_string(),
                }
            }
            error => {
                tracing::error!(%error, "knowledge retrieval operation failed");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "retrieval_failure",
                    message: "the local retrieval engine could not complete the request"
                        .to_string(),
                }
            }
        }
    }
}

impl From<IngestionError> for ApiError {
    fn from(error: IngestionError) -> Self {
        match error {
            IngestionError::Storage(error) => error.into(),
            IngestionError::Retrieval(error) => error.into(),
            IngestionError::InvalidInput(message) | IngestionError::Parse(message) => {
                Self::invalid(message)
            }
            IngestionError::Cancelled => {
                Self::conflict("ingestion_cancelled", "the ingestion job was cancelled")
            }
            error => {
                tracing::error!(%error, "knowledge ingestion operation failed");
                Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    code: "ingestion_failure",
                    message: "the local ingestion engine could not complete the request"
                        .to_string(),
                }
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}
