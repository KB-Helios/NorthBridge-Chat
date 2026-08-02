mod dense;
mod engine;

use async_trait::async_trait;
use thiserror::Error;

pub use dense::{
    DenseIndex, DenseIndexStatus, DenseIndexSync, ScoredId, TurboVecDenseIndex, VectorRecord,
};
pub use engine::{KnowledgeRetrieval, RetrievalConfig, pack_citation_context};

pub type Result<T> = std::result::Result<T, RetrievalError>;

#[derive(Debug, Error)]
pub enum RetrievalError {
    #[error(transparent)]
    Storage(#[from] northbridge_storage::StorageError),
    #[error("dense index I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("dense index manifest is invalid: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("dense index operation failed: {0}")]
    Dense(String),
    #[error("embedding failed: {0}")]
    Embedding(String),
    #[error("invalid retrieval request: {0}")]
    InvalidInput(String),
    #[error("dense index worker stopped unexpectedly")]
    WorkerStopped,
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(
        &self,
        physical_model: &str,
        expected_dimensions: usize,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>>;
}

#[cfg(test)]
mod tests;
