mod blob;
mod chunker;
mod parser;
mod service;
mod watcher;

use thiserror::Error;

pub use blob::{BlobRecord, ContentAddressedBlobStore};
pub use chunker::{TextChunk, chunk_text, chunker_hash};
pub use parser::{ExtractedDocument, extract_document, is_supported_document};
pub use service::{IngestionConfig, IngestionService};
pub use watcher::KnowledgeWatchManager;

pub type Result<T> = std::result::Result<T, IngestionError>;

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error(transparent)]
    Storage(#[from] northbridge_storage::StorageError),
    #[error(transparent)]
    Retrieval(#[from] northbridge_retrieval::RetrievalError),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("file watcher failed: {0}")]
    Watch(#[from] notify::Error),
    #[error("document parsing failed: {0}")]
    Parse(String),
    #[error("invalid ingestion request: {0}")]
    InvalidInput(String),
    #[error("ingestion was cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests;
