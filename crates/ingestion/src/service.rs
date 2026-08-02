use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use northbridge_domain::{
    IngestionJob, IngestionJobStatus, KnowledgeDocument, KnowledgeSourceKind,
};
use northbridge_retrieval::KnowledgeRetrieval;
use northbridge_storage::{Database, NewKnowledgeChunk, NewKnowledgeDocument};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    ContentAddressedBlobStore, IngestionError, Result, chunk_text, extract_document,
    is_supported_document,
};

#[derive(Debug, Clone)]
pub struct IngestionConfig {
    pub blob_directory: PathBuf,
    pub max_file_bytes: u64,
    pub embedding_batch_size: usize,
}

impl IngestionConfig {
    #[must_use]
    pub fn local(blob_directory: impl Into<PathBuf>) -> Self {
        Self {
            blob_directory: blob_directory.into(),
            max_file_bytes: 64 * 1024 * 1024,
            embedding_batch_size: 32,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.blob_directory.is_absolute() {
            return Err(IngestionError::InvalidInput(
                "blob directory must be absolute".to_string(),
            ));
        }
        if self.max_file_bytes == 0 || self.max_file_bytes > 1024 * 1024 * 1024 {
            return Err(IngestionError::InvalidInput(
                "maximum ingestion file size must be between one byte and one GiB".to_string(),
            ));
        }
        if !(1..=256).contains(&self.embedding_batch_size) {
            return Err(IngestionError::InvalidInput(
                "embedding batch size must be between 1 and 256".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct IngestionService {
    inner: Arc<IngestionServiceInner>,
}

struct IngestionServiceInner {
    database: Database,
    retrieval: KnowledgeRetrieval,
    blobs: ContentAddressedBlobStore,
    config: IngestionConfig,
    active: Mutex<HashMap<Uuid, CancellationToken>>,
    active_sources: Mutex<HashMap<(Uuid, String), Uuid>>,
}

impl IngestionService {
    pub async fn open(
        database: Database,
        retrieval: KnowledgeRetrieval,
        config: IngestionConfig,
    ) -> Result<Self> {
        config.validate()?;
        let blobs = ContentAddressedBlobStore::open(config.blob_directory.clone()).await?;
        Ok(Self {
            inner: Arc::new(IngestionServiceInner {
                database,
                retrieval,
                blobs,
                config,
                active: Mutex::new(HashMap::new()),
                active_sources: Mutex::new(HashMap::new()),
            }),
        })
    }

    #[must_use]
    pub fn database(&self) -> &Database {
        &self.inner.database
    }

    #[must_use]
    pub fn retrieval(&self) -> &KnowledgeRetrieval {
        &self.inner.retrieval
    }

    pub async fn start_file(
        &self,
        stack_id: Uuid,
        path: impl AsRef<Path>,
        source_kind: KnowledgeSourceKind,
    ) -> Result<IngestionJob> {
        let path = canonical_document_path(path.as_ref()).await?;
        let source_uri = source_uri(&path)?;
        let source_key = (stack_id, source_uri.clone());
        let mut active_sources = self.inner.active_sources.lock().await;
        if let Some(job_id) = active_sources.get(&source_key).copied() {
            return self
                .inner
                .database
                .ingestion_job(job_id)
                .await
                .map_err(Into::into);
        }
        let job = self
            .inner
            .database
            .create_ingestion_job(stack_id, &source_uri, source_kind)
            .await?;
        active_sources.insert(source_key, job.id);
        drop(active_sources);
        let cancellation = CancellationToken::new();
        self.inner
            .active
            .lock()
            .await
            .insert(job.id, cancellation.clone());
        let service = self.clone();
        let job_id = job.id;
        tokio::spawn(async move {
            service
                .run_job(
                    job_id,
                    stack_id,
                    path,
                    source_kind,
                    source_uri,
                    cancellation,
                )
                .await;
        });
        Ok(job)
    }

    pub async fn cancel(&self, job_id: Uuid) -> Result<IngestionJob> {
        let job = self.inner.database.request_ingestion_cancel(job_id).await?;
        if let Some(cancellation) = self.inner.active.lock().await.get(&job_id).cloned() {
            cancellation.cancel();
        }
        Ok(job)
    }

    pub async fn wait_for_job(&self, job_id: Uuid, timeout: Duration) -> Result<IngestionJob> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let job = self.inner.database.ingestion_job(job_id).await?;
            if matches!(
                job.status,
                IngestionJobStatus::Completed
                    | IngestionJobStatus::Failed
                    | IngestionJobStatus::Cancelled
            ) {
                return Ok(job);
            }
            tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
                .await
                .map_err(|_| {
                    IngestionError::InvalidInput("ingestion wait timed out".to_string())
                })?;
        }
    }

    pub async fn shutdown(&self, timeout: Duration) -> bool {
        for cancellation in self.inner.active.lock().await.values() {
            cancellation.cancel();
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.inner.active.lock().await.is_empty() {
                return true;
            }
            if tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    async fn run_job(
        &self,
        job_id: Uuid,
        stack_id: Uuid,
        path: PathBuf,
        source_kind: KnowledgeSourceKind,
        source_uri: String,
        cancellation: CancellationToken,
    ) {
        let outcome = self
            .ingest(
                job_id,
                stack_id,
                &path,
                source_kind,
                &source_uri,
                &cancellation,
            )
            .await;
        if let Err(error) = outcome {
            let status = if matches!(error, IngestionError::Cancelled) {
                IngestionJobStatus::Cancelled
            } else {
                IngestionJobStatus::Failed
            };
            if let Err(storage_error) = self
                .inner
                .database
                .update_ingestion_job(job_id, status, 0, None, None, Some(&bounded_error(&error)))
                .await
            {
                tracing::error!(%job_id, %storage_error, "failed to persist ingestion failure");
            }
        }
        self.inner.active.lock().await.remove(&job_id);
        self.inner
            .active_sources
            .lock()
            .await
            .remove(&(stack_id, source_uri));
    }

    async fn ingest(
        &self,
        job_id: Uuid,
        stack_id: Uuid,
        path: &Path,
        source_kind: KnowledgeSourceKind,
        source_uri: &str,
        cancellation: &CancellationToken,
    ) -> Result<KnowledgeDocument> {
        check_cancelled(job_id, cancellation, &self.inner.database).await?;
        self.inner
            .database
            .update_ingestion_job(job_id, IngestionJobStatus::Running, 0, Some(4), None, None)
            .await?;
        let metadata = tokio::fs::metadata(path).await?;
        if !metadata.is_file() {
            return Err(IngestionError::InvalidInput(
                "ingestion path must identify a regular file".to_string(),
            ));
        }
        if metadata.len() > self.inner.config.max_file_bytes {
            return Err(IngestionError::InvalidInput(format!(
                "file is {} bytes; the configured limit is {} bytes",
                metadata.len(),
                self.inner.config.max_file_bytes
            )));
        }
        if !is_supported_document(path) {
            return Err(IngestionError::InvalidInput(
                "file type is not supported for knowledge ingestion".to_string(),
            ));
        }
        let bytes = tokio::fs::read(path).await?;
        let blob = self.inner.blobs.put(&bytes).await?;
        self.inner
            .database
            .update_ingestion_job(job_id, IngestionJobStatus::Running, 1, Some(4), None, None)
            .await?;
        check_cancelled(job_id, cancellation, &self.inner.database).await?;

        let parse_path = path.to_path_buf();
        let extracted = tokio::task::spawn_blocking(move || extract_document(&parse_path, &bytes))
            .await
            .map_err(|error| IngestionError::Parse(format!("parser worker panicked: {error}")))??;
        let content_sha256 = hex_sha256(extracted.text.as_bytes());
        if let Some(existing) = self
            .inner
            .database
            .knowledge_document_by_source(stack_id, source_uri)
            .await?
            && existing.content_sha256 == content_sha256
        {
            self.inner.retrieval.synchronize_stack(stack_id).await?;
            self.inner
                .database
                .update_ingestion_job(
                    job_id,
                    IngestionJobStatus::Completed,
                    4,
                    Some(4),
                    Some(existing.id),
                    None,
                )
                .await?;
            return Ok(existing);
        }
        let stack = self.inner.database.knowledge_stack(stack_id).await?;
        let chunks = chunk_text(&extracted.text, stack.chunk_size, stack.chunk_overlap)?;
        self.inner
            .database
            .update_ingestion_job(job_id, IngestionJobStatus::Running, 2, Some(4), None, None)
            .await?;
        check_cancelled(job_id, cancellation, &self.inner.database).await?;

        let mut vectors = Vec::with_capacity(chunks.len());
        for batch in chunks.chunks(self.inner.config.embedding_batch_size) {
            let inputs = batch
                .iter()
                .map(|chunk| chunk.content.clone())
                .collect::<Vec<_>>();
            vectors.extend(self.inner.retrieval.embed_chunks(&stack, &inputs).await?);
            check_cancelled(job_id, cancellation, &self.inner.database).await?;
        }
        self.inner
            .database
            .update_ingestion_job(job_id, IngestionJobStatus::Running, 3, Some(4), None, None)
            .await?;

        let new_chunks = chunks
            .into_iter()
            .zip(vectors)
            .map(|(chunk, embedding)| NewKnowledgeChunk {
                id: Uuid::now_v7(),
                ordinal: chunk.ordinal,
                content_sha256: hex_sha256(chunk.content.as_bytes()),
                vector_sha256: vector_hash(&embedding),
                metadata: json!({
                    "parser": extracted.parser,
                    "chunker": "northbridge-char-v1"
                }),
                content: chunk.content,
                character_count: chunk.character_count,
                start_offset: chunk.start_offset,
                end_offset: chunk.end_offset,
                embedding,
            })
            .collect();
        let source_modified_at_ms = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_millis()).ok());
        let document = self
            .inner
            .database
            .upsert_knowledge_document(NewKnowledgeDocument {
                stack_id,
                workspace_id: stack.workspace_id,
                source_kind,
                source_uri: source_uri.to_string(),
                title: extracted.title,
                mime_type: extracted.mime_type,
                content_sha256,
                blob_sha256: blob.sha256,
                byte_size: blob.byte_size,
                source_modified_at_ms,
                chunks: new_chunks,
            })
            .await?;
        self.inner.retrieval.synchronize_stack(stack_id).await?;
        self.inner
            .database
            .update_ingestion_job(
                job_id,
                IngestionJobStatus::Completed,
                4,
                Some(4),
                Some(document.id),
                None,
            )
            .await?;
        Ok(document)
    }
}

async fn canonical_document_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(IngestionError::InvalidInput(
            "ingestion file path must be absolute".to_string(),
        ));
    }
    let canonical = tokio::fs::canonicalize(path).await?;
    if !canonical.is_file() {
        return Err(IngestionError::InvalidInput(
            "ingestion file path must identify a regular file".to_string(),
        ));
    }
    Ok(canonical)
}

pub(crate) fn source_uri(path: &Path) -> Result<String> {
    Url::from_file_path(path).map(String::from).map_err(|()| {
        IngestionError::InvalidInput("file path cannot be represented as a URI".to_string())
    })
}

pub(crate) fn source_uri_without_canonicalization(path: &Path) -> Option<String> {
    Url::from_file_path(path).ok().map(String::from)
}

async fn check_cancelled(
    job_id: Uuid,
    cancellation: &CancellationToken,
    database: &Database,
) -> Result<()> {
    if cancellation.is_cancelled() || database.ingestion_job(job_id).await?.cancel_requested {
        return Err(IngestionError::Cancelled);
    }
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn vector_hash(vector: &[f32]) -> String {
    let mut hasher = Sha256::new();
    for value in vector {
        hasher.update(value.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn bounded_error(error: &IngestionError) -> String {
    error.to_string().chars().take(16_384).collect()
}
