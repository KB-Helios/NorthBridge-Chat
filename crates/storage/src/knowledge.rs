use std::str::FromStr;

use northbridge_domain::{
    DocumentChunk, IngestionJob, IngestionJobStatus, KnowledgeDocument, KnowledgeDocumentStatus,
    KnowledgeSearchHit, KnowledgeSearchResult, KnowledgeSourceKind, KnowledgeStack,
    KnowledgeWatchKind, KnowledgeWatchRoot,
};
use serde_json::Value;
use sqlx::{FromRow, QueryBuilder, Sqlite};
use uuid::Uuid;

use crate::{Database, Result, StorageError, now_ms};

#[derive(Debug, Clone)]
pub struct NewKnowledgeStack {
    pub workspace_id: Uuid,
    pub name: String,
    pub description: String,
    pub embedding_model: String,
    pub embedding_dimensions: u32,
    pub chunk_size: u32,
    pub chunk_overlap: u32,
    pub chunker_hash: String,
}

#[derive(Debug, Clone)]
pub struct NewKnowledgeChunk {
    pub id: Uuid,
    pub ordinal: u32,
    pub content: String,
    pub content_sha256: String,
    pub character_count: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub metadata: Value,
    pub embedding: Vec<f32>,
    pub vector_sha256: String,
}

#[derive(Debug, Clone)]
pub struct NewKnowledgeDocument {
    pub stack_id: Uuid,
    pub workspace_id: Uuid,
    pub source_kind: KnowledgeSourceKind,
    pub source_uri: String,
    pub title: String,
    pub mime_type: String,
    pub content_sha256: String,
    pub blob_sha256: String,
    pub byte_size: u64,
    pub source_modified_at_ms: Option<i64>,
    pub chunks: Vec<NewKnowledgeChunk>,
}

#[derive(Debug, Clone)]
pub struct NewKnowledgeWatchRoot {
    pub stack_id: Uuid,
    pub workspace_id: Uuid,
    pub kind: KnowledgeWatchKind,
    pub path: String,
    pub recursive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredEmbedding {
    pub dense_id: u64,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenseJournalEntry {
    pub sequence_number: u64,
    pub revision: u64,
    pub dense_id: u64,
    pub operation: String,
    pub vector_sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KnowledgeLexicalHit {
    pub dense_id: u64,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct KnowledgeChunkRecord {
    pub chunk: DocumentChunk,
    pub title: String,
    pub source_uri: String,
}

#[derive(Debug, FromRow)]
struct KnowledgeStackRow {
    id: String,
    workspace_id: String,
    name: String,
    description: String,
    embedding_model: String,
    embedding_dimensions: i64,
    chunk_size: i64,
    chunk_overlap: i64,
    chunker_hash: String,
    index_revision: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct KnowledgeDocumentRow {
    id: String,
    stack_id: String,
    workspace_id: String,
    source_kind: String,
    source_uri: String,
    title: String,
    mime_type: String,
    content_sha256: String,
    blob_sha256: String,
    byte_size: i64,
    status: String,
    last_error: Option<String>,
    source_modified_at_ms: Option<i64>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ChunkRow {
    dense_id: i64,
    id: String,
    document_id: String,
    stack_id: String,
    workspace_id: String,
    ordinal: i64,
    content: String,
    content_sha256: String,
    character_count: i64,
    start_offset: i64,
    end_offset: i64,
    metadata_json: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct ChunkRecordRow {
    dense_id: i64,
    id: String,
    document_id: String,
    stack_id: String,
    workspace_id: String,
    ordinal: i64,
    content: String,
    content_sha256: String,
    character_count: i64,
    start_offset: i64,
    end_offset: i64,
    metadata_json: String,
    created_at_ms: i64,
    updated_at_ms: i64,
    title: String,
    source_uri: String,
}

#[derive(Debug, FromRow)]
struct WatchRootRow {
    id: String,
    stack_id: String,
    workspace_id: String,
    kind: String,
    path: String,
    recursive: i64,
    enabled: i64,
    last_scan_at_ms: Option<i64>,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct IngestionJobRow {
    id: String,
    stack_id: String,
    source_uri: String,
    source_kind: String,
    status: String,
    progress_current: i64,
    progress_total: Option<i64>,
    document_id: Option<String>,
    cancel_requested: i64,
    last_error: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl Database {
    pub async fn create_knowledge_stack(&self, input: NewKnowledgeStack) -> Result<KnowledgeStack> {
        validate_stack_input(&input)?;
        let now = now_ms();
        let id = Uuid::now_v7();
        let result = sqlx::query(
            "INSERT INTO knowledge_stacks (
                 id, workspace_id, name, description, embedding_model, embedding_dimensions,
                 chunk_size, chunk_overlap, chunker_hash, index_revision, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.name.trim())
        .bind(input.description.trim())
        .bind(input.embedding_model.trim())
        .bind(i64::from(input.embedding_dimensions))
        .bind(i64::from(input.chunk_size))
        .bind(i64::from(input.chunk_overlap))
        .bind(&input.chunker_hash)
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => self.knowledge_stack(id).await,
            Err(error) if is_unique_constraint(&error) => Err(StorageError::Conflict(
                "a knowledge stack with this name already exists in the workspace".to_string(),
            )),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn knowledge_stack(&self, id: Uuid) -> Result<KnowledgeStack> {
        let row = sqlx::query_as::<_, KnowledgeStackRow>(
            "SELECT id, workspace_id, name, description, embedding_model,
                    embedding_dimensions, chunk_size, chunk_overlap, chunker_hash,
                    index_revision, created_at_ms, updated_at_ms
             FROM knowledge_stacks WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "knowledge stack",
        })?;
        stack_from_row(row)
    }

    pub async fn list_knowledge_stacks(&self, workspace_id: Uuid) -> Result<Vec<KnowledgeStack>> {
        let rows = sqlx::query_as::<_, KnowledgeStackRow>(
            "SELECT id, workspace_id, name, description, embedding_model,
                    embedding_dimensions, chunk_size, chunk_overlap, chunker_hash,
                    index_revision, created_at_ms, updated_at_ms
             FROM knowledge_stacks
             WHERE workspace_id = ?
             ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(workspace_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(stack_from_row).collect()
    }

    pub async fn delete_knowledge_stack(&self, id: Uuid) -> Result<bool> {
        let result = sqlx::query("DELETE FROM knowledge_stacks WHERE id = ?")
            .bind(id.to_string())
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn knowledge_document(&self, id: Uuid) -> Result<KnowledgeDocument> {
        let row = sqlx::query_as::<_, KnowledgeDocumentRow>(
            "SELECT id, stack_id, workspace_id, source_kind, source_uri, title, mime_type,
                    content_sha256, blob_sha256, byte_size, status, last_error,
                    source_modified_at_ms, created_at_ms, updated_at_ms
             FROM knowledge_documents WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "knowledge document",
        })?;
        document_from_row(row)
    }

    pub async fn knowledge_document_by_source(
        &self,
        stack_id: Uuid,
        source_uri: &str,
    ) -> Result<Option<KnowledgeDocument>> {
        let row = sqlx::query_as::<_, KnowledgeDocumentRow>(
            "SELECT id, stack_id, workspace_id, source_kind, source_uri, title, mime_type,
                    content_sha256, blob_sha256, byte_size, status, last_error,
                    source_modified_at_ms, created_at_ms, updated_at_ms
             FROM knowledge_documents WHERE stack_id = ? AND source_uri = ?",
        )
        .bind(stack_id.to_string())
        .bind(source_uri)
        .fetch_optional(self.pool())
        .await?;
        row.map(document_from_row).transpose()
    }

    pub async fn list_knowledge_documents(&self, stack_id: Uuid) -> Result<Vec<KnowledgeDocument>> {
        let rows = sqlx::query_as::<_, KnowledgeDocumentRow>(
            "SELECT id, stack_id, workspace_id, source_kind, source_uri, title, mime_type,
                    content_sha256, blob_sha256, byte_size, status, last_error,
                    source_modified_at_ms, created_at_ms, updated_at_ms
             FROM knowledge_documents
             WHERE stack_id = ?
             ORDER BY updated_at_ms DESC, id DESC",
        )
        .bind(stack_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(document_from_row).collect()
    }

    pub async fn upsert_knowledge_document(
        &self,
        input: NewKnowledgeDocument,
    ) -> Result<KnowledgeDocument> {
        validate_document_input(&input)?;
        let stack = self.knowledge_stack(input.stack_id).await?;
        if stack.workspace_id != input.workspace_id {
            return Err(StorageError::InvalidInput(
                "knowledge document workspace does not match its stack".to_string(),
            ));
        }
        for chunk in &input.chunks {
            if chunk.embedding.len() != stack.embedding_dimensions as usize {
                return Err(StorageError::InvalidInput(format!(
                    "chunk embedding has {} dimensions; stack requires {}",
                    chunk.embedding.len(),
                    stack.embedding_dimensions
                )));
            }
        }

        let mut transaction = self.begin_write().await?;
        let existing = sqlx::query_as::<_, (String, i64)>(
            "SELECT id, created_at_ms FROM knowledge_documents
             WHERE stack_id = ? AND source_uri = ?",
        )
        .bind(input.stack_id.to_string())
        .bind(&input.source_uri)
        .fetch_optional(&mut *transaction)
        .await?;
        let (document_id, created_at_ms) = match existing {
            Some((id, created_at_ms)) => (parse_uuid("knowledge_document.id", &id)?, created_at_ms),
            None => (Uuid::now_v7(), now_ms()),
        };
        let old_dense_ids = sqlx::query_scalar::<_, i64>(
            "SELECT dense_id FROM document_chunks WHERE document_id = ? ORDER BY dense_id",
        )
        .bind(document_id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        let revision = stack.index_revision.saturating_add(1);
        let revision_i64 = to_i64("knowledge_stack.index_revision", revision)?;
        let now = now_ms();
        for dense_id in old_dense_ids {
            sqlx::query(
                "INSERT INTO dense_index_journal (
                     stack_id, revision, chunk_dense_id, operation, vector_sha256, created_at_ms
                 ) VALUES (?, ?, ?, 'remove', NULL, ?)",
            )
            .bind(input.stack_id.to_string())
            .bind(revision_i64)
            .bind(dense_id)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query("DELETE FROM document_chunks WHERE document_id = ?")
            .bind(document_id.to_string())
            .execute(&mut *transaction)
            .await?;

        sqlx::query(
            "INSERT INTO knowledge_documents (
                 id, stack_id, workspace_id, source_kind, source_uri, title, mime_type,
                 content_sha256, blob_sha256, byte_size, status, last_error,
                 source_modified_at_ms, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'indexing', NULL, ?, ?, ?)
             ON CONFLICT(stack_id, source_uri) DO UPDATE SET
                 source_kind = excluded.source_kind,
                 title = excluded.title,
                 mime_type = excluded.mime_type,
                 content_sha256 = excluded.content_sha256,
                 blob_sha256 = excluded.blob_sha256,
                 byte_size = excluded.byte_size,
                 status = 'indexing',
                 last_error = NULL,
                 source_modified_at_ms = excluded.source_modified_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
        )
        .bind(document_id.to_string())
        .bind(input.stack_id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.source_kind.as_str())
        .bind(&input.source_uri)
        .bind(input.title.trim())
        .bind(input.mime_type.trim())
        .bind(&input.content_sha256)
        .bind(&input.blob_sha256)
        .bind(to_i64("knowledge_document.byte_size", input.byte_size)?)
        .bind(input.source_modified_at_ms)
        .bind(created_at_ms)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        for chunk in input.chunks {
            let metadata_json = serde_json::to_string(&chunk.metadata).map_err(|error| {
                StorageError::InvalidInput(format!("chunk metadata is not valid JSON: {error}"))
            })?;
            let result = sqlx::query(
                "INSERT INTO document_chunks (
                     id, document_id, stack_id, workspace_id, ordinal, content, content_sha256,
                     character_count, start_offset, end_offset, metadata_json, created_at_ms, updated_at_ms
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(chunk.id.to_string())
            .bind(document_id.to_string())
            .bind(input.stack_id.to_string())
            .bind(input.workspace_id.to_string())
            .bind(i64::from(chunk.ordinal))
            .bind(&chunk.content)
            .bind(&chunk.content_sha256)
            .bind(i64::from(chunk.character_count))
            .bind(to_i64("document_chunk.start_offset", chunk.start_offset)?)
            .bind(to_i64("document_chunk.end_offset", chunk.end_offset)?)
            .bind(metadata_json)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            let dense_id = result.last_insert_rowid();
            let vector = encode_vector(&chunk.embedding);
            sqlx::query(
                "INSERT INTO embeddings (
                     chunk_dense_id, model, dimensions, vector, vector_sha256, created_at_ms, updated_at_ms
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(dense_id)
            .bind(&stack.embedding_model)
            .bind(i64::from(stack.embedding_dimensions))
            .bind(vector)
            .bind(&chunk.vector_sha256)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO dense_index_journal (
                     stack_id, revision, chunk_dense_id, operation, vector_sha256, created_at_ms
                 ) VALUES (?, ?, ?, 'upsert', ?, ?)",
            )
            .bind(input.stack_id.to_string())
            .bind(revision_i64)
            .bind(dense_id)
            .bind(&chunk.vector_sha256)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE knowledge_documents SET status = 'ready', updated_at_ms = ? WHERE id = ?",
        )
        .bind(now)
        .bind(document_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE knowledge_stacks SET index_revision = ?, updated_at_ms = ? WHERE id = ?",
        )
        .bind(revision_i64)
        .bind(now)
        .bind(input.stack_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.knowledge_document(document_id).await
    }

    pub async fn delete_knowledge_document(&self, id: Uuid) -> Result<bool> {
        let mut transaction = self.begin_write().await?;
        let stack_id = sqlx::query_scalar::<_, String>(
            "SELECT stack_id FROM knowledge_documents WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(stack_id) = stack_id else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let stack_id = parse_uuid("knowledge_document.stack_id", &stack_id)?;
        let current_revision = sqlx::query_scalar::<_, i64>(
            "SELECT index_revision FROM knowledge_stacks WHERE id = ?",
        )
        .bind(stack_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let revision = current_revision.checked_add(1).ok_or_else(|| {
            StorageError::Conflict("knowledge index revision overflowed".to_string())
        })?;
        let dense_ids = sqlx::query_scalar::<_, i64>(
            "SELECT dense_id FROM document_chunks WHERE document_id = ? ORDER BY dense_id",
        )
        .bind(id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        let now = now_ms();
        for dense_id in dense_ids {
            sqlx::query(
                "INSERT INTO dense_index_journal (
                     stack_id, revision, chunk_dense_id, operation, vector_sha256, created_at_ms
                 ) VALUES (?, ?, ?, 'remove', NULL, ?)",
            )
            .bind(stack_id.to_string())
            .bind(revision)
            .bind(dense_id)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query("DELETE FROM knowledge_documents WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE knowledge_stacks SET index_revision = ?, updated_at_ms = ? WHERE id = ?",
        )
        .bind(revision)
        .bind(now)
        .bind(stack_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn delete_knowledge_document_by_source(
        &self,
        stack_id: Uuid,
        source_uri: &str,
    ) -> Result<bool> {
        let document_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM knowledge_documents WHERE stack_id = ? AND source_uri = ?",
        )
        .bind(stack_id.to_string())
        .bind(source_uri)
        .fetch_optional(self.pool())
        .await?;
        match document_id {
            Some(id) => {
                self.delete_knowledge_document(parse_uuid("knowledge_document.id", &id)?)
                    .await
            }
            None => Ok(false),
        }
    }

    pub async fn list_document_chunks(&self, document_id: Uuid) -> Result<Vec<DocumentChunk>> {
        let rows = sqlx::query_as::<_, ChunkRow>(
            "SELECT dense_id, id, document_id, stack_id, workspace_id, ordinal, content,
                    content_sha256, character_count, start_offset, end_offset, metadata_json,
                    created_at_ms, updated_at_ms
             FROM document_chunks WHERE document_id = ? ORDER BY ordinal, dense_id",
        )
        .bind(document_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(chunk_from_row).collect()
    }

    pub async fn knowledge_chunk_records(
        &self,
        dense_ids: &[u64],
    ) -> Result<Vec<KnowledgeChunkRecord>> {
        if dense_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT c.dense_id, c.id, c.document_id, c.stack_id, c.workspace_id, c.ordinal,
                    c.content, c.content_sha256, c.character_count, c.start_offset, c.end_offset,
                    c.metadata_json, c.created_at_ms, c.updated_at_ms,
                    d.title, d.source_uri
             FROM document_chunks c
             JOIN knowledge_documents d ON d.id = c.document_id
             WHERE c.dense_id IN (",
        );
        let mut separated = query.separated(", ");
        for dense_id in dense_ids {
            separated.push_bind(to_i64("document_chunk.dense_id", *dense_id)?);
        }
        separated.push_unseparated(") ORDER BY c.dense_id");
        let rows = query
            .build_query_as::<ChunkRecordRow>()
            .fetch_all(self.pool())
            .await?;
        rows.into_iter().map(chunk_record_from_row).collect()
    }

    pub async fn search_knowledge_chunks_lexical(
        &self,
        workspace_id: Uuid,
        stack_id: Uuid,
        query: &str,
        limit: u32,
    ) -> Result<Vec<KnowledgeLexicalHit>> {
        if !(1..=200).contains(&limit) {
            return Err(StorageError::InvalidInput(
                "knowledge search limit must be between 1 and 200".to_string(),
            ));
        }
        let fts_query = simple_fts_query(query)?;
        let rows = sqlx::query_as::<_, (i64, f64)>(
            "SELECT CAST(fts.dense_id AS INTEGER), bm25(document_chunks_fts) AS score
             FROM document_chunks_fts AS fts
             JOIN knowledge_documents d ON d.id = fts.document_id
             WHERE document_chunks_fts MATCH ?
               AND fts.workspace_id = ?
               AND fts.stack_id = ?
               AND d.status = 'ready'
             ORDER BY score, fts.dense_id
             LIMIT ?",
        )
        .bind(fts_query)
        .bind(workspace_id.to_string())
        .bind(stack_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|(dense_id, score)| {
                Ok(KnowledgeLexicalHit {
                    dense_id: positive_u64("document_chunk.dense_id", dense_id)?,
                    score,
                })
            })
            .collect()
    }

    pub async fn allowed_knowledge_dense_ids(
        &self,
        workspace_id: Uuid,
        stack_id: Uuid,
    ) -> Result<Vec<u64>> {
        let ids = sqlx::query_scalar::<_, i64>(
            "SELECT c.dense_id
             FROM document_chunks c
             JOIN knowledge_documents d ON d.id = c.document_id
             WHERE c.workspace_id = ? AND c.stack_id = ? AND d.status = 'ready'
             ORDER BY c.dense_id",
        )
        .bind(workspace_id.to_string())
        .bind(stack_id.to_string())
        .fetch_all(self.pool())
        .await?;
        ids.into_iter()
            .map(|id| positive_u64("document_chunk.dense_id", id))
            .collect()
    }

    pub async fn all_stack_embeddings(&self, stack_id: Uuid) -> Result<Vec<StoredEmbedding>> {
        let rows = sqlx::query_as::<_, (i64, i64, Vec<u8>)>(
            "SELECT e.chunk_dense_id, e.dimensions, e.vector
             FROM embeddings e
             JOIN document_chunks c ON c.dense_id = e.chunk_dense_id
             WHERE c.stack_id = ? ORDER BY e.chunk_dense_id",
        )
        .bind(stack_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|(dense_id, dimensions, bytes)| {
                Ok(StoredEmbedding {
                    dense_id: positive_u64("embedding.chunk_dense_id", dense_id)?,
                    vector: decode_vector(dimensions, &bytes)?,
                })
            })
            .collect()
    }

    pub async fn stack_embedding(&self, dense_id: u64) -> Result<Option<StoredEmbedding>> {
        let row = sqlx::query_as::<_, (i64, Vec<u8>)>(
            "SELECT dimensions, vector FROM embeddings WHERE chunk_dense_id = ?",
        )
        .bind(to_i64("embedding.chunk_dense_id", dense_id)?)
        .fetch_optional(self.pool())
        .await?;
        row.map(|(dimensions, bytes)| {
            Ok(StoredEmbedding {
                dense_id,
                vector: decode_vector(dimensions, &bytes)?,
            })
        })
        .transpose()
    }

    pub async fn dense_journal_after(
        &self,
        stack_id: Uuid,
        revision: u64,
    ) -> Result<Vec<DenseJournalEntry>> {
        let rows = sqlx::query_as::<_, (i64, i64, i64, String, Option<String>)>(
            "SELECT sequence_number, revision, chunk_dense_id, operation, vector_sha256
             FROM dense_index_journal
             WHERE stack_id = ? AND revision > ?
             ORDER BY revision, sequence_number",
        )
        .bind(stack_id.to_string())
        .bind(to_i64("dense_index.revision", revision)?)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(
                |(sequence_number, revision, dense_id, operation, vector_sha256)| {
                    Ok(DenseJournalEntry {
                        sequence_number: positive_u64(
                            "dense_index_journal.sequence_number",
                            sequence_number,
                        )?,
                        revision: positive_u64("dense_index_journal.revision", revision)?,
                        dense_id: positive_u64("dense_index_journal.chunk_dense_id", dense_id)?,
                        operation,
                        vector_sha256,
                    })
                },
            )
            .collect()
    }

    pub async fn mark_dense_journal_applied(&self, stack_id: Uuid, revision: u64) -> Result<()> {
        sqlx::query(
            "UPDATE dense_index_journal
             SET applied_at_ms = COALESCE(applied_at_ms, ?)
             WHERE stack_id = ? AND revision <= ?",
        )
        .bind(now_ms())
        .bind(stack_id.to_string())
        .bind(to_i64("dense_index.revision", revision)?)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn create_ingestion_job(
        &self,
        stack_id: Uuid,
        source_uri: &str,
        source_kind: KnowledgeSourceKind,
    ) -> Result<IngestionJob> {
        if source_uri.trim().is_empty() || source_uri.len() > 32_768 {
            return Err(StorageError::InvalidInput(
                "ingestion source URI must be between 1 and 32,768 characters".to_string(),
            ));
        }
        self.knowledge_stack(stack_id).await?;
        let id = Uuid::now_v7();
        let now = now_ms();
        sqlx::query(
            "INSERT INTO ingestion_jobs (
                 id, stack_id, source_uri, source_kind, status, progress_current,
                 progress_total, document_id, cancel_requested, last_error,
                 created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, 'queued', 0, NULL, NULL, 0, NULL, ?, ?)",
        )
        .bind(id.to_string())
        .bind(stack_id.to_string())
        .bind(source_uri)
        .bind(source_kind.as_str())
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await?;
        self.ingestion_job(id).await
    }

    pub async fn ingestion_job(&self, id: Uuid) -> Result<IngestionJob> {
        let row = sqlx::query_as::<_, IngestionJobRow>(
            "SELECT id, stack_id, source_uri, source_kind, status, progress_current,
                    progress_total, document_id, cancel_requested, last_error,
                    created_at_ms, updated_at_ms
             FROM ingestion_jobs WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "ingestion job",
        })?;
        ingestion_job_from_row(row)
    }

    pub async fn list_ingestion_jobs(&self, stack_id: Uuid) -> Result<Vec<IngestionJob>> {
        let rows = sqlx::query_as::<_, IngestionJobRow>(
            "SELECT id, stack_id, source_uri, source_kind, status, progress_current,
                    progress_total, document_id, cancel_requested, last_error,
                    created_at_ms, updated_at_ms
             FROM ingestion_jobs WHERE stack_id = ?
             ORDER BY updated_at_ms DESC, id DESC LIMIT 200",
        )
        .bind(stack_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(ingestion_job_from_row).collect()
    }

    pub async fn update_ingestion_job(
        &self,
        id: Uuid,
        status: IngestionJobStatus,
        progress_current: u64,
        progress_total: Option<u64>,
        document_id: Option<Uuid>,
        last_error: Option<&str>,
    ) -> Result<IngestionJob> {
        let result = sqlx::query(
            "UPDATE ingestion_jobs
             SET status = ?, progress_current = ?, progress_total = ?, document_id = ?,
                 last_error = ?, updated_at_ms = ?
             WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(to_i64("ingestion_job.progress_current", progress_current)?)
        .bind(
            progress_total
                .map(|value| to_i64("ingestion_job.progress_total", value))
                .transpose()?,
        )
        .bind(document_id.map(|id| id.to_string()))
        .bind(last_error.map(|error| bounded(error, 16_384)))
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "ingestion job",
            });
        }
        self.ingestion_job(id).await
    }

    pub async fn request_ingestion_cancel(&self, id: Uuid) -> Result<IngestionJob> {
        let result = sqlx::query(
            "UPDATE ingestion_jobs SET cancel_requested = 1, updated_at_ms = ?
             WHERE id = ? AND status IN ('queued', 'running')",
        )
        .bind(now_ms())
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            let job = self.ingestion_job(id).await?;
            if job.status == IngestionJobStatus::Queued || job.status == IngestionJobStatus::Running
            {
                return Err(StorageError::Conflict(
                    "ingestion cancellation could not be recorded".to_string(),
                ));
            }
            return Ok(job);
        }
        self.ingestion_job(id).await
    }

    pub async fn recover_interrupted_ingestion_jobs(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE ingestion_jobs
             SET status = 'failed', last_error = 'desktop core restarted during ingestion',
                 updated_at_ms = ?
             WHERE status IN ('queued', 'running')",
        )
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn create_knowledge_watch_root(
        &self,
        input: NewKnowledgeWatchRoot,
    ) -> Result<KnowledgeWatchRoot> {
        if input.path.trim().is_empty() || input.path.len() > 32_768 {
            return Err(StorageError::InvalidInput(
                "watch root path must be between 1 and 32,768 characters".to_string(),
            ));
        }
        let stack = self.knowledge_stack(input.stack_id).await?;
        if stack.workspace_id != input.workspace_id {
            return Err(StorageError::InvalidInput(
                "watch root workspace does not match its stack".to_string(),
            ));
        }
        let id = Uuid::now_v7();
        let now = now_ms();
        let result = sqlx::query(
            "INSERT INTO knowledge_watch_roots (
                 id, stack_id, workspace_id, kind, path, recursive, enabled,
                 last_scan_at_ms, last_error, created_at_ms, updated_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, 1, NULL, NULL, ?, ?)",
        )
        .bind(id.to_string())
        .bind(input.stack_id.to_string())
        .bind(input.workspace_id.to_string())
        .bind(input.kind.as_str())
        .bind(input.path)
        .bind(i64::from(input.recursive))
        .bind(now)
        .bind(now)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => self.knowledge_watch_root(id).await,
            Err(error) if is_unique_constraint(&error) => Err(StorageError::Conflict(
                "this folder is already watched by the knowledge stack".to_string(),
            )),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn knowledge_watch_root(&self, id: Uuid) -> Result<KnowledgeWatchRoot> {
        let row = sqlx::query_as::<_, WatchRootRow>(
            "SELECT id, stack_id, workspace_id, kind, path, recursive, enabled,
                    last_scan_at_ms, last_error, created_at_ms, updated_at_ms
             FROM knowledge_watch_roots WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "knowledge watch root",
        })?;
        watch_root_from_row(row)
    }

    pub async fn list_knowledge_watch_roots(
        &self,
        stack_id: Option<Uuid>,
    ) -> Result<Vec<KnowledgeWatchRoot>> {
        let rows = match stack_id {
            Some(stack_id) => {
                sqlx::query_as::<_, WatchRootRow>(
                    "SELECT id, stack_id, workspace_id, kind, path, recursive, enabled,
                            last_scan_at_ms, last_error, created_at_ms, updated_at_ms
                     FROM knowledge_watch_roots WHERE stack_id = ?
                     ORDER BY path COLLATE NOCASE, id",
                )
                .bind(stack_id.to_string())
                .fetch_all(self.pool())
                .await?
            }
            None => {
                sqlx::query_as::<_, WatchRootRow>(
                    "SELECT id, stack_id, workspace_id, kind, path, recursive, enabled,
                            last_scan_at_ms, last_error, created_at_ms, updated_at_ms
                     FROM knowledge_watch_roots WHERE enabled = 1
                     ORDER BY path COLLATE NOCASE, id",
                )
                .fetch_all(self.pool())
                .await?
            }
        };
        rows.into_iter().map(watch_root_from_row).collect()
    }

    pub async fn update_knowledge_watch_scan(&self, id: Uuid, error: Option<&str>) -> Result<()> {
        let now = now_ms();
        let result = sqlx::query(
            "UPDATE knowledge_watch_roots
             SET last_scan_at_ms = ?, last_error = ?, updated_at_ms = ? WHERE id = ?",
        )
        .bind(now)
        .bind(error.map(|error| bounded(error, 16_384)))
        .bind(now)
        .bind(id.to_string())
        .execute(self.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "knowledge watch root",
            });
        }
        Ok(())
    }

    pub async fn delete_knowledge_watch_root(&self, id: Uuid) -> Result<bool> {
        let result = sqlx::query("DELETE FROM knowledge_watch_roots WHERE id = ?")
            .bind(id.to_string())
            .execute(self.pool())
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn attach_generation_knowledge_stacks(
        &self,
        generation_id: Uuid,
        stack_ids: &[Uuid],
    ) -> Result<()> {
        if stack_ids.len() > 8 {
            return Err(StorageError::InvalidInput(
                "a generation can use at most eight knowledge stacks".to_string(),
            ));
        }
        let mut unique = std::collections::BTreeSet::new();
        if !stack_ids.iter().all(|id| unique.insert(*id)) {
            return Err(StorageError::InvalidInput(
                "knowledge stack selections must be unique".to_string(),
            ));
        }
        let workspace_id = sqlx::query_scalar::<_, String>(
            "SELECT c.workspace_id
             FROM generation_runs g JOIN conversations c ON c.id = g.conversation_id
             WHERE g.id = ?",
        )
        .bind(generation_id.to_string())
        .fetch_optional(self.pool())
        .await?
        .ok_or(StorageError::NotFound {
            entity: "generation run",
        })?;
        let mut transaction = self.begin_write().await?;
        sqlx::query("DELETE FROM generation_knowledge_stacks WHERE generation_id = ?")
            .bind(generation_id.to_string())
            .execute(&mut *transaction)
            .await?;
        for (ordinal, stack_id) in stack_ids.iter().enumerate() {
            let stack_workspace = sqlx::query_scalar::<_, String>(
                "SELECT workspace_id FROM knowledge_stacks WHERE id = ?",
            )
            .bind(stack_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(StorageError::NotFound {
                entity: "knowledge stack",
            })?;
            if stack_workspace != workspace_id {
                return Err(StorageError::InvalidInput(
                    "generation knowledge stacks must belong to the conversation workspace"
                        .to_string(),
                ));
            }
            sqlx::query(
                "INSERT INTO generation_knowledge_stacks (generation_id, stack_id, ordinal)
                 VALUES (?, ?, ?)",
            )
            .bind(generation_id.to_string())
            .bind(stack_id.to_string())
            .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn generation_knowledge_stacks(&self, generation_id: Uuid) -> Result<Vec<Uuid>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT stack_id FROM generation_knowledge_stacks
             WHERE generation_id = ? ORDER BY ordinal",
        )
        .bind(generation_id.to_string())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|id| parse_uuid("generation_knowledge_stack.stack_id", &id))
            .collect()
    }

    pub async fn save_retrieval_trace(
        &self,
        workspace_id: Uuid,
        generation_id: Option<Uuid>,
        result: &KnowledgeSearchResult,
    ) -> Result<()> {
        let stack_ids_json = serde_json::to_string(&result.stack_ids).map_err(|error| {
            StorageError::InvalidInput(format!("retrieval stack IDs are invalid: {error}"))
        })?;
        let embedding_models = if result.embedding_models.is_empty() {
            None
        } else {
            Some(result.embedding_models.join(","))
        };
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO retrieval_traces (
                 id, workspace_id, generation_id, query, stack_ids_json, embedding_model,
                 lexical_candidate_count, dense_candidate_count, result_count,
                 elapsed_ms, created_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(result.trace_id.to_string())
        .bind(workspace_id.to_string())
        .bind(generation_id.map(|id| id.to_string()))
        .bind(&result.query)
        .bind(stack_ids_json)
        .bind(embedding_models)
        .bind(i64::from(result.lexical_candidate_count))
        .bind(i64::from(result.dense_candidate_count))
        .bind(i64::try_from(result.hits.len()).unwrap_or(i64::MAX))
        .bind(to_i64("retrieval_trace.elapsed_ms", result.elapsed_ms)?)
        .bind(now_ms())
        .execute(&mut *transaction)
        .await?;
        for (ordinal, hit) in result.hits.iter().enumerate() {
            insert_trace_hit(&mut transaction, result.trace_id, ordinal, hit).await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

async fn insert_trace_hit(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    trace_id: Uuid,
    ordinal: usize,
    hit: &KnowledgeSearchHit,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO retrieval_trace_hits (
             trace_id, ordinal, dense_id, chunk_id, document_id, stack_id, title,
             source_uri, snippet, start_offset, end_offset, lexical_rank,
             dense_rank, fused_score
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(trace_id.to_string())
    .bind(i64::try_from(ordinal).unwrap_or(i64::MAX))
    .bind(to_i64("retrieval_hit.dense_id", hit.dense_id)?)
    .bind(hit.chunk_id.to_string())
    .bind(hit.document_id.to_string())
    .bind(hit.stack_id.to_string())
    .bind(&hit.title)
    .bind(&hit.source_uri)
    .bind(&hit.snippet)
    .bind(to_i64("retrieval_hit.start_offset", hit.start_offset)?)
    .bind(to_i64("retrieval_hit.end_offset", hit.end_offset)?)
    .bind(hit.lexical_rank.map(i64::from))
    .bind(hit.dense_rank.map(i64::from))
    .bind(hit.fused_score)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_stack_input(input: &NewKnowledgeStack) -> Result<()> {
    if input.name.trim().is_empty() || input.name.trim().len() > 120 {
        return Err(StorageError::InvalidInput(
            "knowledge stack name must be between 1 and 120 characters".to_string(),
        ));
    }
    if input.description.len() > 4096 {
        return Err(StorageError::InvalidInput(
            "knowledge stack description cannot exceed 4,096 characters".to_string(),
        ));
    }
    if input.embedding_model.trim().is_empty() || input.embedding_model.len() > 512 {
        return Err(StorageError::InvalidInput(
            "embedding model must be between 1 and 512 characters".to_string(),
        ));
    }
    if !(8..=4096).contains(&input.embedding_dimensions)
        || !input.embedding_dimensions.is_multiple_of(8)
    {
        return Err(StorageError::InvalidInput(
            "embedding dimensions must be a multiple of eight between 8 and 4,096".to_string(),
        ));
    }
    if !(256..=8192).contains(&input.chunk_size) || input.chunk_overlap >= input.chunk_size {
        return Err(StorageError::InvalidInput(
            "chunk size must be 256-8,192 and overlap must be smaller".to_string(),
        ));
    }
    validate_sha256("chunker hash", &input.chunker_hash)
}

fn validate_document_input(input: &NewKnowledgeDocument) -> Result<()> {
    if input.source_uri.trim().is_empty() || input.source_uri.len() > 32_768 {
        return Err(StorageError::InvalidInput(
            "document source URI must be between 1 and 32,768 characters".to_string(),
        ));
    }
    if input.title.trim().is_empty() || input.title.len() > 1024 {
        return Err(StorageError::InvalidInput(
            "document title must be between 1 and 1,024 characters".to_string(),
        ));
    }
    if input.mime_type.trim().is_empty() || input.mime_type.len() > 255 {
        return Err(StorageError::InvalidInput(
            "document MIME type must be between 1 and 255 characters".to_string(),
        ));
    }
    if input.chunks.is_empty() || input.chunks.len() > 100_000 {
        return Err(StorageError::InvalidInput(
            "a document must contain between 1 and 100,000 chunks".to_string(),
        ));
    }
    validate_sha256("content hash", &input.content_sha256)?;
    validate_sha256("blob hash", &input.blob_sha256)?;
    for (expected, chunk) in input.chunks.iter().enumerate() {
        if usize::try_from(chunk.ordinal).ok() != Some(expected) {
            return Err(StorageError::InvalidInput(
                "document chunk ordinals must be contiguous from zero".to_string(),
            ));
        }
        if chunk.content.is_empty() || chunk.character_count == 0 {
            return Err(StorageError::InvalidInput(
                "document chunks cannot be empty".to_string(),
            ));
        }
        if chunk.end_offset <= chunk.start_offset {
            return Err(StorageError::InvalidInput(
                "document chunk offsets are invalid".to_string(),
            ));
        }
        validate_sha256("chunk content hash", &chunk.content_sha256)?;
        validate_sha256("embedding vector hash", &chunk.vector_sha256)?;
        if !chunk.embedding.iter().all(|value| value.is_finite()) {
            return Err(StorageError::InvalidInput(
                "embedding vectors must contain only finite values".to_string(),
            ));
        }
    }
    Ok(())
}

fn stack_from_row(row: KnowledgeStackRow) -> Result<KnowledgeStack> {
    Ok(KnowledgeStack {
        id: parse_uuid("knowledge_stack.id", &row.id)?,
        workspace_id: parse_uuid("knowledge_stack.workspace_id", &row.workspace_id)?,
        name: row.name,
        description: row.description,
        embedding_model: row.embedding_model,
        embedding_dimensions: positive_u32(
            "knowledge_stack.embedding_dimensions",
            row.embedding_dimensions,
        )?,
        chunk_size: positive_u32("knowledge_stack.chunk_size", row.chunk_size)?,
        chunk_overlap: nonnegative_u32("knowledge_stack.chunk_overlap", row.chunk_overlap)?,
        chunker_hash: row.chunker_hash,
        index_revision: nonnegative_u64("knowledge_stack.index_revision", row.index_revision)?,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn document_from_row(row: KnowledgeDocumentRow) -> Result<KnowledgeDocument> {
    Ok(KnowledgeDocument {
        id: parse_uuid("knowledge_document.id", &row.id)?,
        stack_id: parse_uuid("knowledge_document.stack_id", &row.stack_id)?,
        workspace_id: parse_uuid("knowledge_document.workspace_id", &row.workspace_id)?,
        source_kind: KnowledgeSourceKind::from_str(&row.source_kind).map_err(|message| {
            StorageError::CorruptData {
                field: "knowledge_document.source_kind",
                message,
            }
        })?,
        source_uri: row.source_uri,
        title: row.title,
        mime_type: row.mime_type,
        content_sha256: row.content_sha256,
        blob_sha256: row.blob_sha256,
        byte_size: nonnegative_u64("knowledge_document.byte_size", row.byte_size)?,
        status: KnowledgeDocumentStatus::from_str(&row.status).map_err(|message| {
            StorageError::CorruptData {
                field: "knowledge_document.status",
                message,
            }
        })?,
        last_error: row.last_error,
        source_modified_at_ms: row.source_modified_at_ms,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn chunk_from_row(row: ChunkRow) -> Result<DocumentChunk> {
    Ok(DocumentChunk {
        dense_id: positive_u64("document_chunk.dense_id", row.dense_id)?,
        id: parse_uuid("document_chunk.id", &row.id)?,
        document_id: parse_uuid("document_chunk.document_id", &row.document_id)?,
        stack_id: parse_uuid("document_chunk.stack_id", &row.stack_id)?,
        workspace_id: parse_uuid("document_chunk.workspace_id", &row.workspace_id)?,
        ordinal: nonnegative_u32("document_chunk.ordinal", row.ordinal)?,
        content: row.content,
        content_sha256: row.content_sha256,
        character_count: positive_u32("document_chunk.character_count", row.character_count)?,
        start_offset: nonnegative_u64("document_chunk.start_offset", row.start_offset)?,
        end_offset: positive_u64("document_chunk.end_offset", row.end_offset)?,
        metadata: serde_json::from_str(&row.metadata_json).map_err(|error| {
            StorageError::CorruptData {
                field: "document_chunk.metadata_json",
                message: error.to_string(),
            }
        })?,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn chunk_record_from_row(row: ChunkRecordRow) -> Result<KnowledgeChunkRecord> {
    let title = row.title.clone();
    let source_uri = row.source_uri.clone();
    Ok(KnowledgeChunkRecord {
        chunk: chunk_from_row(ChunkRow {
            dense_id: row.dense_id,
            id: row.id,
            document_id: row.document_id,
            stack_id: row.stack_id,
            workspace_id: row.workspace_id,
            ordinal: row.ordinal,
            content: row.content,
            content_sha256: row.content_sha256,
            character_count: row.character_count,
            start_offset: row.start_offset,
            end_offset: row.end_offset,
            metadata_json: row.metadata_json,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        })?,
        title,
        source_uri,
    })
}

fn watch_root_from_row(row: WatchRootRow) -> Result<KnowledgeWatchRoot> {
    Ok(KnowledgeWatchRoot {
        id: parse_uuid("knowledge_watch_root.id", &row.id)?,
        stack_id: parse_uuid("knowledge_watch_root.stack_id", &row.stack_id)?,
        workspace_id: parse_uuid("knowledge_watch_root.workspace_id", &row.workspace_id)?,
        kind: KnowledgeWatchKind::from_str(&row.kind).map_err(|message| {
            StorageError::CorruptData {
                field: "knowledge_watch_root.kind",
                message,
            }
        })?,
        path: row.path,
        recursive: parse_bool("knowledge_watch_root.recursive", row.recursive)?,
        enabled: parse_bool("knowledge_watch_root.enabled", row.enabled)?,
        last_scan_at_ms: row.last_scan_at_ms,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn ingestion_job_from_row(row: IngestionJobRow) -> Result<IngestionJob> {
    Ok(IngestionJob {
        id: parse_uuid("ingestion_job.id", &row.id)?,
        stack_id: parse_uuid("ingestion_job.stack_id", &row.stack_id)?,
        source_uri: row.source_uri,
        source_kind: KnowledgeSourceKind::from_str(&row.source_kind).map_err(|message| {
            StorageError::CorruptData {
                field: "ingestion_job.source_kind",
                message,
            }
        })?,
        status: IngestionJobStatus::from_str(&row.status).map_err(|message| {
            StorageError::CorruptData {
                field: "ingestion_job.status",
                message,
            }
        })?,
        progress_current: nonnegative_u64("ingestion_job.progress_current", row.progress_current)?,
        progress_total: row
            .progress_total
            .map(|value| nonnegative_u64("ingestion_job.progress_total", value))
            .transpose()?,
        document_id: row
            .document_id
            .map(|id| parse_uuid("ingestion_job.document_id", &id))
            .transpose()?,
        cancel_requested: parse_bool("ingestion_job.cancel_requested", row.cancel_requested)?,
        last_error: row.last_error,
        created_at_ms: row.created_at_ms,
        updated_at_ms: row.updated_at_ms,
    })
}

fn encode_vector(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn decode_vector(dimensions: i64, bytes: &[u8]) -> Result<Vec<f32>> {
    let dimensions = positive_u64("embedding.dimensions", dimensions)?;
    let expected = usize::try_from(dimensions)
        .ok()
        .and_then(|dimensions| dimensions.checked_mul(4))
        .ok_or_else(|| StorageError::CorruptData {
            field: "embedding.vector",
            message: "stored embedding dimension overflows this platform".to_string(),
        })?;
    if bytes.len() != expected {
        return Err(StorageError::CorruptData {
            field: "embedding.vector",
            message: format!("expected {expected} bytes but found {}", bytes.len()),
        });
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

fn simple_fts_query(query: &str) -> Result<String> {
    let tokens = query
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .take(32)
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Err(StorageError::InvalidInput(
            "knowledge search query cannot be empty".to_string(),
        ));
    }
    Ok(tokens.join(" OR "))
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(StorageError::InvalidInput(format!(
            "{name} must contain exactly 64 hexadecimal characters"
        )));
    }
    Ok(())
}

fn parse_uuid(field: &'static str, value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| StorageError::CorruptData {
        field,
        message: error.to_string(),
    })
}

fn parse_bool(field: &'static str, value: i64) -> Result<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(StorageError::CorruptData {
            field,
            message: format!("expected zero or one but found {value}"),
        }),
    }
}

fn positive_u32(field: &'static str, value: i64) -> Result<u32> {
    u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StorageError::CorruptData {
            field,
            message: format!("expected a positive 32-bit value but found {value}"),
        })
}

fn nonnegative_u32(field: &'static str, value: i64) -> Result<u32> {
    u32::try_from(value).map_err(|_| StorageError::CorruptData {
        field,
        message: format!("expected a nonnegative 32-bit value but found {value}"),
    })
}

fn positive_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StorageError::CorruptData {
            field,
            message: format!("expected a positive value but found {value}"),
        })
}

fn nonnegative_u64(field: &'static str, value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| StorageError::CorruptData {
        field,
        message: format!("expected a nonnegative value but found {value}"),
    })
}

fn to_i64(field: &'static str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        StorageError::InvalidInput(format!("{field} exceeds SQLite's signed 64-bit range"))
    })
}

fn is_unique_constraint(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database_error) if database_error.is_unique_violation())
}

fn bounded(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}
