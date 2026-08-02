use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use northbridge_domain::{
    DEFAULT_WORKSPACE_ID, IngestionJobStatus, KnowledgeSourceKind, KnowledgeWatchKind,
};
use northbridge_retrieval::{
    EmbeddingProvider, KnowledgeRetrieval, Result as RetrievalResult, RetrievalConfig,
};
use northbridge_storage::{Database, NewKnowledgeStack};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{IngestionConfig, IngestionService, KnowledgeWatchManager};

#[derive(Debug)]
struct TestEmbeddings {
    delay: Duration,
}

#[async_trait]
impl EmbeddingProvider for TestEmbeddings {
    async fn embed(
        &self,
        _physical_model: &str,
        expected_dimensions: usize,
        inputs: &[String],
    ) -> RetrievalResult<Vec<Vec<f32>>> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(inputs
            .iter()
            .map(|input| deterministic_vector(input, expected_dimensions))
            .collect())
    }
}

struct TestContext {
    database: Database,
    directory: TempDir,
    stack: northbridge_domain::KnowledgeStack,
}

async fn context() -> TestContext {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::connect(&directory.path().join("ingestion.sqlite"))
        .await
        .unwrap();
    let stack = database
        .create_knowledge_stack(NewKnowledgeStack {
            workspace_id: Uuid::parse_str(DEFAULT_WORKSPACE_ID).unwrap(),
            name: "Ingestion acceptance".to_string(),
            description: String::new(),
            embedding_model: "test/deterministic".to_string(),
            embedding_dimensions: 8,
            chunk_size: 256,
            chunk_overlap: 32,
            chunker_hash: sha256(b"northbridge-char-v1:256:32"),
        })
        .await
        .unwrap();
    TestContext {
        database,
        directory,
        stack,
    }
}

async fn services(
    context: &TestContext,
    delay: Duration,
    batch_size: usize,
) -> (KnowledgeRetrieval, IngestionService) {
    let retrieval = KnowledgeRetrieval::open(
        context.database.clone(),
        RetrievalConfig::local(context.directory.path().join("indexes")),
        Arc::new(TestEmbeddings { delay }),
    )
    .await
    .unwrap();
    let mut config = IngestionConfig::local(context.directory.path().join("blobs"));
    config.embedding_batch_size = batch_size;
    let ingestion = IngestionService::open(context.database.clone(), retrieval.clone(), config)
        .await
        .unwrap();
    (retrieval, ingestion)
}

#[tokio::test]
async fn file_ingestion_is_durable_deduplicated_searchable_and_updateable() {
    let context = context().await;
    let (retrieval, ingestion) = services(&context, Duration::ZERO, 4).await;
    let path = context.directory.path().join("release-notes.md");
    let original = format!(
        "# Aurora release\n\nThe migration requires a cobalt backup before deployment.\n\n{}",
        "Operational evidence remains local. ".repeat(30)
    );
    tokio::fs::write(&path, &original).await.unwrap();

    let first_job = ingestion
        .start_file(context.stack.id, &path, KnowledgeSourceKind::File)
        .await
        .unwrap();
    let first_job = ingestion
        .wait_for_job(first_job.id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(first_job.status, IngestionJobStatus::Completed);
    let document_id = first_job.document_id.unwrap();
    let document = context
        .database
        .knowledge_document(document_id)
        .await
        .unwrap();
    let chunks = context
        .database
        .list_document_chunks(document_id)
        .await
        .unwrap();
    assert!(chunks.len() > 1);
    assert!(
        context
            .directory
            .path()
            .join("blobs")
            .join(&document.blob_sha256[..2])
            .join(&document.blob_sha256)
            .is_file()
    );
    let first_revision = context
        .database
        .knowledge_stack(context.stack.id)
        .await
        .unwrap()
        .index_revision;
    let result = retrieval
        .search(
            context.stack.workspace_id,
            &[context.stack.id],
            "cobalt backup",
            8,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.hits[0].document_id, document_id);
    assert!(result.hits[0].snippet.contains("cobalt backup"));

    let duplicate = ingestion
        .start_file(context.stack.id, &path, KnowledgeSourceKind::File)
        .await
        .unwrap();
    let duplicate = ingestion
        .wait_for_job(duplicate.id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(duplicate.document_id, Some(document_id));
    assert_eq!(
        context
            .database
            .knowledge_stack(context.stack.id)
            .await
            .unwrap()
            .index_revision,
        first_revision
    );

    let updated = "# Zephyr release\n\nUse the emerald canary before the production rollout.";
    tokio::fs::write(&path, updated).await.unwrap();
    let update_job = ingestion
        .start_file(context.stack.id, &path, KnowledgeSourceKind::File)
        .await
        .unwrap();
    let update_job = ingestion
        .wait_for_job(update_job.id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(update_job.status, IngestionJobStatus::Completed);
    assert_eq!(update_job.document_id, Some(document_id));
    assert_eq!(
        context
            .database
            .knowledge_stack(context.stack.id)
            .await
            .unwrap()
            .index_revision,
        first_revision + 1
    );
    let updated_result = retrieval
        .search(
            context.stack.workspace_id,
            &[context.stack.id],
            "emerald canary",
            8,
            None,
        )
        .await
        .unwrap();
    assert_eq!(updated_result.hits[0].document_id, document_id);
    assert!(updated_result.hits[0].snippet.contains("emerald canary"));
    assert!(ingestion.shutdown(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn ingestion_cancellation_stops_before_document_commit() {
    let context = context().await;
    let (_retrieval, ingestion) = services(&context, Duration::from_millis(300), 1).await;
    let path = context.directory.path().join("slow.md");
    tokio::fs::write(
        &path,
        format!(
            "# Slow\n\n{}",
            "A deliberately large local document. ".repeat(100)
        ),
    )
    .await
    .unwrap();
    let job = ingestion
        .start_file(context.stack.id, &path, KnowledgeSourceKind::File)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;
    ingestion.cancel(job.id).await.unwrap();
    let finished = ingestion
        .wait_for_job(job.id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(finished.status, IngestionJobStatus::Cancelled);
    assert!(
        context
            .database
            .list_knowledge_documents(context.stack.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(ingestion.shutdown(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn concurrent_scans_reserve_one_job_per_stack_source() {
    let context = context().await;
    let (_retrieval, ingestion) = services(&context, Duration::from_millis(100), 1).await;
    let path = context.directory.path().join("concurrent.md");
    tokio::fs::write(
        &path,
        "# Concurrent\n\nOne source must have one active job.",
    )
    .await
    .unwrap();

    let (first, second) = tokio::join!(
        ingestion.start_file(context.stack.id, &path, KnowledgeSourceKind::Folder),
        ingestion.start_file(context.stack.id, &path, KnowledgeSourceKind::Folder),
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(
        context
            .database
            .list_ingestion_jobs(context.stack.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        ingestion
            .wait_for_job(first.id, Duration::from_secs(10))
            .await
            .unwrap()
            .status,
        IngestionJobStatus::Completed
    );
    assert!(ingestion.shutdown(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn obsidian_rescan_filters_vault_metadata_and_persists_the_watch_root() {
    let context = context().await;
    let (_retrieval, ingestion) = services(&context, Duration::ZERO, 8).await;
    let vault = context.directory.path().join("vault");
    tokio::fs::create_dir_all(vault.join(".obsidian"))
        .await
        .unwrap();
    tokio::fs::write(
        vault.join("note.md"),
        "# Linked note\n\nNorthBridge vault evidence.",
    )
    .await
    .unwrap();
    tokio::fs::write(vault.join("ignored.txt"), "not markdown")
        .await
        .unwrap();
    tokio::fs::write(vault.join(".obsidian").join("workspace.json"), "{}")
        .await
        .unwrap();

    let manager = KnowledgeWatchManager::open(ingestion.clone())
        .await
        .unwrap();
    let root = manager
        .add_root(
            context.stack.id,
            context.stack.workspace_id,
            &vault,
            KnowledgeWatchKind::Obsidian,
            true,
        )
        .await
        .unwrap();
    manager.rescan(root.id).await.unwrap();
    wait_for_stack_jobs(&ingestion, context.stack.id).await;
    let documents = context
        .database
        .list_knowledge_documents(context.stack.id)
        .await
        .unwrap();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].source_kind, KnowledgeSourceKind::Obsidian);
    assert_eq!(documents[0].title, "note.md");
    assert!(
        context
            .database
            .list_document_chunks(documents[0].id)
            .await
            .unwrap()[0]
            .content
            .contains("Linked note")
    );
    let stored_root = context
        .database
        .knowledge_watch_root(root.id)
        .await
        .unwrap();
    assert!(stored_root.last_scan_at_ms.is_some());
    assert!(stored_root.last_error.is_none());
    manager.shutdown();
    assert!(ingestion.shutdown(Duration::from_secs(2)).await);
}

async fn wait_for_stack_jobs(ingestion: &IngestionService, stack_id: Uuid) {
    let jobs = ingestion
        .database()
        .list_ingestion_jobs(stack_id)
        .await
        .unwrap();
    for job in jobs {
        ingestion
            .wait_for_job(job.id, Duration::from_secs(10))
            .await
            .unwrap();
    }
}

fn deterministic_vector(input: &str, dimensions: usize) -> Vec<f32> {
    let digest = Sha256::digest(input.as_bytes());
    let mut vector = vec![0.0; dimensions];
    for (index, byte) in digest.iter().enumerate() {
        vector[index % dimensions] += f32::from(*byte) / 255.0;
    }
    vector
}

fn sha256(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}
