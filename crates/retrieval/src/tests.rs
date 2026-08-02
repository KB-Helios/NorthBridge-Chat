use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use northbridge_domain::{DEFAULT_WORKSPACE_ID, KnowledgeSourceKind};
use northbridge_storage::{Database, NewKnowledgeChunk, NewKnowledgeDocument, NewKnowledgeStack};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{EmbeddingProvider, KnowledgeRetrieval, Result, RetrievalConfig};

#[derive(Debug)]
struct DeterministicEmbeddings;

#[async_trait]
impl EmbeddingProvider for DeterministicEmbeddings {
    async fn embed(
        &self,
        _physical_model: &str,
        expected_dimensions: usize,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>> {
        Ok(inputs
            .iter()
            .map(|input| deterministic_vector(input, expected_dimensions))
            .collect())
    }
}

async fn test_context() -> (Database, TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::connect(&directory.path().join("knowledge.sqlite"))
        .await
        .unwrap();
    let index_directory = directory.path().join("indexes");
    (database, directory, index_directory)
}

async fn create_stack(database: &Database, name: &str) -> northbridge_domain::KnowledgeStack {
    create_stack_with_dimensions(database, name, 8).await
}

async fn create_stack_with_dimensions(
    database: &Database,
    name: &str,
    dimensions: u32,
) -> northbridge_domain::KnowledgeStack {
    database
        .create_knowledge_stack(NewKnowledgeStack {
            workspace_id: workspace_id(),
            name: name.to_string(),
            description: String::new(),
            embedding_model: "test/deterministic".to_string(),
            embedding_dimensions: dimensions,
            chunk_size: 256,
            chunk_overlap: 32,
            chunker_hash: sha256(b"northbridge-char-v1:256:32"),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn query_cache_is_dimension_safe_across_stacks_using_the_same_model() {
    let (database, _directory, index_directory) = test_context().await;
    let small = create_stack_with_dimensions(&database, "Eight dimensions", 8).await;
    let large = create_stack_with_dimensions(&database, "Sixteen dimensions", 16).await;
    upsert_document(&database, &small, 1, 1).await;
    upsert_document(&database, &large, 2, 1).await;
    let retrieval = open_retrieval(&database, &index_directory).await;

    let result = retrieval
        .search(
            small.workspace_id,
            &[small.id, large.id],
            "release record",
            8,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.hits.len(), 2);
    assert_eq!(result.stack_ids, vec![small.id, large.id]);
    assert_eq!(result.embedding_models, vec!["test/deterministic"]);
}

async fn upsert_document(
    database: &Database,
    stack: &northbridge_domain::KnowledgeStack,
    slot: usize,
    version: usize,
) -> northbridge_domain::KnowledgeDocument {
    let content =
        format!("keyword{slot} release record version{version} contains durable local evidence");
    let vector = deterministic_vector(&content, stack.embedding_dimensions as usize);
    database
        .upsert_knowledge_document(NewKnowledgeDocument {
            stack_id: stack.id,
            workspace_id: stack.workspace_id,
            source_kind: KnowledgeSourceKind::File,
            source_uri: format!("file:///document-{slot}.md"),
            title: format!("Document {slot} v{version}"),
            mime_type: "text/markdown".to_string(),
            content_sha256: sha256(content.as_bytes()),
            blob_sha256: sha256(format!("blob:{content}").as_bytes()),
            byte_size: content.len() as u64,
            source_modified_at_ms: Some(version as i64),
            chunks: vec![NewKnowledgeChunk {
                id: Uuid::now_v7(),
                ordinal: 0,
                content_sha256: sha256(content.as_bytes()),
                character_count: content.chars().count() as u32,
                start_offset: 0,
                end_offset: content.chars().count() as u64,
                metadata: json!({ "slot": slot, "version": version }),
                content,
                vector_sha256: vector_sha256(&vector),
                embedding: vector,
            }],
        })
        .await
        .unwrap()
}

async fn open_retrieval(
    database: &Database,
    index_directory: &std::path::Path,
) -> KnowledgeRetrieval {
    KnowledgeRetrieval::open(
        database.clone(),
        RetrievalConfig::local(index_directory.to_path_buf()),
        Arc::new(DeterministicEmbeddings),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn hybrid_search_survives_restart_and_corrupt_manifest_rebuild() {
    let (database, _directory, index_directory) = test_context().await;
    let stack = create_stack(&database, "Restart recovery").await;
    let alpha = upsert_document(&database, &stack, 1, 1).await;
    let beta = upsert_document(&database, &stack, 2, 1).await;

    let retrieval = open_retrieval(&database, &index_directory).await;
    let status = retrieval.synchronize_stack(stack.id).await.unwrap();
    assert_eq!(status.record_count, 2);
    assert_eq!(status.revision, 2);
    assert_eq!(status.bit_width, 4);
    assert!(status.snapshot.is_some());
    let first = retrieval
        .search(stack.workspace_id, &[stack.id], "keyword1", 8, None)
        .await
        .unwrap();
    assert_eq!(first.hits[0].document_id, alpha.id);
    assert_eq!(first.hits[0].citation, "[K1]");
    assert!(first.hits[0].lexical_rank.is_some());
    assert!(first.hits[0].dense_rank.is_some());

    drop(retrieval);
    tokio::task::yield_now().await;
    let manifest = index_directory
        .join(stack.id.to_string())
        .join("manifest.json");
    tokio::fs::write(&manifest, b"{corrupt-manifest")
        .await
        .unwrap();
    let recovered = open_retrieval(&database, &index_directory).await;
    let recovered_status = recovered.synchronize_stack(stack.id).await.unwrap();
    assert_eq!(recovered_status.record_count, 2);
    assert_eq!(recovered_status.revision, 2);
    let after_rebuild = recovered
        .search(stack.workspace_id, &[stack.id], "keyword2", 8, None)
        .await
        .unwrap();
    assert_eq!(after_rebuild.hits[0].document_id, beta.id);

    let updated_alpha = upsert_document(&database, &stack, 1, 2).await;
    assert_eq!(updated_alpha.id, alpha.id);
    assert!(database.delete_knowledge_document(beta.id).await.unwrap());
    let converged = recovered.synchronize_stack(stack.id).await.unwrap();
    assert_eq!(converged.record_count, 1);
    assert_eq!(converged.revision, 4);
    let latest = recovered
        .search(
            stack.workspace_id,
            &[stack.id],
            "keyword1 version2",
            8,
            None,
        )
        .await
        .unwrap();
    assert_eq!(latest.hits[0].document_id, alpha.id);
    assert_eq!(latest.hits[0].title, "Document 1 v2");
}

#[tokio::test]
async fn derived_index_converges_for_state_machine_sequences() {
    for seed in [0x1234_5678_u64, 0x9abc_def0, 0x55aa_00ff, 0xdead_beef] {
        let (database, _directory, index_directory) = test_context().await;
        let stack = create_stack(&database, &format!("State machine {seed:x}")).await;
        let mut retrieval = open_retrieval(&database, &index_directory).await;
        let mut state = HashMap::<usize, Uuid>::new();
        let mut random = seed;

        for step in 1..=32_usize {
            random = xorshift64(random);
            let slot = (random as usize) % 6;
            random = xorshift64(random);
            if random % 4 == 0 {
                if let Some(document_id) = state.remove(&slot) {
                    assert!(
                        database
                            .delete_knowledge_document(document_id)
                            .await
                            .unwrap()
                    );
                }
            } else {
                let document = upsert_document(&database, &stack, slot, step).await;
                if let Some(previous) = state.insert(slot, document.id) {
                    assert_eq!(previous, document.id, "seed={seed:x} step={step}");
                }
            }

            let status = retrieval.synchronize_stack(stack.id).await.unwrap();
            let allowed = database
                .allowed_knowledge_dense_ids(stack.workspace_id, stack.id)
                .await
                .unwrap();
            let authoritative_revision = database
                .knowledge_stack(stack.id)
                .await
                .unwrap()
                .index_revision;
            assert_eq!(
                status.record_count,
                state.len(),
                "seed={seed:x} step={step}"
            );
            assert_eq!(allowed.len(), state.len(), "seed={seed:x} step={step}");
            assert_eq!(
                status.revision, authoritative_revision,
                "seed={seed:x} step={step}"
            );

            if let Some((&known_slot, &document_id)) = state.iter().next() {
                let result = retrieval
                    .search(
                        stack.workspace_id,
                        &[stack.id],
                        &format!("keyword{known_slot}"),
                        10,
                        None,
                    )
                    .await
                    .unwrap();
                assert!(
                    result.hits.iter().any(|hit| hit.document_id == document_id),
                    "seed={seed:x} step={step}"
                );
                assert!(
                    result
                        .hits
                        .iter()
                        .all(|hit| allowed.contains(&hit.dense_id)),
                    "seed={seed:x} step={step}"
                );
            }

            if step.is_multiple_of(8) {
                drop(retrieval);
                tokio::task::yield_now().await;
                retrieval = open_retrieval(&database, &index_directory).await;
                let restarted = retrieval.synchronize_stack(stack.id).await.unwrap();
                assert_eq!(
                    restarted.record_count,
                    state.len(),
                    "seed={seed:x} step={step}"
                );
            }
        }
    }
}

fn workspace_id() -> Uuid {
    Uuid::parse_str(DEFAULT_WORKSPACE_ID).unwrap()
}

fn deterministic_vector(input: &str, dimensions: usize) -> Vec<f32> {
    let digest = Sha256::digest(input.to_ascii_lowercase().as_bytes());
    let mut vector = vec![0.0; dimensions];
    for (index, byte) in digest.iter().enumerate() {
        vector[index % dimensions] += f32::from(*byte) / 255.0;
    }
    vector
}

fn vector_sha256(vector: &[f32]) -> String {
    let mut hasher = Sha256::new();
    for value in vector {
        hasher.update(value.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn sha256(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn xorshift64(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    value
}
