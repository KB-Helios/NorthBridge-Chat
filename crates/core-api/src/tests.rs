use std::{path::Path, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{Router, body::Body};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use northbridge_domain::{DEFAULT_WORKSPACE_ID, EnvironmentVariableRef};
use northbridge_model_manager::{ModelManager, ModelManagerConfig};
use northbridge_retrieval::{EmbeddingProvider, Result as RetrievalResult};
use northbridge_storage::{Database, NewCredentialReference, NewEnvironment};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;
use uuid::Uuid;

use crate::{CoreApi, CoreApiConfig, KnowledgeRuntimeConfig, KnowledgeServices};

const TEST_TOKEN: &str = "test-session-token-with-at-least-32-bytes";
const TEST_CONTROL_TOKEN: &str = "test-control-token-with-at-least-32-bytes";

async fn test_api() -> (CoreApi, Database, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::connect(&directory.path().join("core.sqlite"))
        .await
        .unwrap();
    let api = CoreApi::new(
        database.clone(),
        CoreApiConfig {
            session_token: TEST_TOKEN.to_string(),
            control_token: TEST_CONTROL_TOKEN.to_string(),
            allowed_origins: vec!["http://tauri.localhost".to_string()],
            max_json_body_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    (api, database, directory)
}

#[derive(Debug)]
struct TestEmbeddings;

#[async_trait]
impl EmbeddingProvider for TestEmbeddings {
    async fn embed(
        &self,
        _physical_model: &str,
        expected_dimensions: usize,
        inputs: &[String],
    ) -> RetrievalResult<Vec<Vec<f32>>> {
        Ok(inputs
            .iter()
            .map(|input| {
                let mut vector = vec![0.0; expected_dimensions];
                for (index, byte) in input.bytes().enumerate() {
                    vector[index % expected_dimensions] += f32::from(byte) / 255.0;
                }
                vector
            })
            .collect())
    }
}

async fn test_api_with_knowledge() -> (CoreApi, Database, TempDir) {
    let (api, database, directory) = test_api().await;
    let services = KnowledgeServices::open_for_test(
        database.clone(),
        KnowledgeRuntimeConfig {
            blob_directory: directory.path().join("knowledge-blobs"),
            index_directory: directory.path().join("knowledge-indexes"),
            default_embedding_model: "test/deterministic".to_string(),
            default_embedding_dimensions: 8,
        },
        Arc::new(TestEmbeddings),
    )
    .await
    .unwrap();
    (api.with_knowledge_services(services), database, directory)
}

#[tokio::test]
async fn accepts_shutdown_only_with_the_native_control_token() {
    let (api, _database, _directory) = test_api().await;
    let shutdown = api.shutdown_signal();
    let router = api.router();

    let missing = router
        .clone()
        .oneshot(request("POST", "/v1/runtime/shutdown", Value::Null))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert!(!shutdown.is_cancelled());

    let wrong = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runtime/shutdown")
                .header(http::header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                .header("x-northbridge-control-token", "wrong-control-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
    assert!(!shutdown.is_cancelled());

    let accepted = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runtime/shutdown")
                .header(http::header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
                .header("x-northbridge-control-token", TEST_CONTROL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    assert!(shutdown.is_cancelled());
}

fn request(method: &str, uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(http::header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn create_conversation(router: &Router) -> Value {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/conversations",
            json!({ "title": "Desktop foundation" }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    json_body(response).await
}

async fn wait_for_ingestion_job(router: &Router, job_id: &str) -> Value {
    for _ in 0..100 {
        let response = router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/v1/knowledge/jobs/{job_id}"),
                Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let job = json_body(response).await;
        if matches!(
            job["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return job;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("ingestion job {job_id} did not finish");
}

#[tokio::test]
async fn rejects_requests_without_the_session_token() {
    let (api, _database, _directory) = test_api().await;
    let response = api
        .router()
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = json_body(response).await;
    assert_eq!(body["error"]["code"], "unauthorized");
}

#[tokio::test]
async fn knowledge_api_ingests_searches_inspects_watches_and_rebuilds() {
    let (api, _database, directory) = test_api_with_knowledge().await;
    let router = api.router();
    let stack_response = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/knowledge/stacks",
            json!({
                "name": "Product runbooks",
                "description": "Offline operational evidence",
                "chunkSize": 256,
                "chunkOverlap": 32
            }),
        ))
        .await
        .unwrap();
    assert_eq!(stack_response.status(), StatusCode::CREATED);
    let stack = json_body(stack_response).await;
    let stack_id = stack["id"].as_str().unwrap();
    assert_eq!(stack["embeddingDimensions"], 8);

    let source = directory.path().join("orion-runbook.md");
    tokio::fs::write(
        &source,
        "# Orion deployment\n\nCreate a violet snapshot before the production migration.",
    )
    .await
    .unwrap();
    let import_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/knowledge/stacks/{stack_id}/documents/import"),
            json!({ "paths": [source.to_string_lossy()] }),
        ))
        .await
        .unwrap();
    assert_eq!(import_response.status(), StatusCode::ACCEPTED);
    let import_jobs = json_body(import_response).await;
    let imported = wait_for_ingestion_job(&router, import_jobs[0]["id"].as_str().unwrap()).await;
    assert_eq!(
        imported["status"], "completed",
        "ingestion failed: {}",
        imported["lastError"]
    );
    let document_id = imported["documentId"].as_str().unwrap();

    let chunks_response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/knowledge/documents/{document_id}/chunks"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(chunks_response.status(), StatusCode::OK);
    let chunks = json_body(chunks_response).await;
    assert_eq!(chunks.as_array().unwrap().len(), 1);
    assert!(
        chunks[0]["content"]
            .as_str()
            .unwrap()
            .contains("violet snapshot")
    );

    let search_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/knowledge/stacks/{stack_id}/search"),
            json!({ "query": "violet snapshot", "limit": 8 }),
        ))
        .await
        .unwrap();
    assert_eq!(search_response.status(), StatusCode::OK);
    let search = json_body(search_response).await;
    assert_eq!(search["hits"][0]["citation"], "[K1]");
    assert_eq!(search["hits"][0]["documentId"], document_id);
    assert!(search["hits"][0]["lexicalRank"].is_number());
    assert!(search["hits"][0]["denseRank"].is_number());

    let index_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/knowledge/stacks/{stack_id}/index"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(index_response.status(), StatusCode::OK);
    let index = json_body(index_response).await;
    assert_eq!(index["recordCount"], 1);
    assert_eq!(index["bitWidth"], 4);
    assert!(index["snapshot"].is_string());

    let watched_directory = directory.path().join("watched-runbooks");
    tokio::fs::create_dir_all(&watched_directory).await.unwrap();
    tokio::fs::write(
        watched_directory.join("recovery.md"),
        "# Recovery\n\nRestore the indigo checkpoint.",
    )
    .await
    .unwrap();
    let watch_response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/knowledge/stacks/{stack_id}/watch-roots"),
            json!({
                "path": watched_directory.to_string_lossy(),
                "kind": "folder",
                "recursive": true
            }),
        ))
        .await
        .unwrap();
    assert_eq!(watch_response.status(), StatusCode::CREATED);
    let watch = json_body(watch_response).await;
    let watch_id = watch["id"].as_str().unwrap();
    let rescan = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/knowledge/watch-roots/{watch_id}/rescan"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(rescan.status(), StatusCode::OK);
    let jobs_response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/knowledge/stacks/{stack_id}/jobs"),
            Value::Null,
        ))
        .await
        .unwrap();
    let jobs = json_body(jobs_response).await;
    for job in jobs.as_array().unwrap() {
        let finished = wait_for_ingestion_job(&router, job["id"].as_str().unwrap()).await;
        assert_eq!(finished["status"], "completed");
    }
    let documents_response = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/knowledge/stacks/{stack_id}/documents"),
            Value::Null,
        ))
        .await
        .unwrap();
    let documents = json_body(documents_response).await;
    assert_eq!(documents.as_array().unwrap().len(), 2);

    let remove_watch = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/knowledge/watch-roots/{watch_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(remove_watch.status(), StatusCode::NO_CONTENT);
    let remove_stack = router
        .oneshot(request(
            "DELETE",
            &format!("/v1/knowledge/stacks/{stack_id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(remove_stack.status(), StatusCode::NO_CONTENT);
    assert!(api.shutdown_knowledge(Duration::from_secs(2)).await);
}

#[tokio::test]
async fn reports_and_rejects_an_unconfigured_ai_runtime() {
    let (api, _database, _directory) = test_api().await;
    let router = api.router();

    let status = router
        .clone()
        .oneshot(request("GET", "/v1/ai/status", Value::Null))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);
    let status = json_body(status).await;
    assert_eq!(status["configured"], false);
    assert!(status["selection"].is_null());
    assert!(status["runtime"].is_null());

    let catalog = router
        .clone()
        .oneshot(request("GET", "/v1/models/catalog", Value::Null))
        .await
        .unwrap();
    assert_eq!(catalog.status(), StatusCode::OK);
    let catalog = json_body(catalog).await;
    assert!(catalog["models"].as_array().unwrap().len() >= 2);
    assert_eq!(catalog["managedModels"], json!([]));
    assert!(catalog["selection"].is_null());

    let diagnostics = router
        .clone()
        .oneshot(request("GET", "/v1/hardware/diagnostics", Value::Null))
        .await
        .unwrap();
    assert_eq!(diagnostics.status(), StatusCode::SERVICE_UNAVAILABLE);

    let tuning = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/models/tune",
            json!({ "modelId": "HuggingFaceTB/SmolLM2-135M-Instruct" }),
        ))
        .await
        .unwrap();
    assert_eq!(tuning.status(), StatusCode::SERVICE_UNAVAILABLE);

    let invalid_profile = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/models/selection",
            json!({
                "modelId": "HuggingFaceTB/SmolLM2-135M-Instruct",
                "loadProfile": {
                    "quantization": "q4k",
                    "maxSequenceLength": 1,
                    "maxBatchSize": 1,
                    "forceCpu": true
                }
            }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid_profile.status(), StatusCode::BAD_REQUEST);

    let performance = router
        .clone()
        .oneshot(request(
            "GET",
            "/v1/performance/generations?limit=20",
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(performance.status(), StatusCode::OK);
    let performance = json_body(performance).await;
    assert_eq!(performance["completedRuns"], 0);
    assert_eq!(performance["samples"], json!([]));

    let completion = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/ai/chat/completions",
            json!({
                "model": "fast-local",
                "messages": [{ "role": "user", "content": "hello" }],
                "stream": true
            }),
        ))
        .await
        .unwrap();
    assert_eq!(completion.status(), StatusCode::SERVICE_UNAVAILABLE);
    let completion = json_body(completion).await;
    assert_eq!(completion["error"]["code"], "ai_not_configured");

    let conversation = create_conversation(&router).await;
    let conversation_id = conversation["id"].as_str().unwrap();
    let branch_id = conversation["defaultBranchId"].as_str().unwrap();
    let message = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/conversations/{conversation_id}/messages"),
            json!({
                "branchId": branch_id,
                "parentMessageId": null,
                "role": "user",
                "parts": [{ "kind": "text", "content": "hello" }]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(message.status(), StatusCode::CREATED);
    let message = json_body(message).await;
    let message_id = message["id"].as_str().unwrap();

    let invalid_split = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/conversations/{conversation_id}/split-chats"),
            json!({
                "sourceBranchId": branch_id,
                "inputMessageId": message_id,
                "candidates": [{ "modelId": "HuggingFaceTB/SmolLM2-135M-Instruct" }]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid_split.status(), StatusCode::BAD_REQUEST);

    let unavailable_split = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/conversations/{conversation_id}/split-chats"),
            json!({
                "sourceBranchId": branch_id,
                "inputMessageId": message_id,
                "candidates": [
                    { "modelId": "HuggingFaceTB/SmolLM2-135M-Instruct" },
                    { "modelId": "HuggingFaceTB/SmolLM2-360M-Instruct" }
                ]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(unavailable_split.status(), StatusCode::SERVICE_UNAVAILABLE);

    let split_list = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/conversations/{conversation_id}/split-chats"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(split_list.status(), StatusCode::OK);
    assert_eq!(json_body(split_list).await, json!([]));

    let generation = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/conversations/{conversation_id}/generations"),
            json!({ "branchId": branch_id, "modelAlias": "fast-local" }),
        ))
        .await
        .unwrap();
    assert_eq!(generation.status(), StatusCode::SERVICE_UNAVAILABLE);
    let generation = json_body(generation).await;
    assert_eq!(generation["error"]["code"], "ai_not_configured");
}

#[tokio::test]
async fn manages_imported_models_through_the_authenticated_catalog() {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::connect(&directory.path().join("core.sqlite"))
        .await
        .unwrap();
    let manager = ModelManager::open(
        database.clone(),
        ModelManagerConfig::local(directory.path().join("managed")),
    )
    .await
    .unwrap();
    let model_path = directory.path().join("import-source");
    create_model_fixture(&model_path);
    let api = CoreApi::new(
        database,
        CoreApiConfig {
            session_token: TEST_TOKEN.to_string(),
            control_token: TEST_CONTROL_TOKEN.to_string(),
            allowed_origins: vec!["http://tauri.localhost".to_string()],
            max_json_body_bytes: 1024 * 1024,
        },
    )
    .unwrap()
    .with_model_manager(manager);
    let router = api.router();

    let imported = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/models/imports",
            json!({
                "path": model_path.to_string_lossy(),
                "modelId": "imported/api-fixture",
                "displayName": "API fixture"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(imported.status(), StatusCode::CREATED);
    let imported = json_body(imported).await;
    assert_eq!(imported["status"], "imported");
    assert_eq!(imported["modelId"], "imported/api-fixture");
    let id = imported["id"].as_str().unwrap();

    let catalog = router
        .clone()
        .oneshot(request("GET", "/v1/models/catalog", Value::Null))
        .await
        .unwrap();
    assert_eq!(catalog.status(), StatusCode::OK);
    let catalog = json_body(catalog).await;
    assert_eq!(catalog["managedModels"].as_array().unwrap().len(), 1);
    assert!(
        catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["modelId"] == "imported/api-fixture")
    );

    let selected_without_bundle = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/models/selection",
            json!({ "modelId": "imported/api-fixture" }),
        ))
        .await
        .unwrap();
    assert_eq!(
        selected_without_bundle.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let removed = router
        .clone()
        .oneshot(request(
            "DELETE",
            &format!("/v1/models/managed/{id}"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(removed.status(), StatusCode::NO_CONTENT);
    assert!(
        model_path.exists(),
        "unregistering an import deleted user data"
    );

    let invalid_download = router
        .oneshot(request(
            "POST",
            "/v1/models/downloads",
            json!({ "modelId": "../../escape" }),
        ))
        .await
        .unwrap();
    assert_eq!(invalid_download.status(), StatusCode::BAD_REQUEST);
}

fn create_model_fixture(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::write(path.join("config.json"), "{}").unwrap();
    std::fs::write(path.join("tokenizer.json"), "{}").unwrap();
    std::fs::write(path.join("model.safetensors"), "weights").unwrap();
}

#[tokio::test]
async fn persists_conversations_messages_and_searches_text() {
    let (api, _database, _directory) = test_api().await;
    let router = api.router();
    let health = router
        .clone()
        .oneshot(request("GET", "/v1/health", Value::Null))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let conversation = create_conversation(&router).await;
    let conversation_id = conversation["id"].as_str().unwrap();
    let branch_id = conversation["defaultBranchId"].as_str().unwrap();
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/conversations/{conversation_id}/messages"),
            json!({
                "branchId": branch_id,
                "parentMessageId": null,
                "role": "user",
                "parts": [{ "kind": "text", "content": "hybrid retrieval foundation" }]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let messages = router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/v1/conversations/{conversation_id}/messages"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(messages.status(), StatusCode::OK);
    let messages = json_body(messages).await;
    assert_eq!(messages.as_array().unwrap().len(), 1);
    assert_eq!(
        messages[0]["parts"][0]["content"],
        "hybrid retrieval foundation"
    );

    let search = router
        .oneshot(request(
            "GET",
            "/v1/search/messages?q=hybrid%20retrieval",
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(search.status(), StatusCode::OK);
    let search = json_body(search).await;
    assert_eq!(search.as_array().unwrap().len(), 1);
    assert_eq!(search[0]["conversationId"], conversation_id);
}

#[tokio::test]
async fn replays_generation_events_after_the_resume_cursor() {
    let (api, database, _directory) = test_api().await;
    let router = api.router();
    let conversation = create_conversation(&router).await;
    let conversation_id = conversation["id"].as_str().unwrap();
    let branch_id = conversation["defaultBranchId"].as_str().unwrap();
    let generation = database
        .create_generation_run(
            Uuid::parse_str(conversation_id).unwrap(),
            Uuid::parse_str(branch_id).unwrap(),
            "fast-local",
        )
        .await
        .unwrap();
    let generation_id = generation.id;
    let events = api.generation_events();
    events
        .append(generation_id, "delta", json!({ "text": "one" }))
        .await
        .unwrap();
    events
        .append(generation_id, "delta", json!({ "text": "two" }))
        .await
        .unwrap();

    let response = router
        .oneshot(request(
            "GET",
            &format!("/v1/generations/{generation_id}/events?after=1"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[http::header::CONTENT_TYPE],
        "text/event-stream"
    );
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let payload = std::str::from_utf8(frame.data_ref().unwrap()).unwrap();
    assert!(payload.contains("id: 2"));
    assert!(payload.contains("\"text\":\"two\""));
    assert!(!payload.contains("\"text\":\"one\""));
}

#[tokio::test]
async fn runs_durable_transform_and_approval_workflow_through_the_api() {
    let (api, _database, _directory) = test_api().await;
    let router = api.router();
    let created = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/workflows",
            json!({
                "name": "Approval fixture",
                "entryNodeId": "copy",
                "nodes": [
                    {
                        "id": "copy",
                        "kind": "transform",
                        "operation": { "operation": "identity" },
                        "next": "approval"
                    },
                    {
                        "id": "approval",
                        "kind": "approval",
                        "title": "Review",
                        "prompt": "Approve ${input}",
                        "next": "finish"
                    },
                    {
                        "id": "finish",
                        "kind": "transform",
                        "operation": { "operation": "select", "pointer": "/value" }
                    }
                ]
            }),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let workflow = json_body(created).await;
    let workflow_id = workflow["id"].as_str().unwrap();
    let started = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/workflows/{workflow_id}/runs"),
            json!({ "input": { "value": 42 } }),
        ))
        .await
        .unwrap();
    assert_eq!(started.status(), StatusCode::ACCEPTED);
    let run_id = json_body(started).await["id"].as_str().unwrap().to_string();

    let mut found_approval = None;
    for _ in 0..100 {
        let response = router
            .clone()
            .oneshot(request("GET", "/v1/workflow-approvals", Value::Null))
            .await
            .unwrap();
        let approvals = json_body(response).await;
        if let Some(approval) = approvals.as_array().unwrap().first() {
            found_approval = Some(approval.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let approval = found_approval.expect("workflow approval did not become pending");
    let approval_id = approval["id"].as_str().unwrap();
    let decision = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/v1/workflow-approvals/{approval_id}/decision"),
            json!({ "approved": true }),
        ))
        .await
        .unwrap();
    assert_eq!(decision.status(), StatusCode::OK);

    let mut completed = None;
    for _ in 0..100 {
        let response = router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/v1/workflow-runs/{run_id}"),
                Value::Null,
            ))
            .await
            .unwrap();
        let run = json_body(response).await;
        if run["status"] == "completed" {
            completed = Some(run);
            break;
        }
        assert_ne!(run["status"], "failed", "{run}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let completed = completed.expect("workflow did not complete after approval");
    assert_eq!(completed["output"], 42);
    let nodes = router
        .oneshot(request(
            "GET",
            &format!("/v1/workflow-runs/{run_id}/nodes"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(json_body(nodes).await.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn serves_content_addressed_forge_artifacts_with_integrity_metadata() {
    let (api, _database, _directory) = test_api().await;
    let router = api.router();
    let created = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/artifacts",
            json!({
                "name": "forge.md",
                "kind": "document",
                "mediaType": "text/markdown",
                "contentBase64": "IyBGb3JnZQ=="
            }),
        ))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let artifact = json_body(created).await;
    let artifact_id = artifact["id"].as_str().unwrap();
    let content = router
        .oneshot(request(
            "GET",
            &format!("/v1/artifacts/{artifact_id}/content"),
            Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(content.status(), StatusCode::OK);
    assert_eq!(
        content.headers()[http::header::CONTENT_TYPE],
        "text/markdown"
    );
    assert_eq!(
        content.headers()[http::header::ETAG],
        format!("\"{}\"", artifact["sha256"].as_str().unwrap())
    );
    assert_eq!(
        content.into_body().collect().await.unwrap().to_bytes(),
        "# Forge"
    );
}

#[tokio::test]
async fn round_trips_secretless_packages_with_environment_variable_names_and_artifacts() {
    let (source_api, source_database, _source_directory) = test_api().await;
    let workspace_id = Uuid::parse_str(DEFAULT_WORKSPACE_ID).unwrap();
    let credential = source_database
        .create_credential_reference(NewCredentialReference {
            workspace_id,
            name: "Provider token".to_string(),
        })
        .await
        .unwrap();
    source_database
        .create_environment(NewEnvironment {
            workspace_id,
            name: "Production".to_string(),
            description: "Remote provider environment".to_string(),
            variables: vec![EnvironmentVariableRef {
                name: "MEDIA_API_KEY".to_string(),
                credential_id: credential.id,
            }],
        })
        .await
        .unwrap();
    let source_router = source_api.router();
    let artifact = source_router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/artifacts",
            json!({
                "name": "portable.md",
                "kind": "document",
                "mediaType": "text/markdown",
                "contentBase64": "IyBQb3J0YWJsZQ=="
            }),
        ))
        .await
        .unwrap();
    assert_eq!(artifact.status(), StatusCode::CREATED);
    let exported = source_router
        .oneshot(request(
            "POST",
            "/v1/packages/export",
            json!({ "name": "Portable fixture" }),
        ))
        .await
        .unwrap();
    assert_eq!(exported.status(), StatusCode::OK);
    let package = json_body(exported).await;
    assert_eq!(
        package["environments"][0]["variables"][0],
        json!({
            "name": "MEDIA_API_KEY",
            "credentialName": "Provider token"
        })
    );

    let (target_api, target_database, _target_directory) = test_api().await;
    let imported = target_api
        .router()
        .oneshot(request("POST", "/v1/packages/import", package))
        .await
        .unwrap();
    assert_eq!(imported.status(), StatusCode::CREATED);
    let summary = json_body(imported).await;
    assert_eq!(summary["environments"], 1);
    assert_eq!(summary["artifacts"], 1);
    assert_eq!(
        summary["credentialsRequiringValues"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let environments = target_database
        .list_environments(workspace_id)
        .await
        .unwrap();
    assert_eq!(environments[0].variables[0].name, "MEDIA_API_KEY");
    assert_eq!(
        target_database
            .list_credential_references(workspace_id)
            .await
            .unwrap()[0]
            .name,
        "Provider token"
    );
    let artifacts = target_database.list_artifacts(workspace_id).await.unwrap();
    let (_, content) = target_database
        .artifact_content(artifacts[0].id)
        .await
        .unwrap();
    assert_eq!(content, b"# Portable");
}

#[tokio::test]
async fn authenticates_remote_users_applies_policy_and_verifies_audit_chain() {
    let (api, database, _directory) = test_api().await;
    let router = api.router();
    let owner = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/auth/bootstrap",
            json!({
                "email": "owner@example.com",
                "displayName": "Owner",
                "password": "correct horse battery staple",
                "role": "owner"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(owner.status(), StatusCode::OK);
    let member = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/team/users",
            json!({
                "email": "member@example.com",
                "displayName": "Member",
                "password": "member password long enough",
                "role": "member"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(member.status(), StatusCode::OK);
    let policy = router
        .clone()
        .oneshot(request(
            "POST",
            "/v1/team/policies",
            json!({
                "name": "Artifacts require owner",
                "method": "GET",
                "pathPrefix": "/v1/artifacts",
                "minimumRole": "owner",
                "effect": "allow"
            }),
        ))
        .await
        .unwrap();
    assert_eq!(policy.status(), StatusCode::OK);

    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/auth/login")
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "email": "member@example.com",
                        "password": "member password long enough"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let token = json_body(login).await["token"]
        .as_str()
        .unwrap()
        .to_string();
    let denied = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/artifacts")
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(database.verify_audit_chain().await.unwrap());
}
