use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    net::TcpListener as PortListener,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::StreamExt;
use northbridge_ai_gateway::{
    AgentgatewayPlan, GatewayClient, GatewayCredential, ModelAlias, ModelApi, remove_config,
    validate_config_with_binary, write_new_config,
};
use northbridge_sidecar_supervisor::{
    HttpMethod, HttpProbe, IntegrityPolicy, RestartPolicy, ShutdownRequest, SidecarSpec,
    SidecarSupervisor,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, process::Command, sync::Mutex};

const BINARY_ENV: &str = "NORTHBRIDGE_REAL_AGENTGATEWAY";
const HASH_ENV: &str = "NORTHBRIDGE_REAL_AGENTGATEWAY_SHA256";
const MARKER: &str = "northbridge-loopback-v1.4.0";

#[derive(Clone, Default)]
struct UpstreamState {
    requests: Arc<Mutex<Vec<Value>>>,
}

async fn upstream_models() -> Json<Value> {
    Json(json!({
        "object": "list",
        "data": [{"id": "default", "object": "model"}]
    }))
}

async fn upstream_chat(
    State(state): State<UpstreamState>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    state.requests.lock().await.push(request);
    (
        [("content-type", "text/event-stream")],
        concat!(
            "data: {\"id\":\"chatcmpl-test\",\"object\":\"chat.completion.chunk\",",
            "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello through gateway\"}}]}\n\n",
            "data: [DONE]\n\n"
        ),
    )
}

#[tokio::test]
#[ignore = "requires the staged, patched agentgateway binary"]
async fn routes_an_authenticated_stream_through_the_real_gateway() {
    let executable = required_path(BINARY_ENV);
    let integrity = IntegrityPolicy::Sha256(
        std::env::var(HASH_ENV).unwrap_or_else(|_| panic!("{HASH_ENV} is required")),
    );
    assert_marker(&executable).await;

    let upstream_state = UpstreamState::default();
    let upstream = Router::new()
        .route("/v1/models", get(upstream_models))
        .route("/v1/chat/completions", post(upstream_chat))
        .with_state(upstream_state.clone());
    let upstream_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upstream_address = upstream_listener.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        axum::serve(upstream_listener, upstream).await.unwrap();
    });

    let ports = reserve_ports(3);
    let credential = GatewayCredential::parse(
        "nb-real-gateway-test-0000000000000000000000000000000000000000".to_string(),
    )
    .unwrap();
    let plan = AgentgatewayPlan {
        gateway_port: ports[0],
        admin_port: ports[1],
        readiness_port: ports[2],
        inference_base_url: format!("http://{upstream_address}/v1").parse().unwrap(),
        credential: credential.clone(),
        aliases: vec![ModelAlias {
            name: "fast-local".to_string(),
            upstream_model: "default".to_string(),
            apis: BTreeSet::from([ModelApi::Completions]),
        }],
        mcp_targets: Vec::new(),
        a2a_routes: Vec::new(),
        http_routes: Vec::new(),
    };
    let directory = tempfile::tempdir().unwrap();
    let artifact = write_new_config(directory.path(), &plan).await.unwrap();
    validate_config_with_binary(
        &executable,
        &artifact,
        &[credential.expose_secret().to_string()],
    )
    .await
    .unwrap();

    let supervisor = SidecarSupervisor::spawn(SidecarSpec {
        name: "real-agentgateway-test".to_string(),
        executable,
        args: vec![OsString::from("-f"), artifact.path.clone().into_os_string()],
        env: BTreeMap::new(),
        working_directory: Some(directory.path().to_path_buf()),
        readiness: HttpProbe::loopback(plan.readiness_url(), 200).unwrap(),
        shutdown: Some(ShutdownRequest::loopback(plan.shutdown_url(), HttpMethod::Post).unwrap()),
        startup_timeout: Duration::from_secs(30),
        probe_interval: Duration::from_millis(100),
        request_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(10),
        restart: RestartPolicy {
            max_restarts: 0,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(100),
        },
        integrity,
        log_capacity: 256,
        redactions: vec![credential.expose_secret().to_string()],
    })
    .unwrap();
    supervisor.ensure_started().await.unwrap();

    let client = GatewayClient::new(
        plan.gateway_base_url(),
        credential,
        ["fast-local".to_string()],
    )
    .unwrap();
    let models = client.list_models().await.unwrap();
    assert!(
        models["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["id"] == "fast-local")
    );

    let mut stream = client
        .stream_chat(json!({
            "model": "fast-local",
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .await
        .unwrap();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.unwrap());
    }
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("hello through gateway"));
    assert!(body.contains("data: [DONE]"));

    let requests = upstream_state.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["model"], "default");
    assert_eq!(requests[0]["stream"], true);
    drop(requests);

    supervisor.shutdown().await.unwrap();
    remove_config(&artifact).await.unwrap();
    assert!(!artifact.path.exists());
    upstream_task.abort();
}

fn required_path(name: &str) -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required for this test")),
    );
    assert!(path.is_absolute(), "{name} must be absolute");
    path
}

async fn assert_marker(executable: &std::path::Path) {
    let output = Command::new(executable).arg("-V").output().await.unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), MARKER);
}

fn reserve_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| PortListener::bind(("127.0.0.1", 0)).unwrap())
        .collect::<Vec<_>>();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}
