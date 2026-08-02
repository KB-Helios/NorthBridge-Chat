use std::{
    env,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use northbridge_domain::ReadyEvent;
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

const SESSION_TOKEN: &str = "real-ai-session-token-with-at-least-32-bytes";
const CONTROL_TOKEN: &str = "real-ai-control-token-with-at-least-32-bytes";
const MODEL_ALIAS: &str = "fast-local";
const GENERATION_TIMEOUT: Duration = Duration::from_secs(15 * 60);

struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires staged agentgateway and mistral.rs binaries plus a model download"]
async fn routes_a_real_local_generation_and_persists_the_result() -> Result<()> {
    ensure!(
        env::var("NORTHBRIDGE_RUN_REAL_AI_TEST").as_deref() == Ok("1"),
        "run this test through `npm run desktop:test:local-ai`"
    );
    let directory = tempfile::tempdir().context("failed to create real-AI test directory")?;
    let runtime_directory = directory.path().join("runtime");
    let database_path = directory.path().join("real-ai.sqlite");
    std::fs::create_dir_all(&runtime_directory)?;
    let cache_directory = required_path("NORTHBRIDGE_REAL_AI_CACHE")?;
    std::fs::create_dir_all(&cache_directory)?;

    let mut process = spawn_core(&database_path, &runtime_directory, &cache_directory)?;
    let ready = read_ready(&mut process)?;
    let base_url = Url::parse(&ready.address)?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;

    let knowledge_file = directory.path().join("bridge-runbook.md");
    let flow_result = exercise_generation(&client, &base_url, &knowledge_file).await;
    let shutdown_result = shutdown_core(&client, &base_url, &mut process).await;
    let cleanup_result = verify_runtime_cleanup(&runtime_directory);

    shutdown_result.context("desktop-core did not shut down cleanly after the real-AI test")?;
    cleanup_result.context("desktop-core left sensitive runtime artifacts after shutdown")?;
    flow_result?;

    let mut restarted = spawn_core(&database_path, &runtime_directory, &cache_directory)?;
    let ready = read_ready(&mut restarted)?;
    let base_url = Url::parse(&ready.address)?;
    let restored = api_json(&client, &base_url, Method::GET, "/v1/models/catalog", None).await?;
    ensure!(
        restored["selection"]["modelId"] == selected_model_id(),
        "desktop-core did not restore the persisted model selection: {restored}"
    );
    ensure!(restored["selection"]["loadProfile"]["quantization"] == "none");
    let status = api_json(&client, &base_url, Method::GET, "/v1/ai/status", None).await?;
    ensure!(status["configured"] == true);
    shutdown_core(&client, &base_url, &mut restarted).await?;
    verify_runtime_cleanup(&runtime_directory)?;
    Ok(())
}

async fn exercise_generation(client: &Client, base_url: &Url, knowledge_file: &Path) -> Result<()> {
    let diagnostics = api_json(
        client,
        base_url,
        Method::GET,
        "/v1/hardware/diagnostics",
        None,
    )
    .await?;
    ensure!(diagnostics["system"]["build"]["version"] == "0.9.0");
    ensure!(diagnostics["system"]["cpu"]["logicalCores"].is_number());
    ensure!(
        diagnostics["system"]["devices"]
            .as_array()
            .is_some_and(|devices| devices.iter().any(|device| device["kind"] == "cpu"))
    );

    let catalog = api_json(client, base_url, Method::GET, "/v1/models/catalog", None).await?;
    ensure!(catalog["selection"].is_null());
    ensure!(catalog["models"].as_array().is_some_and(|models| {
        models
            .iter()
            .any(|model| model["modelId"] == selected_model_id())
    }));
    let selected = api_json(
        client,
        base_url,
        Method::POST,
        "/v1/models/selection",
        Some(json!({
            "modelId": selected_model_id(),
            "loadProfile": {
                "quantization": "none",
                "maxSequenceLength": 8192,
                "maxBatchSize": 1,
                "forceCpu": true
            }
        })),
    )
    .await?;
    ensure!(selected["selection"]["modelId"] == selected_model_id());
    ensure!(selected["selection"]["loadProfile"]["quantization"] == "none");

    let status = api_json(client, base_url, Method::GET, "/v1/ai/status", None).await?;
    ensure!(
        status["configured"] == true,
        "local AI runtime was not configured"
    );
    ensure!(status["runtime"]["alias"] == MODEL_ALIAS);

    let knowledge_stack_id = exercise_knowledge_ingestion(client, base_url, knowledge_file).await?;

    let conversation = api_json(
        client,
        base_url,
        Method::POST,
        "/v1/conversations",
        Some(json!({ "title": "Real local AI acceptance" })),
    )
    .await?;
    let conversation_id = required_json_string(&conversation, "id")?;
    let branch_id = required_json_string(&conversation, "defaultBranchId")?;

    let input_message = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/conversations/{conversation_id}/messages"),
        Some(json!({
            "branchId": branch_id,
            "parentMessageId": null,
            "role": "user",
            "parts": [{
                "kind": "text",
                "content": "Reply with exactly one short word: bridge"
            }]
        })),
    )
    .await?;
    let input_message_id = required_json_string(&input_message, "id")?;
    let generation = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/conversations/{conversation_id}/generations"),
        Some(json!({
            "branchId": branch_id,
            "modelAlias": MODEL_ALIAS,
            "clientRequestId": "real-ai-acceptance-1",
            "knowledgeStackIds": [knowledge_stack_id]
        })),
    )
    .await?;
    let generation_id = required_json_string(&generation, "id")?;
    ensure!(generation["provider"] == "local_mistral_rs");
    ensure!(generation["modelId"] == selected_model_id());
    ensure!(generation["loadProfile"]["quantization"] == "none");

    api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/generations/{generation_id}/start"),
        None,
    )
    .await?;

    let deadline = Instant::now() + GENERATION_TIMEOUT;
    let final_run = loop {
        let run = api_json(
            client,
            base_url,
            Method::GET,
            &format!("/v1/generations/{generation_id}"),
            None,
        )
        .await?;
        match run["status"].as_str() {
            Some("completed") => break run,
            Some("failed" | "cancelled") => {
                let events = api_text(
                    client,
                    base_url,
                    &format!("/v1/generations/{generation_id}/events?after=0"),
                )
                .await
                .unwrap_or_else(|error| format!("failed to read events: {error:#}"));
                bail!(
                    "real local generation ended in state {}: {run}; events: {events}",
                    run["status"],
                )
            }
            Some("queued" | "running") => {}
            status => bail!("real local generation returned an invalid status: {status:?}"),
        }
        ensure!(
            Instant::now() < deadline,
            "real local generation did not finish within {GENERATION_TIMEOUT:?}"
        );
        sleep(Duration::from_millis(500)).await;
    };
    ensure!(final_run["outputMessageId"].is_string());

    let messages = api_json(
        client,
        base_url,
        Method::GET,
        &format!("/v1/conversations/{conversation_id}/messages?branch_id={branch_id}"),
        None,
    )
    .await?;
    let messages = messages
        .as_array()
        .context("message response was not an array")?;
    let assistant = messages
        .iter()
        .rev()
        .find(|message| message["role"] == "assistant")
        .context("completed generation did not persist an assistant message")?;
    let text = assistant["parts"]
        .as_array()
        .and_then(|parts| parts.first())
        .and_then(|part| part["content"].as_str())
        .context("persisted assistant message did not contain text")?;
    ensure!(
        !text.trim().is_empty(),
        "persisted assistant text was empty"
    );
    let metadata = &assistant["parts"][0]["metadata"];
    ensure!(
        metadata["retrievalTraceId"].is_string(),
        "grounded assistant message omitted its retrieval trace: {assistant}"
    );
    ensure!(
        metadata["citations"]
            .as_array()
            .is_some_and(|citations| !citations.is_empty()),
        "grounded assistant message omitted citations: {assistant}"
    );
    let grounded_events = api_text(
        client,
        base_url,
        &format!("/v1/generations/{generation_id}/events?after=0"),
    )
    .await?;
    ensure!(
        grounded_events.contains("event: retrieval"),
        "grounded generation did not persist a retrieval event: {grounded_events}"
    );
    println!("real local assistant response: {text:?}");

    exercise_split_chat(
        client,
        base_url,
        conversation_id,
        branch_id,
        input_message_id,
    )
    .await?;
    exercise_independent_split_cancellation(
        client,
        base_url,
        conversation_id,
        branch_id,
        required_json_string(&final_run, "outputMessageId")?,
    )
    .await?;
    Ok(())
}

async fn exercise_knowledge_ingestion(
    client: &Client,
    base_url: &Url,
    knowledge_file: &Path,
) -> Result<String> {
    std::fs::write(
        knowledge_file,
        "# Bridge acceptance\n\nThe exact local acceptance word is bridge. Cite this runbook when it is relevant.",
    )?;
    let stack = api_json(
        client,
        base_url,
        Method::POST,
        "/v1/knowledge/stacks",
        Some(json!({
            "name": "Real embedding acceptance",
            "description": "Exercises the shipped local embedding boundary"
        })),
    )
    .await?;
    let stack_id = required_json_string(&stack, "id")?.to_string();
    ensure!(stack["embeddingModel"] == "Qwen/Qwen3-Embedding-0.6B");
    ensure!(stack["embeddingDimensions"] == 1024);
    let jobs = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/knowledge/stacks/{stack_id}/documents/import"),
        Some(json!({ "paths": [knowledge_file.to_string_lossy()] })),
    )
    .await?;
    let job_id = jobs
        .as_array()
        .and_then(|jobs| jobs.first())
        .and_then(|job| job["id"].as_str())
        .context("knowledge import did not return a job ID")?;
    let deadline = Instant::now() + GENERATION_TIMEOUT;
    loop {
        let job = api_json(
            client,
            base_url,
            Method::GET,
            &format!("/v1/knowledge/jobs/{job_id}"),
            None,
        )
        .await?;
        match job["status"].as_str() {
            Some("completed") => break,
            Some("failed" | "cancelled") => bail!("real knowledge ingestion failed: {job}"),
            Some("queued" | "running") => {}
            status => bail!("real knowledge ingestion returned an invalid status: {status:?}"),
        }
        ensure!(
            Instant::now() < deadline,
            "real knowledge ingestion did not finish within {GENERATION_TIMEOUT:?}"
        );
        sleep(Duration::from_millis(500)).await;
    }
    let search = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/knowledge/stacks/{stack_id}/search"),
        Some(json!({ "query": "exact local acceptance word", "limit": 8 })),
    )
    .await?;
    ensure!(
        search["hits"]
            .as_array()
            .is_some_and(|hits| hits.first().is_some_and(|hit| hit["citation"] == "[K1]")),
        "real hybrid retrieval returned no cited result: {search}"
    );
    ensure!(
        search["denseCandidateCount"]
            .as_u64()
            .is_some_and(|count| count > 0)
    );
    let index = api_json(
        client,
        base_url,
        Method::GET,
        &format!("/v1/knowledge/stacks/{stack_id}/index"),
        None,
    )
    .await?;
    ensure!(index["recordCount"].as_u64().is_some_and(|count| count > 0));
    ensure!(index["bitWidth"] == 4);
    println!("real local embedding ingestion and hybrid retrieval completed");
    Ok(stack_id)
}

async fn exercise_split_chat(
    client: &Client,
    base_url: &Url,
    conversation_id: &str,
    source_branch_id: &str,
    input_message_id: &str,
) -> Result<()> {
    let split = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/conversations/{conversation_id}/split-chats"),
        Some(json!({
            "sourceBranchId": source_branch_id,
            "inputMessageId": input_message_id,
            "clientRequestId": "real-split-acceptance-1",
            "candidates": [
                {
                    "modelId": selected_model_id(),
                    "loadProfile": {
                        "quantization": "none",
                        "maxSequenceLength": 8192,
                        "maxBatchSize": 1,
                        "forceCpu": true
                    }
                },
                {
                    "modelId": selected_model_id(),
                    "loadProfile": {
                        "quantization": "none",
                        "maxSequenceLength": 4096,
                        "maxBatchSize": 1,
                        "forceCpu": true
                    }
                }
            ]
        })),
    )
    .await?;
    let split_id = required_json_string(&split, "id")?;
    let lanes = split["lanes"]
        .as_array()
        .context("split-chat response did not contain lanes")?;
    ensure!(
        lanes.len() == 2,
        "split chat did not create two lanes: {split}"
    );
    ensure!(lanes[0]["branchId"] != lanes[1]["branchId"]);
    ensure!(lanes.iter().all(|lane| {
        lane["generation"]["inputMessageId"] == input_message_id
            && lane["generation"]["modelId"] == selected_model_id()
    }));

    let deadline = Instant::now() + GENERATION_TIMEOUT;
    let completed = loop {
        let current = api_json(
            client,
            base_url,
            Method::GET,
            &format!("/v1/split-chats/{split_id}"),
            None,
        )
        .await?;
        let lanes = current["lanes"]
            .as_array()
            .context("persisted split chat did not contain lanes")?;
        if lanes
            .iter()
            .all(|lane| lane["generation"]["status"] == "completed")
        {
            break current;
        }
        if let Some(failed) = lanes.iter().find(|lane| {
            matches!(
                lane["generation"]["status"].as_str(),
                Some("failed" | "cancelled")
            )
        }) {
            bail!("real split-chat lane failed: {failed}");
        }
        ensure!(
            Instant::now() < deadline,
            "real split chat did not finish within {GENERATION_TIMEOUT:?}"
        );
        sleep(Duration::from_millis(500)).await;
    };

    for lane in completed["lanes"]
        .as_array()
        .context("completed split chat did not contain lanes")?
    {
        ensure!(lane["generation"]["outputMessageId"].is_string());
        let generation_id = required_json_string(&lane["generation"], "id")?;
        let events = api_text(
            client,
            base_url,
            &format!("/v1/generations/{generation_id}/events?after=0"),
        )
        .await?;
        ensure!(
            events.contains("event: delta"),
            "split lane emitted no deltas"
        );
        ensure!(
            events.contains("event: completed"),
            "split lane emitted no completed event"
        );
        let branch_id = required_json_string(lane, "branchId")?;
        let messages = api_json(
            client,
            base_url,
            Method::GET,
            &format!("/v1/conversations/{conversation_id}/messages?branchId={branch_id}"),
            None,
        )
        .await?;
        ensure!(
            messages.as_array().is_some_and(|messages| {
                messages
                    .iter()
                    .any(|message| message["role"] == "assistant")
            }),
            "split lane did not persist an assistant message: {messages}"
        );
    }
    let restored = api_json(
        client,
        base_url,
        Method::GET,
        &format!("/v1/conversations/{conversation_id}/split-chats"),
        None,
    )
    .await?;
    ensure!(
        restored.as_array().is_some_and(|runs| runs.len() == 1),
        "split chat was not durably listed: {restored}"
    );
    println!("real split chat completed two independent persisted lanes");
    Ok(())
}

async fn exercise_independent_split_cancellation(
    client: &Client,
    base_url: &Url,
    conversation_id: &str,
    source_branch_id: &str,
    parent_message_id: &str,
) -> Result<()> {
    let input = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/conversations/{conversation_id}/messages"),
        Some(json!({
            "branchId": source_branch_id,
            "parentMessageId": parent_message_id,
            "role": "user",
            "parts": [{
                "kind": "text",
                "content": "Write a detailed forty-word explanation of a suspension bridge, then end with the word done."
            }]
        })),
    )
    .await?;
    let input_message_id = required_json_string(&input, "id")?;
    let split = api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/conversations/{conversation_id}/split-chats"),
        Some(json!({
            "sourceBranchId": source_branch_id,
            "inputMessageId": input_message_id,
            "clientRequestId": "real-split-cancel-1",
            "candidates": [
                {
                    "modelId": selected_model_id(),
                    "loadProfile": {
                        "quantization": "none",
                        "maxSequenceLength": 8192,
                        "maxBatchSize": 1,
                        "forceCpu": true
                    }
                },
                {
                    "modelId": selected_model_id(),
                    "loadProfile": {
                        "quantization": "none",
                        "maxSequenceLength": 4096,
                        "maxBatchSize": 1,
                        "forceCpu": true
                    }
                }
            ]
        })),
    )
    .await?;
    let split_id = required_json_string(&split, "id")?;
    let lanes = split["lanes"]
        .as_array()
        .context("cancellation split did not contain lanes")?;
    let cancelled_generation_id = required_json_string(&lanes[0]["generation"], "id")?;
    api_json(
        client,
        base_url,
        Method::POST,
        &format!("/v1/generations/{cancelled_generation_id}/cancel"),
        None,
    )
    .await?;

    let deadline = Instant::now() + GENERATION_TIMEOUT;
    let terminal = loop {
        let current = api_json(
            client,
            base_url,
            Method::GET,
            &format!("/v1/split-chats/{split_id}"),
            None,
        )
        .await?;
        let lanes = current["lanes"]
            .as_array()
            .context("cancellation split lost its lanes")?;
        if lanes.iter().all(|lane| {
            matches!(
                lane["generation"]["status"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        }) {
            break current;
        }
        ensure!(
            Instant::now() < deadline,
            "independent split cancellation did not settle within {GENERATION_TIMEOUT:?}"
        );
        sleep(Duration::from_millis(250)).await;
    };
    let lanes = terminal["lanes"]
        .as_array()
        .context("terminal cancellation split lost its lanes")?;
    ensure!(
        lanes[0]["generation"]["status"] == "cancelled",
        "cancelled split lane reached the wrong state: {}",
        lanes[0]
    );
    ensure!(
        lanes[1]["generation"]["status"] == "completed",
        "uncancelled split lane did not complete independently: {}",
        lanes[1]
    );
    let cancelled_events = api_text(
        client,
        base_url,
        &format!("/v1/generations/{cancelled_generation_id}/events?after=0"),
    )
    .await?;
    ensure!(cancelled_events.contains("event: cancelled"));
    println!("real split chat cancelled one lane while the other completed");
    Ok(())
}

async fn api_text(client: &Client, base_url: &Url, path: &str) -> Result<String> {
    let url = base_url.join(path.trim_start_matches('/'))?;
    let mut response = client.get(url).bearer_auth(SESSION_TOKEN).send().await?;
    ensure!(
        response.status().is_success(),
        "desktop-core returned {} while reading {path}",
        response.status()
    );
    let mut output = String::new();
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(5), response.chunk())
        .await
        .context("timed out reading generation event history")??
    {
        output.push_str(&String::from_utf8_lossy(&chunk));
        if output.contains("event: completed\n")
            || output.contains("event: failed\n")
            || output.contains("event: cancelled\n")
        {
            break;
        }
    }
    Ok(output)
}

fn spawn_core(
    database_path: &Path,
    runtime_directory: &Path,
    cache_directory: &Path,
) -> Result<ProcessGuard> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_desktop-core"));
    command
        .arg("--database")
        .arg(database_path)
        .env("NORTHBRIDGE_SESSION_TOKEN", SESSION_TOKEN)
        .env("NORTHBRIDGE_CONTROL_TOKEN", CONTROL_TOKEN)
        .env("NORTHBRIDGE_AI_RUNTIME_DIRECTORY", runtime_directory)
        .env("NORTHBRIDGE_HF_CACHE", cache_directory)
        .env(
            "NORTHBRIDGE_AGENTGATEWAY_PATH",
            required_path("NORTHBRIDGE_REAL_AGENTGATEWAY")?,
        )
        .env(
            "NORTHBRIDGE_AGENTGATEWAY_SHA256",
            env::var("NORTHBRIDGE_REAL_AGENTGATEWAY_SHA256")?,
        )
        .env(
            "NORTHBRIDGE_AGENTGATEWAY_PROFILE",
            "northbridge-loopback-v1",
        )
        .env(
            "NORTHBRIDGE_MISTRALRS_PATH",
            required_path("NORTHBRIDGE_REAL_MISTRALRS")?,
        )
        .env(
            "NORTHBRIDGE_MISTRALRS_SHA256",
            env::var("NORTHBRIDGE_REAL_MISTRALRS_SHA256")?,
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    Ok(ProcessGuard(command.spawn()?))
}

fn selected_model_id() -> String {
    env::var("NORTHBRIDGE_REAL_AI_MODEL")
        .unwrap_or_else(|_| "HuggingFaceTB/SmolLM2-135M-Instruct".to_string())
}

fn read_ready(process: &mut ProcessGuard) -> Result<ReadyEvent> {
    let stdout = process
        .0
        .stdout
        .take()
        .context("desktop-core stdout missing")?;
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .context("failed to read desktop-core ready event")?;
    ensure!(
        !line.is_empty(),
        "desktop-core exited before its ready event"
    );
    serde_json::from_str(&line).context("desktop-core emitted an invalid ready event")
}

async fn shutdown_core(client: &Client, base_url: &Url, process: &mut ProcessGuard) -> Result<()> {
    let endpoint = base_url.join("v1/runtime/shutdown")?;
    let response = client
        .post(endpoint)
        .bearer_auth(SESSION_TOKEN)
        .header("x-northbridge-control-token", CONTROL_TOKEN)
        .send()
        .await?;
    ensure!(response.status() == StatusCode::ACCEPTED);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = process.0.try_wait()? {
            ensure!(status.success(), "desktop-core exited with {status}");
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "desktop-core did not exit in time"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn api_json(
    client: &Client,
    base_url: &Url,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<Value> {
    let endpoint = base_url.join(path.trim_start_matches('/'))?;
    let mut request = client.request(method, endpoint).bearer_auth(SESSION_TOKEN);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    ensure!(
        status.is_success(),
        "desktop API returned {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).context("desktop API returned invalid JSON")
}

fn required_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(env::var_os(name).with_context(|| format!("{name} is required"))?);
    ensure!(path.is_absolute(), "{name} must be an absolute path");
    Ok(path)
}

fn required_json_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .with_context(|| format!("desktop API response omitted {field}: {value}"))
}

fn verify_runtime_cleanup(runtime_directory: &Path) -> Result<()> {
    let leftovers = std::fs::read_dir(runtime_directory)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect::<Vec<_>>();
    ensure!(
        leftovers.is_empty(),
        "ephemeral gateway config was not removed: {leftovers:?}"
    );
    Ok(())
}
