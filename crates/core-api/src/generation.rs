use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::StreamExt;
use northbridge_ai_gateway::LocalAiRuntime;
use northbridge_domain::{
    GenerationRun, GenerationStatus, KnowledgeSearchResult, Message, MessagePartKind, MessageRole,
};
use northbridge_retrieval::pack_citation_context;
use northbridge_storage::{Database, StorageError};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, Notify, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{error::ApiError, events::GenerationEvents};
use crate::{knowledge::KnowledgeRuntimeRegistry, runtime::AiRuntimeRegistry};

const MAX_ASSISTANT_TEXT_BYTES: usize = 4 * 1024 * 1024;
const MAX_GATEWAY_EVENT_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT_GENERATIONS: usize = 2;

#[derive(Clone)]
pub(crate) struct GenerationCoordinator {
    inner: Arc<GenerationCoordinatorInner>,
}

struct GenerationCoordinatorInner {
    database: Database,
    events: GenerationEvents,
    runtime: AiRuntimeRegistry,
    knowledge: KnowledgeRuntimeRegistry,
    active: Mutex<HashMap<Uuid, CancellationToken>>,
    idle: Notify,
    concurrency: Arc<Semaphore>,
}

impl GenerationCoordinator {
    pub(crate) fn new(
        database: Database,
        events: GenerationEvents,
        runtime: AiRuntimeRegistry,
        knowledge: KnowledgeRuntimeRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(GenerationCoordinatorInner {
                database,
                events,
                runtime,
                knowledge,
                active: Mutex::new(HashMap::new()),
                idle: Notify::new(),
                concurrency: Arc::new(Semaphore::new(MAX_CONCURRENT_GENERATIONS)),
            }),
        }
    }

    pub(crate) async fn start(&self, generation_id: Uuid) -> Result<GenerationRun, ApiError> {
        let run = self
            .inner
            .database
            .get_generation_run(generation_id)
            .await?;
        if run.input_message_id.is_none() {
            return Err(ApiError::invalid(
                "a generation requires at least one persisted input message",
            ));
        }
        let selected = selected_runtime_for_run(&self.inner.runtime, &run)?;
        let runtime = selected.runtime;
        if runtime.status().alias != run.model_alias {
            return Err(ApiError::invalid(format!(
                "model alias {:?} is not configured",
                run.model_alias
            )));
        }
        let cancellation = CancellationToken::new();
        {
            let mut active = self.inner.active.lock().await;
            if active.contains_key(&generation_id) {
                return Ok(run);
            }
            self.inner.events.start_run(generation_id).await?;
            active.insert(generation_id, cancellation.clone());
        }

        let coordinator = self.clone();
        tokio::spawn(async move {
            coordinator.run_generation(run, runtime, cancellation).await;
        });
        Ok(self
            .inner
            .database
            .get_generation_run(generation_id)
            .await?)
    }

    pub(crate) async fn cancel(&self, generation_id: Uuid) -> Result<GenerationRun, ApiError> {
        let run = self
            .inner
            .database
            .get_generation_run(generation_id)
            .await?;
        if run.status.is_terminal() {
            return Ok(run);
        }
        if let Some(cancellation) = self.inner.active.lock().await.get(&generation_id).cloned() {
            cancellation.cancel();
            return Ok(self
                .inner
                .database
                .get_generation_run(generation_id)
                .await?);
        }
        let run = self
            .inner
            .database
            .get_generation_run(generation_id)
            .await?;
        if run.status.is_terminal() {
            return Ok(run);
        }
        match self
            .inner
            .events
            .finish_without_output(
                generation_id,
                GenerationStatus::Cancelled,
                "cancelled",
                json!({ "reason": "user_requested" }),
            )
            .await
        {
            Ok(_) | Err(StorageError::Conflict(_)) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(self
            .inner
            .database
            .get_generation_run(generation_id)
            .await?)
    }

    pub(crate) async fn cancel_all_and_wait(&self, timeout: Duration) -> bool {
        {
            let active = self.inner.active.lock().await;
            for cancellation in active.values() {
                cancellation.cancel();
            }
            if active.is_empty() {
                return true;
            }
        }

        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.inner.idle.notified();
            if self.inner.active.lock().await.is_empty() {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    pub(crate) async fn has_active(&self) -> bool {
        !self.inner.active.lock().await.is_empty()
    }

    async fn run_generation(
        &self,
        run: GenerationRun,
        runtime: LocalAiRuntime,
        cancellation: CancellationToken,
    ) {
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            permit = self.inner.concurrency.clone().acquire_owned() => permit.ok(),
        };
        let result = match permit {
            Some(_permit) => self.execute_generation(&run, &runtime, &cancellation).await,
            None => Err("generation was cancelled".to_string()),
        };
        if let Err(message) = result {
            let finish_result = if cancellation.is_cancelled() {
                match self.persisted_assistant_text(run.id).await {
                    Ok(text) if !text.is_empty() => self
                        .inner
                        .events
                        .cancel_with_text(run.id, text)
                        .await
                        .map(|_| ()),
                    Ok(_) => self
                        .inner
                        .events
                        .finish_without_output(
                            run.id,
                            GenerationStatus::Cancelled,
                            "cancelled",
                            json!({ "reason": "user_requested" }),
                        )
                        .await
                        .map(|_| ()),
                    Err(error) => Err(error),
                }
            } else {
                self.inner
                    .events
                    .finish_without_output(
                        run.id,
                        GenerationStatus::Failed,
                        "failed",
                        json!({
                            "code": "generation_failed",
                            "message": bounded_error(message),
                        }),
                    )
                    .await
                    .map(|_| ())
            };
            if let Err(error) = finish_result
                && !matches!(error, StorageError::Conflict(_))
            {
                tracing::error!(generation_id = %run.id, %error, "failed to persist terminal generation state");
            }
        }
        self.inner.active.lock().await.remove(&run.id);
        self.inner.idle.notify_waiters();
    }

    async fn persisted_assistant_text(&self, generation_id: Uuid) -> Result<String, StorageError> {
        const PAGE_SIZE: u32 = 10_000;

        let mut after_sequence = 0;
        let mut output = String::new();
        loop {
            let events = self
                .inner
                .database
                .list_generation_events(generation_id, after_sequence, PAGE_SIZE)
                .await?;
            if events.is_empty() {
                break;
            }

            let page_is_full = events.len() == PAGE_SIZE as usize;
            after_sequence = events
                .last()
                .map(|event| event.sequence_number)
                .unwrap_or(after_sequence);
            for event in events {
                if event.event_type == "delta"
                    && let Some(text) = event.payload.get("text").and_then(Value::as_str)
                {
                    output.push_str(text);
                }
            }
            if !page_is_full {
                break;
            }
        }
        Ok(output)
    }

    async fn execute_generation(
        &self,
        run: &GenerationRun,
        runtime: &LocalAiRuntime,
        cancellation: &CancellationToken,
    ) -> Result<(), String> {
        let stored_messages = self
            .inner
            .database
            .list_messages(run.conversation_id, run.branch_id)
            .await
            .map_err(|error| error.to_string())?;
        let retrieval_query = latest_user_query(&stored_messages);
        let mut messages = request_messages(run, stored_messages)?;
        let retrieval = self
            .prepare_knowledge_context(run, retrieval_query.as_deref(), &mut messages)
            .await?;
        let input_characters = messages
            .iter()
            .filter_map(|message| message.get("content").and_then(Value::as_str))
            .fold(0_i64, |total, content| {
                total.saturating_add(i64::try_from(content.chars().count()).unwrap_or(i64::MAX))
            });
        self.inner
            .events
            .append(
                run.id,
                "request_metrics",
                json!({
                    "messageCount": messages.len(),
                    "inputCharacters": input_characters
                }),
            )
            .await
            .map_err(|error| error.to_string())?;
        let request = json!({
            "model": run.model_alias,
            "messages": messages,
            "stream": true,
        });
        let mut stream = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err("generation was cancelled".to_string()),
            result = runtime.stream_chat(request) => result.map_err(|error| error.to_string())?,
        };
        let mut decoder = GatewaySseDecoder::default();
        let mut assistant_text = String::new();

        loop {
            let chunk = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err("generation was cancelled".to_string()),
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            let chunk = chunk.map_err(|error| error.to_string())?;
            for data in decoder.push(&chunk)? {
                self.persist_gateway_event(run.id, &data, &mut assistant_text)
                    .await?;
            }
        }
        for data in decoder.finish()? {
            self.persist_gateway_event(run.id, &data, &mut assistant_text)
                .await?;
        }
        if cancellation.is_cancelled() {
            return Err("generation was cancelled".to_string());
        }
        if assistant_text.is_empty() {
            return Err("the model completed without text output".to_string());
        }
        match retrieval {
            Some(result) => {
                self.inner
                    .events
                    .complete_with_text_metadata(
                        run.id,
                        assistant_text,
                        json!({
                            "retrievalTraceId": result.trace_id,
                            "citations": result.hits,
                        }),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            None => {
                self.inner
                    .events
                    .complete_with_text(run.id, assistant_text)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        Ok(())
    }

    async fn prepare_knowledge_context(
        &self,
        run: &GenerationRun,
        query: Option<&str>,
        messages: &mut Vec<Value>,
    ) -> Result<Option<KnowledgeSearchResult>, String> {
        let stack_ids = self
            .inner
            .database
            .generation_knowledge_stacks(run.id)
            .await
            .map_err(|error| error.to_string())?;
        if stack_ids.is_empty() {
            return Ok(None);
        }
        let query = query.ok_or_else(|| {
            "knowledge retrieval requires a persisted user text message".to_string()
        })?;
        let services = self.inner.knowledge.current().ok_or_else(|| {
            "knowledge stacks were requested but the knowledge runtime is unavailable".to_string()
        })?;
        let conversation = self
            .inner
            .database
            .get_conversation(run.conversation_id)
            .await
            .map_err(|error| error.to_string())?;
        let result = services
            .retrieval
            .search(
                conversation.workspace_id,
                &stack_ids,
                query,
                8,
                Some(run.id),
            )
            .await
            .map_err(|error| error.to_string())?;
        self.inner
            .events
            .append(
                run.id,
                "retrieval",
                serde_json::to_value(&result).map_err(|error| error.to_string())?,
            )
            .await
            .map_err(|error| error.to_string())?;
        if !result.hits.is_empty() {
            messages.insert(
                0,
                json!({
                    "role": "system",
                    "content": pack_citation_context(&result, 24_000),
                }),
            );
        }
        Ok(Some(result))
    }

    async fn persist_gateway_event(
        &self,
        generation_id: Uuid,
        data: &str,
        assistant_text: &mut String,
    ) -> Result<(), String> {
        if data.trim() == "[DONE]" {
            return Ok(());
        }
        let payload: Value = serde_json::from_str(data)
            .map_err(|error| format!("gateway returned invalid SSE JSON: {error}"))?;
        let mut recognized = false;
        if let Some(content) = payload
            .pointer("/choices/0/delta/content")
            .and_then(Value::as_str)
            && !content.is_empty()
        {
            if assistant_text.len().saturating_add(content.len()) > MAX_ASSISTANT_TEXT_BYTES {
                return Err("assistant output exceeded 4 MiB".to_string());
            }
            assistant_text.push_str(content);
            self.inner
                .events
                .append(generation_id, "delta", json!({ "text": content }))
                .await
                .map_err(|error| error.to_string())?;
            recognized = true;
        }
        if let Some(reasoning) = payload
            .pointer("/choices/0/delta/reasoning_content")
            .or_else(|| payload.pointer("/choices/0/delta/reasoning"))
            .and_then(Value::as_str)
            && !reasoning.is_empty()
        {
            self.inner
                .events
                .append(
                    generation_id,
                    "reasoning_delta",
                    json!({ "text": reasoning }),
                )
                .await
                .map_err(|error| error.to_string())?;
            recognized = true;
        }
        if let Some(tool_calls) = payload.pointer("/choices/0/delta/tool_calls")
            && !tool_calls.is_null()
        {
            self.inner
                .events
                .append(generation_id, "tool_call_delta", tool_calls.clone())
                .await
                .map_err(|error| error.to_string())?;
            recognized = true;
        }
        if let Some(usage) = payload.get("usage")
            && !usage.is_null()
        {
            self.inner
                .events
                .append(generation_id, "usage", usage.clone())
                .await
                .map_err(|error| error.to_string())?;
            recognized = true;
        }
        if let Some(reason) = payload
            .pointer("/choices/0/finish_reason")
            .and_then(Value::as_str)
        {
            self.inner
                .events
                .append(generation_id, "finish_reason", json!({ "reason": reason }))
                .await
                .map_err(|error| error.to_string())?;
            recognized = true;
        }
        if !recognized {
            self.inner
                .events
                .append(generation_id, "provider_event", payload)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn latest_user_query(messages: &[Message]) -> Option<String> {
    messages
        .iter()
        .rev()
        .find(|message| message.role == MessageRole::User)
        .map(|message| {
            message
                .parts
                .iter()
                .filter(|part| part.kind == MessagePartKind::Text)
                .map(|part| part.content.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|content| !content.trim().is_empty())
}

fn selected_runtime_for_run(
    registry: &AiRuntimeRegistry,
    run: &GenerationRun,
) -> Result<crate::runtime::SelectedAiRuntime, ApiError> {
    let selected = match (&run.provider, &run.model_id, &run.load_profile) {
        (Some(provider), Some(model_id), Some(load_profile)) => {
            registry.for_selection(&northbridge_domain::ModelSelection {
                provider: *provider,
                model_id: model_id.clone(),
                model_alias: run.model_alias.clone(),
                load_profile: load_profile.clone(),
            })
        }
        (None, None, None) => registry.current(),
        _ => None,
    };
    selected.ok_or_else(|| {
        ApiError::unavailable(
            "generation_model_not_loaded",
            "load the model and load profile recorded for this generation before resuming it",
        )
    })
}

fn request_messages(run: &GenerationRun, mut messages: Vec<Message>) -> Result<Vec<Value>, String> {
    let input_message_id = run
        .input_message_id
        .ok_or_else(|| "generation does not have an input message".to_string())?;
    let input_index = messages
        .iter()
        .position(|message| message.id == input_message_id)
        .ok_or_else(|| "generation input message is missing from its branch".to_string())?;
    messages.truncate(input_index + 1);
    messages
        .into_iter()
        .map(|message| {
            let content = message
                .parts
                .iter()
                .filter(|part| part.kind == MessagePartKind::Text)
                .map(|part| part.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            if content.is_empty() {
                return Err(format!(
                    "message {} does not contain supported text content",
                    message.id
                ));
            }
            Ok(json!({
                "role": role_name(message.role),
                "content": content,
            }))
        })
        .collect()
}

const fn role_name(role: MessageRole) -> &'static str {
    role.as_str()
}

fn bounded_error(mut message: String) -> String {
    if message.len() > 4096 {
        let mut boundary = 4096;
        while !message.is_char_boundary(boundary) {
            boundary -= 1;
        }
        message.truncate(boundary);
        message.push_str("...");
    }
    message
}

#[derive(Default)]
struct GatewaySseDecoder {
    buffer: Vec<u8>,
    data_lines: Vec<String>,
    pending_bytes: usize,
}

impl GatewaySseDecoder {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, String> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let remainder = self.buffer.split_off(newline + 1);
            let mut line = std::mem::replace(&mut self.buffer, remainder);
            line.truncate(newline);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.parse_line(&line, &mut events)?;
        }
        if self.buffer.len().saturating_add(self.pending_bytes) > MAX_GATEWAY_EVENT_BYTES {
            return Err("gateway SSE event exceeded 1 MiB".to_string());
        }
        Ok(events)
    }

    fn finish(&mut self) -> Result<Vec<String>, String> {
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            let mut line = std::mem::take(&mut self.buffer);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.parse_line(&line, &mut events)?;
        }
        self.parse_line(b"", &mut events)?;
        Ok(events)
    }

    fn parse_line(&mut self, line: &[u8], events: &mut Vec<String>) -> Result<(), String> {
        let line = std::str::from_utf8(line)
            .map_err(|error| format!("gateway SSE was not valid UTF-8: {error}"))?;
        if line.is_empty() {
            if !self.data_lines.is_empty() {
                events.push(self.data_lines.join("\n"));
                self.data_lines.clear();
                self.pending_bytes = 0;
            }
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, mut value) = line.split_once(':').unwrap_or((line, ""));
        if let Some(stripped) = value.strip_prefix(' ') {
            value = stripped;
        }
        if field == "data" {
            self.pending_bytes = self.pending_bytes.saturating_add(value.len());
            if self.pending_bytes > MAX_GATEWAY_EVENT_BYTES {
                return Err("gateway SSE event exceeded 1 MiB".to_string());
            }
            self.data_lines.push(value.to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{GatewaySseDecoder, bounded_error};

    #[test]
    fn decodes_sse_across_arbitrary_chunk_boundaries() {
        let mut decoder = GatewaySseDecoder::default();
        assert!(
            decoder
                .push(b"data: {\"choices\":[{\"del")
                .unwrap()
                .is_empty()
        );
        let events = decoder
            .push(b"ta\":{\"content\":\"hello\"}}]}\r\n\r\ndata: [DONE]\n\n")
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].contains("hello"));
        assert_eq!(events[1], "[DONE]");
    }

    #[test]
    fn joins_multiline_data_and_bounds_errors() {
        let mut decoder = GatewaySseDecoder::default();
        let events = decoder.push(b"data: one\ndata: two\n\n").unwrap();
        assert_eq!(events, ["one\ntwo"]);
        assert!(bounded_error("x".repeat(5000)).ends_with("..."));
        let multibyte = bounded_error("é".repeat(3000));
        assert!(multibyte.ends_with("..."));
        assert!(multibyte.len() <= 4099);
    }
}
