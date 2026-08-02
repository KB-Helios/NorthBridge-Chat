use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::STANDARD};
use northbridge_ai_gateway::GatewayHttpRoute;
use northbridge_domain::{
    ArtifactKind, CredentialReference, DEFAULT_WORKSPACE_ID, MediaJob, MediaJobStatus,
    MediaModality, credential_environment_key,
};
use northbridge_storage::{Database, NewArtifact, StorageError};
use reqwest::{Client, redirect::Policy};
use serde_json::{Value, json};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::runtime::AiRuntimeRegistry;

const MAX_MEDIA_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CredentialStoreError {
    #[error("credential store operation failed: {0}")]
    Store(String),
    #[error("credential operation task failed: {0}")]
    Task(String),
}

pub async fn store_credential_secret(
    reference: &CredentialReference,
    secret: String,
) -> Result<(), CredentialStoreError> {
    if secret.is_empty() || secret.len() > 64 * 1024 {
        return Err(CredentialStoreError::Store(
            "credential secret must contain between 1 byte and 64 KiB".to_string(),
        ));
    }
    let service = reference.service.clone();
    let account = reference.account.clone();
    tokio::task::spawn_blocking(move || {
        keyring::Entry::new(&service, &account)
            .and_then(|entry| entry.set_password(&secret))
            .map_err(|error| CredentialStoreError::Store(error.to_string()))
    })
    .await
    .map_err(|error| CredentialStoreError::Task(error.to_string()))?
}

pub async fn load_credential_secret(
    reference: &CredentialReference,
) -> Result<String, CredentialStoreError> {
    let service = reference.service.clone();
    let account = reference.account.clone();
    tokio::task::spawn_blocking(move || {
        keyring::Entry::new(&service, &account)
            .and_then(|entry| entry.get_password())
            .map_err(|error| CredentialStoreError::Store(error.to_string()))
    })
    .await
    .map_err(|error| CredentialStoreError::Task(error.to_string()))?
}

pub async fn delete_credential_secret(
    reference: &CredentialReference,
) -> Result<(), CredentialStoreError> {
    let service = reference.service.clone();
    let account = reference.account.clone();
    tokio::task::spawn_blocking(move || {
        keyring::Entry::new(&service, &account)
            .and_then(|entry| entry.delete_credential())
            .map_err(|error| CredentialStoreError::Store(error.to_string()))
    })
    .await
    .map_err(|error| CredentialStoreError::Task(error.to_string()))?
}

#[derive(Clone)]
pub(crate) struct MediaCoordinator {
    inner: Arc<MediaInner>,
}

struct MediaInner {
    database: Database,
    ai: AiRuntimeRegistry,
    client: Client,
    tasks: Mutex<BTreeMap<Uuid, MediaTask>>,
    shutdown: CancellationToken,
}

struct MediaTask {
    cancellation: CancellationToken,
    handle: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MediaError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("local AI gateway is unavailable")]
    GatewayUnavailable,
    #[error("media gateway configuration failed: {0}")]
    Gateway(String),
    #[error("media provider request failed: {0}")]
    Provider(String),
    #[error("media job was cancelled")]
    Cancelled,
}

impl MediaCoordinator {
    pub(crate) fn new(database: Database, ai: AiRuntimeRegistry) -> Self {
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(10 * 60))
            .build()
            .expect("fixed media HTTP client configuration is valid");
        Self {
            inner: Arc::new(MediaInner {
                database,
                ai,
                client,
                tasks: Mutex::new(BTreeMap::new()),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub(crate) async fn synchronize_gateway_routes(&self) -> Result<(), MediaError> {
        let selected = self
            .inner
            .ai
            .current()
            .ok_or(MediaError::GatewayUnavailable)?;
        let workspace_id = Uuid::parse_str(DEFAULT_WORKSPACE_ID)
            .map_err(|error| MediaError::Gateway(error.to_string()))?;
        let routes = self
            .inner
            .database
            .list_media_providers(workspace_id)
            .await?
            .into_iter()
            .filter(|provider| provider.enabled)
            .map(|provider| {
                Ok(GatewayHttpRoute {
                    name: format!("media-{}", provider.id.simple()),
                    gateway_path: provider.gateway_path,
                    upstream: provider.upstream_url.parse().map_err(|error| {
                        MediaError::Gateway(format!("stored provider URL is invalid: {error}"))
                    })?,
                    auth_environment_key: provider.credential_id.map(credential_environment_key),
                })
            })
            .collect::<Result<Vec<_>, MediaError>>()?;
        selected
            .runtime
            .reconfigure_http_routes(routes)
            .await
            .map_err(|error| MediaError::Gateway(error.to_string()))
    }

    pub(crate) async fn start_job(&self, job: MediaJob) -> Result<MediaJob, MediaError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(MediaError::Gateway(
                "media runtime is shutting down".to_string(),
            ));
        }
        self.synchronize_gateway_routes().await?;
        let cancellation = self.inner.shutdown.child_token();
        let task_cancellation = cancellation.clone();
        let coordinator = self.clone();
        let id = job.id;
        let mut tasks = self.inner.tasks.lock().await;
        let handle = tokio::spawn(async move {
            if let Err(error) = coordinator.execute_job(id, task_cancellation).await {
                let status = if matches!(error, MediaError::Cancelled) {
                    MediaJobStatus::Cancelled
                } else {
                    MediaJobStatus::Failed
                };
                let _ = coordinator
                    .inner
                    .database
                    .set_media_job_state(id, status, None, None, Some(&error.to_string()))
                    .await;
            }
            coordinator.inner.tasks.lock().await.remove(&id);
        });
        tasks.insert(
            id,
            MediaTask {
                cancellation,
                handle,
            },
        );
        drop(tasks);
        Ok(job)
    }

    pub(crate) async fn cancel_job(&self, id: Uuid) -> Result<MediaJob, MediaError> {
        if let Some(task) = self.inner.tasks.lock().await.get(&id) {
            task.cancellation.cancel();
        }
        Ok(self
            .inner
            .database
            .set_media_job_state(id, MediaJobStatus::Cancelled, None, None, None)
            .await?)
    }

    pub(crate) async fn recover(&self) -> Result<u64, MediaError> {
        Ok(self.inner.database.recover_interrupted_media_jobs().await?)
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.shutdown.cancel();
        let tasks = {
            let mut tasks = self.inner.tasks.lock().await;
            for task in tasks.values() {
                task.cancellation.cancel();
            }
            std::mem::take(&mut *tasks)
        };
        tokio::time::timeout(timeout, async move {
            for (_, task) in tasks {
                let _ = task.handle.await;
            }
        })
        .await
        .is_ok()
    }

    async fn execute_job(
        &self,
        id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), MediaError> {
        let job = self.inner.database.media_job(id).await?;
        let provider = self.inner.database.media_provider(job.provider_id).await?;
        self.inner
            .database
            .set_media_job_state(id, MediaJobStatus::Running, None, None, None)
            .await?;
        let selected = self
            .inner
            .ai
            .current()
            .ok_or(MediaError::GatewayUnavailable)?;
        let access = selected.runtime.agentgateway_access();
        let endpoint = access
            .base_url
            .join(provider.gateway_path.trim_start_matches('/'))
            .map_err(|error| MediaError::Gateway(error.to_string()))?;
        let mut response = tokio::select! {
            () = cancellation.cancelled() => return Err(MediaError::Cancelled),
            response = self.inner.client
                .post(endpoint)
                .bearer_auth(access.credential.expose_secret())
                .json(&job.request)
                .send() => response.map_err(|error| MediaError::Provider(error.to_string()))?,
        };
        let status = response.status();
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string());
        let bytes =
            read_bounded_response_body(&mut response, &cancellation, MAX_MEDIA_RESPONSE_BYTES)
                .await?;
        if !status.is_success() {
            let detail = String::from_utf8_lossy(&bytes);
            return Err(MediaError::Provider(format!(
                "provider returned {status}: {}",
                detail.chars().take(500).collect::<String>()
            )));
        }
        let (content, response_json, content_type) = if media_type
            .as_deref()
            .is_some_and(|value| value == "application/json" || value.ends_with("+json"))
        {
            let value: Value = serde_json::from_slice(&bytes)
                .map_err(|error| MediaError::Provider(format!("invalid JSON response: {error}")))?;
            let encoded = find_base64_payload(&value).ok_or_else(|| {
                MediaError::Provider(
                    "provider JSON did not contain inline base64 media; URL-only responses are blocked to keep egress behind agentgateway"
                        .to_string(),
                )
            })?;
            let content = STANDARD
                .decode(encoded)
                .map_err(|error| MediaError::Provider(format!("invalid base64 media: {error}")))?;
            let content_type = infer_media_type(job.modality);
            (content, value, content_type.to_string())
        } else {
            (
                bytes,
                json!({ "contentType": media_type }),
                media_type.unwrap_or_else(|| infer_media_type(job.modality).to_string()),
            )
        };
        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        let artifact = self
            .inner
            .database
            .create_artifact(NewArtifact {
                workspace_id: job.workspace_id,
                name: format!("{} output", provider.name),
                kind: modality_artifact_kind(job.modality),
                media_type: content_type,
                source: format!("media_provider:{}", provider.id),
                metadata: json!({ "mediaJobId": id, "providerId": provider.id }),
                content,
            })
            .await?;
        self.inner
            .database
            .set_media_job_state(
                id,
                MediaJobStatus::Completed,
                Some(&response_json),
                Some(artifact.id),
                None,
            )
            .await?;
        Ok(())
    }
}

async fn read_bounded_response_body(
    response: &mut reqwest::Response,
    cancellation: &CancellationToken,
    max_bytes: usize,
) -> Result<Vec<u8>, MediaError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(MediaError::Provider(format!(
            "provider response exceeded the {max_bytes}-byte limit"
        )));
    }
    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(max_bytes);
    let mut bytes = Vec::with_capacity(capacity);
    loop {
        let chunk = tokio::select! {
            () = cancellation.cancelled() => return Err(MediaError::Cancelled),
            chunk = response.chunk() => chunk
                .map_err(|error| MediaError::Provider(error.to_string()))?,
        };
        let Some(chunk) = chunk else {
            break;
        };
        if chunk.len() > max_bytes.saturating_sub(bytes.len()) {
            return Err(MediaError::Provider(format!(
                "provider response exceeded the {max_bytes}-byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn find_base64_payload(value: &Value) -> Option<&str> {
    match value {
        Value::Object(values) => {
            for key in ["b64_json", "b64Json", "audio", "video", "content"] {
                if let Some(value) = values.get(key).and_then(Value::as_str)
                    && value.len() >= 4
                {
                    return Some(value);
                }
            }
            values.values().find_map(find_base64_payload)
        }
        Value::Array(values) => values.iter().find_map(find_base64_payload),
        _ => None,
    }
}

const fn modality_artifact_kind(modality: MediaModality) -> ArtifactKind {
    match modality {
        MediaModality::Image => ArtifactKind::Image,
        MediaModality::Audio => ArtifactKind::Audio,
        MediaModality::Video => ArtifactKind::Video,
    }
}

const fn infer_media_type(modality: MediaModality) -> &'static str {
    match modality {
        MediaModality::Image => "image/png",
        MediaModality::Audio => "audio/mpeg",
        MediaModality::Video => "video/mp4",
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use axum::{
        Router,
        body::{Body, Bytes},
        routing::get,
    };
    use futures_util::stream;

    use super::*;

    #[tokio::test]
    async fn rejects_chunked_media_responses_before_buffering_past_the_limit() {
        let app = Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(stream::iter([
                    Ok::<_, Infallible>(Bytes::from_static(b"1234")),
                    Ok::<_, Infallible>(Bytes::from_static(b"5678")),
                ]))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut response = Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();

        let error = read_bounded_response_body(&mut response, &CancellationToken::new(), 6)
            .await
            .unwrap_err();

        assert!(matches!(error, MediaError::Provider(message) if message.contains("6-byte limit")));
        server.abort();
    }
}
