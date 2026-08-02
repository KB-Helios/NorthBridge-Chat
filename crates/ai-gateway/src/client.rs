use std::{collections::BTreeSet, pin::Pin, time::Duration};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::{Client, Response, redirect::Policy};
use serde_json::Value;
use url::{Host, Url};

use crate::{GatewayCredential, GatewayEmbeddingResponse, GatewayError, Result};

const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

pub type GatewayByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[derive(Clone)]
pub struct GatewayClient {
    client: Client,
    base_url: Url,
    credential: GatewayCredential,
    aliases: BTreeSet<String>,
}

impl GatewayClient {
    pub fn new(
        base_url: Url,
        credential: GatewayCredential,
        aliases: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        validate_gateway_base_url(&base_url)?;
        let aliases = aliases.into_iter().collect::<BTreeSet<_>>();
        if aliases.is_empty() {
            return Err(GatewayError::InvalidConfig(
                "gateway client requires at least one allowed model alias".to_string(),
            ));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            credential,
            aliases,
        })
    }

    pub async fn list_models(&self) -> Result<Value> {
        let response = self
            .client
            .get(self.endpoint("v1/models")?)
            .bearer_auth(self.credential.expose_secret())
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        let response = require_success(response).await?;
        response
            .json()
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))
    }

    pub async fn stream_chat(&self, mut request: Value) -> Result<GatewayByteStream> {
        let body = request.as_object_mut().ok_or_else(|| {
            GatewayError::InvalidRequest("chat request must be a JSON object".to_string())
        })?;
        let model = body.get("model").and_then(Value::as_str).ok_or_else(|| {
            GatewayError::InvalidRequest("chat request requires a model alias".to_string())
        })?;
        if !self.aliases.contains(model) {
            return Err(GatewayError::InvalidRequest(format!(
                "unknown model alias {model:?}"
            )));
        }
        let messages_valid = body
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| !messages.is_empty());
        if !messages_valid {
            return Err(GatewayError::InvalidRequest(
                "chat request requires at least one message".to_string(),
            ));
        }
        body.insert("stream".to_string(), Value::Bool(true));

        let response = self
            .client
            .post(self.endpoint("v1/chat/completions")?)
            .bearer_auth(self.credential.expose_secret())
            .json(&request)
            .send()
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        let response = require_success(response).await?;
        let stream = response
            .bytes_stream()
            .map(|chunk| chunk.map_err(|error| GatewayError::Transport(error.to_string())));
        Ok(Box::pin(stream))
    }

    pub async fn embeddings(
        &self,
        model_alias: &str,
        inputs: &[String],
    ) -> Result<GatewayEmbeddingResponse> {
        if !self.aliases.contains(model_alias) {
            return Err(GatewayError::InvalidRequest(format!(
                "unknown model alias {model_alias:?}"
            )));
        }
        if inputs.is_empty() || inputs.len() > 256 {
            return Err(GatewayError::InvalidRequest(
                "embedding requests require between one and 256 inputs".to_string(),
            ));
        }
        let mut total_bytes = 0_usize;
        for input in inputs {
            if input.trim().is_empty() || input.len() > 1024 * 1024 {
                return Err(GatewayError::InvalidRequest(
                    "each embedding input must contain 1 byte to 1 MiB of text".to_string(),
                ));
            }
            total_bytes = total_bytes.saturating_add(input.len());
        }
        if total_bytes > 4 * 1024 * 1024 {
            return Err(GatewayError::InvalidRequest(
                "embedding request input exceeded 4 MiB".to_string(),
            ));
        }
        let response = self
            .client
            .post(self.endpoint("v1/embeddings")?)
            .bearer_auth(self.credential.expose_secret())
            .json(&serde_json::json!({
                "model": model_alias,
                "input": inputs,
                "encoding_format": "float",
                "truncate_sequence": true
            }))
            .timeout(Duration::from_secs(10 * 60))
            .send()
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        let response = require_success(response).await?;
        let mut response = response
            .json::<GatewayEmbeddingResponse>()
            .await
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        response.data.sort_by_key(|item| item.index);
        if response.data.len() != inputs.len()
            || response
                .data
                .iter()
                .enumerate()
                .any(|(index, item)| item.index != index)
        {
            return Err(GatewayError::Validation(
                "embedding response indices did not match the request inputs".to_string(),
            ));
        }
        let dimensions = response.data.first().map_or(0, |item| item.embedding.len());
        if dimensions == 0
            || response.data.iter().any(|item| {
                item.embedding.len() != dimensions
                    || !item.embedding.iter().all(|value| value.is_finite())
            })
        {
            return Err(GatewayError::Validation(
                "embedding response contained inconsistent, empty, or non-finite vectors"
                    .to_string(),
            ));
        }
        Ok(response)
    }

    fn endpoint(&self, path: &str) -> Result<Url> {
        self.base_url
            .join(path)
            .map_err(|error| GatewayError::InvalidConfig(error.to_string()))
    }
}

async fn require_success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| GatewayError::Transport(error.to_string()))?;
        let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(body.len());
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if body.len() >= MAX_ERROR_BODY_BYTES {
            break;
        }
    }
    let message = String::from_utf8_lossy(&body).trim().to_string();
    Err(GatewayError::Upstream { status, message })
}

fn validate_gateway_base_url(url: &Url) -> Result<()> {
    if url.scheme() != "http"
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GatewayError::InvalidConfig(format!(
            "gateway base URL must be credential-free loopback HTTP with an explicit port: {url}"
        )));
    }
    let loopback = match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if !loopback || url.path() != "/" {
        return Err(GatewayError::InvalidConfig(format!(
            "gateway base URL must use a loopback IP literal and root path: {url}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use futures_util::StreamExt;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::GatewayClient;
    use crate::GatewayCredential;

    async fn chat(
        State(token): State<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        let expected = format!("Bearer {token}");
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if body["model"] != "fast-local" || body["stream"] != true {
            return StatusCode::BAD_REQUEST.into_response();
        }
        (
            [("content-type", "text/event-stream")],
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n",
        )
            .into_response()
    }

    async fn models() -> Json<Value> {
        Json(json!({"object":"list","data":[{"id":"fast-local"}]}))
    }

    async fn embeddings(
        State(token): State<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        let expected = format!("Bearer {token}");
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
            || body["model"] != "fast-local"
            || body["encoding_format"] != "float"
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        Json(json!({
            "object": "list",
            "data": [
                {"object": "embedding", "embedding": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "index": 0},
                {"object": "embedding", "embedding": [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], "index": 1}
            ],
            "model": "fast-local",
            "usage": {"prompt_tokens": 2, "total_tokens": 2}
        }))
        .into_response()
    }

    async fn mock_gateway(token: String) -> (GatewayClient, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/v1/chat/completions", post(chat))
            .route("/v1/embeddings", post(embeddings))
            .route("/v1/models", get(models))
            .with_state(token.clone());
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = GatewayClient::new(
            format!("http://{address}/").parse().unwrap(),
            GatewayCredential::parse(token).unwrap(),
            ["fast-local".to_string()],
        )
        .unwrap();
        (client, task)
    }

    #[tokio::test]
    async fn authenticates_and_streams_only_known_aliases() {
        let token = "nb-test-0000000000000000000000000000000000000000".to_string();
        let (client, task) = mock_gateway(token).await;
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
        assert!(String::from_utf8(body).unwrap().contains("data: [DONE]"));

        let error = client
            .stream_chat(json!({
                "model": "direct-provider-name",
                "messages": [{"role": "user", "content": "hello"}]
            }))
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("unknown model alias"));
        task.abort();
    }

    #[tokio::test]
    async fn lists_models_through_the_authenticated_gateway() {
        let token = "nb-test-0000000000000000000000000000000000000000".to_string();
        let (client, task) = mock_gateway(token).await;
        let models = client.list_models().await.unwrap();
        assert_eq!(models["data"][0]["id"], "fast-local");
        task.abort();
    }

    #[tokio::test]
    async fn validates_and_orders_embeddings_through_the_gateway() {
        let token = "nb-test-0000000000000000000000000000000000000000".to_string();
        let (client, task) = mock_gateway(token).await;
        let response = client
            .embeddings("fast-local", &["one".to_string(), "two".to_string()])
            .await
            .unwrap();
        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].embedding.len(), 8);
        task.abort();
    }
}
