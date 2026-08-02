use std::{sync::Arc, time::Duration};

use northbridge_ai_gateway::AgentgatewayAccess;
use northbridge_domain::McpToolSnapshot;
use reqwest::{Client, Response, redirect::Policy};
use serde_json::Value;
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_SESSION_HEADER: &str = "mcp-session-id";

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("invalid MCP gateway configuration: {0}")]
    InvalidConfig(String),
    #[error("MCP gateway transport failed: {0}")]
    Transport(String),
    #[error("MCP gateway returned HTTP {status}: {detail}")]
    Http { status: u16, detail: String },
    #[error("MCP JSON-RPC error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("MCP gateway returned an invalid response: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpToolResult {
    pub content: Value,
    pub structured_content: Option<Value>,
    pub is_error: bool,
}

#[derive(Clone)]
pub struct GatewayMcpClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    endpoint: Url,
    credential: northbridge_ai_gateway::GatewayCredential,
    state: Mutex<SessionState>,
}

#[derive(Default)]
struct SessionState {
    session_id: Option<String>,
    next_request_id: u64,
    initialized: bool,
}

impl GatewayMcpClient {
    pub fn new(access: AgentgatewayAccess) -> Result<Self, McpError> {
        let endpoint = access
            .base_url
            .join("mcp")
            .map_err(|error| McpError::InvalidConfig(error.to_string()))?;
        if endpoint.scheme() != "http"
            || endpoint.host_str() != Some("127.0.0.1")
            || endpoint.port().is_none()
        {
            return Err(McpError::InvalidConfig(
                "MCP must target the loopback agentgateway".to_string(),
            ));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10 * 60))
            .build()
            .map_err(|error| McpError::Transport(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                endpoint,
                credential: access.credential,
                state: Mutex::new(SessionState::default()),
            }),
        })
    }

    pub async fn list_tools(&self) -> Result<Vec<McpToolSnapshot>, McpError> {
        self.ensure_initialized().await?;
        let result = self.request("tools/list", serde_json::json!({})).await?;
        let tools = result
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(|| McpError::InvalidResponse("tools/list omitted tools".to_string()))?;
        let discovered_at_ms = northbridge_storage::now_ms();
        tools
            .iter()
            .map(|tool| {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .ok_or_else(|| {
                        McpError::InvalidResponse("tool omitted a valid name".to_string())
                    })?
                    .to_string();
                let annotations = tool
                    .get("annotations")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let read_only = annotations
                    .get("readOnlyHint")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let destructive = annotations
                    .get("destructiveHint")
                    .and_then(Value::as_bool)
                    .unwrap_or(!read_only);
                Ok(McpToolSnapshot {
                    // Agentgateway multiplexes targets into one virtual MCP server. The
                    // owning server is assigned by the discovery coordinator after it
                    // resolves the gateway-prefixed tool name.
                    server_id: Uuid::nil(),
                    name,
                    description: tool
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    input_schema: tool
                        .get("inputSchema")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({"type":"object"})),
                    annotations,
                    read_only,
                    destructive,
                    discovered_at_ms,
                })
            })
            .collect()
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: &Value,
    ) -> Result<McpToolResult, McpError> {
        if name.trim().is_empty() || !arguments.is_object() {
            return Err(McpError::InvalidConfig(
                "MCP tool calls require a name and object arguments".to_string(),
            ));
        }
        self.ensure_initialized().await?;
        let result = self
            .request(
                "tools/call",
                serde_json::json!({ "name": name, "arguments": arguments }),
            )
            .await?;
        Ok(McpToolResult {
            content: result
                .get("content")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
            structured_content: result.get("structuredContent").cloned(),
            is_error: result
                .get("isError")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    pub async fn reset_session(&self) {
        *self.inner.state.lock().await = SessionState::default();
    }

    async fn ensure_initialized(&self) -> Result<(), McpError> {
        let mut state = self.inner.state.lock().await;
        if state.initialized {
            return Ok(());
        }
        state.next_request_id = state.next_request_id.saturating_add(1);
        let request_id = state.next_request_id;
        let response = self
            .send(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": { "name": "northbridge-desktop", "version": env!("CARGO_PKG_VERSION") }
                    }
                }),
                state.session_id.as_deref(),
            )
            .await?;
        if let Some(session_id) = response
            .headers()
            .get(MCP_SESSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
        {
            state.session_id = Some(session_id.to_string());
        }
        let body = parse_rpc_response(response).await?;
        require_rpc_result(body)?;
        self.send_notification(
            serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
            state.session_id.as_deref(),
        )
        .await?;
        state.initialized = true;
        Ok(())
    }

    async fn request(&self, method: &str, params: Value) -> Result<Value, McpError> {
        let (id, session_id) = {
            let mut state = self.inner.state.lock().await;
            state.next_request_id = state.next_request_id.saturating_add(1);
            (state.next_request_id, state.session_id.clone())
        };
        let response = self
            .send(
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": params,
                }),
                session_id.as_deref(),
            )
            .await?;
        require_rpc_result(parse_rpc_response(response).await?)
    }

    async fn send(&self, payload: Value, session_id: Option<&str>) -> Result<Response, McpError> {
        let mut request = self
            .inner
            .client
            .post(self.inner.endpoint.clone())
            .bearer_auth(self.inner.credential.expose_secret())
            .header("accept", "application/json, text/event-stream")
            .json(&payload);
        if let Some(session_id) = session_id {
            request = request.header(MCP_SESSION_HEADER, session_id);
        }
        let response = request
            .send()
            .await
            .map_err(|error| McpError::Transport(error.to_string()))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let detail = response.text().await.unwrap_or_default();
        Err(McpError::Http {
            status,
            detail: detail.chars().take(4096).collect(),
        })
    }

    async fn send_notification(
        &self,
        payload: Value,
        session_id: Option<&str>,
    ) -> Result<(), McpError> {
        let response = self.send(payload, session_id).await?;
        if response.status().as_u16() == 202 || response.content_length() == Some(0) {
            return Ok(());
        }
        let _ = parse_rpc_response(response).await?;
        Ok(())
    }
}

async fn parse_rpc_response(response: Response) -> Result<Value, McpError> {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let body = response
        .text()
        .await
        .map_err(|error| McpError::Transport(error.to_string()))?;
    if content_type.contains("text/event-stream") {
        let data = body
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .find(|line| !line.is_empty())
            .ok_or_else(|| McpError::InvalidResponse("empty MCP event stream".to_string()))?;
        serde_json::from_str(data).map_err(|error| McpError::InvalidResponse(error.to_string()))
    } else {
        serde_json::from_str(&body).map_err(|error| McpError::InvalidResponse(error.to_string()))
    }
}

fn require_rpc_result(body: Value) -> Result<Value, McpError> {
    if let Some(error) = body.get("error") {
        return Err(McpError::Rpc {
            code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown MCP error")
                .to_string(),
        });
    }
    body.get("result")
        .cloned()
        .ok_or_else(|| McpError::InvalidResponse("JSON-RPC response omitted result".to_string()))
}
