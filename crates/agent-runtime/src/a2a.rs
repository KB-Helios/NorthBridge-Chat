use std::{sync::Arc, time::Duration};

use a2a::{
    AgentCard, CancelTaskRequest, GetTaskRequest, Message, Part, Role, SendMessageConfiguration,
    SendMessageRequest, SendMessageResponse, StreamResponse, Task, TaskState,
};
use a2a_client::{A2AClientFactory, BoxStream, middleware::CallInterceptor};
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, InMemoryTaskStore, StaticAgentCard,
    agent_card::agent_card_router, jsonrpc::jsonrpc_router, rest::rest_router,
};
use async_trait::async_trait;
use northbridge_ai_gateway::AgentgatewayAccess;
use reqwest::{Client, redirect::Policy};
use url::Url;

#[derive(Debug, Clone)]
pub struct A2aTextResponse {
    pub task_id: Option<String>,
    pub context_id: Option<String>,
    pub output: String,
    pub response: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum A2aGatewayError {
    #[error("invalid A2A gateway configuration: {0}")]
    InvalidConfig(String),
    #[error("A2A gateway transport failed: {0}")]
    Transport(String),
    #[error("A2A protocol failed: {0}")]
    Protocol(String),
}

#[derive(Clone)]
pub struct A2aGatewayClient {
    client: Client,
    access: AgentgatewayAccess,
}

impl A2aGatewayClient {
    pub fn new(access: AgentgatewayAccess) -> Result<Self, A2aGatewayError> {
        validate_loopback_gateway(&access.base_url)?;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build()
            .map_err(|error| A2aGatewayError::Transport(error.to_string()))?;
        Ok(Self { client, access })
    }

    pub async fn discover(&self, gateway_path: &str) -> Result<AgentCard, A2aGatewayError> {
        let mut card_url = self.gateway_agent_url(gateway_path)?;
        card_url.set_path(&format!(
            "{}/.well-known/agent-card.json",
            gateway_path.trim_end_matches('/')
        ));
        let response = self
            .client
            .get(card_url)
            .bearer_auth(self.access.credential.expose_secret())
            .send()
            .await
            .map_err(|error| A2aGatewayError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(A2aGatewayError::Transport(format!(
                "agent card request returned HTTP {}",
                response.status()
            )));
        }
        let card = response
            .json::<AgentCard>()
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))?;
        self.validate_rewritten_card(gateway_path, &card)?;
        Ok(card)
    }

    pub async fn send_message(
        &self,
        gateway_path: &str,
        request: &SendMessageRequest,
    ) -> Result<SendMessageResponse, A2aGatewayError> {
        let card = self.discover(gateway_path).await?;
        let client = self.official_client(&card).await?;
        client
            .send_message(request)
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))
    }

    pub async fn send_streaming_message(
        &self,
        gateway_path: &str,
        request: &SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, a2a::A2AError>>, A2aGatewayError> {
        let card = self.discover(gateway_path).await?;
        let client = self.official_client(&card).await?;
        client
            .send_streaming_message(request)
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))
    }

    pub async fn get_task(
        &self,
        gateway_path: &str,
        request: &GetTaskRequest,
    ) -> Result<Task, A2aGatewayError> {
        let card = self.discover(gateway_path).await?;
        let client = self.official_client(&card).await?;
        client
            .get_task(request)
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))
    }

    pub async fn cancel_task(
        &self,
        gateway_path: &str,
        request: &CancelTaskRequest,
    ) -> Result<Task, A2aGatewayError> {
        let card = self.discover(gateway_path).await?;
        let client = self.official_client(&card).await?;
        client
            .cancel_task(request)
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))
    }

    pub async fn send_text(
        &self,
        gateway_path: &str,
        prompt: &str,
    ) -> Result<A2aTextResponse, A2aGatewayError> {
        let request = SendMessageRequest {
            message: Message::new(Role::User, vec![Part::text(prompt)]),
            configuration: Some(SendMessageConfiguration {
                accepted_output_modes: Some(vec!["text/plain".to_string()]),
                task_push_notification_config: None,
                history_length: Some(20),
                return_immediately: Some(false),
            }),
            metadata: None,
            tenant: None,
        };
        let response = self.send_message(gateway_path, &request).await?;
        match response {
            SendMessageResponse::Message(message) => text_response_from_message(message),
            SendMessageResponse::Task(task) => self.wait_for_task(gateway_path, task).await,
        }
    }

    async fn wait_for_task(
        &self,
        gateway_path: &str,
        mut task: Task,
    ) -> Result<A2aTextResponse, A2aGatewayError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5 * 60);
        loop {
            match task.status.state {
                TaskState::Completed => return text_response_from_task(task),
                TaskState::Failed | TaskState::Canceled | TaskState::Rejected => {
                    return Err(A2aGatewayError::Protocol(format!(
                        "remote A2A task {} ended as {:?}",
                        task.id, task.status.state
                    )));
                }
                TaskState::InputRequired | TaskState::AuthRequired => {
                    return Err(A2aGatewayError::Protocol(format!(
                        "remote A2A task {} requires interactive input",
                        task.id
                    )));
                }
                TaskState::Unspecified | TaskState::Submitted | TaskState::Working => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(A2aGatewayError::Transport(format!(
                    "remote A2A task {} did not complete within five minutes",
                    task.id
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            task = self
                .get_task(
                    gateway_path,
                    &GetTaskRequest {
                        id: task.id.clone(),
                        history_length: Some(20),
                        tenant: None,
                    },
                )
                .await?;
        }
    }

    async fn official_client(
        &self,
        card: &AgentCard,
    ) -> Result<a2a_client::A2AClient<Box<dyn a2a_client::Transport>>, A2aGatewayError> {
        A2AClientFactory::builder()
            .with_interceptor(Arc::new(BearerInterceptor {
                value: format!("Bearer {}", self.access.credential.expose_secret()),
            }))
            .build()
            .create_from_card(card)
            .await
            .map_err(|error| A2aGatewayError::Protocol(error.to_string()))
    }

    fn gateway_agent_url(&self, gateway_path: &str) -> Result<Url, A2aGatewayError> {
        if !gateway_path.starts_with("/a2a/")
            || gateway_path.contains("..")
            || gateway_path.contains('?')
            || gateway_path.contains('#')
        {
            return Err(A2aGatewayError::InvalidConfig(
                "A2A gateway path must be an assigned /a2a/<id> route".to_string(),
            ));
        }
        let mut url = self.access.base_url.clone();
        url.set_path(gateway_path);
        Ok(url)
    }

    fn validate_rewritten_card(
        &self,
        gateway_path: &str,
        card: &AgentCard,
    ) -> Result<(), A2aGatewayError> {
        if card.supported_interfaces.is_empty() {
            return Err(A2aGatewayError::Protocol(
                "agent card advertises no supported interfaces".to_string(),
            ));
        }
        for interface in &card.supported_interfaces {
            let url = Url::parse(&interface.url)
                .map_err(|error| A2aGatewayError::Protocol(error.to_string()))?;
            if url.scheme() != self.access.base_url.scheme()
                || url.host_str() != self.access.base_url.host_str()
                || url.port_or_known_default() != self.access.base_url.port_or_known_default()
                || !url.path().starts_with(gateway_path)
            {
                return Err(A2aGatewayError::Protocol(format!(
                    "agentgateway did not rewrite advertised A2A interface {} to its assigned route",
                    interface.url
                )));
            }
        }
        Ok(())
    }
}

fn text_response_from_message(message: Message) -> Result<A2aTextResponse, A2aGatewayError> {
    let output = message.text().map(str::to_owned).ok_or_else(|| {
        A2aGatewayError::Protocol("remote A2A response contained no text".to_string())
    })?;
    let context_id = message.context_id.clone();
    let response = serde_json::to_value(&message)
        .map_err(|error| A2aGatewayError::Protocol(error.to_string()))?;
    Ok(A2aTextResponse {
        task_id: message.task_id,
        context_id,
        output,
        response,
    })
}

fn text_response_from_task(task: Task) -> Result<A2aTextResponse, A2aGatewayError> {
    let output = task
        .status
        .message
        .as_ref()
        .and_then(Message::text)
        .or_else(|| {
            task.artifacts.as_ref().and_then(|artifacts| {
                artifacts
                    .iter()
                    .rev()
                    .find_map(|artifact| artifact.parts.iter().find_map(Part::as_text))
            })
        })
        .or_else(|| {
            task.history.as_ref().and_then(|history| {
                history
                    .iter()
                    .rev()
                    .find(|message| message.role == Role::Agent)
                    .and_then(Message::text)
            })
        })
        .map(str::to_owned)
        .ok_or_else(|| {
            A2aGatewayError::Protocol("completed remote A2A task contained no text".to_string())
        })?;
    let task_id = Some(task.id.clone());
    let context_id = Some(task.context_id.clone());
    let response = serde_json::to_value(&task)
        .map_err(|error| A2aGatewayError::Protocol(error.to_string()))?;
    Ok(A2aTextResponse {
        task_id,
        context_id,
        output,
        response,
    })
}

struct BearerInterceptor {
    value: String,
}

#[async_trait]
impl CallInterceptor for BearerInterceptor {
    async fn before(
        &self,
        _method: &str,
        params: &mut a2a_client::ServiceParams,
    ) -> Result<(), a2a::A2AError> {
        params.insert("authorization".to_string(), vec![self.value.clone()]);
        Ok(())
    }
}

pub fn northbridge_agent_card_router<E: AgentExecutor>(
    card: AgentCard,
    executor: E,
) -> axum::Router {
    let handler = Arc::new(DefaultRequestHandler::new(
        executor,
        InMemoryTaskStore::new(),
    ));
    agent_card_router(Arc::new(StaticAgentCard::new(card)))
        .merge(jsonrpc_router(handler.clone()))
        .merge(rest_router(handler))
}

fn validate_loopback_gateway(url: &Url) -> Result<(), A2aGatewayError> {
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(A2aGatewayError::InvalidConfig(
            "A2A client must target an authenticated loopback agentgateway".to_string(),
        ));
    }
    Ok(())
}
