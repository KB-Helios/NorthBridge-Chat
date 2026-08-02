mod a2a;
mod mcp;
mod policy;
mod runtime;

pub use a2a::{A2aGatewayClient, A2aGatewayError, A2aTextResponse, northbridge_agent_card_router};
pub use mcp::{GatewayMcpClient, McpError, McpToolResult};
pub use policy::{ApprovalDecision, ToolPolicyDecision, evaluate_tool_policy};
pub use runtime::{
    AGENT_RUNTIME_VERSION, AgentRuntime, AgentRuntimeError, RigAgentRuntime, RigAgentRuntimeConfig,
    StartAgentRun,
};
