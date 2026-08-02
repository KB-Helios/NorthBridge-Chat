use northbridge_domain::{McpToolSnapshot, ToolPolicyMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolPolicyDecision {
    Allow,
    RequireApproval,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalDecision {
    pub approved: bool,
    pub note: Option<String>,
}

#[must_use]
pub fn evaluate_tool_policy(mode: ToolPolicyMode, tool: &McpToolSnapshot) -> ToolPolicyDecision {
    match mode {
        ToolPolicyMode::Manual => ToolPolicyDecision::RequireApproval,
        ToolPolicyMode::SideEffects if !tool.read_only || tool.destructive => {
            ToolPolicyDecision::RequireApproval
        }
        ToolPolicyMode::SideEffects | ToolPolicyMode::Auto => ToolPolicyDecision::Allow,
    }
}

#[cfg(test)]
mod tests {
    use northbridge_domain::{McpToolSnapshot, ToolPolicyMode};
    use serde_json::json;
    use uuid::Uuid;

    use super::{ToolPolicyDecision, evaluate_tool_policy};

    fn tool(read_only: bool, destructive: bool) -> McpToolSnapshot {
        McpToolSnapshot {
            server_id: Uuid::nil(),
            name: "files_write".to_string(),
            description: String::new(),
            input_schema: json!({"type":"object"}),
            annotations: json!({}),
            read_only,
            destructive,
            discovered_at_ms: 0,
        }
    }

    #[test]
    fn side_effect_policy_only_auto_approves_read_only_tools() {
        assert_eq!(
            evaluate_tool_policy(ToolPolicyMode::SideEffects, &tool(true, false)),
            ToolPolicyDecision::Allow
        );
        assert_eq!(
            evaluate_tool_policy(ToolPolicyMode::SideEffects, &tool(false, false)),
            ToolPolicyDecision::RequireApproval
        );
        assert_eq!(
            evaluate_tool_policy(ToolPolicyMode::SideEffects, &tool(true, true)),
            ToolPolicyDecision::RequireApproval
        );
    }
}
