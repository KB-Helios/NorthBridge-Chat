# NorthBridge mistral.rs build

The desktop build pins mistral.rs 0.9.0 and verifies both the downloaded archive and extracted executable before staging it for Tauri. The current Windows artifact is the upstream CPU build; accelerated Windows binaries require a separately qualified and signed build and must be added as distinct manifest entries rather than silently replacing this baseline.

NorthBridge always launches this binary with an explicit `127.0.0.1` host, dynamic port, disabled UI/access logging/metrics, and without mistral.rs agent, code-execution, shell, or MCP flags. Product orchestration and tool execution remain in the Rust core and agentgateway boundaries.
