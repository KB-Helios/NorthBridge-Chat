# NorthBridge agentgateway build

NorthBridge pins upstream agentgateway 1.4.0 at commit `83c952731ee79b4372e3a031382c4ff419ddfee1` and builds it from the verified source archive in `manifest.json`.

Upstream 1.4.0 binds configured data-plane gateway ports to the unspecified address even though its admin and readiness listeners support explicit loopback addresses. That violates the desktop security contract that every sidecar listener is loopback-only. `northbridge-loopback-v1.4.0.patch` changes the four standalone local-config bind paths to IPv4/IPv6 localhost and adds an explicit short-version marker.

The staging script verifies the source archive, applies this exact patch, builds with upstream's pinned Rust toolchain, checks the marker, hashes the result, and writes a generated sidecar manifest beside the ignored executable. Runtime startup accepts this security profile only when the marker and generated hash verification both succeed.

Do not replace this with the stock release binary. Rebase the patch, schema contract tests, listener probe, and pinned hashes together when upgrading agentgateway.
