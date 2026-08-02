# Desktop implementation status

This file separates implemented runtime behavior from architectural intent. Automated checks prove the items marked complete; they do not substitute for GPU qualification, installer acceptance on a clean machine, or performance comparison against another product.

## Phase 1 — foundation

- [x] Rust workspace with separated domain, storage, API, and process composition crates.
- [x] SQLite WAL, foreign keys, embedded migration ownership, and a default personal workspace.
- [x] Conversations, branch heads, structured message parts, soft deletion, and cursor pagination.
- [x] FTS5 message indexing and workspace-scoped lexical search.
- [x] Persistent generation runs and monotonic append-only generation events.
- [x] Bearer-authenticated loopback Axum API with origin allowlisting and request IDs.
- [x] Resumable SSE event replay followed by live event delivery.
- [x] Tauri 2 shell and restricted sidecar capability manifest.
- [x] Rust-to-WebView launch handshake with a per-session generated token.
- [x] Typed desktop transport package wired into LibreChat startup, including a fail-closed native startup screen.
- [x] Supervised, integrity-checked agentgateway 1.4.0 sidecar patched and verified to bind only to loopback.
- [x] Supervised, integrity-checked mistral.rs 0.9.0 sidecar routed only through agentgateway.
- [x] Persistent curated model selection with a stable `fast-local` alias and per-generation provider/model provenance.
- [x] Real desktop-core -> agentgateway -> mistral.rs streamed generation with SQLite persistence and restart recovery.
- [x] Graceful native shutdown with authenticated core control, bounded waits, cancellation, and force-kill fallback.
- [x] Production Tauri executable, MSI, and NSIS packages containing the verified sidecars.
- [x] Native WebView acceptance through the LibreChat new-chat screen and local model selector; closing the window drains every child process.

## Phase 2 — local model operations and split chat

- [x] Durable model catalog, selection, lazy load, explicit unload, and per-run provenance.
- [x] Cancellable Hugging Face downloads into an app-owned cache plus local-directory import.
- [x] Manifest hashing, interrupted-download recovery, and safe managed-model removal.
- [x] Hardware diagnostics, mistral.rs build capability readback, tuning recommendations, and explicit quantization/load profiles.
- [x] Generation performance persistence for queue time, first output, total latency, tokens, and decode rate.
- [x] Two-to-four-lane split chats with independent immutable branches, persisted runs, resumable streams, per-lane cancellation, and a maximum concurrency of two.
- [x] Native model-runtime dashboard and split-chat comparison UI.
- [x] Real sidecar acceptance for model selection, generation, split completion, independent cancellation, persistence, restart, and shutdown.

## Phase 3 — local knowledge and retrieval

- [x] Content-addressed original blobs and durable, cancellable ingestion jobs.
- [x] Text, Markdown/code, JSON/data, HTML, PDF, DOCX, and ODT extraction with Unicode-safe overlapping chunks and source offsets.
- [x] SQLite documents/chunks/embeddings as authority plus transactional FTS5 triggers.
- [x] TurboVec 4-bit dense indexes behind a single-writer actor, using stable SQLite chunk IDs.
- [x] Versioned manifests and snapshots keyed by embedding model, dimensions, chunker hash, and authoritative revision.
- [x] Dense mutation journal replay, contiguous-revision validation, corrupt snapshot recovery, and rebuild from SQLite embeddings.
- [x] Workspace/stack-filtered lexical and dense retrieval with reciprocal-rank fusion, query-vector caching, citations, and persisted retrieval traces.
- [x] Knowledge Stacks, active chat grounding, source citations, document/chunk inspection, ingestion progress/cancel, index status/rebuild, and authenticated desktop transport.
- [x] Recursive folder and Obsidian vault watchers with persisted roots, debounce, rescans, deletion handling, and `.obsidian` filtering.
- [x] Dedicated embedding runtime through agentgateway and mistral.rs `/v1/embeddings`; the zero-setup default is public `Qwen/Qwen3-Embedding-0.6B` at 1,024 dimensions.
- [x] Deterministic migration/API/ingestion/restart/state-machine coverage and real native-sidecar embedding, retrieval, grounded-generation, citation, persistence, and shutdown acceptance.

## Phase 4 — agents and tools

- [x] Rig-owned persistent agent runs with versioned Personas, Skills, checkpoints, cancellation, and explicit recovery.
- [x] Permission manifests, per-agent tool allowlists, durable approval decisions, and side-effect policy enforcement.
- [x] MCP discovery and invocation exclusively through the authenticated agentgateway route.
- [x] Official A2A Rust SDK client and server surfaces, Agent Card validation, persistent remote-agent records, and gateway-only A2A traffic.
- [x] Durable Crew coordination with local Rig members and optional remote A2A specialists.
- [x] Shadow Persona subscriptions, suggestions, acceptance/dismissal, restart recovery, and bounded shutdown.
- [x] Agent Studio UI and typed desktop transport for Personas, Skills, agents, runs, approvals, MCP, Crew, and Shadow Persona.

## Phase 5 — workflows and media

- [x] Typed Turnstile graphs for prompt, agent, tool, condition, parallel, bounded loop, approval, transform, A2A, and subworkflow nodes.
- [x] Durable workflow/node attempts, checkpoints, pause, cancel, retry, jump, approval, resume, and crash recovery.
- [x] Side-effect ambiguity is preserved as `unknown_outcome`; Tool and A2A nodes are never automatically replayed after an interrupted attempt.
- [x] Restricted deterministic transform worker with no shell, filesystem, or network access and explicit input/output limits.
- [x] Content-addressed Forge artifacts with SHA-256 verification, revisions, deduplication, downloads, and garbage collection.
- [x] Image, audio, and video provider jobs routed only through authenticated agentgateway routes; URL-only provider responses are rejected.
- [x] OS credential-vault integration. SQLite, generated gateway YAML, logs, audit entries, and exported packages contain references rather than secret values.
- [x] Scoped environments plus validated, versioned package export/import with ID remapping, credential placeholders, and artifact integrity checks.
- [x] Platform Studio UI and typed desktop transport for workflows, approvals, artifacts, credentials, environments, media, and packages.

## Phase 6 — remote and teams

- [x] Headless server mode that remains fail-closed unless non-loopback binding, explicit origins, and TLS termination are all asserted.
- [x] Remote desktop transport that requires HTTPS outside loopback and keeps remote bearer tokens in memory.
- [x] Argon2id user credentials, hashed session tokens, expiry/revocation, owner bootstrap, and role-aware principals.
- [x] Team policy evaluation with default role boundaries and auditable denials.
- [x] Serialized SHA-256 hash-chain audit events with verification.
- [x] Idempotent server-backed sync pushes, optimistic base-version conflicts, and cursor-based pulls.
- [x] Platform Studio UI for owner bootstrap, team policy, audit verification, and remote login/health checks.

## Verification contract

The Rust foundation must keep these checks green:

```powershell
npm run desktop:check
npm run desktop:test:gateway
npm run desktop:test:local-ai
npm run desktop:test:model-manager
npm run desktop:native:build
```

`desktop:check` enforces formatting, warning-free Clippy across all targets and features, and the complete default Rust test suite. The explicit real-sidecar tests exercise the pinned native binaries, including embeddings and grounded generation; `desktop:native:build` produces the executable and both Windows installer formats. Native UI acceptance is recorded separately because it is a real window lifecycle check rather than a headless unit test.

Server mode expects a trusted TLS-terminating reverse proxy and is intentionally not a turnkey Internet deployment. Native UI interaction for the new Agent Studio and Platform Studio panels remains a manual acceptance surface even when type checks, lint, API integration tests, and the native package build are green.
