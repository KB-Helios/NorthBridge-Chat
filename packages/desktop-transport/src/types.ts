export interface DesktopBootstrap {
  baseUrl: string;
  sessionToken: string;
  pid: number;
  schemaVersion: number;
}

export type AiProvider = 'local_mistral_rs';

export type ModelQuantization = 'none' | 'q2k' | 'q3k' | 'q4k' | 'q5k' | 'q6k' | 'q8_0';
export type ModelTuningProfile = 'quality' | 'balanced' | 'fast';

export interface ModelLoadProfile {
  quantization: ModelQuantization;
  maxSequenceLength: number;
  maxBatchSize: number;
  forceCpu: boolean;
}

export interface ModelCatalogEntry {
  provider: AiProvider;
  modelId: string;
  modelAlias: string;
  displayName: string;
  description: string;
  contextWindow: number;
  recommended: boolean;
}

export interface ModelSelection {
  provider: AiProvider;
  modelId: string;
  modelAlias: string;
  loadProfile: ModelLoadProfile;
}

export type ManagedModelSource = 'hugging_face' | 'imported';
export type ManagedModelStatus =
  | 'queued'
  | 'downloading'
  | 'installed'
  | 'imported'
  | 'failed'
  | 'cancelled';

export interface ManagedModel {
  id: string;
  modelId: string;
  displayName: string;
  source: ManagedModelSource;
  sourceReference: string;
  revision: string | null;
  localPath: string;
  status: ManagedModelStatus;
  bytesDownloaded: number;
  totalBytes: number | null;
  fileCount: number | null;
  manifestSha256: string | null;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ModelCatalogResponse {
  models: ModelCatalogEntry[];
  managedModels: ManagedModel[];
  selection: ModelSelection | null;
  runtime: object | null;
}

export type MessageRole = 'system' | 'user' | 'assistant' | 'tool';

export type MessagePartKind =
  | 'text'
  | 'reasoning'
  | 'image'
  | 'audio'
  | 'file'
  | 'tool_call'
  | 'tool_result'
  | 'json';

export interface Conversation {
  id: string;
  workspaceId: string;
  defaultBranchId: string;
  title: string;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ConversationPage {
  items: Conversation[];
  nextCursor: string | null;
}

export interface ConversationBranch {
  id: string;
  conversationId: string;
  parentBranchId: string | null;
  forkedFromMessageId: string | null;
  headMessageId: string | null;
  createdAtMs: number;
}

export interface NewMessagePart {
  kind: MessagePartKind;
  content: string;
  metadata?: object | null;
}

export interface MessagePart {
  id: string;
  kind: MessagePartKind;
  content: string;
  metadata: object | null;
}

export interface Message {
  id: string;
  conversationId: string;
  branchId: string;
  parentMessageId: string | null;
  role: MessageRole;
  createdAtMs: number;
  parts: MessagePart[];
}

export type GenerationStatus =
  | 'queued'
  | 'running'
  | 'waiting_for_approval'
  | 'completed'
  | 'failed'
  | 'cancelled';

export interface GenerationRun {
  id: string;
  conversationId: string;
  branchId: string;
  inputMessageId: string | null;
  outputMessageId: string | null;
  clientRequestId: string | null;
  modelAlias: string;
  provider: AiProvider | null;
  modelId: string | null;
  loadProfile: ModelLoadProfile | null;
  status: GenerationStatus;
  lastSequence: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface SplitChatLane {
  splitChatId: string;
  ordinal: number;
  branchId: string;
  generation: GenerationRun;
}

export interface SplitChatRun {
  id: string;
  conversationId: string;
  sourceBranchId: string;
  inputMessageId: string;
  clientRequestId: string | null;
  createdAtMs: number;
  updatedAtMs: number;
  lanes: SplitChatLane[];
}

export interface GenerationEvent {
  generationId: string;
  sequenceNumber: number;
  eventType: string;
  timestampMs: number;
  payload: object | string | number | boolean | null;
}

export interface GenerationPerformanceSample {
  generationId: string;
  provider: AiProvider | null;
  modelId: string | null;
  loadProfile: ModelLoadProfile | null;
  status: GenerationStatus;
  queuedAtMs: number;
  startedAtMs: number | null;
  firstOutputAtMs: number | null;
  finishedAtMs: number | null;
  queueDurationMs: number | null;
  timeToFirstOutputMs: number | null;
  totalDurationMs: number | null;
  inputCharacters: number | null;
  outputCharacters: number;
  promptTokens: number | null;
  completionTokens: number | null;
  totalTokens: number | null;
  decodeTokensPerSecond: number | null;
}

export interface GenerationPerformanceDashboard {
  generatedAtMs: number;
  completedRuns: number;
  failedRuns: number;
  cancelledRuns: number;
  timeToFirstOutputP50Ms: number | null;
  timeToFirstOutputP95Ms: number | null;
  totalDurationP50Ms: number | null;
  totalDurationP95Ms: number | null;
  decodeTokensPerSecondP50: number | null;
  samples: GenerationPerformanceSample[];
}

export interface SearchHit {
  messageId: string;
  conversationId: string;
  partId: string;
  content: string;
  score: number;
}

export type KnowledgeSourceKind = 'file' | 'folder' | 'obsidian';
export type KnowledgeDocumentStatus = 'indexing' | 'ready' | 'failed';
export type IngestionJobStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';
export type KnowledgeWatchKind = 'folder' | 'obsidian';

export interface KnowledgeStack {
  id: string;
  workspaceId: string;
  name: string;
  description: string;
  embeddingModel: string;
  embeddingDimensions: number;
  chunkSize: number;
  chunkOverlap: number;
  chunkerHash: string;
  indexRevision: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface KnowledgeDocument {
  id: string;
  stackId: string;
  workspaceId: string;
  sourceKind: KnowledgeSourceKind;
  sourceUri: string;
  title: string;
  mimeType: string;
  contentSha256: string;
  blobSha256: string;
  byteSize: number;
  status: KnowledgeDocumentStatus;
  lastError: string | null;
  sourceModifiedAtMs: number | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface DocumentChunk {
  denseId: number;
  id: string;
  documentId: string;
  stackId: string;
  workspaceId: string;
  ordinal: number;
  content: string;
  contentSha256: string;
  characterCount: number;
  startOffset: number;
  endOffset: number;
  metadata: Record<string, unknown>;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface IngestionJob {
  id: string;
  stackId: string;
  sourceUri: string;
  sourceKind: KnowledgeSourceKind;
  status: IngestionJobStatus;
  progressCurrent: number;
  progressTotal: number | null;
  documentId: string | null;
  cancelRequested: boolean;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface KnowledgeWatchRoot {
  id: string;
  stackId: string;
  workspaceId: string;
  kind: KnowledgeWatchKind;
  path: string;
  recursive: boolean;
  enabled: boolean;
  lastScanAtMs: number | null;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface KnowledgeSearchHit {
  citation: string;
  denseId: number;
  chunkId: string;
  documentId: string;
  stackId: string;
  title: string;
  sourceUri: string;
  snippet: string;
  startOffset: number;
  endOffset: number;
  lexicalRank: number | null;
  denseRank: number | null;
  fusedScore: number;
}

export interface KnowledgeSearchResult {
  traceId: string;
  query: string;
  stackIds: string[];
  embeddingModels: string[];
  lexicalCandidateCount: number;
  denseCandidateCount: number;
  elapsedMs: number;
  hits: KnowledgeSearchHit[];
}

export interface DenseIndexStatus {
  stackId: string;
  dimensions: number;
  bitWidth: number;
  recordCount: number;
  revision: number;
  snapshot: string | null;
}

export interface CreateConversationInput {
  title?: string;
  workspaceId?: string;
}

export interface ListConversationsInput {
  workspaceId?: string;
  cursor?: string;
  limit?: number;
}

export interface AppendMessageInput {
  branchId: string;
  parentMessageId?: string | null;
  role: MessageRole;
  parts: NewMessagePart[];
}

export interface ForkConversationBranchInput {
  forkedFromMessageId: string;
  activate?: boolean;
}

export interface CreateGenerationInput {
  branchId: string;
  modelAlias: string;
  clientRequestId?: string;
  knowledgeStackIds?: string[];
}

export interface CreateKnowledgeStackInput {
  workspaceId?: string;
  name: string;
  description?: string;
  embeddingModel?: string;
  embeddingDimensions?: number;
  chunkSize?: number;
  chunkOverlap?: number;
}

export interface ImportKnowledgeDocumentsInput {
  paths: string[];
}

export interface SearchKnowledgeInput {
  query: string;
  workspaceId?: string;
  limit?: number;
}

export interface CreateKnowledgeWatchRootInput {
  path: string;
  kind: KnowledgeWatchKind;
  recursive?: boolean;
}

export interface SplitChatCandidateInput {
  modelId: string;
  loadProfile?: ModelLoadProfile;
}

export interface CreateSplitChatInput {
  sourceBranchId: string;
  inputMessageId: string;
  candidates: SplitChatCandidateInput[];
  clientRequestId?: string;
}

export interface StreamGenerationInput {
  afterSequence?: number;
  signal?: AbortSignal;
}

export interface SelectModelInput {
  modelId: string;
  loadProfile?: ModelLoadProfile;
}

export interface TuneModelInput {
  modelId: string;
  loadProfile?: ModelLoadProfile;
  tuningProfile?: ModelTuningProfile;
}

export interface DownloadModelInput {
  modelId: string;
  revision?: string;
  displayName?: string;
}

export interface ImportModelInput {
  path: string;
  modelId?: string;
  displayName?: string;
}

export interface HardwareCpu {
  brand: string;
  logicalCores: number;
  physicalCores: number | null;
  avx: boolean;
  avx2: boolean;
  avx512: boolean;
  fma: boolean;
}

export interface HardwareMemory {
  totalBytes: number;
  availableBytes: number;
}

export interface HardwareDevice {
  kind: string;
  ordinal: number | null;
  name: string;
  totalMemoryBytes: number | null;
  availableMemoryBytes: number | null;
}

export interface MistralBuildCapabilities {
  version: string;
  cuda: boolean;
  metal: boolean;
  cudnn: boolean;
  flashAttn: boolean;
  flashAttnV3: boolean;
  accelerate: boolean;
  mkl: boolean;
  gitRevision: string | null;
}

export interface HardwareCheck {
  name: string;
  status: 'ok' | 'warning' | 'error' | string;
  message: string;
  suggestion?: string | null;
}

export interface HardwareDiagnostics {
  system: {
    os: string;
    kernel: string;
    cpu: HardwareCpu;
    memory: HardwareMemory;
    devices: HardwareDevice[];
    build: MistralBuildCapabilities;
    hfCachePath: string;
  };
  checks: HardwareCheck[];
}

export interface ModelTuneCandidate {
  isq: string | null;
  isqName: string;
  estimatedSizeBytes: number;
  vramUsagePercent: number;
  maxContextTokens: number;
  contextIsModelMax: boolean;
  quality: string;
  fitStatus: string;
  recommended: boolean;
}

export interface ModelTuneResult {
  modelId: string;
  profile: ModelTuningProfile;
  backend: string;
  candidates: ModelTuneCandidate[];
  recommendedIsq: string | null;
  deviceLayers: object | null;
  deviceLayersCli: string | null;
  pagedAttnMode: string;
  recommendedCommand: string;
  totalVramBytes: number;
  warnings: string[];
  notes: string[];
}

export type ToolPolicyMode = 'manual' | 'side_effects' | 'auto';
export type AgentRunStatus =
  | 'queued'
  | 'running'
  | 'waiting_for_approval'
  | 'suspended'
  | 'completed'
  | 'failed'
  | 'cancelled';

export interface Persona {
  id: string;
  workspaceId: string;
  name: string;
  description: string;
  systemPrompt: string;
  version: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface Skill {
  id: string;
  workspaceId: string;
  slug: string;
  name: string;
  description: string;
  instructions: string;
  permissionManifest: Record<string, unknown>;
  version: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface AgentToolPermission {
  serverId: string;
  toolName: string;
}

export interface AgentProfile {
  id: string;
  workspaceId: string;
  personaId: string | null;
  name: string;
  description: string;
  modelAlias: string;
  instructions: string;
  policyMode: ToolPolicyMode;
  maxTurns: number;
  version: number;
  skillIds: string[];
  knowledgeStackIds: string[];
  toolAllowlist: AgentToolPermission[];
  createdAtMs: number;
  updatedAtMs: number;
}

export interface AgentRun {
  id: string;
  workspaceId: string;
  agentId: string;
  conversationId: string | null;
  status: AgentRunStatus;
  prompt: string;
  rigVersion: string;
  checkpoint: unknown;
  checkpointGeneration: number;
  currentTurn: number;
  output: string | null;
  cancelRequested: boolean;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
  completedAtMs: number | null;
}

export interface AgentApproval {
  id: string;
  runId: string;
  toolCallId: string;
  status: 'pending' | 'approved' | 'denied' | 'cancelled';
  reason: string;
  decisionNote: string | null;
  requestedAtMs: number;
  decidedAtMs: number | null;
}

export type McpTransport = 'stdio' | 'streamable_http' | 'sse' | 'openapi';

export interface McpServer {
  id: string;
  workspaceId: string;
  name: string;
  transport: McpTransport;
  endpoint: string | null;
  command: string | null;
  args: string[];
  environmentReferences: Record<string, string>;
  openapiSchema: string | null;
  enabled: boolean;
  configRevision: number;
  lastError: string | null;
  lastDiscoveredAtMs: number | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface CrewMember {
  ordinal: number;
  role: string;
  localAgentId: string | null;
  remoteAgentId: string | null;
}

export interface Crew {
  id: string;
  workspaceId: string;
  name: string;
  description: string;
  coordinatorAgentId: string;
  members: CrewMember[];
  createdAtMs: number;
  updatedAtMs: number;
}

export interface CrewRunMember extends CrewMember {
  agentRunId: string | null;
  remoteTaskId: string | null;
  remoteContextId: string | null;
  status: string;
  output: string | null;
  response: unknown;
  lastError: string | null;
  startedAtMs: number | null;
  completedAtMs: number | null;
  updatedAtMs: number;
}

export interface CrewRun {
  id: string;
  workspaceId: string;
  crewId: string;
  prompt: string;
  status: 'queued' | 'running' | 'suspended' | 'completed' | 'failed' | 'cancelled';
  coordinatorRunId: string | null;
  output: string | null;
  lastError: string | null;
  members: CrewRunMember[];
  createdAtMs: number;
  updatedAtMs: number;
  completedAtMs: number | null;
}

export interface ShadowSubscription {
  id: string;
  workspaceId: string;
  conversationId: string;
  agentId: string;
  enabled: boolean;
  lastMessageId: string | null;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ShadowSuggestion {
  id: string;
  subscriptionId: string;
  sourceMessageId: string;
  runId: string | null;
  content: string;
  status: 'processing' | 'unread' | 'accepted' | 'dismissed' | 'failed';
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface CreatePersonaInput {
  name: string;
  description?: string;
  systemPrompt: string;
}

export interface CreateSkillInput {
  slug: string;
  name: string;
  description?: string;
  instructions: string;
  permissionManifest?: Record<string, unknown>;
}

export interface CreateAgentInput {
  personaId?: string | null;
  name: string;
  description?: string;
  modelAlias: string;
  instructions?: string;
  policyMode?: ToolPolicyMode;
  maxTurns?: number;
  skillIds?: string[];
  knowledgeStackIds?: string[];
  toolAllowlist?: AgentToolPermission[];
}

export type WorkflowRunStatus =
  | 'queued'
  | 'running'
  | 'paused'
  | 'waiting_for_approval'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'unknown_outcome';

export type WorkflowNodeKind =
  | { kind: 'prompt' | 'agent'; agentId: string; prompt: string }
  | { kind: 'tool'; agentId: string; instruction: string }
  | {
      kind: 'condition';
      pointer: string;
      equals?: unknown;
      ifTrue?: string;
      ifFalse?: string;
    }
  | { kind: 'parallel'; branches: WorkflowNode[][] }
  | { kind: 'loop'; body: WorkflowNode[]; maxIterations: number; untilPointer?: string }
  | { kind: 'approval'; title: string; prompt: string }
  | {
      kind: 'transform';
      operation:
        | { operation: 'identity' }
        | { operation: 'select'; pointer: string }
        | { operation: 'object'; fields: Record<string, unknown> }
        | { operation: 'template'; template: string };
    }
  | { kind: 'a2a_task'; remoteAgentId: string; prompt: string }
  | { kind: 'subworkflow'; workflowId: string; input?: unknown };

export type WorkflowNode = { id: string; next?: string } & WorkflowNodeKind;

export interface WorkflowDefinition {
  id: string;
  workspaceId: string;
  name: string;
  description: string;
  version: number;
  entryNodeId: string;
  nodes: WorkflowNode[];
  createdAtMs: number;
  updatedAtMs: number;
}

export interface WorkflowRun {
  id: string;
  workflowId: string;
  workspaceId: string;
  parentRunId: string | null;
  status: WorkflowRunStatus;
  input: unknown;
  output: unknown | null;
  currentNodeId: string | null;
  resumeCursor: unknown | null;
  lastError: string | null;
  pauseRequested: boolean;
  cancelRequested: boolean;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface WorkflowNodeRun {
  id: string;
  workflowRunId: string;
  nodePath: string;
  nodeKind: string;
  attempt: number;
  status: string;
  input: unknown;
  output: unknown | null;
  tokenUsage: unknown | null;
  toolCalls: unknown[];
  error: string | null;
  resumeCursor: unknown | null;
  artifactIds: string[];
  sideEffectKey: string | null;
  startedAtMs: number | null;
  finishedAtMs: number | null;
}

export interface WorkflowApproval {
  id: string;
  workflowRunId: string;
  nodeRunId: string;
  title: string;
  prompt: string;
  status: 'pending' | 'approved' | 'denied';
  decidedByUserId: string | null;
  decisionNote: string | null;
  createdAtMs: number;
  decidedAtMs: number | null;
}

export type ArtifactKind = 'document' | 'code' | 'image' | 'audio' | 'video' | 'data';

export interface Artifact {
  id: string;
  workspaceId: string;
  name: string;
  kind: ArtifactKind;
  mediaType: string;
  sha256: string;
  sizeBytes: number;
  source: string;
  metadata: unknown;
  revision: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface CredentialReference {
  id: string;
  workspaceId: string;
  name: string;
  service: string;
  account: string;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface EnvironmentVariableRef {
  name: string;
  credentialId: string;
}

export interface EnvironmentDefinition {
  id: string;
  workspaceId: string;
  name: string;
  description: string;
  variables: EnvironmentVariableRef[];
  createdAtMs: number;
  updatedAtMs: number;
}

export type MediaModality = 'image' | 'audio' | 'video';

export interface MediaProvider {
  id: string;
  workspaceId: string;
  name: string;
  modality: MediaModality;
  upstreamUrl: string;
  gatewayPath: string;
  credentialId: string | null;
  enabled: boolean;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface MediaJob {
  id: string;
  providerId: string;
  workspaceId: string;
  modality: MediaModality;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'cancelled' | 'unknown_outcome';
  prompt: string;
  request: unknown;
  response: unknown | null;
  artifactId: string | null;
  lastError: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export type TeamRole = 'owner' | 'admin' | 'member' | 'viewer';

export interface TeamUser {
  id: string;
  workspaceId: string;
  email: string;
  displayName: string;
  role: TeamRole;
  active: boolean;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface AuthSession {
  id: string;
  userId: string;
  expiresAtMs: number;
  createdAtMs: number;
}

export interface LoginResponse {
  token: string;
  session: AuthSession;
  user: TeamUser;
}

export interface TeamPolicy {
  id: string;
  workspaceId: string;
  name: string;
  method: string;
  pathPrefix: string;
  minimumRole: TeamRole;
  effect: 'allow' | 'deny';
  createdAtMs: number;
  updatedAtMs: number;
}

export interface AuditEvent {
  sequence: number;
  workspaceId: string;
  principal: string;
  action: string;
  resource: string;
  outcome: string;
  requestId: string | null;
  metadata: unknown;
  occurredAtMs: number;
  previousHash: string;
  eventHash: string;
}

export interface SyncChange {
  sequence: number;
  workspaceId: string;
  entityType: string;
  entityId: string;
  version: number;
  deleted: boolean;
  payload: unknown;
  deviceId: string;
  clientChangeId: string;
  createdAtMs: number;
}
