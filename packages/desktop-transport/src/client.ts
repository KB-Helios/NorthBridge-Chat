import { parseGenerationEventStream } from './sse';

import type {
  AppendMessageInput,
  Conversation,
  ConversationBranch,
  ConversationPage,
  CreateConversationInput,
  CreateGenerationInput,
  CreateKnowledgeStackInput,
  CreateKnowledgeWatchRootInput,
  CreateSplitChatInput,
  DenseIndexStatus,
  DesktopBootstrap,
  DownloadModelInput,
  GenerationEvent,
  GenerationPerformanceDashboard,
  GenerationRun,
  HardwareDiagnostics,
  ImportModelInput,
  ImportKnowledgeDocumentsInput,
  IngestionJob,
  ForkConversationBranchInput,
  ListConversationsInput,
  Message,
  ManagedModel,
  KnowledgeDocument,
  KnowledgeSearchResult,
  KnowledgeStack,
  KnowledgeWatchRoot,
  ModelCatalogResponse,
  SearchHit,
  SearchKnowledgeInput,
  SelectModelInput,
  SplitChatRun,
  StreamGenerationInput,
  ModelTuneResult,
  TuneModelInput,
  DocumentChunk,
  AgentApproval,
  AgentProfile,
  AgentRun,
  CreateAgentInput,
  CreatePersonaInput,
  CreateSkillInput,
  Crew,
  CrewMember,
  CrewRun,
  McpServer,
  Persona,
  ShadowSubscription,
  ShadowSuggestion,
  Skill,
  Artifact,
  ArtifactKind,
  AuditEvent,
  CredentialReference,
  EnvironmentDefinition,
  EnvironmentVariableRef,
  LoginResponse,
  MediaJob,
  MediaModality,
  MediaProvider,
  SyncChange,
  TeamPolicy,
  TeamRole,
  TeamUser,
  WorkflowApproval,
  WorkflowDefinition,
  WorkflowNode,
  WorkflowNodeRun,
  WorkflowRun,
} from './types';

interface ApiErrorEnvelope {
  error?: {
    code?: string;
    message?: string;
  };
}

type FetchImplementation = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export class DesktopTransportError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'DesktopTransportError';
    this.status = status;
    this.code = code;
  }
}

export class DesktopTransport {
  readonly bootstrap: DesktopBootstrap;
  private readonly fetchImplementation: FetchImplementation;

  constructor(bootstrap: DesktopBootstrap, fetchImplementation: FetchImplementation = fetch) {
    this.bootstrap = bootstrap;
    this.fetchImplementation = fetchImplementation;
  }

  health(): Promise<{ status: string; schemaVersion: number }> {
    return this.request('/v1/health');
  }

  modelCatalog(): Promise<ModelCatalogResponse> {
    return this.request('/v1/models/catalog');
  }

  hardwareDiagnostics(): Promise<HardwareDiagnostics> {
    return this.request('/v1/hardware/diagnostics');
  }

  tuneModel(input: TuneModelInput): Promise<ModelTuneResult> {
    return this.request('/v1/models/tune', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  generationPerformance(limit = 50): Promise<GenerationPerformanceDashboard> {
    const query = new URLSearchParams({ limit: String(limit) });
    return this.request(`/v1/performance/generations?${query.toString()}`);
  }

  selectModel(input: SelectModelInput): Promise<ModelCatalogResponse> {
    return this.request('/v1/models/selection', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  unloadModel(): Promise<ModelCatalogResponse> {
    return this.request('/v1/models/selection', { method: 'DELETE' });
  }

  managedModels(): Promise<ManagedModel[]> {
    return this.request('/v1/models/managed');
  }

  managedModel(modelId: string): Promise<ManagedModel> {
    return this.request(`/v1/models/managed/${encodeURIComponent(modelId)}`);
  }

  downloadModel(input: DownloadModelInput): Promise<ManagedModel> {
    return this.request('/v1/models/downloads', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  cancelModelDownload(modelId: string): Promise<ManagedModel> {
    return this.request(`/v1/models/downloads/${encodeURIComponent(modelId)}/cancel`, {
      method: 'POST',
    });
  }

  importModel(input: ImportModelInput): Promise<ManagedModel> {
    return this.request('/v1/models/imports', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  async removeManagedModel(modelId: string): Promise<void> {
    await this.request(`/v1/models/managed/${encodeURIComponent(modelId)}`, {
      method: 'DELETE',
    });
  }

  createConversation(input: CreateConversationInput = {}): Promise<Conversation> {
    return this.request('/v1/conversations', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listConversations(input: ListConversationsInput = {}): Promise<ConversationPage> {
    const query = new URLSearchParams();
    if (input.workspaceId != null) {
      query.set('workspaceId', input.workspaceId);
    }
    if (input.cursor != null) {
      query.set('cursor', input.cursor);
    }
    if (input.limit != null) {
      query.set('limit', String(input.limit));
    }
    const suffix = query.size > 0 ? `?${query.toString()}` : '';
    return this.request(`/v1/conversations${suffix}`);
  }

  getConversation(conversationId: string): Promise<Conversation> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}`);
  }

  async deleteConversation(conversationId: string): Promise<void> {
    await this.request(`/v1/conversations/${encodeURIComponent(conversationId)}`, {
      method: 'DELETE',
    });
  }

  appendMessage(conversationId: string, input: AppendMessageInput): Promise<Message> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/messages`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listMessages(conversationId: string, branchId?: string): Promise<Message[]> {
    const query = branchId == null ? '' : `?branchId=${encodeURIComponent(branchId)}`;
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/messages${query}`);
  }

  listAllConversationMessages(conversationId: string): Promise<Message[]> {
    return this.request(
      `/v1/conversations/${encodeURIComponent(conversationId)}/messages?allBranches=true`,
    );
  }

  listConversationBranches(conversationId: string): Promise<ConversationBranch[]> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/branches`);
  }

  forkConversationBranch(
    conversationId: string,
    input: ForkConversationBranchInput,
  ): Promise<ConversationBranch> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/branches`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  activateConversationBranch(conversationId: string, branchId: string): Promise<Conversation> {
    return this.request(
      `/v1/conversations/${encodeURIComponent(conversationId)}/branches/${encodeURIComponent(branchId)}/activate`,
      { method: 'POST' },
    );
  }

  createGeneration(conversationId: string, input: CreateGenerationInput): Promise<GenerationRun> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/generations`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listActiveGenerations(conversationId: string): Promise<GenerationRun[]> {
    return this.request(
      `/v1/conversations/${encodeURIComponent(conversationId)}/generations/active`,
    );
  }

  createSplitChat(conversationId: string, input: CreateSplitChatInput): Promise<SplitChatRun> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/split-chats`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listSplitChats(conversationId: string): Promise<SplitChatRun[]> {
    return this.request(`/v1/conversations/${encodeURIComponent(conversationId)}/split-chats`);
  }

  getSplitChat(splitChatId: string): Promise<SplitChatRun> {
    return this.request(`/v1/split-chats/${encodeURIComponent(splitChatId)}`);
  }

  cancelSplitChat(splitChatId: string): Promise<SplitChatRun> {
    return this.request(`/v1/split-chats/${encodeURIComponent(splitChatId)}/cancel`, {
      method: 'POST',
    });
  }

  getGeneration(generationId: string): Promise<GenerationRun> {
    return this.request(`/v1/generations/${encodeURIComponent(generationId)}`);
  }

  startGeneration(generationId: string): Promise<GenerationRun> {
    return this.request(`/v1/generations/${encodeURIComponent(generationId)}/start`, {
      method: 'POST',
    });
  }

  cancelGeneration(generationId: string): Promise<GenerationRun> {
    return this.request(`/v1/generations/${encodeURIComponent(generationId)}/cancel`, {
      method: 'POST',
    });
  }

  searchMessages(query: string, limit = 20): Promise<SearchHit[]> {
    const params = new URLSearchParams({ q: query, limit: String(limit) });
    return this.request(`/v1/search/messages?${params.toString()}`);
  }

  listKnowledgeStacks(workspaceId?: string): Promise<KnowledgeStack[]> {
    const query = workspaceId == null ? '' : `?workspaceId=${encodeURIComponent(workspaceId)}`;
    return this.request(`/v1/knowledge/stacks${query}`);
  }

  createKnowledgeStack(input: CreateKnowledgeStackInput): Promise<KnowledgeStack> {
    return this.request('/v1/knowledge/stacks', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  getKnowledgeStack(stackId: string): Promise<KnowledgeStack> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}`);
  }

  async deleteKnowledgeStack(stackId: string): Promise<void> {
    await this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}`, {
      method: 'DELETE',
    });
  }

  listKnowledgeDocuments(stackId: string): Promise<KnowledgeDocument[]> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/documents`);
  }

  importKnowledgeDocuments(
    stackId: string,
    input: ImportKnowledgeDocumentsInput,
  ): Promise<IngestionJob[]> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/documents/import`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  getKnowledgeDocument(documentId: string): Promise<KnowledgeDocument> {
    return this.request(`/v1/knowledge/documents/${encodeURIComponent(documentId)}`);
  }

  async deleteKnowledgeDocument(documentId: string): Promise<void> {
    await this.request(`/v1/knowledge/documents/${encodeURIComponent(documentId)}`, {
      method: 'DELETE',
    });
  }

  listKnowledgeDocumentChunks(documentId: string): Promise<DocumentChunk[]> {
    return this.request(`/v1/knowledge/documents/${encodeURIComponent(documentId)}/chunks`);
  }

  searchKnowledge(stackId: string, input: SearchKnowledgeInput): Promise<KnowledgeSearchResult> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/search`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listIngestionJobs(stackId: string): Promise<IngestionJob[]> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/jobs`);
  }

  getIngestionJob(jobId: string): Promise<IngestionJob> {
    return this.request(`/v1/knowledge/jobs/${encodeURIComponent(jobId)}`);
  }

  cancelIngestionJob(jobId: string): Promise<IngestionJob> {
    return this.request(`/v1/knowledge/jobs/${encodeURIComponent(jobId)}/cancel`, {
      method: 'POST',
    });
  }

  listKnowledgeWatchRoots(stackId: string): Promise<KnowledgeWatchRoot[]> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/watch-roots`);
  }

  createKnowledgeWatchRoot(
    stackId: string,
    input: CreateKnowledgeWatchRootInput,
  ): Promise<KnowledgeWatchRoot> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/watch-roots`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  getKnowledgeWatchRoot(rootId: string): Promise<KnowledgeWatchRoot> {
    return this.request(`/v1/knowledge/watch-roots/${encodeURIComponent(rootId)}`);
  }

  async deleteKnowledgeWatchRoot(rootId: string): Promise<void> {
    await this.request(`/v1/knowledge/watch-roots/${encodeURIComponent(rootId)}`, {
      method: 'DELETE',
    });
  }

  rescanKnowledgeWatchRoot(rootId: string): Promise<KnowledgeWatchRoot> {
    return this.request(`/v1/knowledge/watch-roots/${encodeURIComponent(rootId)}/rescan`, {
      method: 'POST',
    });
  }

  knowledgeIndexStatus(stackId: string): Promise<DenseIndexStatus> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/index`);
  }

  rebuildKnowledgeIndex(stackId: string): Promise<DenseIndexStatus> {
    return this.request(`/v1/knowledge/stacks/${encodeURIComponent(stackId)}/index`, {
      method: 'POST',
    });
  }

  listPersonas(): Promise<Persona[]> {
    return this.request('/v1/agents/personas');
  }

  createPersona(input: CreatePersonaInput): Promise<Persona> {
    return this.request('/v1/agents/personas', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listSkills(): Promise<Skill[]> {
    return this.request('/v1/agents/skills');
  }

  createSkill(input: CreateSkillInput): Promise<Skill> {
    return this.request('/v1/agents/skills', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listAgents(): Promise<AgentProfile[]> {
    return this.request('/v1/agents');
  }

  createAgent(input: CreateAgentInput): Promise<AgentProfile> {
    return this.request('/v1/agents', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listAgentRuns(limit = 50): Promise<AgentRun[]> {
    return this.request(`/v1/agents/runs?limit=${encodeURIComponent(limit)}`);
  }

  startAgentRun(input: {
    agentId: string;
    conversationId?: string | null;
    prompt: string;
  }): Promise<AgentRun> {
    return this.request('/v1/agents/runs', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  cancelAgentRun(runId: string): Promise<{ cancelRequested: boolean }> {
    return this.request(`/v1/agents/runs/${encodeURIComponent(runId)}/cancel`, {
      method: 'POST',
    });
  }

  listAgentApprovals(): Promise<AgentApproval[]> {
    return this.request('/v1/agents/approvals');
  }

  decideAgentApproval(approvalId: string, approved: boolean, note?: string): Promise<AgentRun> {
    return this.request(`/v1/agents/approvals/${encodeURIComponent(approvalId)}/decision`, {
      method: 'POST',
      body: JSON.stringify({ approved, note }),
    });
  }

  listMcpServers(): Promise<McpServer[]> {
    return this.request('/v1/agents/mcp/servers');
  }

  syncAgentProtocols(): Promise<unknown[]> {
    return this.request('/v1/agents/mcp/sync', { method: 'POST' });
  }

  listCrews(): Promise<Crew[]> {
    return this.request('/v1/agents/crews');
  }

  createCrew(input: {
    name: string;
    description?: string;
    coordinatorAgentId: string;
    members?: CrewMember[];
  }): Promise<Crew> {
    return this.request('/v1/agents/crews', {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  listCrewRuns(): Promise<CrewRun[]> {
    return this.request('/v1/agents/crew-runs');
  }

  startCrewRun(crewId: string, prompt: string): Promise<CrewRun> {
    return this.request(`/v1/agents/crews/${encodeURIComponent(crewId)}/runs`, {
      method: 'POST',
      body: JSON.stringify({ prompt }),
    });
  }

  listShadowSubscriptions(): Promise<ShadowSubscription[]> {
    return this.request('/v1/agents/shadows');
  }

  createShadowSubscription(conversationId: string, agentId: string): Promise<ShadowSubscription> {
    return this.request('/v1/agents/shadows', {
      method: 'POST',
      body: JSON.stringify({ conversationId, agentId }),
    });
  }

  listShadowSuggestions(subscriptionId: string): Promise<ShadowSuggestion[]> {
    return this.request(`/v1/agents/shadows/${encodeURIComponent(subscriptionId)}/suggestions`);
  }

  decideShadowSuggestion(suggestionId: string, accepted: boolean): Promise<ShadowSuggestion> {
    return this.request(
      `/v1/agents/shadow-suggestions/${encodeURIComponent(suggestionId)}/decision`,
      {
        method: 'POST',
        body: JSON.stringify({ accepted }),
      },
    );
  }

  listWorkflows(): Promise<WorkflowDefinition[]> {
    return this.request('/v1/workflows');
  }

  createWorkflow(input: {
    name: string;
    description?: string;
    entryNodeId: string;
    nodes: WorkflowNode[];
  }): Promise<WorkflowDefinition> {
    return this.request('/v1/workflows', { method: 'POST', body: JSON.stringify(input) });
  }

  listWorkflowRuns(): Promise<WorkflowRun[]> {
    return this.request('/v1/workflow-runs');
  }

  startWorkflow(workflowId: string, input: unknown = {}): Promise<WorkflowRun> {
    return this.request(`/v1/workflows/${encodeURIComponent(workflowId)}/runs`, {
      method: 'POST',
      body: JSON.stringify({ input }),
    });
  }

  listWorkflowNodeRuns(runId: string): Promise<WorkflowNodeRun[]> {
    return this.request(`/v1/workflow-runs/${encodeURIComponent(runId)}/nodes`);
  }

  controlWorkflow(
    runId: string,
    action: 'pause' | 'resume' | 'retry' | 'cancel',
  ): Promise<WorkflowRun> {
    return this.request(`/v1/workflow-runs/${encodeURIComponent(runId)}/${action}`, {
      method: 'POST',
    });
  }

  jumpWorkflow(runId: string, nodeId: string): Promise<WorkflowRun> {
    return this.request(`/v1/workflow-runs/${encodeURIComponent(runId)}/jump`, {
      method: 'POST',
      body: JSON.stringify({ nodeId }),
    });
  }

  listWorkflowApprovals(): Promise<WorkflowApproval[]> {
    return this.request('/v1/workflow-approvals');
  }

  decideWorkflowApproval(
    approvalId: string,
    approved: boolean,
    note?: string,
  ): Promise<WorkflowRun> {
    return this.request(`/v1/workflow-approvals/${encodeURIComponent(approvalId)}/decision`, {
      method: 'POST',
      body: JSON.stringify({ approved, note }),
    });
  }

  listArtifacts(): Promise<Artifact[]> {
    return this.request('/v1/artifacts');
  }

  createArtifact(input: {
    name: string;
    kind: ArtifactKind;
    mediaType: string;
    source?: string;
    metadata?: unknown;
    contentBase64: string;
  }): Promise<Artifact> {
    return this.request('/v1/artifacts', { method: 'POST', body: JSON.stringify(input) });
  }

  reviseArtifact(
    artifactId: string,
    input: {
      name: string;
      kind: ArtifactKind;
      mediaType: string;
      source?: string;
      metadata?: unknown;
      contentBase64: string;
    },
  ): Promise<Artifact> {
    return this.request(`/v1/artifacts/${encodeURIComponent(artifactId)}`, {
      method: 'POST',
      body: JSON.stringify(input),
    });
  }

  async artifactContent(artifactId: string): Promise<ArrayBuffer> {
    const response = await this.callFetch(
      this.url(`/v1/artifacts/${encodeURIComponent(artifactId)}/content`),
      { headers: this.headers() },
    );
    await this.assertOk(response);
    return response.arrayBuffer();
  }

  listCredentials(): Promise<CredentialReference[]> {
    return this.request('/v1/credentials');
  }

  createCredential(name: string, secret: string): Promise<CredentialReference> {
    return this.request('/v1/credentials', {
      method: 'POST',
      body: JSON.stringify({ name, secret }),
    });
  }

  updateCredential(credentialId: string, secret: string): Promise<CredentialReference> {
    return this.request(`/v1/credentials/${encodeURIComponent(credentialId)}`, {
      method: 'POST',
      body: JSON.stringify({ secret }),
    });
  }

  listEnvironments(): Promise<EnvironmentDefinition[]> {
    return this.request('/v1/environments');
  }

  createEnvironment(input: {
    name: string;
    description?: string;
    variables?: EnvironmentVariableRef[];
  }): Promise<EnvironmentDefinition> {
    return this.request('/v1/environments', { method: 'POST', body: JSON.stringify(input) });
  }

  listMediaProviders(): Promise<MediaProvider[]> {
    return this.request('/v1/media/providers');
  }

  createMediaProvider(input: {
    name: string;
    modality: MediaModality;
    upstreamUrl: string;
    credentialId?: string | null;
    enabled?: boolean;
  }): Promise<MediaProvider> {
    return this.request('/v1/media/providers', { method: 'POST', body: JSON.stringify(input) });
  }

  listMediaJobs(): Promise<MediaJob[]> {
    return this.request('/v1/media/jobs');
  }

  createMediaJob(providerId: string, prompt: string, request: unknown = {}): Promise<MediaJob> {
    return this.request('/v1/media/jobs', {
      method: 'POST',
      body: JSON.stringify({ providerId, prompt, request }),
    });
  }

  exportPackage(name: string, description = ''): Promise<unknown> {
    return this.request('/v1/packages/export', {
      method: 'POST',
      body: JSON.stringify({ name, description }),
    });
  }

  importPackage(packageManifest: unknown): Promise<unknown> {
    return this.request('/v1/packages/import', {
      method: 'POST',
      body: JSON.stringify(packageManifest),
    });
  }

  listTeamUsers(): Promise<TeamUser[]> {
    return this.request('/v1/team/users');
  }

  bootstrapTeamOwner(email: string, displayName: string, password: string): Promise<TeamUser> {
    return this.request('/v1/auth/bootstrap', {
      method: 'POST',
      body: JSON.stringify({ email, displayName, password, role: 'owner' }),
    });
  }

  createTeamUser(input: {
    email: string;
    displayName: string;
    password: string;
    role: TeamRole;
  }): Promise<TeamUser> {
    return this.request('/v1/team/users', { method: 'POST', body: JSON.stringify(input) });
  }

  listTeamPolicies(): Promise<TeamPolicy[]> {
    return this.request('/v1/team/policies');
  }

  createTeamPolicy(input: {
    name: string;
    method: string;
    pathPrefix: string;
    minimumRole: TeamRole;
    effect: 'allow' | 'deny';
  }): Promise<TeamPolicy> {
    return this.request('/v1/team/policies', { method: 'POST', body: JSON.stringify(input) });
  }

  listAuditEvents(after = 0, limit = 200): Promise<AuditEvent[]> {
    const query = new URLSearchParams({ after: String(after), limit: String(limit) });
    return this.request(`/v1/audit/events?${query.toString()}`);
  }

  verifyAuditChain(): Promise<{ valid: boolean }> {
    return this.request('/v1/audit/verify');
  }

  pushSyncChanges(
    changes: Array<{
      entityType: string;
      entityId: string;
      baseVersion: number;
      deleted?: boolean;
      payload?: unknown;
      deviceId: string;
      clientChangeId: string;
    }>,
  ): Promise<SyncChange[]> {
    return this.request('/v1/sync/push', {
      method: 'POST',
      body: JSON.stringify({ changes }),
    });
  }

  pullSyncChanges(after = 0, limit = 500): Promise<SyncChange[]> {
    const query = new URLSearchParams({ after: String(after), limit: String(limit) });
    return this.request(`/v1/sync/pull?${query.toString()}`);
  }

  async *streamGenerationEvents(
    generationId: string,
    input: StreamGenerationInput = {},
  ): AsyncGenerator<GenerationEvent> {
    const query =
      input.afterSequence == null ? '' : `?after=${encodeURIComponent(input.afterSequence)}`;
    const response = await this.callFetch(
      this.url(`/v1/generations/${encodeURIComponent(generationId)}/events${query}`),
      {
        method: 'GET',
        headers: this.headers({ Accept: 'text/event-stream' }),
        signal: input.signal,
      },
    );
    await this.assertOk(response);
    if (response.body == null) {
      throw new DesktopTransportError(0, 'empty_stream', 'desktop-core returned an empty stream');
    }
    yield* parseGenerationEventStream(response.body);
  }

  private async request<TResponse>(path: string, options: RequestInit = {}): Promise<TResponse> {
    const response = await this.callFetch(this.url(path), {
      ...options,
      headers: this.headers(options.headers),
    });
    await this.assertOk(response);
    if (response.status === 204) {
      return undefined as TResponse;
    }
    return (await response.json()) as TResponse;
  }

  private url(path: string): string {
    return new URL(path, `${this.bootstrap.baseUrl}/`).toString();
  }

  private callFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    return this.fetchImplementation.call(globalThis, input, init);
  }

  private headers(additional?: HeadersInit): Headers {
    const headers = new Headers(additional);
    headers.set('Authorization', `Bearer ${this.bootstrap.sessionToken}`);
    if (!headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }
    return headers;
  }

  private async assertOk(response: Response): Promise<void> {
    if (response.ok) {
      return;
    }
    const body = (await response
      .clone()
      .json()
      .catch(() => null)) as ApiErrorEnvelope | null;
    throw new DesktopTransportError(
      response.status,
      body?.error?.code ?? 'request_failed',
      body?.error?.message ?? `desktop-core request failed with HTTP ${response.status}`,
    );
  }
}

export function createRemoteDesktopTransport(
  baseUrl: string,
  sessionToken: string,
  fetchImplementation: FetchImplementation = fetch,
): DesktopTransport {
  const url = validatedRemoteUrl(baseUrl);
  if (sessionToken.length < 32 || /\s/.test(sessionToken)) {
    throw new Error('remote session token is malformed');
  }
  return new DesktopTransport(
    { baseUrl: url.toString().replace(/\/$/, ''), sessionToken, pid: 0, schemaVersion: 0 },
    fetchImplementation,
  );
}

export async function loginRemoteCore(
  baseUrl: string,
  email: string,
  password: string,
  fetchImplementation: FetchImplementation = fetch,
): Promise<LoginResponse> {
  const url = new URL('/v1/auth/login', validatedRemoteUrl(baseUrl));
  const response = await fetchImplementation.call(globalThis, url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!response.ok) {
    throw new DesktopTransportError(response.status, 'remote_login_failed', 'Remote login failed');
  }
  return (await response.json()) as LoginResponse;
}

function validatedRemoteUrl(baseUrl: string): URL {
  const url = new URL(baseUrl);
  const loopback = ['127.0.0.1', '::1', '[::1]', 'localhost'].includes(url.hostname);
  if (url.protocol !== 'https:' && !(loopback && url.protocol === 'http:')) {
    throw new Error('remote core connections require HTTPS; HTTP is allowed only on loopback');
  }
  if (url.username !== '' || url.password !== '' || url.search !== '' || url.hash !== '') {
    throw new Error('remote core URL cannot contain credentials, query, or fragment');
  }
  return url;
}
