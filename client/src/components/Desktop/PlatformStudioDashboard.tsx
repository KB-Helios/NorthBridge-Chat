import { useCallback, useMemo, useState, type ReactNode } from 'react';
import { Boxes, Check, Download, Play, RefreshCw, Shield, Upload, X } from 'lucide-react';
import {
  Button,
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  Spinner,
} from '@librechat/client';
import {
  createRemoteDesktopTransport,
  getDesktopTransport,
  isDesktopRuntime,
  loginRemoteCore,
  type Artifact,
  type AuditEvent,
  type CredentialReference,
  type EnvironmentDefinition,
  type MediaJob,
  type MediaModality,
  type MediaProvider,
  type TeamPolicy,
  type TeamUser,
  type WorkflowApproval,
  type WorkflowDefinition,
  type WorkflowNode,
  type WorkflowRun,
} from '@northbridge/desktop-transport';
import { useLocalize } from '~/hooks';

export default function PlatformStudioDashboard() {
  const localize = useLocalize();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [workflows, setWorkflows] = useState<WorkflowDefinition[]>([]);
  const [runs, setRuns] = useState<WorkflowRun[]>([]);
  const [approvals, setApprovals] = useState<WorkflowApproval[]>([]);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [credentials, setCredentials] = useState<CredentialReference[]>([]);
  const [environments, setEnvironments] = useState<EnvironmentDefinition[]>([]);
  const [providers, setProviders] = useState<MediaProvider[]>([]);
  const [mediaJobs, setMediaJobs] = useState<MediaJob[]>([]);
  const [users, setUsers] = useState<TeamUser[]>([]);
  const [policies, setPolicies] = useState<TeamPolicy[]>([]);
  const [audit, setAudit] = useState<AuditEvent[]>([]);
  const [auditValid, setAuditValid] = useState<boolean | null>(null);
  const [name, setName] = useState('Review gate');
  const [editor, setEditor] = useState('# Forge artifact\n\nEdit and version this local artifact.');
  const [secretName, setSecretName] = useState('media-api-key');
  const [secret, setSecret] = useState('');
  const [providerUrl, setProviderUrl] = useState('https://api.example.com/v1/images/generations');
  const [modality, setModality] = useState<MediaModality>('image');
  const [mediaPrompt, setMediaPrompt] = useState('A calm Nordic bridge at blue hour');
  const [email, setEmail] = useState('owner@example.com');
  const [password, setPassword] = useState('');
  const [packageJson, setPackageJson] = useState('');
  const [remoteUrl, setRemoteUrl] = useState('https://northbridge.example.com');
  const [remoteStatus, setRemoteStatus] = useState('');

  const refresh = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null) return;
    setLoading(true);
    setError(null);
    try {
      const [
        nextWorkflows,
        nextRuns,
        nextApprovals,
        nextArtifacts,
        nextCredentials,
        nextEnvironments,
        nextProviders,
        nextMediaJobs,
        nextUsers,
        nextPolicies,
        nextAudit,
        verification,
      ] = await Promise.all([
        transport.listWorkflows(),
        transport.listWorkflowRuns(),
        transport.listWorkflowApprovals(),
        transport.listArtifacts(),
        transport.listCredentials(),
        transport.listEnvironments(),
        transport.listMediaProviders(),
        transport.listMediaJobs(),
        transport.listTeamUsers(),
        transport.listTeamPolicies(),
        transport.listAuditEvents(0, 50),
        transport.verifyAuditChain(),
      ]);
      setWorkflows(nextWorkflows);
      setRuns(nextRuns);
      setApprovals(nextApprovals);
      setArtifacts(nextArtifacts);
      setCredentials(nextCredentials);
      setEnvironments(nextEnvironments);
      setProviders(nextProviders);
      setMediaJobs(nextMediaJobs);
      setUsers(nextUsers);
      setPolicies(nextPolicies);
      setAudit(nextAudit);
      setAuditValid(verification.valid);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLoading(false);
    }
  }, []);

  const runAction = useCallback(
    async (action: () => Promise<unknown>) => {
      setSaving(true);
      setError(null);
      try {
        await action();
        await refresh();
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        setSaving(false);
      }
    },
    [refresh],
  );

  const activeRun = useMemo(
    () => runs.find((run) => !['completed', 'failed', 'cancelled'].includes(run.status)),
    [runs],
  );

  if (!isDesktopRuntime()) return null;

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (next) void refresh();
      }}
    >
      <button
        type="button"
        className="flex h-9 w-9 items-center justify-center rounded-xl border border-border-light bg-presentation text-text-secondary hover:bg-surface-active-alt hover:text-text-primary"
        aria-label={localize('com_ui_desktop_platform_studio')}
        title={localize('com_ui_desktop_platform_studio')}
        onClick={() => setOpen(true)}
      >
        <Boxes className="h-4 w-4" aria-hidden="true" />
      </button>
      <DialogContent className="max-h-[92vh] w-[min(96vw,1180px)] max-w-7xl overflow-y-auto bg-surface-primary text-text-primary shadow-2xl">
        <DialogHeader>
          <div className="flex items-center justify-between gap-4 pr-8">
            <div>
              <DialogTitle>{localize('com_ui_desktop_platform_studio')}</DialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {localize('com_ui_desktop_platform_description')}
              </p>
            </div>
            <Button type="button" variant="outline" size="sm" disabled={loading} onClick={refresh}>
              <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              {localize('com_ui_desktop_platform_refresh')}
            </Button>
          </div>
        </DialogHeader>

        {loading && workflows.length === 0 ? (
          <div className="flex min-h-64 items-center justify-center">
            <Spinner />
          </div>
        ) : (
          <div className="space-y-4 py-2">
            {error != null && (
              <div
                role="alert"
                className="rounded-xl border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-700 dark:text-red-300"
              >
                {error}
              </div>
            )}

            <div className="grid gap-4 xl:grid-cols-2">
              <Panel
                title={localize('com_ui_desktop_platform_turnstiles')}
                meta={localize('com_ui_desktop_platform_count', { 0: workflows.length })}
              >
                <TextField value={name} onChange={setName} />
                <div className="flex flex-wrap gap-2">
                  <Button
                    disabled={saving}
                    onClick={() =>
                      void runAction(async () => {
                        const transport = requiredTransport();
                        const nodes: WorkflowNode[] = [
                          {
                            id: 'snapshot',
                            kind: 'transform',
                            operation: { operation: 'identity' },
                            next: 'approval',
                          },
                          {
                            id: 'approval',
                            kind: 'approval',
                            title: name,
                            prompt: 'Approve the durable workflow input: ${input}',
                            next: 'result',
                          },
                          {
                            id: 'result',
                            kind: 'transform',
                            operation: { operation: 'template', template: 'Approved: ${input}' },
                          },
                        ];
                        await transport.createWorkflow({
                          name,
                          description: localize('com_ui_desktop_platform_workflow_sample'),
                          entryNodeId: 'snapshot',
                          nodes,
                        });
                      })
                    }
                  >
                    {localize('com_ui_desktop_platform_create_workflow')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving || workflows.length === 0}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().startWorkflow(workflows[0].id, {
                          requestedAt: new Date().toISOString(),
                          artifactCount: artifacts.length,
                        }),
                      )
                    }
                  >
                    <Play className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_platform_run_workflow')}
                  </Button>
                  {activeRun != null && (
                    <Button
                      variant="outline"
                      disabled={saving}
                      onClick={() =>
                        void runAction(() =>
                          requiredTransport().controlWorkflow(activeRun.id, 'pause'),
                        )
                      }
                    >
                      {localize('com_ui_desktop_platform_pause')}
                    </Button>
                  )}
                </div>
                {approvals.map((approval) => (
                  <div
                    key={approval.id}
                    className="flex items-center justify-between rounded-lg bg-surface-secondary p-3 text-sm"
                  >
                    <span>{approval.prompt}</span>
                    <div className="flex gap-2">
                      <Button
                        size="sm"
                        onClick={() =>
                          void runAction(() =>
                            requiredTransport().decideWorkflowApproval(approval.id, true),
                          )
                        }
                      >
                        <Check className="mr-1 h-4 w-4" />
                        {localize('com_ui_desktop_platform_approve')}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() =>
                          void runAction(() =>
                            requiredTransport().decideWorkflowApproval(approval.id, false),
                          )
                        }
                      >
                        <X className="mr-1 h-4 w-4" />
                        {localize('com_ui_desktop_platform_deny')}
                      </Button>
                    </div>
                  </div>
                ))}
                {runs.slice(0, 4).map((run) => (
                  <Status
                    key={run.id}
                    title={run.currentNodeId ?? run.workflowId}
                    status={run.status}
                  />
                ))}
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_forge')}
                meta={localize('com_ui_desktop_platform_count', { 0: artifacts.length })}
              >
                <textarea
                  className="min-h-36 w-full rounded-lg border border-border-light bg-surface-secondary p-3 font-mono text-sm"
                  value={editor}
                  onChange={(event) => setEditor(event.target.value)}
                />
                <div className="flex gap-2">
                  <Button
                    disabled={saving}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().createArtifact({
                          name: `${name}.md`,
                          kind: 'document',
                          mediaType: 'text/markdown',
                          contentBase64: encodeUtf8(editor),
                        }),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_save_artifact')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving || artifacts.length === 0}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().reviseArtifact(artifacts[0].id, {
                          name: artifacts[0].name,
                          kind: artifacts[0].kind,
                          mediaType: artifacts[0].mediaType,
                          source: 'forge',
                          metadata: artifacts[0].metadata,
                          contentBase64: encodeUtf8(editor),
                        }),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_new_revision')}
                  </Button>
                </div>
                {artifacts.slice(0, 5).map((artifact) => (
                  <Status
                    key={artifact.id}
                    title={artifact.name}
                    status={localize('com_ui_desktop_platform_revision', { 0: artifact.revision })}
                  />
                ))}
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_environments')}
                meta={localize('com_ui_desktop_platform_environment_meta', {
                  0: credentials.length,
                  1: environments.length,
                })}
              >
                <TextField value={secretName} onChange={setSecretName} />
                <input
                  type="password"
                  autoComplete="new-password"
                  className="w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
                  value={secret}
                  placeholder={localize('com_ui_desktop_platform_secret_placeholder')}
                  onChange={(event) => setSecret(event.target.value)}
                />
                <Button
                  disabled={saving || secret === ''}
                  onClick={() =>
                    void runAction(async () => {
                      const transport = requiredTransport();
                      const credential = await transport.createCredential(secretName, secret);
                      await transport.createEnvironment({
                        name: `${name} environment`,
                        variables: [{ name: 'MEDIA_API_KEY', credentialId: credential.id }],
                      });
                      setSecret('');
                    })
                  }
                >
                  <Shield className="mr-2 h-4 w-4" />
                  {localize('com_ui_desktop_platform_store_secret')}
                </Button>
                <p className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_platform_secret_note')}
                </p>
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_media')}
                meta={localize('com_ui_desktop_platform_media_meta', {
                  0: providers.length,
                  1: mediaJobs.length,
                })}
              >
                <TextField value={providerUrl} onChange={setProviderUrl} />
                <select
                  className="w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
                  value={modality}
                  onChange={(event) => setModality(event.target.value as MediaModality)}
                >
                  <option value="image">{localize('com_ui_desktop_platform_image')}</option>
                  <option value="audio">{localize('com_ui_desktop_platform_audio')}</option>
                  <option value="video">{localize('com_ui_desktop_platform_video')}</option>
                </select>
                <TextField value={mediaPrompt} onChange={setMediaPrompt} />
                <div className="flex gap-2">
                  <Button
                    disabled={saving}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().createMediaProvider({
                          name: `${name} ${modality}`,
                          modality,
                          upstreamUrl: providerUrl,
                          credentialId: credentials[0]?.id,
                        }),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_add_provider')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving || providers.length === 0}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().createMediaJob(providers[0].id, mediaPrompt),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_generate')}
                  </Button>
                </div>
                {mediaJobs.slice(0, 4).map((job) => (
                  <Status key={job.id} title={job.prompt} status={job.status} />
                ))}
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_packages')}
                meta={localize('com_ui_desktop_platform_packages_meta')}
              >
                <div className="flex gap-2">
                  <Button
                    disabled={saving}
                    onClick={() =>
                      void runAction(async () => {
                        const manifest = await requiredTransport().exportPackage(name);
                        setPackageJson(JSON.stringify(manifest, null, 2));
                      })
                    }
                  >
                    <Download className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_platform_export')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving || packageJson.trim() === ''}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().importPackage(JSON.parse(packageJson) as unknown),
                      )
                    }
                  >
                    <Upload className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_platform_import')}
                  </Button>
                </div>
                <textarea
                  className="min-h-32 w-full rounded-lg border border-border-light bg-surface-secondary p-3 font-mono text-xs"
                  value={packageJson}
                  placeholder={localize('com_ui_desktop_platform_package_placeholder')}
                  onChange={(event) => setPackageJson(event.target.value)}
                />
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_team')}
                meta={localize('com_ui_desktop_platform_team_meta', {
                  0: users.length,
                  1: policies.length,
                })}
              >
                <TextField value={email} onChange={setEmail} />
                <input
                  type="password"
                  autoComplete="new-password"
                  className="w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
                  value={password}
                  placeholder={localize('com_ui_desktop_platform_password_placeholder')}
                  onChange={(event) => setPassword(event.target.value)}
                />
                <div className="flex flex-wrap gap-2">
                  <Button
                    disabled={saving || password.length < 12 || users.length > 0}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().bootstrapTeamOwner(email, name, password),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_bootstrap_owner')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving}
                    onClick={() =>
                      void runAction(() =>
                        requiredTransport().createTeamPolicy({
                          name: 'Members read knowledge',
                          method: 'GET',
                          pathPrefix: '/v1/knowledge/',
                          minimumRole: 'member',
                          effect: 'allow',
                        }),
                      )
                    }
                  >
                    {localize('com_ui_desktop_platform_add_policy')}
                  </Button>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  {auditValid ? (
                    <Check className="h-4 w-4 text-green-600" />
                  ) : (
                    <X className="h-4 w-4 text-red-600" />
                  )}
                  {localize('com_ui_desktop_platform_audit_meta', { 0: audit.length })}
                </div>
              </Panel>

              <Panel
                title={localize('com_ui_desktop_platform_remote')}
                meta={localize('com_ui_desktop_platform_remote_meta')}
              >
                <TextField value={remoteUrl} onChange={setRemoteUrl} />
                <Button
                  variant="outline"
                  disabled={saving || password === ''}
                  onClick={() =>
                    void runAction(async () => {
                      const login = await loginRemoteCore(remoteUrl, email, password);
                      const remote = createRemoteDesktopTransport(remoteUrl, login.token);
                      const health = await remote.health();
                      setRemoteStatus(`${health.status} · schema ${health.schemaVersion}`);
                    })
                  }
                >
                  {localize('com_ui_desktop_platform_test_remote')}
                </Button>
                {remoteStatus !== '' && <Status title={remoteUrl} status={remoteStatus} />}
                <p className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_platform_remote_note')}
                </p>
              </Panel>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function requiredTransport() {
  const transport = getDesktopTransport();
  if (transport == null) throw new Error('desktop transport is unavailable');
  return transport;
}

function Panel({ title, meta, children }: { title: string; meta: string; children: ReactNode }) {
  return (
    <section className="space-y-3 rounded-xl border border-border-light p-4">
      <div className="flex items-center justify-between gap-3">
        <h3 className="font-semibold">{title}</h3>
        <span className="text-xs text-text-secondary">{meta}</span>
      </div>
      {children}
    </section>
  );
}

function TextField({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  return (
    <input
      className="w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  );
}

function Status({ title, status }: { title: string; status: string }) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-lg bg-surface-secondary p-3 text-sm">
      <span className="line-clamp-2">{title}</span>
      <span className="shrink-0 text-xs uppercase tracking-wide text-text-secondary">
        {status.replaceAll('_', ' ')}
      </span>
    </div>
  );
}

function encodeUtf8(value: string): string {
  const bytes = new TextEncoder().encode(value);
  const chunks: string[] = [];
  const chunkSize = 32 * 1024;
  for (let offset = 0; offset < bytes.length; offset += chunkSize) {
    chunks.push(String.fromCharCode(...bytes.subarray(offset, offset + chunkSize)));
  }
  return window.btoa(chunks.join(''));
}
