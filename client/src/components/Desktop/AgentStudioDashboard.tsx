import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { Bot, Check, RefreshCw, ShieldCheck, Users, X } from 'lucide-react';
import { Constants } from 'librechat-data-provider';
import {
  Button,
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  Spinner,
} from '@librechat/client';
import {
  getDesktopTransport,
  isDesktopRuntime,
  type AgentApproval,
  type AgentProfile,
  type AgentRun,
  type Crew,
  type CrewRun,
  type McpServer,
  type Persona,
  type ShadowSubscription,
  type ShadowSuggestion,
  type Skill,
} from '@northbridge/desktop-transport';
import { useChatContext } from '~/Providers';
import { useLocalize } from '~/hooks';

export default function AgentStudioDashboard() {
  const localize = useLocalize();
  const { conversation } = useChatContext();
  const conversationId = conversation?.conversationId;
  const persistedConversation =
    conversationId != null && conversationId !== Constants.NEW_CONVO ? conversationId : null;
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [personas, setPersonas] = useState<Persona[]>([]);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [agents, setAgents] = useState<AgentProfile[]>([]);
  const [runs, setRuns] = useState<AgentRun[]>([]);
  const [approvals, setApprovals] = useState<AgentApproval[]>([]);
  const [servers, setServers] = useState<McpServer[]>([]);
  const [crews, setCrews] = useState<Crew[]>([]);
  const [crewRuns, setCrewRuns] = useState<CrewRun[]>([]);
  const [shadows, setShadows] = useState<ShadowSubscription[]>([]);
  const [suggestions, setSuggestions] = useState<ShadowSuggestion[]>([]);
  const [name, setName] = useState('Research coach');
  const [instructions, setInstructions] = useState(
    'Be concise, cite uncertainty, and state the safest next action.',
  );
  const [prompt, setPrompt] = useState('Review this task and propose a robust next step.');
  const [selectedAgentId, setSelectedAgentId] = useState('');
  const [selectedCrewId, setSelectedCrewId] = useState('');

  const refresh = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null) return;
    setLoading(true);
    setError(null);
    try {
      const [
        nextPersonas,
        nextSkills,
        nextAgents,
        nextRuns,
        nextApprovals,
        nextServers,
        nextCrews,
        nextCrewRuns,
        nextShadows,
      ] = await Promise.all([
        transport.listPersonas(),
        transport.listSkills(),
        transport.listAgents(),
        transport.listAgentRuns(),
        transport.listAgentApprovals(),
        transport.listMcpServers(),
        transport.listCrews(),
        transport.listCrewRuns(),
        transport.listShadowSubscriptions(),
      ]);
      const nextSuggestions = (
        await Promise.all(nextShadows.map((shadow) => transport.listShadowSuggestions(shadow.id)))
      ).flat();
      setPersonas(nextPersonas);
      setSkills(nextSkills);
      setAgents(nextAgents);
      setRuns(nextRuns);
      setApprovals(nextApprovals);
      setServers(nextServers);
      setCrews(nextCrews);
      setCrewRuns(nextCrewRuns);
      setShadows(nextShadows);
      setSuggestions(nextSuggestions);
      setSelectedAgentId((current) => current || nextAgents[0]?.id || '');
      setSelectedCrewId((current) => current || nextCrews[0]?.id || '');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    void refresh();
    const interval = window.setInterval(() => void refresh(), 2000);
    return () => window.clearInterval(interval);
  }, [open, refresh]);

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

  const selectedAgent = useMemo(
    () => agents.find((agent) => agent.id === selectedAgentId) ?? null,
    [agents, selectedAgentId],
  );

  if (!isDesktopRuntime()) return null;

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <button
        type="button"
        className="flex h-9 w-9 items-center justify-center rounded-xl border border-border-light bg-presentation text-text-secondary hover:bg-surface-active-alt hover:text-text-primary"
        aria-label={localize('com_ui_desktop_agent_studio')}
        title={localize('com_ui_desktop_agent_studio')}
        onClick={() => setOpen(true)}
      >
        <Bot className="h-4 w-4" aria-hidden="true" />
      </button>
      <DialogContent className="max-h-[90vh] w-[min(96vw,1080px)] max-w-6xl overflow-y-auto bg-surface-primary text-text-primary shadow-2xl">
        <DialogHeader>
          <div className="flex items-center justify-between gap-4 pr-8">
            <div>
              <DialogTitle>{localize('com_ui_desktop_agent_studio')}</DialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {localize('com_ui_desktop_agent_studio_description')}
              </p>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={loading}
              onClick={() => void refresh()}
            >
              <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              {localize('com_ui_desktop_agent_refresh')}
            </Button>
          </div>
        </DialogHeader>

        {loading && agents.length === 0 ? (
          <div className="flex min-h-64 items-center justify-center">
            <Spinner />
          </div>
        ) : (
          <div className="space-y-5 py-2">
            {error != null && (
              <div
                role="alert"
                className="rounded-xl border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-700 dark:text-red-300"
              >
                {error}
              </div>
            )}

            <section className="grid gap-3 lg:grid-cols-3">
              <StudioCard title={localize('com_ui_desktop_agent_persona')} count={personas.length}>
                <TextInput
                  value={name}
                  onChange={setName}
                  label={localize('com_ui_desktop_agent_name')}
                />
                <TextArea
                  value={instructions}
                  onChange={setInstructions}
                  label={localize('com_ui_desktop_agent_system_prompt')}
                />
                <Button
                  disabled={saving}
                  onClick={() =>
                    void runAction(async () => {
                      const transport = getDesktopTransport();
                      if (transport == null) return;
                      await transport.createPersona({ name, systemPrompt: instructions });
                    })
                  }
                >
                  {localize('com_ui_desktop_agent_create_persona')}
                </Button>
              </StudioCard>

              <StudioCard title={localize('com_ui_desktop_agent_skill')} count={skills.length}>
                <p className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_agent_skill_description')}
                </p>
                <Button
                  disabled={saving}
                  onClick={() =>
                    void runAction(async () => {
                      const transport = getDesktopTransport();
                      if (transport == null) return;
                      const slug =
                        name
                          .toLowerCase()
                          .replace(/[^a-z0-9_-]+/g, '-')
                          .replace(/^-|-$/g, '') || 'skill';
                      await transport.createSkill({
                        slug,
                        name,
                        instructions,
                        permissionManifest: {},
                      });
                    })
                  }
                >
                  {localize('com_ui_desktop_agent_create_skill')}
                </Button>
              </StudioCard>

              <StudioCard title={localize('com_ui_desktop_agent_agent')} count={agents.length}>
                <p className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_agent_agent_description')}
                </p>
                <Button
                  disabled={saving}
                  onClick={() =>
                    void runAction(async () => {
                      const transport = getDesktopTransport();
                      if (transport == null) return;
                      const created = await transport.createAgent({
                        name,
                        personaId: personas[0]?.id,
                        modelAlias: 'fast-local',
                        instructions,
                        policyMode: 'side_effects',
                        skillIds: skills[0] == null ? [] : [skills[0].id],
                      });
                      setSelectedAgentId(created.id);
                    })
                  }
                >
                  {localize('com_ui_desktop_agent_create_agent')}
                </Button>
              </StudioCard>
            </section>

            <section className="rounded-xl border border-border-light p-4">
              <div className="mb-3 flex items-center justify-between gap-3">
                <h3 className="flex items-center gap-2 font-semibold">
                  <ShieldCheck className="h-4 w-4" />
                  {localize('com_ui_desktop_agent_runs_and_approvals')}
                </h3>
                <span className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_agent_run_summary', {
                    0: runs.length,
                    1: approvals.length,
                  })}
                </span>
              </div>
              <div className="grid gap-3 md:grid-cols-[220px_1fr_auto]">
                <select
                  className="rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
                  value={selectedAgentId}
                  onChange={(event) => setSelectedAgentId(event.target.value)}
                >
                  <option value="">{localize('com_ui_desktop_agent_choose')}</option>
                  {agents.map((agent) => (
                    <option key={agent.id} value={agent.id}>
                      {agent.name}
                    </option>
                  ))}
                </select>
                <input
                  className="rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm"
                  value={prompt}
                  onChange={(event) => setPrompt(event.target.value)}
                />
                <Button
                  disabled={saving || selectedAgent == null || prompt.trim() === ''}
                  onClick={() =>
                    void runAction(async () => {
                      const transport = getDesktopTransport();
                      if (transport == null || selectedAgent == null) return;
                      await transport.startAgentRun({
                        agentId: selectedAgent.id,
                        conversationId: persistedConversation,
                        prompt,
                      });
                    })
                  }
                >
                  {localize('com_ui_desktop_agent_run')}
                </Button>
              </div>
              <div className="mt-3 space-y-2">
                {approvals.map((approval) => (
                  <div
                    key={approval.id}
                    className="flex items-center justify-between rounded-lg bg-surface-secondary p-3 text-sm"
                  >
                    <span>{approval.reason}</span>
                    <div className="flex gap-2">
                      <Button
                        size="sm"
                        onClick={() =>
                          void runAction(() =>
                            getDesktopTransport()!.decideAgentApproval(approval.id, true),
                          )
                        }
                      >
                        <Check className="mr-1 h-4 w-4" />
                        {localize('com_ui_desktop_agent_approve')}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() =>
                          void runAction(() =>
                            getDesktopTransport()!.decideAgentApproval(approval.id, false),
                          )
                        }
                      >
                        <X className="mr-1 h-4 w-4" />
                        {localize('com_ui_desktop_agent_deny')}
                      </Button>
                    </div>
                  </div>
                ))}
                {runs.slice(0, 6).map((run) => (
                  <StatusRow
                    key={run.id}
                    title={agents.find((agent) => agent.id === run.agentId)?.name ?? run.agentId}
                    status={run.status}
                    detail={run.output ?? run.lastError ?? run.prompt}
                  />
                ))}
              </div>
            </section>

            <section className="grid gap-3 lg:grid-cols-2">
              <StudioCard
                title={localize('com_ui_desktop_agent_mcp_toolbox')}
                count={servers.length}
              >
                <p className="text-xs text-text-secondary">
                  {localize('com_ui_desktop_agent_mcp_description')}
                </p>
                {servers.map((server) => (
                  <StatusRow
                    key={server.id}
                    title={server.name}
                    status={
                      server.enabled
                        ? localize('com_ui_desktop_agent_enabled')
                        : localize('com_ui_desktop_agent_disabled')
                    }
                    detail={server.lastError ?? server.transport}
                  />
                ))}
                <Button
                  variant="outline"
                  disabled={saving}
                  onClick={() => void runAction(() => getDesktopTransport()!.syncAgentProtocols())}
                >
                  {localize('com_ui_desktop_agent_sync_tools')}
                </Button>
              </StudioCard>

              <StudioCard title={localize('com_ui_desktop_agent_crew')} count={crews.length}>
                <div className="flex gap-2">
                  <Button
                    disabled={saving || selectedAgent == null}
                    onClick={() =>
                      void runAction(async () => {
                        const transport = getDesktopTransport();
                        if (transport == null || selectedAgent == null) return;
                        const crew = await transport.createCrew({
                          name: `${selectedAgent.name} Crew`,
                          coordinatorAgentId: selectedAgent.id,
                          members: [],
                        });
                        setSelectedCrewId(crew.id);
                      })
                    }
                  >
                    <Users className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_agent_create')}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={saving || selectedCrewId === ''}
                    onClick={() =>
                      void runAction(() =>
                        getDesktopTransport()!.startCrewRun(selectedCrewId, prompt),
                      )
                    }
                  >
                    {localize('com_ui_desktop_agent_run_crew')}
                  </Button>
                </div>
                {crewRuns.slice(0, 4).map((run) => (
                  <StatusRow
                    key={run.id}
                    title={crews.find((crew) => crew.id === run.crewId)?.name ?? run.crewId}
                    status={run.status}
                    detail={run.output ?? run.lastError ?? run.prompt}
                  />
                ))}
              </StudioCard>
            </section>

            <section className="rounded-xl border border-border-light p-4">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="font-semibold">{localize('com_ui_desktop_agent_shadow_persona')}</h3>
                <Button
                  size="sm"
                  disabled={
                    saving ||
                    persistedConversation == null ||
                    selectedAgent == null ||
                    selectedAgent.toolAllowlist.length > 0
                  }
                  onClick={() =>
                    void runAction(() =>
                      getDesktopTransport()!.createShadowSubscription(
                        persistedConversation!,
                        selectedAgentId,
                      ),
                    )
                  }
                >
                  {localize('com_ui_desktop_agent_watch_chat')}
                </Button>
              </div>
              {persistedConversation == null && (
                <p className="text-sm text-text-secondary">
                  {localize('com_ui_desktop_agent_shadow_requires_conversation')}
                </p>
              )}
              <div className="space-y-2">
                {suggestions.map((suggestion) => (
                  <div key={suggestion.id} className="rounded-lg bg-surface-secondary p-3 text-sm">
                    <p>
                      {suggestion.content ||
                        suggestion.lastError ||
                        localize('com_ui_desktop_agent_thinking')}
                    </p>
                    {suggestion.status === 'unread' && (
                      <div className="mt-2 flex gap-2">
                        <Button
                          size="sm"
                          onClick={() =>
                            void runAction(() =>
                              getDesktopTransport()!.decideShadowSuggestion(suggestion.id, true),
                            )
                          }
                        >
                          {localize('com_ui_desktop_agent_accept')}
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() =>
                            void runAction(() =>
                              getDesktopTransport()!.decideShadowSuggestion(suggestion.id, false),
                            )
                          }
                        >
                          {localize('com_ui_desktop_agent_dismiss')}
                        </Button>
                      </div>
                    )}
                  </div>
                ))}
                {shadows.length === 0 && (
                  <p className="text-sm text-text-secondary">
                    {localize('com_ui_desktop_agent_no_watched_chats')}
                  </p>
                )}
              </div>
            </section>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function StudioCard({
  title,
  count,
  children,
}: {
  title: string;
  count: number;
  children: ReactNode;
}) {
  return (
    <section className="space-y-3 rounded-xl border border-border-light p-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold">{title}</h3>
        <span className="rounded-full bg-surface-secondary px-2 py-0.5 text-xs">{count}</span>
      </div>
      {children}
    </section>
  );
}

function TextInput({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block text-xs text-text-secondary">
      {label}
      <input
        className="mt-1 w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm text-text-primary"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function TextArea({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="block text-xs text-text-secondary">
      {label}
      <textarea
        className="mt-1 min-h-20 w-full rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-sm text-text-primary"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function StatusRow({ title, status, detail }: { title: string; status: string; detail: string }) {
  return (
    <div className="rounded-lg bg-surface-secondary p-3 text-sm">
      <div className="flex items-center justify-between gap-2">
        <strong>{title}</strong>
        <span className="text-xs uppercase tracking-wide text-text-secondary">
          {status.replaceAll('_', ' ')}
        </span>
      </div>
      <p className="mt-1 line-clamp-3 text-xs text-text-secondary">{detail}</p>
    </div>
  );
}
