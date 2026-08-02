import { useCallback, useEffect, useMemo, useState } from 'react';
import { Columns2, RefreshCw, Square } from 'lucide-react';
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
  type GenerationEvent,
  type GenerationStatus,
  type ModelCatalogResponse,
  type SplitChatLane,
  type SplitChatRun,
} from '@northbridge/desktop-transport';
import { useChatContext } from '~/Providers';
import { useLocalize } from '~/hooks';
import {
  desktopConversationToLibreChat,
  desktopMessageToLibreChat,
} from '~/desktop/requestOverride';

interface LaneView {
  text: string;
  status: GenerationStatus;
  error: string | null;
}

export default function SplitChatDialog() {
  const localize = useLocalize();
  const { conversation, isSubmitting, setConversation, setMessages } = useChatContext();
  const conversationId = conversation?.conversationId;
  const persistedConversation =
    conversationId != null && conversationId !== Constants.NEW_CONVO ? conversationId : null;
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<ModelCatalogResponse | null>(null);
  const [candidateIds, setCandidateIds] = useState<string[]>([]);
  const [splitChat, setSplitChat] = useState<SplitChatRun | null>(null);
  const [laneViews, setLaneViews] = useState<Record<string, LaneView>>({});

  const orderedModels = useMemo(() => {
    const models = catalog?.models ?? [];
    const selectedId = catalog?.selection?.modelId;
    return [...models].sort((left, right) => {
      if (left.modelId === selectedId) return -1;
      if (right.modelId === selectedId) return 1;
      return Number(right.recommended) - Number(left.recommended);
    });
  }, [catalog]);

  const refresh = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null) return;
    setLoading(true);
    setError(null);
    try {
      const [nextCatalog, splitChats] = await Promise.all([
        transport.modelCatalog(),
        persistedConversation == null
          ? Promise.resolve([])
          : transport.listSplitChats(persistedConversation),
      ]);
      setCatalog(nextCatalog);
      setSplitChat(splitChats[0] ?? null);
      const nextOrdered = [...nextCatalog.models].sort((left, right) => {
        if (left.modelId === nextCatalog.selection?.modelId) return -1;
        if (right.modelId === nextCatalog.selection?.modelId) return 1;
        return Number(right.recommended) - Number(left.recommended);
      });
      setCandidateIds((current) => {
        const available = new Set(nextOrdered.map((model) => model.modelId));
        const retained = current.filter((modelId) => available.has(modelId)).slice(0, 4);
        return retained.length >= 2
          ? retained
          : nextOrdered.slice(0, Math.min(2, nextOrdered.length)).map((model) => model.modelId);
      });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLoading(false);
    }
  }, [persistedConversation]);

  useEffect(() => {
    if (open) void refresh();
  }, [open, refresh]);

  useEffect(() => {
    if (!open || splitChat == null) return;
    const controllers = splitChat.lanes.map(() => new AbortController());
    setLaneViews(
      Object.fromEntries(
        splitChat.lanes.map((lane) => [
          lane.generation.id,
          { text: '', status: lane.generation.status, error: null },
        ]),
      ),
    );
    splitChat.lanes.forEach((lane, index) => {
      void watchLane(lane, controllers[index].signal, setLaneViews);
    });
    return () => controllers.forEach((controller) => controller.abort());
  }, [open, splitChat]);

  const toggleCandidate = useCallback((modelId: string) => {
    setCandidateIds((current) => {
      if (current.includes(modelId)) return current.filter((candidate) => candidate !== modelId);
      return current.length >= 4 ? current : [...current, modelId];
    });
  }, []);

  const createComparison = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || persistedConversation == null || candidateIds.length < 2) return;
    setCreating(true);
    setError(null);
    try {
      const storedConversation = await transport.getConversation(persistedConversation);
      const sourceMessages = await transport.listMessages(
        persistedConversation,
        storedConversation.defaultBranchId,
      );
      const inputMessage = [...sourceMessages].reverse().find((message) => message.role === 'user');
      if (inputMessage == null) {
        throw new Error(localize('com_ui_desktop_split_requires_prompt'));
      }
      const created = await transport.createSplitChat(persistedConversation, {
        sourceBranchId: storedConversation.defaultBranchId,
        inputMessageId: inputMessage.id,
        clientRequestId: globalThis.crypto.randomUUID(),
        candidates: candidateIds.map((modelId) => ({
          modelId,
          loadProfile:
            catalog?.selection?.modelId === modelId ? catalog.selection.loadProfile : undefined,
        })),
      });
      setSplitChat(created);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setCreating(false);
    }
  }, [candidateIds, catalog?.selection, localize, persistedConversation]);

  const cancelComparison = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || splitChat == null) return;
    setError(null);
    try {
      setSplitChat(await transport.cancelSplitChat(splitChat.id));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, [splitChat]);

  const cancelLane = useCallback(async (generationId: string) => {
    const transport = getDesktopTransport();
    if (transport == null) return;
    setError(null);
    try {
      const generation = await transport.cancelGeneration(generationId);
      setLaneViews((current) => ({
        ...current,
        [generationId]: {
          ...(current[generationId] ?? { text: '', error: null }),
          status: generation.status,
        },
      }));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, []);

  const useLane = useCallback(
    async (lane: SplitChatLane) => {
      const transport = getDesktopTransport();
      if (transport == null || persistedConversation == null) return;
      setError(null);
      try {
        const storedConversation = await transport.activateConversationBranch(
          persistedConversation,
          lane.branchId,
        );
        const branchMessages = await transport.listMessages(persistedConversation, lane.branchId);
        setConversation(desktopConversationToLibreChat(storedConversation));
        setMessages(branchMessages.map(desktopMessageToLibreChat));
        setOpen(false);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      }
    },
    [persistedConversation, setConversation, setMessages],
  );

  if (!isDesktopRuntime()) return null;

  const hasActiveLanes = splitChat?.lanes.some(
    (lane) => !isTerminal(laneViews[lane.generation.id]?.status ?? lane.generation.status),
  );

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <button
        type="button"
        className="flex h-9 w-9 items-center justify-center rounded-xl border border-border-light bg-presentation text-text-secondary hover:bg-surface-active-alt hover:text-text-primary disabled:opacity-50"
        aria-label={localize('com_ui_desktop_split_chat')}
        title={localize('com_ui_desktop_split_chat')}
        disabled={persistedConversation == null || isSubmitting}
        onClick={() => setOpen(true)}
      >
        <Columns2 className="h-4 w-4" aria-hidden="true" />
      </button>
      <DialogContent className="max-h-[90vh] w-[min(96vw,1180px)] max-w-6xl overflow-y-auto bg-surface-primary text-text-primary shadow-2xl">
        <DialogHeader>
          <div className="flex items-center justify-between gap-4 pr-8">
            <div>
              <DialogTitle>{localize('com_ui_desktop_split_chat')}</DialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {localize('com_ui_desktop_split_description')}
              </p>
            </div>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={loading}
              onClick={() => void refresh()}
            >
              <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              {localize('com_ui_desktop_runtime_refresh')}
            </Button>
          </div>
        </DialogHeader>

        {error != null && (
          <div
            className="rounded-xl border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-700 dark:text-red-300"
            role="alert"
          >
            {error}
          </div>
        )}

        {loading && catalog == null ? (
          <div className="flex min-h-48 items-center justify-center">
            <Spinner />
          </div>
        ) : (
          <div className="space-y-5">
            <section className="rounded-xl border border-border-light bg-surface-secondary p-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h3 className="font-semibold">{localize('com_ui_desktop_split_models')}</h3>
                  <p className="text-xs text-text-secondary">
                    {localize('com_ui_desktop_split_models_hint')}
                  </p>
                </div>
                <Button
                  type="button"
                  size="sm"
                  disabled={creating || candidateIds.length < 2 || candidateIds.length > 4}
                  onClick={() => void createComparison()}
                >
                  {creating ? <Spinner className="mr-2" /> : <Columns2 className="mr-2 h-4 w-4" />}
                  {localize('com_ui_desktop_split_run')}
                </Button>
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                {orderedModels.map((model) => {
                  const checked = candidateIds.includes(model.modelId);
                  return (
                    <label
                      key={model.modelId}
                      className="flex cursor-pointer gap-2 rounded-lg border border-border-light bg-surface-primary p-3 text-sm"
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={!checked && candidateIds.length >= 4}
                        onChange={() => toggleCandidate(model.modelId)}
                      />
                      <span className="min-w-0">
                        <span className="block truncate font-medium">{model.displayName}</span>
                        <span
                          className="block truncate text-xs text-text-secondary"
                          title={model.modelId}
                        >
                          {model.modelId}
                        </span>
                      </span>
                    </label>
                  );
                })}
              </div>
            </section>

            {splitChat != null && (
              <section>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <h3 className="font-semibold">{localize('com_ui_desktop_split_results')}</h3>
                  {hasActiveLanes && (
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      onClick={() => void cancelComparison()}
                    >
                      <Square className="mr-2 h-3.5 w-3.5" />
                      {localize('com_ui_desktop_split_cancel')}
                    </Button>
                  )}
                </div>
                <div
                  className="grid gap-3"
                  style={{
                    gridTemplateColumns: `repeat(${Math.min(splitChat.lanes.length, 4)}, minmax(220px, 1fr))`,
                  }}
                >
                  {splitChat.lanes.map((lane) => {
                    const view = laneViews[lane.generation.id] ?? {
                      text: '',
                      status: lane.generation.status,
                      error: null,
                    };
                    return (
                      <article
                        key={lane.generation.id}
                        className="flex min-h-72 flex-col rounded-xl border border-border-light bg-surface-secondary p-4"
                      >
                        <div className="mb-3 flex items-start justify-between gap-2">
                          <div className="min-w-0">
                            <h4
                              className="truncate text-sm font-semibold"
                              title={lane.generation.modelId ?? lane.generation.modelAlias}
                            >
                              {modelName(
                                catalog,
                                lane.generation.modelId ?? lane.generation.modelAlias,
                              )}
                            </h4>
                            <p className="text-xs text-text-secondary">
                              {view.status.replaceAll('_', ' ')}
                            </p>
                          </div>
                          {!isTerminal(view.status) && <Spinner className="shrink-0" />}
                        </div>
                        <div className="min-h-36 flex-1 whitespace-pre-wrap text-sm leading-6">
                          {view.text || (view.error ?? localize('com_ui_desktop_split_waiting'))}
                        </div>
                        <div className="mt-4 flex gap-2">
                          {!isTerminal(view.status) && (
                            <Button
                              type="button"
                              size="sm"
                              variant="outline"
                              className="flex-1"
                              onClick={() => void cancelLane(lane.generation.id)}
                            >
                              {localize('com_ui_desktop_split_cancel_lane')}
                            </Button>
                          )}
                          <Button
                            type="button"
                            size="sm"
                            variant="outline"
                            className="flex-1"
                            disabled={view.status !== 'completed'}
                            onClick={() => void useLane(lane)}
                          >
                            {localize('com_ui_desktop_split_use_branch')}
                          </Button>
                        </div>
                      </article>
                    );
                  })}
                </div>
              </section>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

async function watchLane(
  lane: SplitChatLane,
  signal: AbortSignal,
  setLaneViews: React.Dispatch<React.SetStateAction<Record<string, LaneView>>>,
) {
  const transport = getDesktopTransport();
  if (transport == null) return;
  try {
    for await (const event of transport.streamGenerationEvents(lane.generation.id, { signal })) {
      setLaneViews((current) => {
        const previous = current[lane.generation.id] ?? {
          text: '',
          status: lane.generation.status,
          error: null,
        };
        return {
          ...current,
          [lane.generation.id]: reduceLaneEvent(previous, event),
        };
      });
      if (
        event.eventType === 'completed' ||
        event.eventType === 'failed' ||
        event.eventType === 'cancelled'
      )
        return;
    }
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === 'AbortError') return;
    setLaneViews((current) => ({
      ...current,
      [lane.generation.id]: {
        ...(current[lane.generation.id] ?? {
          text: '',
          status: lane.generation.status,
          error: null,
        }),
        status: 'failed',
        error: cause instanceof Error ? cause.message : String(cause),
      },
    }));
  }
}

function reduceLaneEvent(view: LaneView, event: GenerationEvent): LaneView {
  if (event.eventType === 'delta')
    return { ...view, status: 'running', text: view.text + payloadText(event) };
  if (event.eventType === 'started') return { ...view, status: 'running' };
  if (event.eventType === 'completed') return { ...view, status: 'completed' };
  if (event.eventType === 'cancelled') return { ...view, status: 'cancelled' };
  if (event.eventType === 'failed')
    return { ...view, status: 'failed', error: payloadError(event) };
  return view;
}

function payloadText(event: GenerationEvent): string {
  const payload = event.payload;
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload)) return '';
  const text = (payload as Record<string, unknown>).text;
  return typeof text === 'string' ? text : '';
}

function payloadError(event: GenerationEvent): string {
  const payload = event.payload;
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload))
    return 'Generation failed.';
  const message = (payload as Record<string, unknown>).message;
  return typeof message === 'string' ? message : 'Generation failed.';
}

function isTerminal(status: GenerationStatus) {
  return status === 'completed' || status === 'failed' || status === 'cancelled';
}

function modelName(catalog: ModelCatalogResponse | null, modelId: string) {
  return catalog?.models.find((model) => model.modelId === modelId)?.displayName ?? modelId;
}
