import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  BookOpen,
  Check,
  Database,
  Eye,
  FilePlus2,
  FolderOpen,
  FolderSync,
  Plus,
  RefreshCw,
  Search,
  Trash2,
  X,
} from 'lucide-react';
import {
  Button,
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  Spinner,
} from '@librechat/client';
import { open as openDialog } from '@tauri-apps/plugin-dialog';
import {
  getDesktopTransport,
  isDesktopRuntime,
  type DenseIndexStatus,
  type DocumentChunk,
  type IngestionJob,
  type KnowledgeDocument,
  type KnowledgeSearchResult,
  type KnowledgeStack,
  type KnowledgeWatchKind,
  type KnowledgeWatchRoot,
} from '@northbridge/desktop-transport';
import {
  setActiveKnowledgeStackIds,
  useActiveKnowledgeStackIds,
} from '~/desktop/knowledgeSelection';
import { useLocalize } from '~/hooks';

const activeJobStatuses = new Set(['queued', 'running']);

export default function KnowledgeDashboard() {
  const localize = useLocalize();
  const activeStackIds = useActiveKnowledgeStackIds();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [stacks, setStacks] = useState<KnowledgeStack[]>([]);
  const [selectedStackId, setSelectedStackId] = useState<string | null>(null);
  const [documents, setDocuments] = useState<KnowledgeDocument[]>([]);
  const [jobs, setJobs] = useState<IngestionJob[]>([]);
  const [watchRoots, setWatchRoots] = useState<KnowledgeWatchRoot[]>([]);
  const [indexStatus, setIndexStatus] = useState<DenseIndexStatus | null>(null);
  const [selectedDocumentId, setSelectedDocumentId] = useState<string | null>(null);
  const [chunks, setChunks] = useState<DocumentChunk[]>([]);
  const [newStackName, setNewStackName] = useState('');
  const [newStackDescription, setNewStackDescription] = useState('');
  const [query, setQuery] = useState('');
  const [searchResult, setSearchResult] = useState<KnowledgeSearchResult | null>(null);

  const selectedStack = useMemo(
    () => stacks.find((stack) => stack.id === selectedStackId) ?? null,
    [selectedStackId, stacks],
  );

  const reportError = useCallback((cause: unknown) => {
    setError(cause instanceof Error ? cause.message : String(cause));
  }, []);

  const refreshStack = useCallback(async (stackId: string) => {
    const transport = getDesktopTransport();
    if (transport == null) {
      return;
    }
    const [nextDocuments, nextJobs, nextRoots, nextIndex] = await Promise.all([
      transport.listKnowledgeDocuments(stackId),
      transport.listIngestionJobs(stackId),
      transport.listKnowledgeWatchRoots(stackId),
      transport.knowledgeIndexStatus(stackId),
    ]);
    setDocuments(nextDocuments);
    setJobs(nextJobs);
    setWatchRoots(nextRoots);
    setIndexStatus(nextIndex);
    setSelectedDocumentId((current) =>
      current != null && nextDocuments.some((document) => document.id === current) ? current : null,
    );
  }, []);

  const refreshAll = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const nextStacks = await transport.listKnowledgeStacks();
      setStacks(nextStacks);
      const validIds = new Set(nextStacks.map((stack) => stack.id));
      const validActive = activeStackIds.filter((id) => validIds.has(id));
      if (validActive.length !== activeStackIds.length) {
        setActiveKnowledgeStackIds(validActive);
      }
      let nextSelected = selectedStackId;
      if (nextSelected == null || !validIds.has(nextSelected)) {
        nextSelected = nextStacks[0]?.id ?? null;
      }
      setSelectedStackId(nextSelected);
      if (nextSelected == null) {
        setDocuments([]);
        setJobs([]);
        setWatchRoots([]);
        setIndexStatus(null);
      } else {
        await refreshStack(nextSelected);
      }
    } catch (cause) {
      reportError(cause);
    } finally {
      setLoading(false);
    }
  }, [activeStackIds, refreshStack, reportError, selectedStackId]);

  useEffect(() => {
    if (open) {
      void refreshAll();
    }
    // Refresh only on opening; stack-specific changes have their own handlers.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (
      !open ||
      selectedStackId == null ||
      !jobs.some((job) => activeJobStatuses.has(job.status))
    ) {
      return;
    }
    const interval = window.setInterval(() => {
      void refreshStack(selectedStackId).catch(reportError);
    }, 1000);
    return () => window.clearInterval(interval);
  }, [jobs, open, refreshStack, reportError, selectedStackId]);

  const selectStack = useCallback(
    (stackId: string) => {
      setSelectedStackId(stackId);
      setSelectedDocumentId(null);
      setChunks([]);
      setSearchResult(null);
      setError(null);
      setLoading(true);
      void refreshStack(stackId)
        .catch(reportError)
        .finally(() => setLoading(false));
    },
    [refreshStack, reportError],
  );

  const createStack = useCallback(async () => {
    const transport = getDesktopTransport();
    const name = newStackName.trim();
    if (transport == null || name === '') {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const stack = await transport.createKnowledgeStack({
        name,
        description: newStackDescription.trim(),
      });
      setNewStackName('');
      setNewStackDescription('');
      setStacks((current) => [...current, stack]);
      setActiveKnowledgeStackIds([...activeStackIds, stack.id]);
      selectStack(stack.id);
    } catch (cause) {
      reportError(cause);
    } finally {
      setBusy(false);
    }
  }, [activeStackIds, newStackDescription, newStackName, reportError, selectStack]);

  const deleteStack = useCallback(async () => {
    const transport = getDesktopTransport();
    if (
      transport == null ||
      selectedStack == null ||
      !window.confirm(localize('com_ui_desktop_knowledge_delete_stack_confirm'))
    ) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await transport.deleteKnowledgeStack(selectedStack.id);
      setActiveKnowledgeStackIds(activeStackIds.filter((id) => id !== selectedStack.id));
      setSelectedStackId(null);
      await refreshAll();
    } catch (cause) {
      reportError(cause);
    } finally {
      setBusy(false);
    }
  }, [activeStackIds, localize, refreshAll, reportError, selectedStack]);

  const toggleActive = useCallback(
    (stackId: string) => {
      setActiveKnowledgeStackIds(
        activeStackIds.includes(stackId)
          ? activeStackIds.filter((id) => id !== stackId)
          : [...activeStackIds, stackId],
      );
    },
    [activeStackIds],
  );

  const importFiles = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || selectedStackId == null) {
      return;
    }
    setError(null);
    try {
      const selected = await openDialog({
        multiple: true,
        directory: false,
        title: localize('com_ui_desktop_knowledge_import_files'),
        filters: [
          {
            name: 'Documents',
            extensions: [
              'txt',
              'md',
              'markdown',
              'html',
              'htm',
              'pdf',
              'docx',
              'odt',
              'json',
              'csv',
              'tsv',
              'rs',
              'ts',
              'tsx',
              'js',
              'jsx',
              'py',
              'java',
              'kt',
              'swift',
              'c',
              'h',
              'cpp',
              'hpp',
              'go',
              'toml',
              'yaml',
              'yml',
              'xml',
            ],
          },
        ],
      });
      const paths = typeof selected === 'string' ? [selected] : (selected ?? []);
      if (paths.length === 0) {
        return;
      }
      setBusy(true);
      const nextJobs = await transport.importKnowledgeDocuments(selectedStackId, { paths });
      setJobs((current) => [...nextJobs, ...current]);
    } catch (cause) {
      reportError(cause);
    } finally {
      setBusy(false);
    }
  }, [localize, reportError, selectedStackId]);

  const addWatchRoot = useCallback(
    async (kind: KnowledgeWatchKind) => {
      const transport = getDesktopTransport();
      if (transport == null || selectedStackId == null) {
        return;
      }
      setError(null);
      try {
        const selected = await openDialog({
          directory: true,
          multiple: false,
          canCreateDirectories: false,
          title: localize(
            kind === 'obsidian'
              ? 'com_ui_desktop_knowledge_add_vault'
              : 'com_ui_desktop_knowledge_add_folder',
          ),
        });
        if (typeof selected !== 'string') {
          return;
        }
        setBusy(true);
        const root = await transport.createKnowledgeWatchRoot(selectedStackId, {
          path: selected,
          kind,
          recursive: true,
        });
        setWatchRoots((current) => [...current, root]);
        await refreshStack(selectedStackId);
      } catch (cause) {
        reportError(cause);
      } finally {
        setBusy(false);
      }
    },
    [localize, refreshStack, reportError, selectedStackId],
  );

  const inspectDocument = useCallback(
    async (documentId: string) => {
      const transport = getDesktopTransport();
      if (transport == null) {
        return;
      }
      setSelectedDocumentId(documentId);
      setChunks([]);
      setLoading(true);
      setError(null);
      try {
        setChunks(await transport.listKnowledgeDocumentChunks(documentId));
      } catch (cause) {
        reportError(cause);
      } finally {
        setLoading(false);
      }
    },
    [reportError],
  );

  const deleteDocument = useCallback(
    async (documentId: string) => {
      const transport = getDesktopTransport();
      if (transport == null || selectedStackId == null) {
        return;
      }
      setBusy(true);
      setError(null);
      try {
        await transport.deleteKnowledgeDocument(documentId);
        if (selectedDocumentId === documentId) {
          setSelectedDocumentId(null);
          setChunks([]);
        }
        await refreshStack(selectedStackId);
      } catch (cause) {
        reportError(cause);
      } finally {
        setBusy(false);
      }
    },
    [refreshStack, reportError, selectedDocumentId, selectedStackId],
  );

  const searchStack = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || selectedStackId == null || query.trim() === '') {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      setSearchResult(
        await transport.searchKnowledge(selectedStackId, { query: query.trim(), limit: 12 }),
      );
    } catch (cause) {
      reportError(cause);
    } finally {
      setBusy(false);
    }
  }, [query, reportError, selectedStackId]);

  const cancelJob = useCallback(
    async (jobId: string) => {
      const transport = getDesktopTransport();
      if (transport == null || selectedStackId == null) {
        return;
      }
      try {
        await transport.cancelIngestionJob(jobId);
        await refreshStack(selectedStackId);
      } catch (cause) {
        reportError(cause);
      }
    },
    [refreshStack, reportError, selectedStackId],
  );

  const rescanRoot = useCallback(
    async (rootId: string) => {
      const transport = getDesktopTransport();
      if (transport == null || selectedStackId == null) {
        return;
      }
      setBusy(true);
      setError(null);
      try {
        await transport.rescanKnowledgeWatchRoot(rootId);
        await refreshStack(selectedStackId);
      } catch (cause) {
        reportError(cause);
      } finally {
        setBusy(false);
      }
    },
    [refreshStack, reportError, selectedStackId],
  );

  const deleteRoot = useCallback(
    async (rootId: string) => {
      const transport = getDesktopTransport();
      if (transport == null || selectedStackId == null) {
        return;
      }
      setBusy(true);
      setError(null);
      try {
        await transport.deleteKnowledgeWatchRoot(rootId);
        await refreshStack(selectedStackId);
      } catch (cause) {
        reportError(cause);
      } finally {
        setBusy(false);
      }
    },
    [refreshStack, reportError, selectedStackId],
  );

  const rebuildIndex = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || selectedStackId == null) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      setIndexStatus(await transport.rebuildKnowledgeIndex(selectedStackId));
    } catch (cause) {
      reportError(cause);
    } finally {
      setBusy(false);
    }
  }, [reportError, selectedStackId]);

  if (!isDesktopRuntime()) {
    return null;
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <button
        type="button"
        className="relative flex h-9 w-9 items-center justify-center rounded-xl border border-border-light bg-presentation text-text-secondary hover:bg-surface-active-alt hover:text-text-primary"
        aria-label={localize('com_ui_desktop_knowledge')}
        title={localize('com_ui_desktop_knowledge')}
        onClick={() => setOpen(true)}
      >
        <BookOpen className="h-4 w-4" aria-hidden="true" />
        {activeStackIds.length > 0 && (
          <span className="absolute -right-1 -top-1 min-w-4 rounded-full bg-green-600 px-1 text-center text-[10px] leading-4 text-white">
            {activeStackIds.length}
          </span>
        )}
      </button>
      <DialogContent className="max-h-[92vh] w-[min(96vw,1180px)] max-w-6xl overflow-y-auto bg-surface-primary text-text-primary shadow-2xl">
        <DialogHeader>
          <div className="flex items-start justify-between gap-4 pr-8">
            <div>
              <DialogTitle className="text-xl">{localize('com_ui_desktop_knowledge')}</DialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {localize('com_ui_desktop_knowledge_description')}
              </p>
            </div>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={loading}
              onClick={() => void refreshAll()}
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

        <div className="grid gap-4 py-2 lg:grid-cols-[280px_minmax(0,1fr)]">
          <aside className="space-y-3">
            <section className="rounded-xl border border-border-light bg-surface-secondary p-3">
              <h3 className="font-semibold">{localize('com_ui_desktop_knowledge_stacks')}</h3>
              <p className="mt-1 text-xs text-text-secondary">
                {localize('com_ui_desktop_knowledge_active_hint')}
              </p>
              <div className="mt-3 max-h-64 space-y-2 overflow-y-auto">
                {stacks.map((stack) => {
                  const active = activeStackIds.includes(stack.id);
                  const selected = stack.id === selectedStackId;
                  return (
                    <div
                      key={stack.id}
                      className={`flex items-center gap-2 rounded-lg border p-2 ${
                        selected
                          ? 'border-blue-500/60 bg-blue-500/10'
                          : 'border-border-light bg-surface-primary'
                      }`}
                    >
                      <button
                        type="button"
                        className="min-w-0 flex-1 text-left"
                        onClick={() => selectStack(stack.id)}
                      >
                        <span className="block truncate text-sm font-medium">{stack.name}</span>
                        <span className="block text-[11px] text-text-secondary">
                          {localize('com_ui_desktop_knowledge_stack_meta', {
                            revision: stack.indexRevision,
                            dimensions: stack.embeddingDimensions,
                          })}
                        </span>
                      </button>
                      <button
                        type="button"
                        className={`flex h-7 w-7 items-center justify-center rounded-md border ${
                          active
                            ? 'border-green-500 bg-green-500/15 text-green-600 dark:text-green-300'
                            : 'border-border-medium text-text-secondary'
                        }`}
                        aria-label={localize('com_ui_desktop_knowledge_toggle_active')}
                        title={localize('com_ui_desktop_knowledge_toggle_active')}
                        onClick={() => toggleActive(stack.id)}
                      >
                        {active ? <Check className="h-4 w-4" /> : null}
                      </button>
                    </div>
                  );
                })}
                {!loading && stacks.length === 0 && (
                  <p className="rounded-lg bg-surface-primary p-3 text-center text-xs text-text-secondary">
                    {localize('com_ui_desktop_knowledge_empty')}
                  </p>
                )}
              </div>
            </section>

            <section className="space-y-2 rounded-xl border border-border-light bg-surface-secondary p-3">
              <h3 className="font-semibold">{localize('com_ui_desktop_knowledge_new_stack')}</h3>
              <input
                className="h-9 w-full rounded-lg border border-border-medium bg-surface-primary px-3 text-sm"
                value={newStackName}
                placeholder={localize('com_ui_desktop_knowledge_name')}
                onChange={(event) => setNewStackName(event.target.value)}
              />
              <textarea
                className="min-h-16 w-full resize-y rounded-lg border border-border-medium bg-surface-primary px-3 py-2 text-sm"
                value={newStackDescription}
                placeholder={localize('com_ui_desktop_knowledge_stack_description')}
                onChange={(event) => setNewStackDescription(event.target.value)}
              />
              <Button
                type="button"
                size="sm"
                className="w-full"
                disabled={busy || newStackName.trim() === ''}
                onClick={() => void createStack()}
              >
                <Plus className="mr-2 h-4 w-4" />
                {localize('com_ui_desktop_knowledge_create')}
              </Button>
            </section>
          </aside>

          <main className="min-w-0 space-y-4">
            {loading && selectedStack == null && (
              <div className="flex min-h-72 items-center justify-center">
                <Spinner />
              </div>
            )}
            {!loading && selectedStack == null && (
              <div className="flex min-h-72 items-center justify-center rounded-xl border border-dashed border-border-medium text-sm text-text-secondary">
                {localize('com_ui_desktop_knowledge_select_stack')}
              </div>
            )}
            {selectedStack != null && (
              <>
                <section className="rounded-xl border border-border-light bg-surface-secondary p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="flex items-center gap-2">
                        <Database className="h-4 w-4" />
                        <h2 className="font-semibold">{selectedStack.name}</h2>
                        {activeStackIds.includes(selectedStack.id) && (
                          <span className="rounded-full bg-green-500/15 px-2 py-0.5 text-[11px] font-medium text-green-700 dark:text-green-300">
                            {localize('com_ui_desktop_knowledge_active')}
                          </span>
                        )}
                      </div>
                      <p className="mt-1 text-sm text-text-secondary">
                        {selectedStack.description ||
                          localize('com_ui_desktop_knowledge_no_description')}
                      </p>
                      <p className="mt-2 text-xs text-text-secondary">
                        {localize('com_ui_desktop_knowledge_index_meta', {
                          model: selectedStack.embeddingModel,
                          chunkSize: selectedStack.chunkSize,
                          chunkOverlap: selectedStack.chunkOverlap,
                          records: indexStatus?.recordCount ?? 0,
                          bits: indexStatus?.bitWidth ?? 4,
                        })}
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void importFiles()}
                      >
                        <FilePlus2 className="mr-2 h-4 w-4" />
                        {localize('com_ui_desktop_knowledge_import_files')}
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void addWatchRoot('folder')}
                      >
                        <FolderOpen className="mr-2 h-4 w-4" />
                        {localize('com_ui_desktop_knowledge_add_folder')}
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void addWatchRoot('obsidian')}
                      >
                        <FolderSync className="mr-2 h-4 w-4" />
                        {localize('com_ui_desktop_knowledge_add_vault')}
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void rebuildIndex()}
                      >
                        <RefreshCw className="mr-2 h-4 w-4" />
                        {localize('com_ui_desktop_knowledge_rebuild')}
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        aria-label={localize('com_ui_desktop_knowledge_delete_stack')}
                        onClick={() => void deleteStack()}
                      >
                        <Trash2 className="h-4 w-4 text-red-600" />
                      </Button>
                    </div>
                  </div>
                </section>

                <section className="rounded-xl border border-border-light bg-surface-secondary p-4">
                  <h3 className="flex items-center gap-2 font-semibold">
                    <Search className="h-4 w-4" />
                    {localize('com_ui_desktop_knowledge_search')}
                  </h3>
                  <form
                    className="mt-3 flex gap-2"
                    onSubmit={(event) => {
                      event.preventDefault();
                      void searchStack();
                    }}
                  >
                    <input
                      className="h-9 min-w-0 flex-1 rounded-lg border border-border-medium bg-surface-primary px-3 text-sm"
                      value={query}
                      placeholder={localize('com_ui_desktop_knowledge_search_placeholder')}
                      onChange={(event) => setQuery(event.target.value)}
                    />
                    <Button type="submit" size="sm" disabled={busy || query.trim() === ''}>
                      {localize('com_ui_desktop_knowledge_search_action')}
                    </Button>
                  </form>
                  {searchResult != null && (
                    <div className="mt-3 space-y-2">
                      <p className="text-xs text-text-secondary">
                        {localize('com_ui_desktop_knowledge_search_meta', {
                          results: searchResult.hits.length,
                          lexical: searchResult.lexicalCandidateCount,
                          dense: searchResult.denseCandidateCount,
                          elapsed: searchResult.elapsedMs,
                        })}
                      </p>
                      {searchResult.hits.map((hit) => (
                        <button
                          key={`${hit.stackId}:${hit.denseId}`}
                          type="button"
                          className="block w-full rounded-lg border border-border-light bg-surface-primary p-3 text-left hover:border-blue-500/50"
                          onClick={() => void inspectDocument(hit.documentId)}
                        >
                          <span className="flex items-center justify-between gap-3 text-xs">
                            <span className="font-semibold text-blue-600 dark:text-blue-300">
                              {hit.citation} {hit.title}
                            </span>
                            <span className="text-text-secondary">
                              {localize('com_ui_desktop_knowledge_rrf', {
                                score: hit.fusedScore.toFixed(4),
                              })}
                            </span>
                          </span>
                          <span className="mt-1 block whitespace-pre-wrap text-sm text-text-secondary">
                            {hit.snippet}
                          </span>
                          <span className="mt-1 block truncate text-[11px] text-text-tertiary">
                            {localize('com_ui_desktop_knowledge_source_meta', {
                              source: hit.sourceUri,
                              start: hit.startOffset,
                              end: hit.endOffset,
                            })}
                          </span>
                        </button>
                      ))}
                    </div>
                  )}
                </section>

                {(jobs.length > 0 || watchRoots.length > 0) && (
                  <section className="grid gap-4 md:grid-cols-2">
                    <div className="rounded-xl border border-border-light bg-surface-secondary p-4">
                      <h3 className="font-semibold">
                        {localize('com_ui_desktop_knowledge_ingestion')}
                      </h3>
                      <div className="mt-3 max-h-52 space-y-2 overflow-y-auto">
                        {jobs.map((job) => (
                          <div
                            key={job.id}
                            className="rounded-lg border border-border-light bg-surface-primary p-2 text-xs"
                          >
                            <div className="flex items-center justify-between gap-2">
                              <span className="truncate font-medium" title={job.sourceUri}>
                                {pathLabel(job.sourceUri)}
                              </span>
                              <span className="uppercase text-text-secondary">{job.status}</span>
                            </div>
                            <div className="mt-1 flex items-center gap-2">
                              <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-surface-active-alt">
                                <div
                                  className="h-full rounded-full bg-blue-500"
                                  style={{ width: `${jobProgress(job)}%` }}
                                />
                              </div>
                              {activeJobStatuses.has(job.status) && (
                                <button
                                  type="button"
                                  aria-label={localize('com_ui_desktop_knowledge_cancel_job')}
                                  onClick={() => void cancelJob(job.id)}
                                >
                                  <X className="h-3.5 w-3.5" />
                                </button>
                              )}
                            </div>
                            {job.lastError != null && (
                              <p className="mt-1 text-red-600 dark:text-red-300">{job.lastError}</p>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                    <div className="rounded-xl border border-border-light bg-surface-secondary p-4">
                      <h3 className="font-semibold">
                        {localize('com_ui_desktop_knowledge_watch_roots')}
                      </h3>
                      <div className="mt-3 max-h-52 space-y-2 overflow-y-auto">
                        {watchRoots.map((root) => (
                          <div
                            key={root.id}
                            className="flex items-start gap-2 rounded-lg border border-border-light bg-surface-primary p-2 text-xs"
                          >
                            <FolderSync className="mt-0.5 h-4 w-4 shrink-0" />
                            <div className="min-w-0 flex-1">
                              <p className="truncate font-medium" title={root.path}>
                                {pathLabel(root.path)}
                              </p>
                              <p className="truncate text-text-secondary">
                                {root.kind} · {root.path}
                              </p>
                              {root.lastError != null && (
                                <p className="text-red-600 dark:text-red-300">{root.lastError}</p>
                              )}
                            </div>
                            <button
                              type="button"
                              aria-label={localize('com_ui_desktop_knowledge_rescan')}
                              onClick={() => void rescanRoot(root.id)}
                            >
                              <RefreshCw className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              aria-label={localize('com_ui_desktop_knowledge_remove_watch')}
                              onClick={() => void deleteRoot(root.id)}
                            >
                              <Trash2 className="h-3.5 w-3.5 text-red-600" />
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  </section>
                )}

                <section className="rounded-xl border border-border-light bg-surface-secondary p-4">
                  <h3 className="font-semibold">
                    {localize('com_ui_desktop_knowledge_documents')}
                  </h3>
                  <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(260px,0.75fr)_minmax(0,1.25fr)]">
                    <div className="max-h-80 space-y-2 overflow-y-auto">
                      {documents.map((document) => (
                        <div
                          key={document.id}
                          className={`flex items-start gap-2 rounded-lg border p-2 ${selectedDocumentId === document.id ? 'border-blue-500/60 bg-blue-500/10' : 'border-border-light bg-surface-primary'}`}
                        >
                          <button
                            type="button"
                            className="min-w-0 flex-1 text-left"
                            onClick={() => void inspectDocument(document.id)}
                          >
                            <span className="block truncate text-sm font-medium">
                              {document.title}
                            </span>
                            <span className="block truncate text-[11px] text-text-secondary">
                              {document.sourceKind} · {formatBytes(document.byteSize)} ·{' '}
                              {document.status}
                            </span>
                            {document.lastError != null && (
                              <span className="mt-1 block text-[11px] text-red-600 dark:text-red-300">
                                {document.lastError}
                              </span>
                            )}
                          </button>
                          <button
                            type="button"
                            aria-label={localize('com_ui_desktop_knowledge_inspect')}
                            onClick={() => void inspectDocument(document.id)}
                          >
                            <Eye className="h-4 w-4" />
                          </button>
                          <button
                            type="button"
                            aria-label={localize('com_ui_desktop_knowledge_delete_document')}
                            onClick={() => void deleteDocument(document.id)}
                          >
                            <Trash2 className="h-4 w-4 text-red-600" />
                          </button>
                        </div>
                      ))}
                      {documents.length === 0 && (
                        <p className="rounded-lg bg-surface-primary p-4 text-center text-sm text-text-secondary">
                          {localize('com_ui_desktop_knowledge_no_documents')}
                        </p>
                      )}
                    </div>
                    <div className="max-h-80 space-y-2 overflow-y-auto rounded-lg bg-surface-primary p-2">
                      {selectedDocumentId == null && (
                        <p className="p-4 text-center text-sm text-text-secondary">
                          {localize('com_ui_desktop_knowledge_select_document')}
                        </p>
                      )}
                      {selectedDocumentId != null && loading && chunks.length === 0 && (
                        <div className="flex min-h-24 items-center justify-center">
                          <Spinner />
                        </div>
                      )}
                      {selectedDocumentId != null &&
                        (!loading || chunks.length > 0) &&
                        chunks.map((chunk) => (
                          <article
                            key={chunk.id}
                            className="rounded-lg border border-border-light p-3"
                          >
                            <header className="flex flex-wrap items-center justify-between gap-2 text-[11px] text-text-secondary">
                              <span>
                                {localize('com_ui_desktop_knowledge_chunk_meta', {
                                  position: chunk.ordinal + 1,
                                  denseId: chunk.denseId,
                                  start: chunk.startOffset,
                                  end: chunk.endOffset,
                                })}
                              </span>
                              <span>{chunk.contentSha256.slice(0, 10)}</span>
                            </header>
                            <p className="mt-2 whitespace-pre-wrap text-sm leading-relaxed">
                              {chunk.content}
                            </p>
                          </article>
                        ))}
                    </div>
                  </div>
                </section>
              </>
            )}
          </main>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function jobProgress(job: IngestionJob): number {
  if (job.status === 'completed') {
    return 100;
  }
  if (job.progressTotal == null || job.progressTotal <= 0) {
    return job.status === 'running' ? 35 : 5;
  }
  return Math.min(100, Math.round((job.progressCurrent / job.progressTotal) * 100));
}

function pathLabel(path: string): string {
  return path.split(/[\\/]/u).filter(Boolean).at(-1) ?? path;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
