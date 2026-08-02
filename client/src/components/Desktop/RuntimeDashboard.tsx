import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Activity,
  Cpu,
  Download,
  FolderInput,
  RefreshCw,
  SlidersHorizontal,
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
  type GenerationPerformanceDashboard,
  type HardwareDiagnostics,
  type ManagedModel,
  type ModelCatalogResponse,
  type ModelQuantization,
  type ModelTuneResult,
} from '@northbridge/desktop-transport';
import { useLocalize } from '~/hooks';

const quantizationOptions: Array<{ value: ModelQuantization; label: string }> = [
  { value: 'none', label: 'FP16 / model default' },
  { value: 'q8_0', label: 'Q8_0 · near-lossless' },
  { value: 'q6k', label: 'Q6K · high quality' },
  { value: 'q5k', label: 'Q5K · balanced quality' },
  { value: 'q4k', label: 'Q4K · recommended local' },
  { value: 'q3k', label: 'Q3K · compact' },
  { value: 'q2k', label: 'Q2K · smallest' },
];

export default function RuntimeDashboard() {
  const localize = useLocalize();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [diagnostics, setDiagnostics] = useState<HardwareDiagnostics | null>(null);
  const [performance, setPerformance] = useState<GenerationPerformanceDashboard | null>(null);
  const [catalog, setCatalog] = useState<ModelCatalogResponse | null>(null);
  const [tuning, setTuning] = useState<ModelTuneResult | null>(null);
  const [quantization, setQuantization] = useState<ModelQuantization>('none');
  const [downloadModelId, setDownloadModelId] = useState(
    'HuggingFaceTB/SmolLM2-360M-Instruct',
  );
  const [importPath, setImportPath] = useState('');

  const refresh = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null) {
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const [nextDiagnostics, nextPerformance, nextCatalog] = await Promise.all([
        transport.hardwareDiagnostics(),
        transport.generationPerformance(50),
        transport.modelCatalog(),
      ]);
      setDiagnostics(nextDiagnostics);
      setPerformance(nextPerformance);
      setCatalog(nextCatalog);
      setQuantization(nextCatalog.selection?.loadProfile.quantization ?? 'none');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open) {
      void refresh();
    }
  }, [open, refresh]);

  useEffect(() => {
    if (!open || !catalog?.managedModels.some((model) => isTransferActive(model))) {
      return;
    }
    const interval = window.setInterval(() => {
      const transport = getDesktopTransport();
      if (transport == null) {
        return;
      }
      void transport
        .modelCatalog()
        .then(setCatalog)
        .catch((cause) => setError(cause instanceof Error ? cause.message : String(cause)));
    }, 1000);
    return () => window.clearInterval(interval);
  }, [catalog?.managedModels, open]);

  const selectedModel = useMemo(
    () => catalog?.models.find((model) => model.modelId === catalog.selection?.modelId) ?? null,
    [catalog],
  );

  const applyProfile = useCallback(async () => {
    const transport = getDesktopTransport();
    const selection = catalog?.selection;
    if (transport == null || selection == null) {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const updated = await transport.selectModel({
        modelId: selection.modelId,
        loadProfile: { ...selection.loadProfile, quantization },
      });
      setCatalog(updated);
      setTuning(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSaving(false);
    }
  }, [catalog, quantization]);

  const tuneProfile = useCallback(async () => {
    const transport = getDesktopTransport();
    const selection = catalog?.selection;
    if (transport == null || selection == null) {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      setTuning(
        await transport.tuneModel({
          modelId: selection.modelId,
          loadProfile: { ...selection.loadProfile, quantization },
          tuningProfile: 'balanced',
        }),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSaving(false);
    }
  }, [catalog, quantization]);

  const downloadModel = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || downloadModelId.trim() === '') {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await transport.downloadModel({ modelId: downloadModelId.trim() });
      setCatalog(await transport.modelCatalog());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSaving(false);
    }
  }, [downloadModelId]);

  const importModel = useCallback(async () => {
    const transport = getDesktopTransport();
    if (transport == null || importPath.trim() === '') {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await transport.importModel({ path: importPath.trim() });
      setImportPath('');
      setCatalog(await transport.modelCatalog());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSaving(false);
    }
  }, [importPath]);

  const browseForModel = useCallback(async () => {
    setError(null);
    try {
      const selected = await openDialog({
        directory: true,
        multiple: false,
        canCreateDirectories: false,
        title: localize('com_ui_desktop_model_import'),
      });
      if (typeof selected === 'string') {
        setImportPath(selected);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, [localize]);

  const cancelDownload = useCallback(async (modelId: string) => {
    const transport = getDesktopTransport();
    if (transport == null) {
      return;
    }
    setError(null);
    try {
      await transport.cancelModelDownload(modelId);
      setCatalog(await transport.modelCatalog());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, []);

  const removeModel = useCallback(async (modelId: string) => {
    const transport = getDesktopTransport();
    if (transport == null) {
      return;
    }
    setError(null);
    try {
      await transport.removeManagedModel(modelId);
      setCatalog(await transport.modelCatalog());
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }, []);

  const selectManagedModel = useCallback(
    async (modelId: string) => {
      const transport = getDesktopTransport();
      if (transport == null) {
        return;
      }
      setSaving(true);
      setError(null);
      try {
        setCatalog(
          await transport.selectModel({
            modelId,
            loadProfile: catalog?.selection?.loadProfile,
          }),
        );
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        setSaving(false);
      }
    },
    [catalog?.selection?.loadProfile],
  );

  if (!isDesktopRuntime()) {
    return null;
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <button
        type="button"
        className="flex h-9 w-9 items-center justify-center rounded-xl border border-border-light bg-presentation text-text-secondary hover:bg-surface-active-alt hover:text-text-primary"
        aria-label={localize('com_ui_desktop_runtime_dashboard')}
        title={localize('com_ui_desktop_runtime_dashboard')}
        onClick={() => setOpen(true)}
      >
        <Activity className="h-4 w-4" aria-hidden="true" />
      </button>
      <DialogContent className="max-h-[88vh] w-[min(94vw,960px)] max-w-5xl overflow-y-auto bg-surface-primary text-text-primary shadow-2xl">
        <DialogHeader>
          <div className="flex items-center justify-between gap-4 pr-8">
            <div>
              <DialogTitle className="text-xl">
                {localize('com_ui_desktop_runtime_dashboard')}
              </DialogTitle>
              <p className="mt-1 text-sm text-text-secondary">
                {localize('com_ui_desktop_runtime_dashboard_description')}
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
              {localize('com_ui_desktop_runtime_refresh')}
            </Button>
          </div>
        </DialogHeader>

        {loading && diagnostics == null ? (
          <div className="flex min-h-64 items-center justify-center">
            <Spinner className="text-text-primary" />
          </div>
        ) : (
          <div className="space-y-5 py-2">
            {error != null && (
              <div className="rounded-xl border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-700 dark:text-red-300" role="alert">
                {error}
              </div>
            )}

            <section aria-labelledby="runtime-hardware-title">
              <h3 id="runtime-hardware-title" className="mb-2 flex items-center gap-2 font-semibold">
                <Cpu className="h-4 w-4" />
                {localize('com_ui_desktop_runtime_hardware')}
              </h3>
              <div className="grid gap-3 md:grid-cols-3">
                <MetricCard
                  label={localize('com_ui_desktop_runtime_backend')}
                  value={diagnostics == null ? '—' : diagnostics.system.build.cuda ? 'CUDA' : diagnostics.system.build.metal ? 'Metal' : 'CPU'}
                  detail={diagnostics == null ? undefined : `mistral.rs ${diagnostics.system.build.version}`}
                />
                <MetricCard
                  label={localize('com_ui_desktop_runtime_processor')}
                  value={diagnostics?.system.cpu.brand.trim() ?? '—'}
                  detail={diagnostics == null ? undefined : `${diagnostics.system.cpu.physicalCores ?? '—'} physical · ${diagnostics.system.cpu.logicalCores} logical cores`}
                />
                <MetricCard
                  label={localize('com_ui_desktop_runtime_memory')}
                  value={diagnostics == null ? '—' : formatBytes(diagnostics.system.memory.availableBytes)}
                  detail={diagnostics == null ? undefined : `${formatBytes(diagnostics.system.memory.totalBytes)} total`}
                />
              </div>
              {diagnostics != null && (
                <div className="mt-3 grid gap-2 md:grid-cols-2">
                  {diagnostics.checks.map((check) => (
                    <div key={check.name} className="rounded-lg border border-border-light bg-surface-secondary px-3 py-2 text-xs">
                      <div className="flex items-center justify-between gap-3">
                        <span className="font-medium">{check.name.replaceAll('_', ' ')}</span>
                        <span className={check.status === 'ok' ? 'text-green-600 dark:text-green-400' : check.status === 'warning' ? 'text-amber-600 dark:text-amber-300' : 'text-red-600 dark:text-red-300'}>
                          {check.status}
                        </span>
                      </div>
                      <p className="mt-1 text-text-secondary">{check.message}</p>
                    </div>
                  ))}
                </div>
              )}
            </section>

            <section aria-labelledby="runtime-model-title">
              <h3 id="runtime-model-title" className="mb-2 flex items-center gap-2 font-semibold">
                <SlidersHorizontal className="h-4 w-4" />
                {localize('com_ui_desktop_runtime_model_profile')}
              </h3>
              <div className="rounded-xl border border-border-light bg-surface-secondary p-4">
                <div className="grid items-end gap-3 md:grid-cols-[minmax(0,1fr)_minmax(220px,0.7fr)_auto]">
                  <div>
                    <span className="text-xs text-text-secondary">{localize('com_ui_desktop_runtime_selected_model')}</span>
                    <p className="truncate font-medium">{selectedModel?.displayName ?? catalog?.selection?.modelId ?? '—'}</p>
                  </div>
                  <label className="text-xs text-text-secondary">
                    {localize('com_ui_desktop_runtime_quantization')}
                    <select
                      className="mt-1 h-9 w-full rounded-lg border border-border-medium bg-surface-primary px-2 text-sm text-text-primary"
                      value={quantization}
                      disabled={catalog?.selection == null || saving}
                      onChange={(event) => setQuantization(event.target.value as ModelQuantization)}
                    >
                      {quantizationOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="flex gap-2">
                    <Button type="button" size="sm" disabled={catalog?.selection == null || saving} onClick={() => void tuneProfile()}>
                      {localize('com_ui_desktop_runtime_tune')}
                    </Button>
                    <Button type="button" size="sm" disabled={catalog?.selection == null || saving} onClick={() => void applyProfile()}>
                      {localize('com_ui_desktop_runtime_apply')}
                    </Button>
                  </div>
                </div>
                {tuning != null && (
                  <p className="mt-3 rounded-lg bg-surface-primary p-3 text-sm text-text-secondary">
                    {localize('com_ui_desktop_runtime_recommendation')}: <span className="font-medium text-text-primary">{tuning.recommendedIsq ?? 'model default'}</span>
                    {' · '}{formatBytes(tuning.candidates.find((candidate) => candidate.recommended)?.estimatedSizeBytes ?? 0)}
                    {' · '}{tuning.backend}
                  </p>
                )}
              </div>
            </section>

            <section aria-labelledby="runtime-model-hub-title">
              <h3 id="runtime-model-hub-title" className="mb-2 flex items-center gap-2 font-semibold">
                <Download className="h-4 w-4" />
                {localize('com_ui_desktop_model_hub')}
              </h3>
              <div className="space-y-3 rounded-xl border border-border-light bg-surface-secondary p-4">
                <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
                  <label className="text-xs text-text-secondary">
                    {localize('com_ui_desktop_model_download')}
                    <select
                      className="mt-1 h-9 w-full rounded-lg border border-border-medium bg-surface-primary px-2 text-sm text-text-primary"
                      value={downloadModelId}
                      disabled={saving}
                      onChange={(event) => setDownloadModelId(event.target.value)}
                    >
                      {(catalog?.models ?? [])
                        .filter((model) => !model.modelId.startsWith('imported/'))
                        .map((model) => (
                          <option key={model.modelId} value={model.modelId}>
                            {model.displayName}
                          </option>
                        ))}
                    </select>
                  </label>
                  <Button
                    type="button"
                    size="sm"
                    className="self-end"
                    disabled={saving || downloadModelId.trim() === ''}
                    onClick={() => void downloadModel()}
                  >
                    <Download className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_model_download_action')}
                  </Button>
                </div>
                <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto_auto]">
                  <label className="text-xs text-text-secondary">
                    {localize('com_ui_desktop_model_import')}
                    <input
                      className="mt-1 h-9 w-full rounded-lg border border-border-medium bg-surface-primary px-3 text-sm text-text-primary"
                      value={importPath}
                      disabled={saving}
                      placeholder={localize('com_ui_desktop_model_import_placeholder')}
                      onChange={(event) => setImportPath(event.target.value)}
                    />
                  </label>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="self-end"
                    disabled={saving}
                    onClick={() => void browseForModel()}
                  >
                    {localize('com_ui_desktop_model_browse')}
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="self-end"
                    disabled={saving || importPath.trim() === ''}
                    onClick={() => void importModel()}
                  >
                    <FolderInput className="mr-2 h-4 w-4" />
                    {localize('com_ui_desktop_model_import_action')}
                  </Button>
                </div>

                {catalog?.managedModels.length === 0 ? (
                  <p className="rounded-lg bg-surface-primary px-3 py-4 text-center text-sm text-text-secondary">
                    {localize('com_ui_desktop_model_empty')}
                  </p>
                ) : (
                  <div className="space-y-2">
                    {catalog?.managedModels.map((model) => {
                      const progress = modelProgress(model);
                      return (
                        <div key={model.id} className="rounded-lg border border-border-light bg-surface-primary p-3">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0 flex-1">
                              <div className="flex flex-wrap items-center gap-2">
                                <p className="truncate text-sm font-medium">{model.displayName}</p>
                                <span className="rounded-full bg-surface-active-alt px-2 py-0.5 text-[11px] uppercase tracking-wide text-text-secondary">
                                  {model.status}
                                </span>
                              </div>
                              <p className="mt-1 truncate text-xs text-text-secondary" title={model.modelId}>
                                {model.modelId}
                              </p>
                              {progress != null && (
                                <div className="mt-2">
                                  <div className="h-1.5 overflow-hidden rounded-full bg-surface-active-alt">
                                    <div
                                      className="h-full rounded-full bg-blue-500 transition-[width]"
                                      style={{ width: `${progress.percent}%` }}
                                    />
                                  </div>
                                  <p className="mt-1 text-[11px] text-text-secondary">
                                    {formatBytes(model.bytesDownloaded)} / {formatBytes(model.totalBytes ?? 0)}
                                  </p>
                                </div>
                              )}
                              {model.lastError != null && (
                                <p className="mt-2 text-xs text-red-600 dark:text-red-300">
                                  {model.lastError}
                                </p>
                              )}
                            </div>
                            <div className="flex shrink-0 gap-1">
                              {model.status === 'installed' || model.status === 'imported' ? (
                                <Button
                                  type="button"
                                  size="sm"
                                  variant="outline"
                                  disabled={saving || catalog.selection?.modelId === model.modelId}
                                  onClick={() => void selectManagedModel(model.modelId)}
                                >
                                  {catalog.selection?.modelId === model.modelId
                                    ? localize('com_ui_desktop_model_selected')
                                    : localize('com_ui_desktop_model_use')}
                                </Button>
                              ) : null}
                              {isTransferActive(model) ? (
                                <button
                                  type="button"
                                  className="flex h-8 w-8 items-center justify-center rounded-lg text-text-secondary hover:bg-surface-active-alt hover:text-text-primary"
                                  aria-label={localize('com_ui_desktop_model_cancel')}
                                  onClick={() => void cancelDownload(model.id)}
                                >
                                  <X className="h-4 w-4" />
                                </button>
                              ) : (
                                <button
                                  type="button"
                                  className="flex h-8 w-8 items-center justify-center rounded-lg text-text-secondary hover:bg-red-500/10 hover:text-red-600"
                                  aria-label={localize('com_ui_desktop_model_remove')}
                                  onClick={() => void removeModel(model.id)}
                                >
                                  <Trash2 className="h-4 w-4" />
                                </button>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </section>

            <section aria-labelledby="runtime-performance-title">
              <h3 id="runtime-performance-title" className="mb-2 font-semibold">
                {localize('com_ui_desktop_runtime_performance')}
              </h3>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <MetricCard label="TTFT p50" value={formatMilliseconds(performance?.timeToFirstOutputP50Ms)} detail={`p95 ${formatMilliseconds(performance?.timeToFirstOutputP95Ms)}`} />
                <MetricCard label={localize('com_ui_desktop_runtime_total_latency')} value={formatMilliseconds(performance?.totalDurationP50Ms)} detail={`p95 ${formatMilliseconds(performance?.totalDurationP95Ms)}`} />
                <MetricCard label={localize('com_ui_desktop_runtime_decode')} value={performance?.decodeTokensPerSecondP50 == null ? '—' : `${performance.decodeTokensPerSecondP50.toFixed(1)} tok/s`} />
                <MetricCard label={localize('com_ui_desktop_runtime_runs')} value={String(performance?.completedRuns ?? 0)} detail={`${performance?.failedRuns ?? 0} failed · ${performance?.cancelledRuns ?? 0} cancelled`} />
              </div>
            </section>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="min-w-0 rounded-xl border border-border-light bg-surface-secondary p-3">
      <p className="text-xs text-text-secondary">{label}</p>
      <p className="mt-1 truncate font-semibold" title={value}>{value}</p>
      {detail != null && <p className="mt-1 truncate text-xs text-text-secondary" title={detail}>{detail}</p>}
    </div>
  );
}

function isTransferActive(model: ManagedModel): boolean {
  return model.status === 'queued' || model.status === 'downloading';
}

function modelProgress(model: ManagedModel): { percent: number } | null {
  if (!isTransferActive(model) || model.totalBytes == null || model.totalBytes <= 0) {
    return null;
  }
  return {
    percent: Math.min(100, Math.max(0, (model.bytesDownloaded / model.totalBytes) * 100)),
  };
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return '—';
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** index).toFixed(index >= 3 ? 1 : 0)} ${units[index]}`;
}

function formatMilliseconds(value: number | null | undefined): string {
  if (value == null) {
    return '—';
  }
  return value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value} ms`;
}
