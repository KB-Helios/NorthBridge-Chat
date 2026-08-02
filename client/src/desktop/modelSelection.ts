import type {
  DesktopTransport,
  ModelCatalogResponse,
  ModelSelection,
} from '@northbridge/desktop-transport';

export const DESKTOP_FALLBACK_MODEL_ID = 'HuggingFaceTB/SmolLM2-135M-Instruct';

let catalogSnapshot: ModelCatalogResponse | null = null;

export async function refreshDesktopModelCatalog(
  transport: DesktopTransport,
): Promise<ModelCatalogResponse> {
  catalogSnapshot = await transport.modelCatalog();
  return catalogSnapshot;
}

export async function ensureDesktopModelSelection(
  transport: DesktopTransport,
  requestedModelId?: string | null,
): Promise<ModelSelection> {
  const catalog = await refreshDesktopModelCatalog(transport);
  const requested = catalog.models.find((model) => model.modelId === requestedModelId);
  const target =
    requested ??
    catalog.models.find((model) => model.modelId === catalog.selection?.modelId) ??
    catalog.models.find((model) => model.recommended) ??
    catalog.models[0];
  if (target == null) {
    throw new Error('NorthBridge did not publish any compatible local models.');
  }
  if (
    catalog.selection?.provider === target.provider &&
    catalog.selection.modelId === target.modelId
  ) {
    return catalog.selection;
  }
  catalogSnapshot = await transport.selectModel({ modelId: target.modelId });
  if (catalogSnapshot.selection == null) {
    throw new Error('NorthBridge did not activate the selected local model.');
  }
  return catalogSnapshot.selection;
}

export function getDesktopDisplayModelId(): string {
  return (
    catalogSnapshot?.selection?.modelId ??
    catalogSnapshot?.models.find((model) => model.recommended)?.modelId ??
    catalogSnapshot?.models[0]?.modelId ??
    DESKTOP_FALLBACK_MODEL_ID
  );
}

export function setDesktopModelCatalogForTest(snapshot: ModelCatalogResponse | null): void {
  catalogSnapshot = snapshot;
}
