import type { DesktopTransport, ModelCatalogResponse } from '@northbridge/desktop-transport';
import {
  DESKTOP_FALLBACK_MODEL_ID,
  ensureDesktopModelSelection,
  getDesktopDisplayModelId,
  setDesktopModelCatalogForTest,
} from './modelSelection';

const secondModelId = 'HuggingFaceTB/SmolLM2-360M-Instruct';

function catalog(selectedModelId: string | null = null): ModelCatalogResponse {
  const models = [
    {
      provider: 'local_mistral_rs' as const,
      modelId: DESKTOP_FALLBACK_MODEL_ID,
      modelAlias: 'fast-local',
      displayName: 'SmolLM2 135M Instruct',
      description: 'Starter',
      contextWindow: 8192,
      recommended: true,
    },
    {
      provider: 'local_mistral_rs' as const,
      modelId: secondModelId,
      modelAlias: 'fast-local',
      displayName: 'SmolLM2 360M Instruct',
      description: 'Larger starter',
      contextWindow: 8192,
      recommended: false,
    },
  ];
  const selected = models.find((model) => model.modelId === selectedModelId);
  return {
    models,
    managedModels: [],
    selection:
      selected == null
        ? null
        : {
            provider: selected.provider,
            modelId: selected.modelId,
            modelAlias: selected.modelAlias,
            loadProfile: {
              quantization: 'none',
              maxSequenceLength: 8192,
              maxBatchSize: 1,
              forceCpu: true,
            },
          },
    runtime: null,
  };
}

function transport(initial: ModelCatalogResponse): jest.Mocked<DesktopTransport> {
  return {
    modelCatalog: jest.fn().mockResolvedValue(initial),
    selectModel: jest.fn().mockImplementation(({ modelId }: { modelId: string }) => {
      return Promise.resolve(catalog(modelId));
    }),
  } as unknown as jest.Mocked<DesktopTransport>;
}

describe('desktop model selection', () => {
  beforeEach(() => setDesktopModelCatalogForTest(null));

  it('activates the requested catalog model behind the stable alias', async () => {
    const client = transport(catalog());
    const selected = await ensureDesktopModelSelection(client, secondModelId);

    expect(client.selectModel).toHaveBeenCalledWith({ modelId: secondModelId });
    expect(selected).toMatchObject({ modelId: secondModelId, modelAlias: 'fast-local' });
    expect(getDesktopDisplayModelId()).toBe(secondModelId);
  });

  it('keeps an already selected model without reconfiguring the runtime', async () => {
    const client = transport(catalog(DESKTOP_FALLBACK_MODEL_ID));
    const selected = await ensureDesktopModelSelection(client, DESKTOP_FALLBACK_MODEL_ID);

    expect(client.selectModel).not.toHaveBeenCalled();
    expect(selected.modelId).toBe(DESKTOP_FALLBACK_MODEL_ID);
  });

  it('falls back to the recommended model for stale LibreChat model values', async () => {
    const client = transport(catalog());
    const selected = await ensureDesktopModelSelection(client, 'fast-local');

    expect(selected.modelId).toBe(DESKTOP_FALLBACK_MODEL_ID);
  });
});
