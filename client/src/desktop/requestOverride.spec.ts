import type { DesktopTransport } from '@northbridge/desktop-transport';
import { DESKTOP_FALLBACK_MODEL_ID, setDesktopModelCatalogForTest } from './modelSelection';
import { DESKTOP_ENDPOINT, createDesktopRequestOverride } from './requestOverride';

const modelCatalog = {
  models: [
    {
      provider: 'local_mistral_rs' as const,
      modelId: DESKTOP_FALLBACK_MODEL_ID,
      modelAlias: 'fast-local',
      displayName: 'SmolLM2 135M Instruct',
      description: 'Starter model',
      contextWindow: 8192,
      recommended: true,
    },
  ],
  selection: null,
  runtime: null,
};

const conversation = {
  id: '018f1000-0000-7000-8000-000000000001',
  workspaceId: '018f1000-0000-7000-8000-000000000002',
  defaultBranchId: '018f1000-0000-7000-8000-000000000003',
  title: 'Local conversation',
  createdAtMs: 1_700_000_000_000,
  updatedAtMs: 1_700_000_001_000,
};

const message = {
  id: '018f1000-0000-7000-8000-000000000004',
  conversationId: conversation.id,
  branchId: conversation.defaultBranchId,
  parentMessageId: null,
  role: 'user' as const,
  createdAtMs: 1_700_000_000_500,
  parts: [{ id: 'part-1', kind: 'text' as const, content: 'Hello locally', metadata: null }],
};

function transportMock(): jest.Mocked<DesktopTransport> {
  return {
    health: jest.fn().mockResolvedValue({ status: 'ok', schemaVersion: 2 }),
    modelCatalog: jest.fn().mockResolvedValue(modelCatalog),
    listConversations: jest
      .fn()
      .mockResolvedValue({ items: [conversation], nextCursor: 'next-page' }),
    getConversation: jest.fn().mockResolvedValue(conversation),
    deleteConversation: jest.fn().mockResolvedValue(undefined),
    listAllConversationMessages: jest.fn().mockResolvedValue([message]),
    searchMessages: jest.fn().mockResolvedValue([
      {
        messageId: message.id,
        conversationId: conversation.id,
        partId: 'part-1',
        content: 'Hello locally',
        score: -1,
      },
    ]),
  } as unknown as jest.Mocked<DesktopTransport>;
}

describe('createDesktopRequestOverride', () => {
  beforeEach(() => setDesktopModelCatalogForTest(null));

  it('serves the local startup, endpoint, and model contracts', async () => {
    const override = createDesktopRequestOverride(transportMock());

    await expect(override({ method: 'GET', url: '/api/config' })).resolves.toMatchObject({
      handled: true,
      data: { appTitle: 'NorthBridge', emailLoginEnabled: false },
    });
    await expect(override({ method: 'GET', url: '/api/endpoints' })).resolves.toMatchObject({
      handled: true,
      data: { [DESKTOP_ENDPOINT]: { name: DESKTOP_ENDPOINT } },
    });
    await expect(override({ method: 'GET', url: '/api/models' })).resolves.toEqual({
      handled: true,
      data: { initial: [DESKTOP_ENDPOINT], [DESKTOP_ENDPOINT]: [DESKTOP_FALLBACK_MODEL_ID] },
    });
  });

  it('maps conversation and message reads to desktop-core', async () => {
    const transport = transportMock();
    const override = createDesktopRequestOverride(transport);

    const conversations = await override({ method: 'GET', url: '/api/convos?cursor=cursor-1' });
    const messages = await override({
      method: 'GET',
      url: `/api/messages/${conversation.id}`,
    });

    expect(transport.listConversations).toHaveBeenCalledWith({ cursor: 'cursor-1', limit: 100 });
    expect(conversations).toMatchObject({
      handled: true,
      data: {
        conversations: [{ conversationId: conversation.id, title: conversation.title }],
        nextCursor: 'next-page',
      },
    });
    expect(messages).toMatchObject({
      handled: true,
      data: [{ messageId: message.id, text: 'Hello locally', isCreatedByUser: true }],
    });
  });

  it('routes deletion to desktop-core and leaves unknown requests untouched', async () => {
    const transport = transportMock();
    const override = createDesktopRequestOverride(transport);

    await expect(
      override({
        method: 'DELETE',
        url: '/api/convos',
        data: { arg: { conversationId: conversation.id } },
      }),
    ).resolves.toMatchObject({ handled: true, data: { acknowledged: true, deletedCount: 1 } });
    expect(transport.deleteConversation).toHaveBeenCalledWith(conversation.id);
    await expect(override({ method: 'GET', url: '/api/not-desktop-owned' })).resolves.toEqual({
      handled: false,
    });
  });

  it('maps desktop full-text search to the message-list response contract', async () => {
    const transport = transportMock();
    const override = createDesktopRequestOverride(transport);

    const result = await override({
      method: 'GET',
      url: '/api/messages?search=Hello&pageSize=10',
    });

    expect(transport.searchMessages).toHaveBeenCalledWith('Hello', 10);
    expect(result).toMatchObject({
      handled: true,
      data: {
        messages: [
          {
            messageId: message.id,
            conversationId: conversation.id,
            title: conversation.title,
            text: 'Hello locally',
          },
        ],
        nextCursor: null,
      },
    });
  });
});
