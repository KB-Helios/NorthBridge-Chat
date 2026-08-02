import {
  Constants,
  EModelEndpoint,
  RetentionMode,
  type RequestOverride,
  type TConversation,
  type TEndpointsConfig,
  type TMessage,
  type TModelsConfig,
  type TStartupConfig,
} from 'librechat-data-provider';
import type {
  Conversation as DesktopConversation,
  DesktopTransport,
  Message as DesktopMessage,
} from '@northbridge/desktop-transport';
import { getDesktopDisplayModelId, refreshDesktopModelCatalog } from './modelSelection';

export const DESKTOP_ENDPOINT = 'NorthBridge Local';
export const DESKTOP_MODEL_ALIAS = 'fast-local';

const startupConfig: TStartupConfig = {
  appTitle: 'NorthBridge',
  discordLoginEnabled: false,
  facebookLoginEnabled: false,
  githubLoginEnabled: false,
  googleLoginEnabled: false,
  openidLoginEnabled: false,
  appleLoginEnabled: false,
  samlLoginEnabled: false,
  openidLabel: '',
  openidImageUrl: '',
  openidAutoRedirect: false,
  samlLabel: '',
  samlImageUrl: '',
  serverDomain: '127.0.0.1',
  emailLoginEnabled: false,
  registrationEnabled: false,
  socialLoginEnabled: false,
  passwordResetEnabled: false,
  emailEnabled: false,
  showBirthdayIcon: false,
  helpAndFaqURL: '',
  sharedLinksEnabled: false,
  publicSharedLinksEnabled: false,
  allowAccountDeletion: false,
  interface: {
    retentionMode: RetentionMode.ALL,
    modelSelect: true,
    parameters: true,
    presets: false,
    bookmarks: false,
    multiConvo: true,
    agents: false,
  },
};

const endpointsConfig: TEndpointsConfig = {
  [DESKTOP_ENDPOINT]: {
    order: 0,
    type: EModelEndpoint.custom,
    name: DESKTOP_ENDPOINT,
    modelDisplayLabel: 'Local model',
    userProvide: false,
  },
};

export function createDesktopRequestOverride(transport: DesktopTransport): RequestOverride {
  return async ({ method, url, data }) => {
    const parsed = new URL(url, window.location.origin);
    const path = parsed.pathname;

    if (method === 'GET' && path === '/api/config') {
      return handled(startupConfig);
    }
    if (method === 'GET' && path === '/api/endpoints') {
      return handled(endpointsConfig);
    }
    if (method === 'GET' && path === '/api/endpoints/token-config') {
      const catalog = await refreshDesktopModelCatalog(transport);
      return handled({
        [DESKTOP_ENDPOINT]: Object.fromEntries(
          catalog.models.map((model) => [model.modelId, { context: model.contextWindow }]),
        ),
      });
    }
    if (method === 'GET' && path === '/api/models') {
      const catalog = await refreshDesktopModelCatalog(transport);
      const modelsConfig: TModelsConfig = {
        initial: [DESKTOP_ENDPOINT],
        [DESKTOP_ENDPOINT]: catalog.models.map((model) => model.modelId),
      };
      return handled(modelsConfig);
    }
    if (method === 'GET' && path === '/health') {
      await transport.health();
      return handled('OK');
    }
    if (method === 'GET' && path === '/api/search/enable') {
      return handled(true);
    }
    if (method === 'GET' && path === '/api/banner') {
      return handled(null);
    }
    if (method === 'GET' && path === '/api/convos') {
      const page = await transport.listConversations({
        cursor: parsed.searchParams.get('cursor') ?? undefined,
        limit: 100,
      });
      return handled({
        conversations: page.items.map(desktopConversationToLibreChat),
        nextCursor: page.nextCursor,
      });
    }
    const conversationMatch = path.match(/^\/api\/convos\/([^/]+)$/u);
    if (method === 'GET' && conversationMatch) {
      const conversation = await transport.getConversation(
        decodeURIComponent(conversationMatch[1]),
      );
      return handled(desktopConversationToLibreChat(conversation));
    }
    if (method === 'DELETE' && path === '/api/convos') {
      const payload = data as { arg?: { conversationId?: string } } | undefined;
      const conversationId = payload?.arg?.conversationId;
      if (!conversationId) {
        throw new Error('conversationId is required');
      }
      await transport.deleteConversation(conversationId);
      return handled({
        acknowledged: true,
        deletedCount: 1,
        messages: { acknowledged: true, deletedCount: 1 },
      });
    }
    const messagesMatch = path.match(/^\/api\/messages\/([^/]+)$/u);
    if (method === 'GET' && messagesMatch) {
      const conversationId = decodeURIComponent(messagesMatch[1]);
      const messages = await transport.listAllConversationMessages(conversationId);
      return handled(messages.map(desktopMessageToLibreChat));
    }
    if (method === 'GET' && path === '/api/projects') {
      return handled({ projects: [], nextCursor: null });
    }
    if (method === 'GET' && path === '/api/tags') {
      return handled([]);
    }
    if (method === 'GET' && path.startsWith('/api/prompts')) {
      return handled([]);
    }

    return { handled: false };
  };
}

export function desktopConversationToLibreChat(conversation: DesktopConversation): TConversation {
  return {
    conversationId: conversation.id,
    endpoint: DESKTOP_ENDPOINT as EModelEndpoint,
    endpointType: EModelEndpoint.custom,
    title: conversation.title || 'New Chat',
    model: getDesktopDisplayModelId(),
    createdAt: new Date(conversation.createdAtMs).toISOString(),
    updatedAt: new Date(conversation.updatedAtMs).toISOString(),
  };
}

export function desktopMessageToLibreChat(message: DesktopMessage): TMessage {
  const isCreatedByUser = message.role === 'user';
  const text = message.parts
    .filter((part) => part.kind === 'text' || part.kind === 'reasoning')
    .map((part) => part.content)
    .join('\n');
  const createdAt = new Date(message.createdAtMs).toISOString();
  const unfinished = message.parts.some(
    (part) =>
      part.metadata != null && 'unfinished' in part.metadata && part.metadata.unfinished === true,
  );
  const metadata = message.parts.reduce<Record<string, unknown>>((merged, part) => {
    if (part.metadata != null) {
      Object.assign(merged, part.metadata);
    }
    return merged;
  }, {});
  return {
    messageId: message.id,
    conversationId: message.conversationId,
    parentMessageId: message.parentMessageId ?? Constants.NO_PARENT,
    responseMessageId: null,
    endpoint: DESKTOP_ENDPOINT,
    model: getDesktopDisplayModelId(),
    sender: isCreatedByUser ? 'User' : 'NorthBridge',
    text,
    isCreatedByUser,
    error: false,
    unfinished,
    metadata: Object.keys(metadata).length === 0 ? undefined : metadata,
    createdAt,
    updatedAt: createdAt,
  };
}

function handled(data: unknown) {
  return { handled: true as const, data };
}
