import { useEffect, useRef } from 'react';
import { useRecoilValue, useSetRecoilState } from 'recoil';
import { Constants, type TMessage, type TSubmission } from 'librechat-data-provider';
import {
  getDesktopTransport,
  isDesktopRuntime,
  type GenerationRun,
} from '@northbridge/desktop-transport';
import {
  DESKTOP_ENDPOINT,
  desktopConversationToLibreChat,
  desktopMessageToLibreChat,
} from '~/desktop/requestOverride';
import { getDesktopDisplayModelId } from '~/desktop/modelSelection';
import store from '~/store';

type DesktopResumeSubmission = TSubmission & {
  desktopResumeGenerationId: string;
};

export default function useDesktopResumeOnLoad(
  conversationId: string | undefined,
  runIndex = 0,
  messagesLoaded = true,
): void {
  const currentSubmission = useRecoilValue(store.submissionByIndex(runIndex));
  const setSubmission = useSetRecoilState(store.submissionByIndex(runIndex));
  const processedGeneration = useRef<string | null>(null);

  useEffect(() => {
    if (
      !isDesktopRuntime() ||
      !messagesLoaded ||
      currentSubmission != null ||
      conversationId == null ||
      conversationId === Constants.NEW_CONVO
    ) {
      return;
    }

    let detached = false;
    const resume = async () => {
      const transport = getDesktopTransport();
      if (transport == null) {
        return;
      }
      const generations = await transport.listActiveGenerations(conversationId);
      const generation = generations.at(-1);
      if (generation == null || generation.id === processedGeneration.current || detached) {
        return;
      }
      const [conversation, messages] = await Promise.all([
        transport.getConversation(conversationId),
        transport.listAllConversationMessages(conversationId),
      ]);
      const inputMessage = findInputMessage(generation, messages.map(desktopMessageToLibreChat));
      if (inputMessage == null || detached) {
        return;
      }
      processedGeneration.current = generation.id;
      const desktopConversation = desktopConversationToLibreChat(conversation);
      const initialResponse: TMessage = {
        conversationId,
        messageId: `${generation.id}_`,
        parentMessageId: inputMessage.messageId,
        endpoint: DESKTOP_ENDPOINT,
        model: generation.modelId ?? getDesktopDisplayModelId(),
        sender: 'NorthBridge',
        text: '',
        isCreatedByUser: false,
        error: false,
      };
      const resumedSubmission: DesktopResumeSubmission = {
        conversation: desktopConversation,
        endpointOption: {
          endpoint: desktopConversation.endpoint,
          endpointType: desktopConversation.endpointType,
          model: generation.modelId ?? getDesktopDisplayModelId(),
        },
        userMessage: inputMessage,
        messages: [],
        initialResponse,
        isTemporary: false,
        clientRequestId: generation.clientRequestId ?? undefined,
        desktopResumeGenerationId: generation.id,
      };
      setSubmission(resumedSubmission);
    };

    void resume().catch((error: unknown) => {
      if (!detached) {
        console.error('[NorthBridge desktop] Failed to resume generation:', error);
      }
    });
    return () => {
      detached = true;
    };
  }, [conversationId, currentSubmission, messagesLoaded, setSubmission]);
}

function findInputMessage(generation: GenerationRun, messages: TMessage[]): TMessage | undefined {
  return messages.find(
    (message) =>
      message.messageId === generation.inputMessageId && message.isCreatedByUser === true,
  );
}
