import { useEffect, useState } from 'react';
import { useSetRecoilState } from 'recoil';
import {
  Constants,
  type EventSubmission,
  type TConversation,
  type TMessage,
  type TSubmission,
} from 'librechat-data-provider';
import type { GenerationEvent, GenerationRun } from '@northbridge/desktop-transport';
import type { EventHandlerParams } from './useEventHandlers';
import type { TResData } from '~/common';
import {
  desktopConversationToLibreChat,
  desktopMessageToLibreChat,
} from '~/desktop/requestOverride';
import { ensureDesktopModelSelection } from '~/desktop/modelSelection';
import { getActiveKnowledgeStackIds } from '~/desktop/knowledgeSelection';
import { clearDesktopGeneration, registerDesktopGeneration } from '~/desktop/generationRegistry';
import { getDesktopTransport } from '@northbridge/desktop-transport';
import useEventHandlers from './useEventHandlers';
import store from '~/store';

type ChatHelpers = Pick<
  EventHandlerParams,
  'setMessages' | 'getMessages' | 'setConversation' | 'setIsSubmitting' | 'newConversation'
>;

type DesktopResumeSubmission = TSubmission & {
  desktopResumeGenerationId?: string;
};

const claimedSubmissions = new WeakSet<object>();

export default function useDesktopSSE(
  submission: TSubmission | null,
  chatHelpers: ChatHelpers,
  isAddedRequest = false,
  runIndex = 0,
): string | null {
  const [streamId, setStreamId] = useState<string | null>(null);
  const [, setCompleted] = useState(new Set<unknown>());
  const setActiveRunId = useSetRecoilState(store.activeRunFamily(runIndex));
  const setAbortScroll = useSetRecoilState(store.abortScrollFamily(runIndex));
  const setRunEnd = useSetRecoilState(store.runEndByIndex(runIndex));
  const setSubmission = useSetRecoilState(store.submissionByIndex(runIndex));
  const setShowStopButton = useSetRecoilState(store.showStopButtonByIndex(runIndex));
  const { setMessages, getMessages, setConversation, setIsSubmitting, newConversation } =
    chatHelpers;
  const { createdHandler, errorHandler, finalHandler, messageHandler } = useEventHandlers({
    setMessages,
    getMessages,
    setCompleted,
    isAddedRequest,
    setConversation,
    setIsSubmitting,
    newConversation,
    setShowStopButton,
  });

  useEffect(() => {
    if (submission == null || Object.keys(submission).length === 0) {
      setStreamId(null);
      return;
    }
    if (claimedSubmissions.has(submission)) {
      return;
    }
    claimedSubmissions.add(submission);

    const initialResponse = submission.initialResponse;
    if (initialResponse == null) {
      setIsSubmitting(false);
      setShowStopButton(false);
      setSubmission(null);
      return;
    }

    let generationId: string | null = null;
    let conversationId: string | null = null;
    let effectiveSubmission: EventSubmission | null = null;
    let accumulatedText = '';
    let detached = false;
    const streamController = new AbortController();

    const finish = (outcome: 'completed' | 'aborted' | 'error') => {
      if (generationId != null) {
        clearDesktopGeneration(runIndex, generationId);
      }
      if (conversationId != null) {
        setRunEnd({ conversationId, outcome, endedAt: Date.now() });
      }
      setActiveRunId(null);
      setStreamId(null);
      setShowStopButton(false);
      setIsSubmitting(false);
      setSubmission(null);
    };

    const run = async () => {
      const transport = getDesktopTransport();
      if (transport == null) {
        throw new Error('The NorthBridge desktop transport is not initialized.');
      }
      if (submission.isContinued || submission.isEdited) {
        throw new Error('Editing and continuing desktop messages is not available yet.');
      }
      if ((submission.userMessage.files?.length ?? 0) > 0) {
        throw new Error('Desktop attachments are not available yet.');
      }

      const requestedConversationId =
        submission.conversation.conversationId ?? submission.userMessage.conversationId;
      const isNewConversation =
        requestedConversationId == null || requestedConversationId === Constants.NEW_CONVO;
      const resumeGenerationId = (submission as DesktopResumeSubmission).desktopResumeGenerationId;
      if (resumeGenerationId != null && isNewConversation) {
        throw new Error('A resumed generation requires a persisted conversation.');
      }
      let desktopConversation = isNewConversation
        ? await transport.createConversation({ title: conversationTitle(submission.userMessage) })
        : await transport.getConversation(requestedConversationId as string);
      conversationId = desktopConversation.id;

      const allPreviousMessages = await transport.listAllConversationMessages(
        desktopConversation.id,
      );
      let branchId = desktopConversation.defaultBranchId;
      let generation: GenerationRun | null = null;
      let branchMessages;
      let storedUserMessage;
      if (resumeGenerationId != null) {
        generation = await transport.getGeneration(resumeGenerationId);
        if (generation.conversationId !== desktopConversation.id) {
          throw new Error('The resumed generation does not belong to this conversation.');
        }
        branchId = generation.branchId;
        branchMessages = await transport.listMessages(desktopConversation.id, branchId);
        storedUserMessage = allPreviousMessages.find(
          (message) => message.id === generation?.inputMessageId && message.role === 'user',
        );
        if (storedUserMessage == null) {
          throw new Error('The persisted input for the resumed generation is missing.');
        }
      } else if (submission.isRegenerate) {
        branchMessages = await transport.listMessages(desktopConversation.id, branchId);
        storedUserMessage = allPreviousMessages.find(
          (message) => message.id === submission.userMessage.messageId && message.role === 'user',
        );
        if (storedUserMessage == null) {
          throw new Error('The message selected for regeneration is not in desktop storage.');
        }
        const branch = await transport.forkConversationBranch(desktopConversation.id, {
          forkedFromMessageId: storedUserMessage.id,
          activate: true,
        });
        branchId = branch.id;
        desktopConversation = await transport.getConversation(desktopConversation.id);
        branchMessages = await transport.listMessages(desktopConversation.id, branchId);
      } else {
        branchMessages = await transport.listMessages(desktopConversation.id, branchId);
        const currentHead = branchMessages.at(-1)?.id ?? null;
        const selectedParent = concreteParentId(submission.userMessage.parentMessageId);
        if (selectedParent != null && selectedParent !== currentHead) {
          const branch = await transport.forkConversationBranch(desktopConversation.id, {
            forkedFromMessageId: selectedParent,
            activate: true,
          });
          branchId = branch.id;
          desktopConversation = await transport.getConversation(desktopConversation.id);
          branchMessages = await transport.listMessages(desktopConversation.id, branchId);
        }
        storedUserMessage = await transport.appendMessage(desktopConversation.id, {
          branchId,
          parentMessageId: branchMessages.at(-1)?.id ?? null,
          role: 'user',
          parts: [{ kind: 'text', content: submission.userMessage.text ?? '' }],
        });
      }
      const selectedModel = await ensureDesktopModelSelection(
        transport,
        generation?.modelId ?? submission.conversation.model ?? submission.endpointOption?.model,
      );
      const conversation = desktopConversationToLibreChat(desktopConversation);
      const requestMessage: TMessage = {
        ...submission.userMessage,
        ...desktopMessageToLibreChat(storedUserMessage),
      };
      const responseMessage: TMessage = {
        ...initialResponse,
        conversationId: desktopConversation.id,
        parentMessageId: storedUserMessage.id,
        messageId: `${storedUserMessage.id}_`,
      };
      effectiveSubmission = {
        ...submission,
        conversation,
        messages: allPreviousMessages
          .filter((message) => resumeGenerationId == null || message.id !== storedUserMessage.id)
          .map(desktopMessageToLibreChat),
        userMessage: requestMessage,
        initialResponse: responseMessage,
      };

      setAbortScroll(false);
      createdHandler(
        {
          conversation,
          requestMessage,
          responseMessage,
        },
        effectiveSubmission,
      );

      generation ??= await transport.createGeneration(desktopConversation.id, {
        branchId,
        modelAlias: selectedModel.modelAlias,
        clientRequestId: submission.clientRequestId,
        knowledgeStackIds: getActiveKnowledgeStackIds(),
      });
      generationId = generation.id;
      setStreamId(generation.id);
      setActiveRunId(generation.id);
      registerDesktopGeneration(runIndex, {
        conversationId: desktopConversation.id,
        generationId: generation.id,
        transport,
      });
      if (resumeGenerationId == null) {
        await transport.startGeneration(generation.id);
      }

      for await (const event of transport.streamGenerationEvents(generation.id, {
        signal: streamController.signal,
      })) {
        if (event.eventType === 'delta') {
          accumulatedText += payloadText(event);
          messageHandler(accumulatedText, effectiveSubmission);
          continue;
        }
        if (event.eventType === 'completed') {
          const [storedConversation, storedMessages] = await Promise.all([
            transport.getConversation(desktopConversation.id),
            transport.listAllConversationMessages(desktopConversation.id),
          ]);
          const runMessages = storedMessages.map(desktopMessageToLibreChat);
          const finalRequest = runMessages.find(
            (message) => message.messageId === storedUserMessage.id,
          );
          const finalResponse = runMessages.find(
            (message) => message.messageId === generationOutputMessageId(event),
          );
          finalHandler(
            {
              conversation: desktopConversationToLibreChat(storedConversation),
              requestMessage: finalRequest,
              responseMessage: finalResponse,
              runMessages,
            },
            effectiveSubmission,
          );
          finish('completed');
          return;
        }
        if (event.eventType === 'cancelled') {
          const outputMessageId = generationOutputMessageId(event);
          if (outputMessageId != null) {
            const [storedConversation, storedMessages] = await Promise.all([
              transport.getConversation(desktopConversation.id),
              transport.listAllConversationMessages(desktopConversation.id),
            ]);
            const runMessages = storedMessages.map(desktopMessageToLibreChat);
            finalHandler(
              {
                conversation: desktopConversationToLibreChat(storedConversation),
                requestMessage: runMessages.find(
                  (message) => message.messageId === storedUserMessage.id,
                ),
                responseMessage: runMessages.find(
                  (message) => message.messageId === outputMessageId,
                ),
                runMessages,
              },
              effectiveSubmission,
            );
            finish('aborted');
            return;
          }
          const partialResponse: TMessage = {
            ...responseMessage,
            text: accumulatedText,
            unfinished: true,
          };
          finalHandler(
            {
              conversation,
              requestMessage,
              responseMessage: partialResponse,
              runMessages: [...effectiveSubmission.messages, requestMessage, partialResponse],
            },
            effectiveSubmission,
          );
          finish('aborted');
          return;
        }
        if (event.eventType === 'failed') {
          throw new Error(payloadError(event));
        }
      }
      throw new Error('The desktop generation stream ended before a terminal event.');
    };

    setIsSubmitting(true);
    setShowStopButton(true);
    void run().catch((error: unknown) => {
      if (detached || (error instanceof DOMException && error.name === 'AbortError')) {
        return;
      }
      const message = error instanceof Error ? error.message : 'Desktop generation failed.';
      if (effectiveSubmission != null) {
        const responseMessage: TMessage = {
          ...effectiveSubmission.initialResponse,
          text: message,
          error: true,
        };
        errorHandler({
          data: {
            conversation: effectiveSubmission.conversation as TConversation,
            requestMessage: effectiveSubmission.userMessage,
            responseMessage,
          } as TResData,
          submission: effectiveSubmission,
        });
      } else {
        console.error('[NorthBridge desktop] Generation failed before persistence:', error);
      }
      finish('error');
    });
    return () => {
      detached = true;
      streamController.abort();
    };
    // The stream owns the handler snapshot for this submission. Handler identity changes during
    // navigation must close only on an actual submission change, not restart the persisted run.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [submission]);

  return streamId;
}

function conversationTitle(message: TMessage): string {
  const title = (message.text ?? '').replace(/\s+/gu, ' ').trim();
  return title.length > 72 ? `${title.slice(0, 69)}...` : title || 'New Chat';
}

function concreteParentId(parentMessageId?: string | null): string | null {
  if (
    parentMessageId == null ||
    parentMessageId === Constants.NO_PARENT ||
    parentMessageId === Constants.NEW_CONVO ||
    parentMessageId === Constants.PENDING_CONVO
  ) {
    return null;
  }
  return parentMessageId;
}

function payloadText(event: GenerationEvent): string {
  const payload = event.payload;
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload)) {
    return '';
  }
  const record = payload as Record<string, unknown>;
  return typeof record.text === 'string' ? record.text : '';
}

function generationOutputMessageId(event: GenerationEvent): string | undefined {
  const payload = event.payload;
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload)) {
    return undefined;
  }
  const record = payload as Record<string, unknown>;
  return typeof record.outputMessageId === 'string' ? record.outputMessageId : undefined;
}

function payloadError(event: GenerationEvent): string {
  const payload = event.payload;
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload)) {
    return 'Desktop generation failed.';
  }
  const record = payload as Record<string, unknown>;
  return typeof record.message === 'string' ? record.message : 'Desktop generation failed.';
}
