import type { GenerationEvent } from './types';

interface PendingEvent {
  id: string;
  event: string;
  data: string[];
}

const emptyEvent = (): PendingEvent => ({ id: '', event: '', data: [] });

export async function* parseGenerationEventStream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<GenerationEvent> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let pending = emptyEvent();

  const parseLine = (line: string): GenerationEvent | null => {
    if (line === '') {
      if (pending.data.length === 0) {
        pending = emptyEvent();
        return null;
      }
      const data = pending.data.join('\n');
      pending = emptyEvent();
      return JSON.parse(data) as GenerationEvent;
    }
    if (line.startsWith(':')) {
      return null;
    }
    const separator = line.indexOf(':');
    const field = separator === -1 ? line : line.slice(0, separator);
    let value = separator === -1 ? '' : line.slice(separator + 1);
    if (value.startsWith(' ')) {
      value = value.slice(1);
    }
    if (field === 'data') {
      pending.data.push(value);
    } else if (field === 'id' && !value.includes('\0')) {
      pending.id = value;
    } else if (field === 'event') {
      pending.event = value;
    }
    return null;
  };

  try {
    while (true) {
      const result = await reader.read();
      buffer += decoder.decode(result.value, { stream: !result.done });
      let newline = buffer.indexOf('\n');
      while (newline !== -1) {
        const rawLine = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        const event = parseLine(rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine);
        if (event != null) {
          yield event;
        }
        newline = buffer.indexOf('\n');
      }
      if (result.done) {
        break;
      }
    }

    if (buffer !== '') {
      const event = parseLine(buffer.endsWith('\r') ? buffer.slice(0, -1) : buffer);
      if (event != null) {
        yield event;
      }
    }
    const finalEvent = parseLine('');
    if (finalEvent != null) {
      yield finalEvent;
    }
  } finally {
    reader.releaseLock();
  }
}
