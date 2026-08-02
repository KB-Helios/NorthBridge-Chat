import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DesktopTransport,
  DesktopTransportError,
  parseGenerationEventStream,
} from '../dist/index.mjs';

const encoder = new TextEncoder();

test('parses split CRLF and multi-line SSE events', async () => {
  const chunks = [
    'id: 2\r\nevent: delta\r\nda',
    'ta: {"generationId":"run","sequenceNumber":2,\r\n',
    'data: "eventType":"delta","timestampMs":1,"payload":{"text":"two"}}\r\n\r\n',
  ];
  const stream = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  const events = [];
  for await (const event of parseGenerationEventStream(stream)) {
    events.push(event);
  }
  assert.equal(events.length, 1);
  assert.equal(events[0].sequenceNumber, 2);
  assert.equal(events[0].payload.text, 'two');
});

test('sends the per-session bearer token and preserves API errors', async () => {
  const requests = [];
  const transport = new DesktopTransport(
    {
      baseUrl: 'http://127.0.0.1:49152',
      sessionToken: 'session-token',
      pid: 42,
      schemaVersion: 1,
    },
    async (url, options) => {
      requests.push({ url: String(url), options });
      return new Response(
        JSON.stringify({ error: { code: 'not_found', message: 'conversation was not found' } }),
        { status: 404, headers: { 'Content-Type': 'application/json' } },
      );
    },
  );

  await assert.rejects(
    transport.getConversation('missing'),
    (error) =>
      error instanceof DesktopTransportError && error.status === 404 && error.code === 'not_found',
  );
  assert.equal(requests.length, 1);
  assert.equal(requests[0].options.headers.get('Authorization'), 'Bearer session-token');
});

test('invokes fetch with the browser global receiver required by WebView2', async () => {
  const transport = new DesktopTransport(
    {
      baseUrl: 'http://127.0.0.1:49152',
      sessionToken: 'session-token',
      pid: 42,
      schemaVersion: 1,
    },
    async function receiverAwareFetch() {
      assert.equal(this, globalThis);
      return new Response(JSON.stringify({ status: 'ok', schemaVersion: 1 }), {
        headers: { 'Content-Type': 'application/json' },
      });
    },
  );

  assert.deepEqual(await transport.health(), { status: 'ok', schemaVersion: 1 });
});
