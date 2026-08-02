import assert from 'node:assert/strict';
import test from 'node:test';

import { DesktopTransport } from '../dist/index.mjs';

function testTransport(requests) {
  return new DesktopTransport(
    {
      baseUrl: 'http://127.0.0.1:49152',
      sessionToken: 'knowledge-token',
      pid: 42,
      schemaVersion: 8,
    },
    async (url, options = {}) => {
      requests.push({ url: new URL(String(url)), options });
      return new Response(JSON.stringify({}), {
        headers: { 'Content-Type': 'application/json' },
      });
    },
  );
}

test('sends the complete knowledge API contract with authenticated JSON bodies', async () => {
  const requests = [];
  const transport = testTransport(requests);

  await transport.createKnowledgeStack({ name: 'Engineering', chunkSize: 900 });
  await transport.importKnowledgeDocuments('stack/one', {
    paths: ['C:\\notes\\one.md', 'C:\\notes\\two.pdf'],
  });
  await transport.searchKnowledge('stack/one', { query: 'release process', limit: 6 });
  await transport.createKnowledgeWatchRoot('stack/one', {
    path: 'C:\\vault',
    kind: 'obsidian',
    recursive: true,
  });
  await transport.rebuildKnowledgeIndex('stack/one');
  await transport.createGeneration('conversation', {
    branchId: 'branch',
    modelAlias: 'local-default',
    clientRequestId: 'request',
    knowledgeStackIds: ['stack/one', 'stack-two'],
  });

  assert.deepEqual(
    requests.map((request) => [request.url.pathname, request.options.method ?? 'GET']),
    [
      ['/v1/knowledge/stacks', 'POST'],
      ['/v1/knowledge/stacks/stack%2Fone/documents/import', 'POST'],
      ['/v1/knowledge/stacks/stack%2Fone/search', 'POST'],
      ['/v1/knowledge/stacks/stack%2Fone/watch-roots', 'POST'],
      ['/v1/knowledge/stacks/stack%2Fone/index', 'POST'],
      ['/v1/conversations/conversation/generations', 'POST'],
    ],
  );
  assert.ok(
    requests.every(
      (request) => request.options.headers.get('Authorization') === 'Bearer knowledge-token',
    ),
  );
  assert.deepEqual(JSON.parse(requests[1].options.body), {
    paths: ['C:\\notes\\one.md', 'C:\\notes\\two.pdf'],
  });
  assert.deepEqual(JSON.parse(requests[2].options.body), {
    query: 'release process',
    limit: 6,
  });
  assert.deepEqual(JSON.parse(requests[5].options.body).knowledgeStackIds, [
    'stack/one',
    'stack-two',
  ]);
});

test('encodes knowledge identifiers and workspace filters', async () => {
  const requests = [];
  const transport = testTransport(requests);

  await transport.listKnowledgeStacks('workspace/id');
  await transport.listKnowledgeDocumentChunks('document/id');
  await transport.rescanKnowledgeWatchRoot('root/id');
  await transport.cancelIngestionJob('job/id');

  assert.equal(requests[0].url.pathname, '/v1/knowledge/stacks');
  assert.equal(requests[0].url.searchParams.get('workspaceId'), 'workspace/id');
  assert.equal(requests[1].url.pathname, '/v1/knowledge/documents/document%2Fid/chunks');
  assert.equal(requests[2].url.pathname, '/v1/knowledge/watch-roots/root%2Fid/rescan');
  assert.equal(requests[3].url.pathname, '/v1/knowledge/jobs/job%2Fid/cancel');
});
