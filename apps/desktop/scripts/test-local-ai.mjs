import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { hostTargetTriple } from './sidecar-utils.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');
const targetTriple = hostTargetTriple(repositoryRoot);
const extension = process.platform === 'win32' ? '.exe' : '';
const binariesDirectory = join(repositoryRoot, 'apps', 'desktop', 'src-tauri', 'binaries');
const agentgateway = join(binariesDirectory, `agentgateway-${targetTriple}${extension}`);
const mistralrs = join(binariesDirectory, `mistralrs-${targetTriple}${extension}`);
const agentgatewayManifestPath = join(
  binariesDirectory,
  `agentgateway-${targetTriple}.manifest.json`,
);
const mistralrsManifestPath = join(binariesDirectory, `mistralrs-${targetTriple}.manifest.json`);

for (const path of [agentgateway, mistralrs, agentgatewayManifestPath, mistralrsManifestPath]) {
  if (!existsSync(path)) {
    throw new Error(`Missing staged sidecar artifact: ${path}`);
  }
}
const agentgatewayManifest = JSON.parse(readFileSync(agentgatewayManifestPath, 'utf8'));
const mistralrsManifest = JSON.parse(readFileSync(mistralrsManifestPath, 'utf8'));
const cacheDirectory = join(repositoryRoot, 'target', 'real-ai-cache');
mkdirSync(cacheDirectory, { recursive: true });
const modelId = process.env.NORTHBRIDGE_REAL_AI_MODEL ?? 'HuggingFaceTB/SmolLM2-135M-Instruct';
const embeddingModelId = 'Qwen/Qwen3-Embedding-0.6B';
const cachedModelDirectory = join(
  cacheDirectory,
  'hub',
  `models--${modelId.replaceAll('/', '--')}`,
);
const cachedEmbeddingModelDirectory = join(
  cacheDirectory,
  'hub',
  `models--${embeddingModelId.replaceAll('/', '--')}`,
);
const offlineCacheReady =
  existsSync(cachedModelDirectory) && existsSync(cachedEmbeddingModelDirectory);

execFileSync(
  'cargo',
  ['test', '-p', 'northbridge-desktop-core', '--test', 'real_ai', '--', '--ignored', '--nocapture'],
  {
    cwd: repositoryRoot,
    env: {
      ...process.env,
      NORTHBRIDGE_RUN_REAL_AI_TEST: '1',
      NORTHBRIDGE_REAL_AGENTGATEWAY: agentgateway,
      NORTHBRIDGE_REAL_AGENTGATEWAY_SHA256: agentgatewayManifest.binarySha256,
      NORTHBRIDGE_REAL_MISTRALRS: mistralrs,
      NORTHBRIDGE_REAL_MISTRALRS_SHA256: mistralrsManifest.binarySha256,
      NORTHBRIDGE_REAL_AI_CACHE: cacheDirectory,
      ...(offlineCacheReady ? { HF_HUB_OFFLINE: '1' } : {}),
    },
    stdio: 'inherit',
  },
);
