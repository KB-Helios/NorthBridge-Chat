import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { hostTargetTriple } from './sidecar-utils.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');
const targetTriple = hostTargetTriple(repositoryRoot);
const extension = process.platform === 'win32' ? '.exe' : '';
const binariesDirectory = join(repositoryRoot, 'apps', 'desktop', 'src-tauri', 'binaries');
const executable = join(binariesDirectory, `agentgateway-${targetTriple}${extension}`);
const manifestPath = join(binariesDirectory, `agentgateway-${targetTriple}.manifest.json`);

if (!existsSync(executable) || !existsSync(manifestPath)) {
  throw new Error('Run npm run desktop:stage-sidecars before the real gateway test');
}
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
execFileSync(
  'cargo',
  [
    'test',
    '-p',
    'northbridge-ai-gateway',
    '--test',
    'real_gateway',
    '--',
    '--ignored',
    '--nocapture',
  ],
  {
    cwd: repositoryRoot,
    env: {
      ...process.env,
      NORTHBRIDGE_REAL_AGENTGATEWAY: executable,
      NORTHBRIDGE_REAL_AGENTGATEWAY_SHA256: manifest.binarySha256,
    },
    stdio: 'inherit',
  },
);
