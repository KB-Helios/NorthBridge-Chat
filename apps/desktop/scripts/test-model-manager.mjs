import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');

execFileSync(
  'cargo',
  [
    'test',
    '-p',
    'northbridge-model-manager',
    '--test',
    'real_download',
    '--',
    '--ignored',
    '--nocapture',
  ],
  {
    cwd: repositoryRoot,
    env: {
      ...process.env,
      NORTHBRIDGE_RUN_REAL_MODEL_DOWNLOAD: '1',
    },
    stdio: 'inherit',
  },
);
