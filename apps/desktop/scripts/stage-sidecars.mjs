import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));

for (const script of ['build-agentgateway.mjs', 'stage-mistralrs.mjs']) {
  execFileSync(process.execPath, [join(scriptDirectory, script)], {
    cwd: scriptDirectory,
    stdio: 'inherit',
  });
}
