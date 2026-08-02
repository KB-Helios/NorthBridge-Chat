import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  renameSync,
  rmSync,
} from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { dirname, resolve, sep } from 'node:path';

export async function sha256File(path) {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(path)) {
    digest.update(chunk);
  }
  return digest.digest('hex');
}

export async function downloadVerified(url, destination, expectedSha256) {
  mkdirSync(dirname(destination), { recursive: true });
  if (existsSync(destination) && (await sha256File(destination)) === expectedSha256) {
    return;
  }

  const partial = `${destination}.partial`;
  rmSync(partial, { force: true });
  const response = await fetch(url, { redirect: 'follow' });
  if (!response.ok || response.body == null) {
    throw new Error(`Failed to download ${url}: HTTP ${response.status}`);
  }
  await pipeline(Readable.fromWeb(response.body), createWriteStream(partial, { flags: 'wx' }));
  const actual = await sha256File(partial);
  if (actual !== expectedSha256) {
    rmSync(partial, { force: true });
    throw new Error(
      `Downloaded artifact failed SHA-256 verification: expected ${expectedSha256}, got ${actual}`,
    );
  }
  rmSync(destination, { force: true });
  renameSync(partial, destination);
}

export function hostTargetTriple(repositoryRoot, toolchain) {
  const command =
    toolchain == null ? ['rustc', ['-vV']] : ['rustup', ['run', toolchain, 'rustc', '-vV']];
  const output = execFileSync(command[0], command[1], {
    cwd: repositoryRoot,
    encoding: 'utf8',
  });
  const host = output.split(/\r?\n/u).find((line) => line.startsWith('host: '));
  if (host == null) {
    throw new Error('rustc did not report its host target triple');
  }
  return host.slice('host: '.length).trim();
}

export function removeWithin(target, allowedRoot) {
  const resolvedTarget = resolve(target);
  const resolvedRoot = `${resolve(allowedRoot)}${sep}`;
  if (!resolvedTarget.startsWith(resolvedRoot)) {
    throw new Error(`Refusing to remove outside ${allowedRoot}: ${resolvedTarget}`);
  }
  rmSync(resolvedTarget, { recursive: true, force: true });
}
