import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');
const release = process.argv.includes('--release');
const profile = release ? 'release' : 'debug';
const cargoArguments = ['build', '-p', 'northbridge-desktop-core'];

if (release) {
  cargoArguments.push('--release');
}

execFileSync('cargo', cargoArguments, { cwd: repositoryRoot, stdio: 'inherit' });

const versionDetails = execFileSync('rustc', ['-vV'], {
  cwd: repositoryRoot,
  encoding: 'utf8',
});
const hostLine = versionDetails.split(/\r?\n/u).find((line) => line.startsWith('host: '));

if (hostLine == null) {
  throw new Error('rustc did not report its host target triple');
}

const targetTriple = hostLine.slice('host: '.length).trim();
const extension = process.platform === 'win32' ? '.exe' : '';
const source = join(repositoryRoot, 'target', profile, `desktop-core${extension}`);
const binariesDirectory = join(repositoryRoot, 'apps', 'desktop', 'src-tauri', 'binaries');
const destination = join(binariesDirectory, `desktop-core-${targetTriple}${extension}`);

mkdirSync(binariesDirectory, { recursive: true });
copyFileSync(source, destination);
console.log(`Staged ${destination}`);
