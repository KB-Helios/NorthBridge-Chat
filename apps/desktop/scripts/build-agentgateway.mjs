import { execFileSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { downloadVerified, hostTargetTriple, removeWithin, sha256File } from './sidecar-utils.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');
const vendorDirectory = join(repositoryRoot, 'third_party', 'agentgateway');
const manifest = JSON.parse(readFileSync(join(vendorDirectory, 'manifest.json'), 'utf8'));
const patchPath = join(vendorDirectory, 'northbridge-loopback-v1.4.0.patch');
const patchSha256 = await sha256File(patchPath);

if (patchSha256 !== manifest.patchSha256) {
  throw new Error(
    `agentgateway patch hash changed: expected ${manifest.patchSha256}, got ${patchSha256}`,
  );
}

ensureToolchain(manifest.rustToolchain);
const targetTriple = hostTargetTriple(repositoryRoot, manifest.rustToolchain);
const extension = process.platform === 'win32' ? '.exe' : '';
const binariesDirectory = join(repositoryRoot, 'apps', 'desktop', 'src-tauri', 'binaries');
const stagedBinary = join(binariesDirectory, `agentgateway-${targetTriple}${extension}`);
const stagedManifest = join(binariesDirectory, `agentgateway-${targetTriple}.manifest.json`);

if (await stagedArtifactIsCurrent()) {
  console.log(`Using verified cached ${stagedBinary}`);
  process.exit(0);
}

const downloadsDirectory = join(repositoryRoot, 'target', 'sidecar-downloads');
const sourcesDirectory = join(repositoryRoot, 'target', 'sidecar-sources');
const archive = join(downloadsDirectory, `agentgateway-${manifest.upstreamCommit}.zip`);
const sourceDirectory = join(sourcesDirectory, `agentgateway-${manifest.upstreamCommit}`);
await downloadVerified(manifest.sourceArchiveUrl, archive, manifest.sourceArchiveSha256);

if (!existsSync(join(sourceDirectory, 'Cargo.toml'))) {
  if (existsSync(sourceDirectory)) {
    removeWithin(sourceDirectory, sourcesDirectory);
  }
  mkdirSync(sourcesDirectory, { recursive: true });
  execFileSync('tar', ['-xf', archive, '-C', sourcesDirectory], {
    cwd: repositoryRoot,
    stdio: 'inherit',
  });
}

const markerSource = join(
  sourceDirectory,
  'crates',
  'agentgateway-app',
  'src',
  'commands',
  'run.rs',
);
const sourcePatchDirectory = relative(repositoryRoot, sourceDirectory).replaceAll('\\', '/');

if (!readFileSync(markerSource, 'utf8').includes(manifest.versionMarker)) {
  execFileSync('git', ['apply', '--check', `--directory=${sourcePatchDirectory}`, patchPath], {
    cwd: repositoryRoot,
    stdio: 'inherit',
  });
  execFileSync('git', ['apply', `--directory=${sourcePatchDirectory}`, patchPath], {
    cwd: repositoryRoot,
    stdio: 'inherit',
  });
}
if (!readFileSync(markerSource, 'utf8').includes(manifest.versionMarker)) {
  throw new Error('agentgateway source patch did not install the expected version marker');
}

const cargoTarget = join(repositoryRoot, 'target', 'sidecar-build', 'agentgateway');
execFileSync(
  'rustup',
  [
    'run',
    manifest.rustToolchain,
    'cargo',
    'build',
    '--locked',
    '--release',
    '-p',
    'agentgateway-app',
  ],
  {
    cwd: sourceDirectory,
    env: { ...process.env, CARGO_TARGET_DIR: cargoTarget },
    stdio: 'inherit',
  },
);

const builtBinary = join(cargoTarget, 'release', `agentgateway${extension}`);
mkdirSync(binariesDirectory, { recursive: true });
copyFileSync(builtBinary, stagedBinary);
const versionMarker = execFileSync(stagedBinary, ['-V'], { encoding: 'utf8' }).trim();
if (versionMarker !== manifest.versionMarker) {
  throw new Error(
    `Patched agentgateway marker mismatch: expected ${manifest.versionMarker}, got ${versionMarker}`,
  );
}
const binarySha256 = await sha256File(stagedBinary);
writeFileSync(
  stagedManifest,
  `${JSON.stringify(
    {
      component: 'agentgateway',
      upstreamVersion: manifest.upstreamVersion,
      upstreamCommit: manifest.upstreamCommit,
      sourceArchiveSha256: manifest.sourceArchiveSha256,
      patchSha256,
      securityProfile: manifest.securityProfile,
      versionMarker,
      targetTriple,
      binarySha256,
    },
    null,
    2,
  )}\n`,
);
console.log(`Staged verified loopback-only agentgateway at ${stagedBinary}`);

function ensureToolchain(toolchain) {
  const installed = execFileSync('rustup', ['toolchain', 'list'], { encoding: 'utf8' });
  if (
    !installed
      .split(/\r?\n/u)
      .some((line) => line.startsWith(`${toolchain}-`) || line.startsWith(`${toolchain}.`))
  ) {
    execFileSync('rustup', ['toolchain', 'install', toolchain, '--profile', 'minimal'], {
      stdio: 'inherit',
    });
  }
}

async function stagedArtifactIsCurrent() {
  if (!existsSync(stagedBinary) || !existsSync(stagedManifest)) {
    return false;
  }
  try {
    const generated = JSON.parse(readFileSync(stagedManifest, 'utf8'));
    if (
      generated.upstreamCommit !== manifest.upstreamCommit ||
      generated.patchSha256 !== patchSha256 ||
      generated.securityProfile !== manifest.securityProfile ||
      generated.targetTriple !== targetTriple ||
      generated.versionMarker !== manifest.versionMarker
    ) {
      return false;
    }
    if ((await sha256File(stagedBinary)) !== generated.binarySha256) {
      return false;
    }
    return (
      execFileSync(stagedBinary, ['-V'], { encoding: 'utf8' }).trim() === manifest.versionMarker
    );
  } catch {
    return false;
  }
}
