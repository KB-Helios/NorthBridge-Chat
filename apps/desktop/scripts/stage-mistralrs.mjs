import { execFileSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { downloadVerified, hostTargetTriple, removeWithin, sha256File } from './sidecar-utils.mjs';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(scriptDirectory, '../../..');
const vendorDirectory = join(repositoryRoot, 'third_party', 'mistralrs');
const manifest = JSON.parse(readFileSync(join(vendorDirectory, 'manifest.json'), 'utf8'));
const targetTriple = hostTargetTriple(repositoryRoot);
const platform = manifest.platforms[targetTriple];

if (platform == null) {
  throw new Error(
    `No qualified mistral.rs ${manifest.upstreamVersion} artifact is pinned for ${targetTriple}`,
  );
}

const extension = process.platform === 'win32' ? '.exe' : '';
const downloadsDirectory = join(repositoryRoot, 'target', 'sidecar-downloads');
const extractionRoot = join(
  repositoryRoot,
  'target',
  'sidecar-sources',
  `mistralrs-${manifest.upstreamVersion}-${targetTriple}`,
);
const archive = join(
  downloadsDirectory,
  `mistralrs-${manifest.upstreamVersion}-${targetTriple}.zip`,
);
const extractedBinary = join(extractionRoot, platform.archiveMember);
const binariesDirectory = join(repositoryRoot, 'apps', 'desktop', 'src-tauri', 'binaries');
const stagedBinary = join(binariesDirectory, `mistralrs-${targetTriple}${extension}`);
const stagedManifest = join(binariesDirectory, `mistralrs-${targetTriple}.manifest.json`);

if (await stagedArtifactIsCurrent()) {
  console.log(`Using verified cached ${stagedBinary}`);
  process.exit(0);
}

await downloadVerified(platform.archiveUrl, archive, platform.archiveSha256);
if (existsSync(extractionRoot)) {
  removeWithin(extractionRoot, join(repositoryRoot, 'target', 'sidecar-sources'));
}
mkdirSync(extractionRoot, { recursive: true });
execFileSync('tar', ['-xf', archive, '-C', extractionRoot], {
  cwd: repositoryRoot,
  stdio: 'inherit',
});
const binarySha256 = await sha256File(extractedBinary);
if (binarySha256 !== platform.binarySha256) {
  throw new Error(
    `Extracted mistral.rs failed SHA-256 verification: expected ${platform.binarySha256}, got ${binarySha256}`,
  );
}
const versionMarker = execFileSync(extractedBinary, ['--version'], { encoding: 'utf8' }).trim();
if (versionMarker !== platform.versionMarker) {
  throw new Error(
    `mistral.rs version mismatch: expected ${platform.versionMarker}, got ${versionMarker}`,
  );
}

mkdirSync(binariesDirectory, { recursive: true });
copyFileSync(extractedBinary, stagedBinary);
writeFileSync(
  stagedManifest,
  `${JSON.stringify(
    {
      component: 'mistral.rs',
      upstreamVersion: manifest.upstreamVersion,
      securityProfile: manifest.securityProfile,
      targetTriple,
      archiveSha256: platform.archiveSha256,
      binarySha256,
      versionMarker,
      acceleration: platform.acceleration,
    },
    null,
    2,
  )}\n`,
);
console.log(`Staged verified mistral.rs at ${stagedBinary}`);

async function stagedArtifactIsCurrent() {
  if (!existsSync(stagedBinary) || !existsSync(stagedManifest)) {
    return false;
  }
  try {
    const generated = JSON.parse(readFileSync(stagedManifest, 'utf8'));
    return (
      generated.upstreamVersion === manifest.upstreamVersion &&
      generated.securityProfile === manifest.securityProfile &&
      generated.targetTriple === targetTriple &&
      generated.binarySha256 === platform.binarySha256 &&
      (await sha256File(stagedBinary)) === platform.binarySha256 &&
      execFileSync(stagedBinary, ['--version'], { encoding: 'utf8' }).trim() ===
        platform.versionMarker
    );
  } catch {
    return false;
  }
}
