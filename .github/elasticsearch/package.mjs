import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repositoryRoot = fileURLToPath(new URL('../../', import.meta.url));
const packageDirectory = path.join(repositoryRoot, 'stores/elasticsearch');
const manifestPath = path.join(packageDirectory, 'package.json');
const outputDirectory = process.argv[2];
assert.ok(outputDirectory && path.isAbsolute(outputDirectory), 'Pass an absolute artifact output directory');
assert.ok(process.env.TMPDIR, 'Set TMPDIR to an owned temporary directory');

const revision = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repositoryRoot, encoding: 'utf8' }).trim();
const status = execFileSync('git', ['status', '--porcelain'], { cwd: repositoryRoot, encoding: 'utf8' }).trim();
assert.equal(status, '', 'Commit the source before producing a versioned release artifact');
const originalManifest = await readFile(manifestPath, 'utf8');
const manifest = JSON.parse(originalManifest);
const upstreamVersion = manifest.version;
const version = upstreamVersion.includes('-')
  ? `${upstreamVersion}.digows.g${revision.slice(0, 12)}`
  : `${upstreamVersion}-digows.g${revision.slice(0, 12)}`;

await mkdir(outputDirectory, { recursive: true });
try {
  // Rebuild after stamping so the bundled provider version and package manifest agree.
  await writeFile(manifestPath, `${JSON.stringify({ ...manifest, version, gitHead: revision }, null, 2)}\n`);
  execFileSync('pnpm', ['run', 'build:lib'], { cwd: packageDirectory, stdio: 'inherit' });
  execFileSync('pnpm', ['pack', '--pack-destination', outputDirectory], {
    cwd: packageDirectory,
    stdio: 'inherit',
  });
} finally {
  await writeFile(manifestPath, originalManifest);
}

const filename = `mastra-elasticsearch-${version}.tgz`;
const bytes = await readFile(path.join(outputDirectory, filename));
const sha256 = createHash('sha256').update(bytes).digest('hex');
await writeFile(path.join(outputDirectory, 'SHA256SUMS'), `${sha256}  ${filename}\n`);
await writeFile(
  path.join(outputDirectory, 'build-info.json'),
  `${JSON.stringify({ package: manifest.name, version, upstreamVersion, revision, node: process.version, filename, sha256 }, null, 2)}\n`,
);
console.log(JSON.stringify({ filename, version, revision, sha256 }));
