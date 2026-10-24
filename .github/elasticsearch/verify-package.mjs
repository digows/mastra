import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const artifactPath = process.argv[2];
const workingDirectory = process.argv[3];
assert.ok(artifactPath && path.isAbsolute(artifactPath), 'Pass an absolute package tarball path');
assert.ok(workingDirectory && path.isAbsolute(workingDirectory), 'Pass an absolute owned consumer directory');
assert.ok(process.env.ELASTICSEARCH_URL, 'Set ELASTICSEARCH_URL to the isolated test service');
assert.ok(process.env.TMPDIR, 'Set TMPDIR to an owned temporary directory');
const scriptDirectory = fileURLToPath(new URL('.', import.meta.url));

await mkdir(workingDirectory, { recursive: true });
await writeFile(
  path.join(workingDirectory, 'package.json'),
  `${JSON.stringify(
    {
      name: 'elasticsearch-fork-consumer',
      private: true,
      type: 'module',
      dependencies: {
        '@mastra/elasticsearch': `file:${artifactPath}`,
        '@mastra/core': '1.79.0',
        '@mastra/observability': '1.21.0',
        '@mastra/server': '1.79.0',
        '@mastra/hono': '1.7.21',
        '@mastra/client-js': '1.56.0',
        '@hono/node-server': '1.19.17',
        hono: '4.13.13',
        zod: '4.6.7',
      },
    },
    null,
    2,
  )}\n`,
);
await copyFile(path.join(scriptDirectory, 'observability.mjs'), path.join(workingDirectory, 'observability.mjs'));
execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund'], {
  cwd: workingDirectory,
  stdio: 'inherit',
});
execFileSync(process.execPath, ['observability.mjs'], { cwd: workingDirectory, stdio: 'inherit' });
const result = JSON.parse(
  await readFile(process.env.RESULT_FILE ?? path.join(workingDirectory, 'result.json'), 'utf8'),
);
assert.equal(result.ok, true);
assert.equal(result.cleanup.remaining, 0);
