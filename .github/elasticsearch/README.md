# Elasticsearch fork distribution

This directory belongs to the digows fork. Keep it and
`.github/workflows/elasticsearch-fork.yml` out of the upstream feature PR.
The feature commit contains the adapter, its conformance/regression tests,
documentation, and changeset. The fork release commit contains this automation.

The workflow runs on relevant pushes to `digows/mastra` main or manual dispatch.
It installs and builds the adapter's workspace dependency closure, checks the
adapter, and runs its package suite against an isolated Elasticsearch 9.4.3.
It does not call the upstream workflows or build/test the entire monorepo.
After the explicit frozen-lockfile install, commands disable pnpm 11's automatic
dependency reinstall. In particular, version stamping must not trigger an
unfiltered install of unrelated workspaces. Missing build dependencies still fail
the build; type, lint, formatting, and conformance checks remain required.

Packaging stamps the full source commit into `gitHead` and a unique prerelease
version into both the compiled provider and package manifest. Source must be
committed. The source manifest is restored even if packaging fails. No version
changes or registry credentials are required in the source package.

The installed tarball is then qualified outside the monorepo using published
`@mastra/core@1.79.0`, `@mastra/observability@1.21.0`, `@mastra/server@1.79.0`,
`@mastra/hono@1.7.21`, and `@mastra/client-js@1.56.0`. The test exercises ESM
and CommonJS store constructors, real workflow telemetry, the native storage
exporter, HTTP APIs used by Studio, scores/feedback, and prefix isolation.
It does not exercise browser rendering, Studio delta live views, load, or Jarvix.

Only after those checks pass does the workflow upload the tarball, SHA256SUMS,
and build-info.json to a commit-specific GitHub prerelease. It never publishes
under the upstream npm scope or deploys a runtime. Repeated publication of the
same commit fails rather than overwriting an existing release.

Install the release's exact `.tgz` URL as the dependency for
`@mastra/elasticsearch` and commit the consumer lockfile. The package keeps its
upstream import name. The root of this monorepo is not an installable substitute
for the built package. Verify SHA256SUMS when downloading an artifact manually.

For local reproduction, set TMPDIR and caches to an owned task directory,
start an isolated Elasticsearch service, and set ELASTICSEARCH_URL. Run the
same build/check/test commands as the workflow, then:

```sh
node .github/elasticsearch/package.mjs "$TMPDIR/artifacts"
node .github/elasticsearch/verify-package.mjs "$TMPDIR/artifacts/<package>.tgz" "$TMPDIR/consumer"
```
