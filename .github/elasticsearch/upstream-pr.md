Title: feat(elasticsearch): add native observability storage

## Description

Applications using `ElasticSearchStore` can persist native Mastra traces, spans,
logs, metrics, scores, and feedback in Elasticsearch through
`MastraStorageExporter`. The adapter implements the read and aggregation APIs
used by Studio, with capability discovery and feature-gated delta polling.

The implementation adds typed filters, native Elasticsearch aggregations,
parent/child trace error filtering, update and deletion behavior, and a store-wide
`indexPrefix` for application/environment isolation. Existing index names remain
the default. Documentation and a minor changeset describe the public behavior.

```ts
const storage = new ElasticSearchStore({
  id: 'storage',
  url: process.env.ELASTICSEARCH_URL!,
  indexPrefix: 'staging_',
});

const mastra = new Mastra({
  storage,
  observability: new Observability({
    configs: {
      default: {
        serviceName: 'support',
        exporters: [new MastraStorageExporter()],
      },
    },
  }),
});
```

Percentiles and distinct counts use Elasticsearch's approximate algorithms.
Deep offset pagination rejects requests beyond 10,000 results; complete trace
reads use cursor pages. Delta cursors fence and republish late live writers.
Recovery after a process crash during late publication requires retrying the
batch; redelivery is possible and abandoned lease IDs are retained for 24 hours.
This is not a transaction or exactly-once delivery guarantee. Query capabilities
that are not implemented remain unadvertised.

## Related issue(s)

Follow-up to the observability domain requested in #21757. The initial storage
adapter landed separately; this adds that domain to the existing package.

## Type of change

- [x] New feature (non-breaking change that adds functionality)
- [x] Documentation update
- [x] Test update

## Validation

On Node 26.8.1 against an isolated Elasticsearch 9.4.3 ARM64 service:

- `pnpm exec vitest run --reporter verbose` from `stores/elasticsearch`:
  629 passed, 0 failed, 413 skipped; includes both shared storage suites and
  regressions for concurrent/expired delta writers.
- `pnpm exec tsc --noEmit -p tsconfig.json`: passed.
- `pnpm exec oxlint --type-aware src`: passed.
- `pnpm exec oxfmt --check stores/elasticsearch/src`: passed from the root.
- Scoped Turbo build: 15 successful tasks; no root-wide test/build command.
- Installed tarball outside the monorepo with released core/server 1.79.0,
  observability 1.21.0, Hono adapter 1.7.21, and client-js 1.56.0: real workflow
  telemetry through the native exporter, HTTP reads/writes used by Studio,
  ESM/CommonJS store construction, and prefix isolation passed.
- Browser rendering, Studio delta live views, load, and production use were
  not exercised. The existing skipped cases are included in the totals above.

## Checklist

- [x] I have linked the related issue(s) in the description above
- [x] I have made corresponding changes to the documentation
- [x] I have added tests that prove the feature works
- [ ] I have addressed all Coderabbit comments on this PR

Fork-only release files under `.github/elasticsearch/` and
`.github/workflows/elasticsearch-fork.yml` are excluded from the feature commit
intended for this PR.
