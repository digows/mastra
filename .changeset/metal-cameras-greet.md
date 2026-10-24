---
'@mastra/elasticsearch': minor
---

Added observability storage to `ElasticSearchStore`. Traces, spans, logs, metrics, scores, and feedback written by `MastraStorageExporter` are now stored in Elasticsearch and served through the observability APIs that Studio reads, including metrics, filter discovery, and delta polling for live views.

```ts
import { Mastra } from '@mastra/core';
import { ElasticSearchStore } from '@mastra/elasticsearch';
import { Observability, MastraStorageExporter } from '@mastra/observability';

export const mastra = new Mastra({
  storage: new ElasticSearchStore({ id: 'storage', url: process.env.ELASTICSEARCH_URL! }),
  observability: new Observability({
    configs: { default: { serviceName: 'mastra', exporters: [new MastraStorageExporter()] } },
  }),
});
```

Added the `indexPrefix` option so several applications or environments can share one cluster. Every index the store creates starts with the prefix. Without it, index names stay the same as before.

```ts
const storage = new ElasticSearchStore({ id: 'storage', url, indexPrefix: 'staging_' });
// staging_mastra_threads, staging_mastra_observability_spans, ...
```
