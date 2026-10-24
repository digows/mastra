import { randomUUID } from 'node:crypto';

import { Client as ElasticSearchClient } from '@elastic/elasticsearch';
import {
  createTestSuite,
  createConfigValidationTests,
  createClientAcceptanceTests,
  createDomainDirectTests,
  createObservabilityVNextTests,
} from '@internal/storage-test-utils';
import { coreFeatures } from '@mastra/core/features';
import { SpanType } from '@mastra/core/observability';
import type { CreateLogRecord, CreateMetricRecord, ObservabilityStorage, SpanRecord } from '@mastra/core/storage';
import { EntityType, TraceStatus } from '@mastra/core/storage';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { MemoryElasticSearch } from './domains/memory';
import { ObservabilityElasticSearch } from './domains/observability';
import {
  CURSOR_LEASE_TTL_MS,
  OBSERVABILITY_DELTA_POLLING_FEATURE,
  hasExpiredLease,
  safeHorizon,
} from './domains/observability/polling';
import { ScoresElasticSearch } from './domains/scores';
import { WorkflowsElasticSearch } from './domains/workflows';
import { ElasticSearchStore } from './store';
import type { ElasticSearchConfig } from './types';

vi.setConfig({ testTimeout: 200_000, hookTimeout: 200_000 });

const url = process.env.ELASTICSEARCH_URL || 'http://localhost:9200';

/**
 * Every suite in this run writes under its own index prefix, so suites never share indices or mappings and
 * a previous run's indices never affect this one. All indices of the run are removed at the end.
 */
const runPrefix = `test_${randomUUID().slice(0, 8)}_`;
const prefixFor = (label: string) => `${runPrefix}${label}_`;

afterAll(async () => {
  const client = new ElasticSearchClient({ node: url });
  try {
    const indices = (await client.cat.indices({ index: `${runPrefix}*`, format: 'json', h: 'index' })) as Array<{
      index?: string;
    }>;
    const names = indices.map(entry => entry.index).filter((name): name is string => Boolean(name));
    if (names.length > 0) {
      await client.indices.delete({ index: names });
    }
  } finally {
    await client.close();
  }
});

createTestSuite(
  new ElasticSearchStore({ id: 'elasticsearch-test-store', url, indexPrefix: prefixFor('suite') }),
  // Elasticsearch spans carry organizationId and resourceId, so tenant-scoped trace deletion is supported.
  { workflowResultsNoopWhenRunMissing: false, scopedTraceDeletion: true },
);

const vnextStore = new ElasticSearchStore({
  id: 'elasticsearch-vnext-test-store',
  url,
  indexPrefix: prefixFor('vnext'),
});
createObservabilityVNextTests({
  capabilities: {
    label: 'ElasticSearch',
    preferredStrategy: 'batch-with-updates',
  },
  getStorage: async () => (await vnextStore.getStore('observability')) as ObservabilityStorage,
  cleanup: async storage => {
    await storage.dangerouslyClearAll();
  },
});

createConfigValidationTests({
  storeName: 'ElasticSearchStore',
  createStore: config => new ElasticSearchStore(config as ElasticSearchConfig),
  validConfigs: [
    {
      description: 'url config',
      config: { id: 'test-store', url },
    },
    {
      description: 'disableInit with url config',
      config: { id: 'test-store', url, disableInit: true },
    },
    {
      description: 'index prefix with dots and dashes',
      config: { id: 'test-store', url, indexPrefix: 'tenant-a.staging_' },
    },
  ],
  invalidConfigs: [
    {
      description: 'empty url',
      config: { id: 'test-store', url: '' },
      expectedError: /client.*or.*url/i,
    },
    {
      description: 'missing url and client',
      config: { id: 'test-store' },
      expectedError: /client.*or.*url/i,
    },
    {
      description: 'uppercase index prefix, which Elasticsearch rejects at index creation',
      config: { id: 'test-store', url, indexPrefix: 'Prod_' },
      expectedError: /indexPrefix/,
    },
    {
      description: 'index prefix starting with an underscore',
      config: { id: 'test-store', url, indexPrefix: '_prod' },
      expectedError: /indexPrefix/,
    },
    {
      description: 'index prefix with a wildcard',
      config: { id: 'test-store', url, indexPrefix: 'prod*' },
      expectedError: /indexPrefix/,
    },
  ],
});

// Pre-configured client acceptance + domain direct tests
let sharedClient: ElasticSearchClient;

beforeAll(() => {
  sharedClient = new ElasticSearchClient({ node: url });
});

afterAll(async () => {
  await sharedClient.close();
});

createClientAcceptanceTests({
  storeName: 'ElasticSearchStore',
  expectedStoreName: 'ElasticSearch',
  createStoreWithClient: () =>
    new ElasticSearchStore({
      id: 'elasticsearch-client-test',
      client: sharedClient,
    }),
});

createDomainDirectTests({
  storeName: 'ElasticSearch',
  createMemoryDomain: () => new MemoryElasticSearch({ client: sharedClient }),
  createWorkflowsDomain: () => new WorkflowsElasticSearch({ client: sharedClient }),
  createScoresDomain: () => new ScoresElasticSearch({ client: sharedClient }),
});

describe('ElasticSearchStore connection options', () => {
  it('should connect using a url', async () => {
    const storage = new ElasticSearchStore({ id: 'url-test', url });

    await storage.init();
    const memory = await storage.getStore('memory');
    expect(memory).toBeDefined();
    await storage.close();
  });

  it('should expose the underlying client via getClient()', async () => {
    const storage = new ElasticSearchStore({ id: 'getclient-test', url });

    await storage.init();
    const client = storage.getClient();
    expect(client).toBeDefined();
    expect(typeof client.index).toBe('function');
    expect(typeof client.search).toBe('function');
    await storage.close();
  });

  it('should not close a user-provided client', async () => {
    const client = new ElasticSearchClient({ node: url });
    const storage = new ElasticSearchStore({ id: 'external-client-test', client });

    await storage.init();
    await storage.close();

    // Client is still usable after storage.close()
    const info = await client.info();
    expect(info.cluster_name).toBeDefined();
    await client.close();
  });
});

describe('ElasticSearch domain with client config', () => {
  it('should allow domains to use client config directly', async () => {
    const client = new ElasticSearchClient({ node: url });
    const memoryDomain = new MemoryElasticSearch({ client });

    expect(memoryDomain).toBeDefined();
    await memoryDomain.init();

    const thread = {
      id: `thread-client-test-${Date.now()}`,
      resourceId: 'test-resource',
      title: 'Test Client Thread',
      metadata: {},
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const savedThread = await memoryDomain.saveThread({ thread });
    expect(savedThread.id).toBe(thread.id);

    await memoryDomain.deleteThread({ threadId: thread.id });
    await client.close();
  });
});

// ============================================================================
// Shared fixtures for the Elasticsearch-specific observability scenarios
// ============================================================================

const baseDate = new Date('2026-01-01T00:00:00.000Z');

const makeSpan = (traceId: string, spanId: string, overrides: Partial<SpanRecord> = {}): SpanRecord =>
  ({
    traceId,
    spanId,
    parentSpanId: null,
    name: spanId,
    spanType: SpanType.AGENT_RUN,
    isEvent: false,
    startedAt: baseDate,
    endedAt: new Date(baseDate.getTime() + 1000),
    ...overrides,
  }) as SpanRecord;

const rootSpan = (traceId: string, overrides: Partial<SpanRecord> = {}) =>
  makeSpan(traceId, `${traceId}-root`, overrides);

const childSpan = (traceId: string, name: string, overrides: Partial<SpanRecord> = {}) =>
  makeSpan(traceId, `${traceId}-${name}`, {
    parentSpanId: `${traceId}-root`,
    spanType: SpanType.TOOL_CALL,
    ...overrides,
  });

const makeLog = (logId: string, overrides: Partial<CreateLogRecord> = {}): CreateLogRecord =>
  ({ logId, timestamp: baseDate, level: 'info', message: logId, ...overrides }) as CreateLogRecord;

const makeMetric = (metricId: string, overrides: Partial<CreateMetricRecord> = {}): CreateMetricRecord =>
  ({ metricId, timestamp: baseDate, name: 'tokens', value: 1, labels: {}, ...overrides }) as CreateMetricRecord;

/** A store-backed observability domain under its own index prefix, cleared before each test. */
function useObservability(label: string) {
  const client = new ElasticSearchClient({ node: url });
  const observability = new ObservabilityElasticSearch({ client, indexPrefix: prefixFor(label) });
  beforeEach(async () => {
    await observability.dangerouslyClearAll();
  });
  afterAll(async () => {
    await client.close();
  });
  return { client, observability };
}

async function listTraceIds(observability: ObservabilityStorage, filters: Record<string, unknown>): Promise<string[]> {
  const result = await observability.listTraces({ filters: filters as never, pagination: { page: 0, perPage: 100 } });
  return result.spans.map(span => span.traceId).sort();
}

describe('Two applications sharing one Elasticsearch cluster through index prefixes', () => {
  const client = new ElasticSearchClient({ node: url });
  const staging = new ElasticSearchStore({ id: 'staging', client, indexPrefix: prefixFor('staging') });
  const production = new ElasticSearchStore({ id: 'production', client, indexPrefix: prefixFor('production') });

  afterAll(async () => {
    await client.close();
  });

  it('keeps traces, logs, and memory threads of one application out of the other', async () => {
    const stagingObservability = (await staging.getStore('observability'))!;
    const productionObservability = (await production.getStore('observability'))!;
    await stagingObservability.batchCreateSpans({ records: [rootSpan('staging-trace')] });
    await stagingObservability.batchCreateLogs({ logs: [makeLog('staging-log')] });
    const stagingMemory = (await staging.getStore('memory'))!;
    await stagingMemory.saveThread({
      thread: {
        id: 'staging-thread',
        resourceId: 'user-1',
        title: 'Staging only',
        metadata: {},
        createdAt: baseDate,
        updatedAt: baseDate,
      },
    });

    expect(await listTraceIds(stagingObservability, {})).toEqual(['staging-trace']);
    expect(await listTraceIds(productionObservability, {})).toEqual([]);
    expect((await productionObservability.listLogs({})).logs).toEqual([]);
    const productionMemory = (await production.getStore('memory'))!;
    expect(await productionMemory.getThreadById({ threadId: 'staging-thread' })).toBeNull();
    expect((await stagingMemory.getThreadById({ threadId: 'staging-thread' }))?.title).toBe('Staging only');

    const indices = await client.indices.get({ index: `${prefixFor('staging')}*` });
    expect(Object.keys(indices)).toEqual(
      expect.arrayContaining([
        `${prefixFor('staging')}mastra_threads`,
        `${prefixFor('staging')}mastra_observability_spans`,
      ]),
    );
  });
});

describe('Index prefix on domains composed directly', () => {
  // A composite store builds the observability or memory domain itself, without ElasticSearchStore.
  it('rejects a prefix Elasticsearch would refuse before anything is written', () => {
    const client = new ElasticSearchClient({ node: url });
    expect(() => new ObservabilityElasticSearch({ client, indexPrefix: 'Observability_' })).toThrow(/indexPrefix/);
    expect(() => new MemoryElasticSearch({ client, indexPrefix: '_memory' })).toThrow(/indexPrefix/);
    expect(() => new ObservabilityElasticSearch({ client, indexPrefix: 'team-a.' })).not.toThrow();
  });
});

describe('ElasticSearch observability trace filters', () => {
  const { observability } = useObservability('trace_filters');

  it('matches metadata by exact type and deep value, never by string coercion', async () => {
    await observability.batchCreateSpans({
      records: [
        rootSpan('number', { metadata: { n: 1 } }),
        rootSpan('string', { metadata: { n: '1' } }),
        rootSpan('boolean', { metadata: { n: true } }),
        rootSpan('nested', { metadata: { shape: { a: [1, 2] } } }),
        rootSpan('null', { metadata: { n: null } }),
        rootSpan('dotted', { metadata: { 'http.route': '/checkout' } }),
        rootSpan('none'),
      ],
    });

    expect(await listTraceIds(observability, { metadata: { n: 1 } })).toEqual(['number']);
    expect(await listTraceIds(observability, { metadata: { n: '1' } })).toEqual(['string']);
    expect(await listTraceIds(observability, { metadata: { n: true } })).toEqual(['boolean']);
    expect(await listTraceIds(observability, { metadata: { n: null } })).toEqual(['null']);
    expect(await listTraceIds(observability, { metadata: { shape: { a: [1, 2] } } })).toEqual(['nested']);
    expect(await listTraceIds(observability, { metadata: { shape: { a: ['1', 2] } } })).toEqual([]);
    expect(await listTraceIds(observability, { metadata: { 'http.route': '/checkout' } })).toEqual(['dotted']);
    expect(await listTraceIds(observability, { metadata: { missing: null } })).toEqual([]);
  });

  it('matches scope by partial key equality and requires the root span to carry scope', async () => {
    await observability.batchCreateSpans({
      records: [
        rootSpan('tenant-a', { scope: { tenant: 'a', region: 'eu' } }),
        rootSpan('tenant-b', { scope: { tenant: 'b' } }),
        rootSpan('unscoped'),
      ],
    });

    expect(await listTraceIds(observability, { scope: { tenant: 'a' } })).toEqual(['tenant-a']);
    expect(await listTraceIds(observability, { scope: { tenant: 'a', region: 'eu' } })).toEqual(['tenant-a']);
    expect(await listTraceIds(observability, { scope: { tenant: 'a', region: 'us' } })).toEqual([]);
    expect(await listTraceIds(observability, { scope: {} })).toEqual(['tenant-a', 'tenant-b']);
  });

  // hasChildError means "any span in the trace has an error" (core contract and in-memory adapter), so a
  // failure on the root span alone also counts.
  it('finds traces with any failing span, including traces that only failed at the root', async () => {
    await observability.batchCreateSpans({
      records: [
        rootSpan('ok'),
        childSpan('ok', 'tool'),
        rootSpan('running', { endedAt: undefined }),
        rootSpan('root-error', { error: { message: 'boom' } }),
        rootSpan('child-error'),
        childSpan('child-error', 'tool', { error: { message: 'tool failed' } }),
        rootSpan('both-error', { error: { message: 'boom' } }),
        childSpan('both-error', 'tool', { error: { message: 'tool failed' } }),
      ],
    });

    expect(await listTraceIds(observability, { status: TraceStatus.SUCCESS })).toEqual(['child-error', 'ok']);
    expect(await listTraceIds(observability, { status: TraceStatus.RUNNING })).toEqual(['running']);
    expect(await listTraceIds(observability, { status: TraceStatus.ERROR })).toEqual(['both-error', 'root-error']);
    expect(await listTraceIds(observability, { hasChildError: true })).toEqual([
      'both-error',
      'child-error',
      'root-error',
    ]);
    expect(await listTraceIds(observability, { hasChildError: false })).toEqual(['ok', 'running']);
  });

  // Regression: hasChildError used to collect error trace ids client-side and failed above 10,000 traces.
  it('filters by child errors when more than 10,000 traces have failing tool calls', async () => {
    const failingTraces = 10_050;
    for (let start = 0; start < failingTraces; start += 2_500) {
      const records = Array.from({ length: Math.min(2_500, failingTraces - start) }, (_, offset) => {
        const traceId = `failing-${start + offset}`;
        return [rootSpan(traceId), childSpan(traceId, 'tool', { error: { message: 'tool failed' } })];
      }).flat();
      await observability.batchCreateSpans({ records });
    }
    await observability.batchCreateSpans({ records: [rootSpan('healthy'), childSpan('healthy', 'tool')] });

    const failing = await observability.listTraces({
      filters: { hasChildError: true },
      pagination: { page: 0, perPage: 1 },
    });
    expect(failing.pagination?.total).toBe(failingTraces);
    expect(await listTraceIds(observability, { hasChildError: false })).toEqual(['healthy']);
  });

  it('explains that pages past the first 10,000 traces need narrower filters', async () => {
    await expect(observability.listTraces({ pagination: { page: 100, perPage: 100 } })).rejects.toThrow(
      /first 10000 results/,
    );
    await expect(observability.listTraces({ pagination: { page: 99, perPage: 100 } })).resolves.toMatchObject({
      spans: [],
    });
  });

  it('keeps an update made concurrently by another exporter when ending a span', async () => {
    await observability.batchCreateSpans({ records: [rootSpan('concurrent', { endedAt: undefined })] });

    await Promise.all([
      observability.updateSpan({
        traceId: 'concurrent',
        spanId: 'concurrent-root',
        updates: { endedAt: new Date(baseDate.getTime() + 5000) },
      }),
      observability.updateSpan({
        traceId: 'concurrent',
        spanId: 'concurrent-root',
        updates: { output: { answer: 42 } },
      }),
      observability.updateSpan({
        traceId: 'concurrent',
        spanId: 'concurrent-root',
        updates: { attributes: { model: 'test' } },
      }),
    ]);

    const { span } = (await observability.getSpan({ traceId: 'concurrent', spanId: 'concurrent-root' }))!;
    expect(span.endedAt).toEqual(new Date(baseDate.getTime() + 5000));
    expect(span.output).toEqual({ answer: 42 });
    expect(span.attributes).toEqual({ model: 'test' });
  });
});

describe('Deleting traces in Elasticsearch observability storage', () => {
  const { observability } = useObservability('trace_delete');

  it('removes only the requested tenant spans and keeps the trace for the other tenant', async () => {
    await observability.batchCreateSpans({
      records: [
        rootSpan('shared', { organizationId: 'org-b' }),
        childSpan('shared', 'org-a-tool', { organizationId: 'org-a' }),
        rootSpan('org-a-only', { organizationId: 'org-a' }),
      ],
    });

    await observability.batchDeleteTraces({ traceIds: ['shared', 'org-a-only'], organizationId: 'org-a' });

    expect(await listTraceIds(observability, {})).toEqual(['shared']);
    expect((await observability.getTrace({ traceId: 'shared' }))?.spans.map(span => span.spanId)).toEqual([
      'shared-root',
    ]);
    expect(await observability.getTrace({ traceId: 'org-a-only' })).toBeNull();
  });

  it('removes the logs, metrics, scores, and feedback of a deleted trace and keeps other traces', async () => {
    await observability.batchCreateSpans({ records: [rootSpan('deleted'), rootSpan('kept')] });
    await observability.batchCreateLogs({
      logs: [makeLog('log-deleted', { traceId: 'deleted' }), makeLog('log-kept', { traceId: 'kept' })],
    });
    await observability.batchCreateMetrics({
      metrics: [makeMetric('metric-deleted', { traceId: 'deleted' }), makeMetric('metric-kept', { traceId: 'kept' })],
    });
    await observability.batchCreateScores({
      scores: [
        { scoreId: 'score-deleted', timestamp: baseDate, traceId: 'deleted', scorerId: 'quality', score: 1 },
        { scoreId: 'score-kept', timestamp: baseDate, traceId: 'kept', scorerId: 'quality', score: 1 },
      ],
    });
    await observability.batchCreateFeedback({
      feedbacks: [
        { feedbackId: 'feedback-deleted', timestamp: baseDate, traceId: 'deleted', feedbackType: 'thumbs', value: 1 },
        { feedbackId: 'feedback-kept', timestamp: baseDate, traceId: 'kept', feedbackType: 'thumbs', value: 1 },
      ] as never,
    });

    await observability.batchDeleteTraces({ traceIds: ['deleted'] });

    expect(await listTraceIds(observability, {})).toEqual(['kept']);
    expect((await observability.listLogs({})).logs.map(log => log.logId)).toEqual(['log-kept']);
    expect((await observability.listMetrics({})).metrics.map(metric => metric.metricId)).toEqual(['metric-kept']);
    expect((await observability.listScores({})).scores.map(score => score.scoreId)).toEqual(['score-kept']);
    expect((await observability.listFeedback({})).feedback.map(feedback => feedback.feedbackId)).toEqual([
      'feedback-kept',
    ]);
  });
});

describe('ElasticSearch observability branches', () => {
  const { observability } = useObservability('branches');

  it('lists only branch anchor span types and filters on the span itself', async () => {
    await observability.batchCreateSpans({
      records: [
        makeSpan('t1', 'tool', { spanType: SpanType.TOOL_CALL, parentEntityId: 'agent-1' }),
        makeSpan('t1', 'model', { spanType: SpanType.MODEL_STEP, parentSpanId: 'tool' }),
        makeSpan('t2', 'agent', { spanType: SpanType.AGENT_RUN, entityId: 'agent-1', rootEntityId: 'root-1' }),
        makeSpan('t2', 'child-tool', {
          spanType: SpanType.TOOL_CALL,
          parentSpanId: 'agent',
          parentEntityId: 'agent-1',
          error: { message: 'failed' },
        }),
      ],
    });

    const listBranchIds = async (filters: Record<string, unknown>) =>
      (await observability.listBranches({ filters: filters as never })).branches.map(branch => branch.spanId).sort();

    expect(await listBranchIds({})).toEqual(['agent', 'child-tool', 'tool']);
    expect(await listBranchIds({ spanType: SpanType.MODEL_STEP })).toEqual([]);
    expect(await listBranchIds({ parentEntityId: 'agent-1' })).toEqual(['child-tool', 'tool']);
    expect(await listBranchIds({ rootEntityId: 'root-1' })).toEqual(['agent']);
    expect(await listBranchIds({ status: TraceStatus.ERROR })).toEqual(['child-tool']);
    expect(await listBranchIds({ status: TraceStatus.SUCCESS })).toEqual(['agent', 'tool']);
  });

  it('returns branches with their own computed status and paginates', async () => {
    await observability.batchCreateSpans({
      records: [
        makeSpan('t1', 'older', {
          spanType: SpanType.TOOL_CALL,
          startedAt: new Date(baseDate.getTime() - 3000),
          tags: ['keep'],
        }),
        makeSpan('t1', 'newer', {
          spanType: SpanType.TOOL_CALL,
          startedAt: new Date(baseDate.getTime() - 1000),
          tags: ['keep'],
        }),
        makeSpan('t1', 'untagged', { spanType: SpanType.TOOL_CALL }),
      ],
    });

    const page = await observability.listBranches({ filters: { tags: ['keep'] }, pagination: { page: 0, perPage: 1 } });
    expect(page.pagination).toMatchObject({ total: 2, page: 0, perPage: 1, hasMore: true });
    expect(page.branches.map(branch => branch.spanId)).toEqual(['newer']);
    expect(page.branches[0]!.status).toBe(TraceStatus.SUCCESS);
  });
});

describe('ElasticSearch observability feedback review', () => {
  const { observability } = useObservability('feedback');

  it('defaults review status and source, and reviews and deletes within tenant scope', async () => {
    const feedback = (feedbackId: string, overrides: Record<string, unknown> = {}) =>
      ({ feedbackId, timestamp: baseDate, feedbackType: 'quality', value: 1, ...overrides }) as never;
    await observability.batchCreateFeedback({
      feedbacks: [
        feedback('fb-a', { organizationId: 'org-a', userId: 'app-user' }),
        feedback('fb-b', { organizationId: 'org-b', reviewStatus: 'reviewed', feedbackSource: 'human' }),
      ],
    });

    const listed = await observability.listFeedback({ filters: { reviewStatus: 'needs-review' } });
    expect(listed.feedback.map(record => record.feedbackId)).toEqual(['fb-a']);
    expect(listed.feedback[0]!.feedbackUserId).toBe('app-user');
    expect(listed.feedback[0]!.feedbackSource).toBe('');

    const reviewed = await observability.updateFeedbackReviewStatus({ feedbackId: 'fb-a', reviewStatus: 'reviewed' });
    expect(reviewed.reviewStatus).toBe('reviewed');
    expect((await observability.listFeedback({ filters: { reviewStatus: 'needs-review' } })).feedback).toEqual([]);

    await expect(
      observability.updateFeedbackReviewStatus({ feedbackId: 'missing', reviewStatus: 'reviewed' }),
    ).rejects.toThrow(/Feedback record not found/);

    await observability.deleteFeedback({ feedbackIds: ['fb-a', 'fb-b'], organizationId: 'org-a' });
    const remaining = await observability.listFeedback({});
    expect(remaining.feedback.map(record => record.feedbackId)).toEqual(['fb-b']);
    expect(remaining.feedback[0]!.feedbackSource).toBe('human');
  });
});

describe('ElasticSearch observability metrics', () => {
  const { observability } = useObservability('metrics');

  it('groups and filters by label keys that contain dots and spaces', async () => {
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('m1', { labels: { 'http.status': '500', 'route name': '/checkout' }, value: 2 }),
        makeMetric('m2', { labels: { 'http.status': '500', 'route name': '/cart' }, value: 3 }),
        makeMetric('m3', { labels: { 'http.status': '200', 'route name': '/checkout' }, value: 4 }),
        makeMetric('m4', { labels: {}, value: 7 }),
      ],
    });

    const breakdown = await observability.getMetricBreakdown({
      name: ['tokens'],
      groupBy: ['http.status'],
      aggregation: 'sum',
    });
    expect(breakdown.groups.map(group => [group.dimensions['http.status'], group.value])).toEqual([
      [null, 7],
      ['500', 5],
      ['200', 4],
    ]);

    const filtered = await observability.getMetricAggregate({
      name: ['tokens'],
      aggregation: 'sum',
      filters: { labels: { 'route name': '/checkout' } },
    });
    expect(filtered.value).toBe(6);
  });

  it('ranks a high-cardinality breakdown inside Elasticsearch when a limit is given', async () => {
    await observability.batchCreateMetrics({
      metrics: Array.from({ length: 40 }, (_, index) =>
        makeMetric(`agent-${index}`, { entityName: `agent-${String(index).padStart(2, '0')}`, value: index }),
      ),
    });

    const top = await observability.getMetricBreakdown({
      name: ['tokens'],
      groupBy: ['entityName'],
      aggregation: 'sum',
      limit: 3,
    });
    expect(top.groups.map(group => group.dimensions.entityName)).toEqual(['agent-39', 'agent-38', 'agent-37']);

    const bottom = await observability.getMetricBreakdown({
      name: ['tokens'],
      groupBy: ['entityName', 'model'],
      aggregation: 'sum',
      limit: 2,
      orderDirection: 'ASC',
    });
    expect(bottom.groups.map(group => [group.dimensions.entityName, group.dimensions.model])).toEqual([
      ['agent-00', null],
      ['agent-01', null],
    ]);
  });

  it('reports a cost unit only when every costed row shares it', async () => {
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('usd-1', { entityName: 'single', estimatedCost: 0.1, costUnit: 'usd' }),
        makeMetric('usd-2', { entityName: 'single', estimatedCost: 0.2, costUnit: 'usd' }),
        makeMetric('mixed-1', { entityName: 'mixed', estimatedCost: 1, costUnit: 'usd' }),
        makeMetric('mixed-2', { entityName: 'mixed', estimatedCost: 1, costUnit: 'eur' }),
        makeMetric('uncosted', { entityName: 'uncosted' }),
      ],
    });

    const { groups } = await observability.getMetricBreakdown({
      name: ['tokens'],
      groupBy: ['entityName'],
      aggregation: 'count',
    });
    const byName = Object.fromEntries(groups.map(group => [group.dimensions.entityName, group]));
    expect(byName.single).toMatchObject({ costUnit: 'usd' });
    expect(byName.single!.estimatedCost).toBeCloseTo(0.3);
    expect(byName.mixed).toMatchObject({ costUnit: null, estimatedCost: 2 });
    expect(byName.uncosted).toMatchObject({ costUnit: null, estimatedCost: null });
  });

  it('counts distinct threads and compares with the previous period', async () => {
    const day = 86_400_000;
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('today-1', { threadId: 'thread-a', timestamp: new Date(baseDate.getTime() + day + 10) }),
        makeMetric('today-2', { threadId: 'thread-a', timestamp: new Date(baseDate.getTime() + day + 20) }),
        makeMetric('today-3', { threadId: 'thread-b', timestamp: new Date(baseDate.getTime() + day + 30) }),
        makeMetric('yesterday-1', { threadId: 'thread-c', timestamp: new Date(baseDate.getTime() + 10) }),
      ],
    });

    const result = await observability.getMetricAggregate({
      name: ['tokens'],
      aggregation: 'count_distinct',
      distinctColumn: 'threadId',
      filters: {
        timestamp: { start: new Date(baseDate.getTime() + day), end: new Date(baseDate.getTime() + 2 * day) },
      },
      comparePeriod: 'previous_day',
    });
    expect(result).toMatchObject({ value: 2, previousValue: 1, changePercent: 100 });
  });

  it('filters by execution source using the deprecated source field when executionSource is missing', async () => {
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('legacy', { source: 'cloud' } as never),
        makeMetric('current', { executionSource: 'cloud' } as never),
        makeMetric('other', { executionSource: 'local' } as never),
      ],
    });
    await observability.batchCreateLogs({
      logs: [
        makeLog('legacy-log', { source: 'cloud' } as never),
        makeLog('other-log', { executionSource: 'local' } as never),
      ],
    });

    const metrics = await observability.listMetrics({ filters: { executionSource: 'cloud' } });
    expect(metrics.metrics.map(metric => metric.metricId).sort()).toEqual(['current', 'legacy']);
    const logs = await observability.listLogs({ filters: { executionSource: 'cloud' } });
    expect(logs.logs.map(log => log.logId)).toEqual(['legacy-log']);
  });

  it('discovers metric names and label values by prefix, sorted and limited', async () => {
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('a', { name: 'mastra_agent_tokens', labels: { model: 'gpt-4o' } }),
        makeMetric('b', { name: 'mastra_agent_duration_ms', labels: { model: 'gpt-4o-mini' } }),
        makeMetric('c', { name: 'mastra_agent_tokens', labels: { model: 'claude', 'cache.hit': 'true' } }),
        makeMetric('d', { name: 'other_metric' }),
      ],
    });

    expect((await observability.getMetricNames({ prefix: 'mastra_' })).names).toEqual([
      'mastra_agent_duration_ms',
      'mastra_agent_tokens',
    ]);
    expect((await observability.getMetricNames({ limit: 1 })).names).toEqual(['mastra_agent_duration_ms']);
    expect((await observability.getMetricLabelKeys({ metricName: 'mastra_agent_tokens' })).keys).toEqual([
      'cache.hit',
      'model',
    ]);
    expect(
      (await observability.getMetricLabelValues({ metricName: 'mastra_agent_tokens', labelKey: 'model', prefix: 'g' }))
        .values,
    ).toEqual(['gpt-4o']);
  });
});

describe('ElasticSearch observability filter discovery', () => {
  const { observability } = useObservability('discovery');

  it('offers entities, services, environments, and tags seen on spans, logs, or metrics', async () => {
    await observability.batchCreateSpans({
      records: [
        rootSpan('t1', {
          entityType: EntityType.AGENT,
          entityName: 'support-agent',
          serviceName: 'api',
          environment: 'production',
          tags: ['vip'],
        }),
        rootSpan('t2', { entityName: '', serviceName: '', environment: '' }),
      ],
    });
    await observability.batchCreateLogs({
      logs: [makeLog('l1', { entityType: EntityType.WORKFLOW_RUN, entityName: 'refund-flow', serviceName: 'worker' })],
    });
    await observability.batchCreateMetrics({
      metrics: [
        makeMetric('m1', { entityType: 'not-an-entity-type' as never, environment: 'staging', tags: ['beta'] }),
      ],
    });

    expect((await observability.getEntityTypes({})).entityTypes).toEqual([EntityType.AGENT, EntityType.WORKFLOW_RUN]);
    expect((await observability.getEntityNames({})).names).toEqual(['refund-flow', 'support-agent']);
    expect((await observability.getEntityNames({ entityType: EntityType.AGENT })).names).toEqual(['support-agent']);
    expect((await observability.getServiceNames({})).serviceNames).toEqual(['api', 'worker']);
    expect((await observability.getEnvironments({})).environments).toEqual(['production', 'staging']);
    expect((await observability.getTags({})).tags).toEqual(['beta', 'vip']);
  });
});

describe('ElasticSearch observability delta polling', () => {
  const { client, observability } = useObservability('delta');
  const hadFeature = coreFeatures.has(OBSERVABILITY_DELTA_POLLING_FEATURE);

  beforeEach(() => {
    coreFeatures.add(OBSERVABILITY_DELTA_POLLING_FEATURE);
  });

  afterEach(() => {
    if (!hadFeature) {
      coreFeatures.delete(OBSERVABILITY_DELTA_POLLING_FEATURE);
    }
  });

  it('declares delta polling only while the core feature is enabled', () => {
    expect(observability.getFeatures()).toContain('delta-polling');
    coreFeatures.delete(OBSERVABILITY_DELTA_POLLING_FEATURE);
    expect(observability.getFeatures()).not.toContain('delta-polling');
  });

  it('does not show a log twice in a live tail when the exporter retries a batch Studio already showed', async () => {
    const start = await observability.listLogs({ mode: 'delta' });
    await observability.batchCreateLogs({ logs: [makeLog('retried-1'), makeLog('retried-2')] });
    const shown = await observability.listLogs({ mode: 'delta', after: start.deltaCursor });
    expect(shown.logs.map(log => log.logId)).toEqual(['retried-1', 'retried-2']);

    await observability.batchCreateLogs({ logs: [makeLog('retried-1'), makeLog('retried-2')] });
    await observability.batchCreateLogs({ logs: [makeLog('new-log')] });

    const next = await observability.listLogs({ mode: 'delta', after: shown.deltaCursor });
    expect(next.logs.map(log => log.logId)).toEqual(['new-log']);
  });

  it('shows a trace once in the live trace list even when its spans keep updating', async () => {
    const start = await observability.listTraces({ mode: 'delta' });
    await observability.batchCreateSpans({ records: [rootSpan('live', { endedAt: undefined })] });
    const first = await observability.listTraces({ mode: 'delta', after: start.deltaCursor });
    expect(first.spans.map(span => span.traceId)).toEqual(['live']);

    // The exporter writes the child, updates the root, and re-sends the finished root span.
    await observability.batchCreateSpans({ records: [childSpan('live', 'tool')] });
    await observability.updateSpan({ traceId: 'live', spanId: 'live-root', updates: { output: { done: true } } });
    await observability.batchCreateSpans({ records: [rootSpan('live', { endedAt: baseDate })] });
    await observability.batchCreateSpans({ records: [rootSpan('later')] });

    const second = await observability.listTraces({ mode: 'delta', after: first.deltaCursor });
    expect(second.spans.map(span => span.traceId)).toEqual(['later']);
  });

  it('pages through a delta window with hasMore', async () => {
    const start = await observability.listScores({ mode: 'delta' });
    await observability.batchCreateScores({
      scores: Array.from({ length: 5 }, (_, index) => ({
        scoreId: `score-${index}`,
        timestamp: baseDate,
        scorerId: 'quality',
        score: index,
      })),
    });

    const first = await observability.listScores({ mode: 'delta', after: start.deltaCursor, limit: 3 });
    expect(first.scores.map(score => score.scoreId)).toEqual(['score-0', 'score-1', 'score-2']);
    expect(first.delta).toEqual({ limit: 3, hasMore: true });
    const second = await observability.listScores({ mode: 'delta', after: first.deltaCursor, limit: 3 });
    expect(second.scores.map(score => score.scoreId)).toEqual(['score-3', 'score-4']);
    expect(second.delta).toEqual({ limit: 3, hasMore: false });
  });

  // Several application instances export logs at the same time while Studio tails them. Cursor ids are allocated
  // before the writes become visible, so without the lease horizon a poll could pass a cursor of the faster
  // writer and skip the slower one's logs.
  it('never misses or repeats logs exported concurrently by several application instances', async () => {
    const instances = [
      observability,
      ...Array.from({ length: 3 }, () => new ObservabilityElasticSearch({ client, indexPrefix: prefixFor('delta') })),
    ];
    const batches = 30;
    const perBatch = 10;
    let cursor = (await observability.listLogs({ mode: 'delta' })).deltaCursor;
    const seen: string[] = [];
    let writing = true;

    const poll = async () => {
      const window = await observability.listLogs({ mode: 'delta', after: cursor, limit: 100 });
      seen.push(...window.logs.map(log => log.logId!));
      cursor = window.deltaCursor;
      return window.delta?.hasMore ?? false;
    };

    const writers = instances.map(async (instance, instanceIndex) => {
      for (let batch = 0; batch < batches; batch++) {
        await instance.batchCreateLogs({
          logs: Array.from({ length: perBatch }, (_, offset) => makeLog(`i${instanceIndex}-b${batch}-${offset}`)),
        });
      }
    });
    const poller = (async () => {
      while (writing) {
        await poll();
      }
    })();
    await Promise.all(writers);
    writing = false;
    await poller;
    while (await poll()) {
      // Drain what is left after the writers finished.
    }

    expect(seen).toHaveLength(instances.length * batches * perBatch);
    expect(new Set(seen).size).toBe(seen.length);
  });
});

/**
 * A real Elasticsearch client whose first bulk request waits until the test lets it through, like an exporter
 * stalled by a garbage-collection pause or a slow network. After `failNextBulk()`, the bulk request that
 * follows the stalled one fails, like a connection dropped while the exporter republishes.
 */
function stallingClient(real: ElasticSearchClient) {
  let letThrough!: () => void;
  let markStalled!: () => void;
  const gate = new Promise<void>(resolve => (letThrough = resolve));
  const stalled = new Promise<void>(resolve => (markStalled = resolve));
  let bulkCalls = 0;
  let failAfterStall = false;
  const client = new Proxy(real, {
    get(target, property, receiver) {
      if (property === 'bulk') {
        return async (...args: unknown[]) => {
          bulkCalls += 1;
          if (bulkCalls === 1) {
            markStalled();
            await gate;
          } else if (failAfterStall) {
            throw new Error('connection reset while republishing');
          }
          return (target.bulk as (...bulkArgs: unknown[]) => Promise<unknown>).apply(target, args);
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  return { client, stalled, letThrough, failNextBulk: () => (failAfterStall = true) };
}

describe('Delta polling with a slow concurrent exporter', () => {
  const { client, observability } = useObservability('delta_slow');
  const hadFeature = coreFeatures.has(OBSERVABILITY_DELTA_POLLING_FEATURE);
  const realNow = Date.now.bind(Date);

  beforeEach(() => {
    coreFeatures.add(OBSERVABILITY_DELTA_POLLING_FEATURE);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (!hadFeature) {
      coreFeatures.delete(OBSERVABILITY_DELTA_POLLING_FEATURE);
    }
  });

  const slowExporter = () => {
    const stalling = stallingClient(client);
    return {
      ...stalling,
      observability: new ObservabilityElasticSearch({ client: stalling.client, indexPrefix: prefixFor('delta_slow') }),
    };
  };

  /** Moves every clock reading past the cursor lease timeout, as if the stall lasted longer than that. */
  const outlastLeaseTimeout = () => {
    vi.spyOn(Date, 'now').mockImplementation(() => realNow() + CURSOR_LEASE_TTL_MS + 1);
  };

  // Deterministic version of the concurrent-exporters race: one exporter holds a lower cursor block while
  // another exporter's logs become searchable. The tail must wait instead of moving its cursor past it.
  it('holds a live tail back while a slower exporter is still writing earlier logs', async () => {
    const slow = slowExporter();
    const start = await observability.listLogs({ mode: 'delta' });
    const slowWrite = slow.observability.batchCreateLogs({ logs: [makeLog('slow')] });
    await slow.stalled;

    await observability.batchCreateLogs({ logs: [makeLog('fast')] });
    expect((await observability.listLogs({ mode: 'delta', after: start.deltaCursor })).logs).toEqual([]);

    slow.letThrough();
    await slowWrite;
    const window = await observability.listLogs({ mode: 'delta', after: start.deltaCursor });
    expect(window.logs.map(log => log.logId)).toEqual(['slow', 'fast']);
  });

  // The tail stops waiting for a lease that outlived its timeout, so it moves past the stalled exporter's
  // cursor. When that exporter was only slow, not dead, its log must still reach the tail afterwards.
  it('shows logs from an exporter that stalled past the lease timeout once it finishes', async () => {
    const slow = slowExporter();
    const start = await observability.listLogs({ mode: 'delta' });
    const slowWrite = slow.observability.batchCreateLogs({ logs: [makeLog('slow')] });
    await slow.stalled;

    outlastLeaseTimeout();
    await observability.batchCreateLogs({ logs: [makeLog('fast')] });
    const first = await observability.listLogs({ mode: 'delta', after: start.deltaCursor });
    expect(first.logs.map(log => log.logId)).toEqual(['fast']);

    slow.letThrough();
    await slowWrite;
    const second = await observability.listLogs({ mode: 'delta', after: first.deltaCursor });
    expect(second.logs.map(log => log.logId)).toEqual(['slow']);
    expect((await observability.listLogs({ mode: 'delta', after: second.deltaCursor })).logs).toEqual([]);
  });

  // Here the other exporter finished before the timeout, so no writer touches the stalled lease afterwards: the
  // tail itself gives up on it after the timeout, and the stalled exporter must notice when it finishes.
  it('shows logs from a stalled exporter when the tail gave up waiting on its own', async () => {
    const slow = slowExporter();
    const start = await observability.listLogs({ mode: 'delta' });
    const slowWrite = slow.observability.batchCreateLogs({ logs: [makeLog('slow')] });
    await slow.stalled;
    await observability.batchCreateLogs({ logs: [makeLog('fast')] });

    outlastLeaseTimeout();
    const first = await observability.listLogs({ mode: 'delta', after: start.deltaCursor });
    expect(first.logs.map(log => log.logId)).toEqual(['fast']);

    slow.letThrough();
    await slowWrite;
    const second = await observability.listLogs({ mode: 'delta', after: first.deltaCursor });
    expect(second.logs.map(log => log.logId)).toEqual(['slow']);
  });

  it('shows a stalled log once the exporter retries a batch whose publication failed', async () => {
    const slow = slowExporter();
    const start = await observability.listLogs({ mode: 'delta' });
    const slowWrite = slow.observability.batchCreateLogs({ logs: [makeLog('slow')] });
    await slow.stalled;

    outlastLeaseTimeout();
    await observability.batchCreateLogs({ logs: [makeLog('fast')] });
    const first = await observability.listLogs({ mode: 'delta', after: start.deltaCursor });
    expect(first.logs.map(log => log.logId)).toEqual(['fast']);

    slow.failNextBulk();
    slow.letThrough();
    await expect(slowWrite).rejects.toThrow();

    await observability.batchCreateLogs({ logs: [makeLog('slow')] });
    const second = await observability.listLogs({ mode: 'delta', after: first.deltaCursor });
    expect(second.logs.map(log => log.logId)).toEqual(['slow']);
  });
});

describe('Delta polling read horizon', () => {
  const now = 1_000_000;

  it('reads everything allocated when no write is in flight', () => {
    expect(safeHorizon({ next: 11, leases: {} })).toBe(10);
    expect(safeHorizon(null)).toBe(0);
  });

  it('stops below the lowest block a writer is still writing', () => {
    expect(
      safeHorizon({
        next: 31,
        leases: { slow: { start: 11, expiresAt: now + 1 }, fast: { start: 21, expiresAt: now + 1 } },
      }),
    ).toBe(10);
  });

  it('treats a lease as expired only after its timeout has passed', () => {
    const state = { next: 31, leases: { stalled: { start: 11, expiresAt: now } } };
    expect(hasExpiredLease(state, now)).toBe(false);
    expect(hasExpiredLease(state, now + 1)).toBe(true);
    expect(hasExpiredLease(null, now)).toBe(false);
  });
});
