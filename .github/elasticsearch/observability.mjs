// Studio read-path acceptance for ElasticSearchStore observability.
// Real ElasticSearchStore + MastraStorageExporter, a real Mastra server (Hono adapter over @hono/node-server),
// and reads through @mastra/client-js, the client the Studio React hooks use. Browser rendering is not exercised.
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { writeFile } from 'node:fs/promises';

import { serve } from '@hono/node-server';
import { MastraClient } from '@mastra/client-js';
import { ConsoleLogger } from '@mastra/core/logger';
import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { ElasticSearchStore } from '@mastra/elasticsearch';
import { MastraServer } from '@mastra/hono';
import { MastraStorageExporter, Observability } from '@mastra/observability';
import { Hono } from 'hono';
import { z } from 'zod';

const require = createRequire(import.meta.url);
const { ElasticSearchStore: CommonJsElasticSearchStore } = require('@mastra/elasticsearch');

const url = process.env.ELASTICSEARCH_URL;
assert.ok(url, 'ELASTICSEARCH_URL is required');
const runId = randomUUID().slice(0, 8);
const prefixes = { a: `it_${runId}_a_`, b: `it_${runId}_b_` };
const results = { runId, prefixes, checks: [] };
const check = (name, details) => results.checks.push({ name, ...details });

function buildApp(prefix, serviceName, StorageConstructor = ElasticSearchStore) {
  const storage = new StorageConstructor({ id: `es-${prefix}`, url, indexPrefix: prefix });
  const observability = new Observability({
    // Observability exports logs from `warn` up by default; the step logs at `info`.
    configs: { default: { serviceName, logging: { level: 'info' }, exporters: [new MastraStorageExporter()] } },
  });
  const lookupOrder = createStep({
    id: 'lookup-order',
    inputSchema: z.object({ orderId: z.string() }),
    outputSchema: z.object({ status: z.string() }),
    execute: async ({ inputData, mastra }) => {
      mastra.getLogger().info('order looked up', { orderId: inputData.orderId });
      return { status: 'shipped' };
    },
  });
  const orderStatus = createWorkflow({
    id: 'order-status',
    inputSchema: z.object({ orderId: z.string() }),
    outputSchema: z.object({ status: z.string() }),
  })
    .then(lookupOrder)
    .commit();
  // One logger per Mastra instance: a shared logger exports to the observability of the last Mastra that
  // attached it (documented core behavior), which would route A's logs into B's storage.
  const logger = new ConsoleLogger({ name: serviceName, level: 'info' });
  const mastra = new Mastra({ storage, observability, logger, workflows: { orderStatus } });
  return { mastra, storage, observability };
}

async function startServer(mastra) {
  const app = new Hono();
  const server = new MastraServer({ app, mastra });
  await server.init();
  return await new Promise(resolve => {
    const httpServer = serve({ fetch: app.fetch, port: 0, hostname: '127.0.0.1' }, info =>
      resolve({ httpServer, port: info.port }),
    );
  });
}

async function waitFor(description, read, isReady, attempts = 40) {
  let last;
  for (let attempt = 0; attempt < attempts; attempt++) {
    last = await read();
    if (isReady(last)) return last;
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error(`Timed out waiting for ${description}: ${JSON.stringify(last)?.slice(0, 500)}`);
}

const apps = [];
const servers = [];
try {
  const appA = buildApp(prefixes.a, 'es-integration-a');
  const appB = buildApp(prefixes.b, 'es-integration-b', CommonJsElasticSearchStore);
  apps.push(appA, appB);
  const serverA = await startServer(appA.mastra);
  const serverB = await startServer(appB.mastra);
  servers.push(serverA, serverB);
  const clientA = new MastraClient({ baseUrl: `http://127.0.0.1:${serverA.port}` });
  const clientB = new MastraClient({ baseUrl: `http://127.0.0.1:${serverB.port}` });

  // 1. Produce telemetry through the real exporter: a workflow run with a logged step.
  const run = await appA.mastra.getWorkflow('orderStatus').createRun();
  const runResult = await run.start({ inputData: { orderId: 'A-1' } });
  assert.equal(runResult.status, 'success');
  await appA.observability.flush();
  check('workflow run exported', { status: runResult.status });

  // 2. Capabilities, as Studio reads them to decide which views to show.
  const capabilities = await clientA.observability.getCapabilities();
  assert.equal(capabilities.observabilityStorageType, 'ObservabilityElasticSearch');
  assert.equal(capabilities.capabilities.metrics, true);
  assert.equal(capabilities.capabilities.logs, true);
  assert.equal(capabilities.capabilities.feedback, true);
  check('capabilities', { capabilities });

  // 3. Trace list and trace detail (Studio traces page).
  const tracesPage = await waitFor(
    'the workflow trace in listTracesLight',
    () => clientA.observability.listTracesLight({}),
    page => page.spans?.some(span => span.entityName === 'order-status' && span.endedAt),
  );
  const root = tracesPage.spans.find(span => span.entityName === 'order-status');
  assert.equal(root.status, 'success');
  check('listTracesLight', {
    total: tracesPage.pagination?.total,
    root: { traceId: root.traceId, spanType: root.spanType, status: root.status, entityName: root.entityName },
  });

  const fullList = await clientA.observability.listTraces({ filters: { entityName: 'order-status' } });
  assert.ok(fullList.spans.some(span => span.traceId === root.traceId));
  check('listTraces filtered by entityName', { count: fullList.spans.length });

  const light = await clientA.observability.getTraceLight(root.traceId);
  const spanTypes = light.spans.map(span => span.spanType);
  assert.ok(spanTypes.includes('workflow_run'), `span types: ${spanTypes}`);
  assert.ok(light.spans.length >= 2, `expected the run and its step, got ${light.spans.length}`);
  check('getTraceLight', {
    spans: light.spans.map(span => ({ name: span.name, spanType: span.spanType, ended: Boolean(span.endedAt) })),
  });

  const trace = await clientA.observability.getTrace(root.traceId);
  assert.ok(
    trace.spans.every(span => span.endedAt),
    'every span ended (span updates were persisted)',
  );
  const stepSpan = trace.spans.find(span => span.parentSpanId);
  check('getTrace', { spans: trace.spans.length, allEnded: true });

  const span = await clientA.observability.getSpan(root.traceId, stepSpan.spanId);
  assert.equal(span.span.spanId, stepSpan.spanId);
  check('getSpan', { spanId: span.span.spanId, spanType: span.span.spanType });

  // 4. Logs correlated with the trace (Studio logs view).
  const logs = await waitFor(
    'the step log',
    () => clientA.observability.listLogs({ filters: { traceId: root.traceId } }),
    page => page.logs?.some(log => log.message === 'order looked up'),
  );
  const stepLog = logs.logs.find(log => log.message === 'order looked up');
  assert.equal(stepLog.level, 'info');
  assert.equal(stepLog.spanId, stepSpan.spanId);
  assert.equal(stepLog.traceId, root.traceId);
  check('listLogs by traceId', { count: logs.logs.length, level: stepLog.level, spanId: stepLog.spanId });

  // 5. Metrics automatically derived from the run (Studio metrics dashboard).
  const metricNames = await waitFor(
    'workflow duration metric',
    () => clientA.observability.getMetricNames({}),
    response => response.names?.includes('mastra_workflow_duration_ms'),
  );
  const metricAggregate = await clientA.observability.getMetricAggregate({
    name: ['mastra_workflow_duration_ms'],
    aggregation: 'count',
  });
  assert.ok(metricAggregate.value >= 1);
  const metricBreakdown = await clientA.observability.getMetricBreakdown({
    name: ['mastra_workflow_duration_ms'],
    groupBy: ['entityName'],
    aggregation: 'avg',
  });
  assert.ok(metricBreakdown.groups.some(group => group.dimensions.entityName === 'order-status'));
  const metricSeries = await clientA.observability.getMetricTimeSeries({
    name: ['mastra_workflow_duration_ms'],
    interval: '1h',
    aggregation: 'count',
  });
  assert.ok(metricSeries.series[0]?.points.length >= 1);
  check('metrics', {
    names: metricNames.names,
    count: metricAggregate.value,
    breakdown: metricBreakdown.groups,
    seriesPoints: metricSeries.series[0].points.length,
  });

  // 6. Scores and feedback through the same server routes Studio writes with.
  const scoreId = `score-${runId}`;
  await clientA.observability.createScore({
    score: {
      scoreId,
      traceId: root.traceId,
      spanId: root.spanId,
      scorerId: 'helpfulness',
      scoreSource: 'manual',
      score: 0.9,
      entityType: root.entityType,
      entityName: root.entityName,
    },
  });
  const scores = await clientA.observability.listScores({ filters: { traceId: root.traceId } });
  assert.deepEqual(
    scores.scores.map(score => score.scoreId),
    [scoreId],
  );
  const scoreAggregate = await clientA.observability.getScoreAggregate({ scorerId: 'helpfulness', aggregation: 'avg' });
  assert.equal(scoreAggregate.value, 0.9);
  check('scores', { listed: scores.scores.length, avg: scoreAggregate.value });

  const feedbackId = `feedback-${runId}`;
  await clientA.observability.createFeedback({
    feedback: {
      feedbackId,
      traceId: root.traceId,
      spanId: root.spanId,
      feedbackType: 'thumbs',
      feedbackSource: 'studio',
      value: 1,
    },
  });
  const feedback = await clientA.observability.listFeedback({ filters: { traceId: root.traceId } });
  assert.equal(feedback.feedback.length, 1);
  assert.equal(feedback.feedback[0].reviewStatus, 'needs-review');
  const reviewed = await clientA.observability.updateFeedbackReviewStatus({ feedbackId, reviewStatus: 'reviewed' });
  assert.equal(reviewed.reviewStatus, 'reviewed');
  const feedbackAfter = await clientA.observability.listFeedback({ filters: { traceId: root.traceId } });
  assert.equal(feedbackAfter.feedback[0].reviewStatus, 'reviewed');
  const feedbackAggregate = await clientA.observability.getFeedbackAggregate({
    feedbackType: 'thumbs',
    aggregation: 'sum',
  });
  assert.equal(feedbackAggregate.value, 1);
  check('feedback', {
    listed: feedback.feedback.length,
    reviewStatus: feedbackAfter.feedback[0].reviewStatus,
    sum: feedbackAggregate.value,
  });

  // 7. Filter discovery (Studio filter pickers).
  const entityNames = await clientA.observability.getEntityNames({});
  const serviceNames = await clientA.observability.getServiceNames();
  assert.ok(entityNames.names.includes('order-status'));
  assert.ok(serviceNames.serviceNames.includes('es-integration-a'));
  check('discovery', { entityNames: entityNames.names, serviceNames: serviceNames.serviceNames });

  // 8. Isolation: application B shares the cluster under another prefix and sees none of A's data.
  const tracesB = await clientB.observability.listTracesLight({});
  const logsB = await clientB.observability.listLogs({});
  const metricNamesB = await clientB.observability.getMetricNames({});
  const scoresB = await clientB.observability.listScores({});
  const feedbackB = await clientB.observability.listFeedback({});
  assert.equal(tracesB.spans.length, 0);
  assert.equal(logsB.logs.length, 0);
  assert.deepEqual(metricNamesB.names, []);
  assert.equal(scoresB.scores.length, 0);
  assert.equal(feedbackB.feedback.length, 0);
  check('prefix isolation', { traces: 0, logs: 0, metricNames: 0, scores: 0, feedback: 0 });

  const indices = await appA.storage.getClient().cat.indices({ index: `${prefixes.a}*`, format: 'json', h: 'index' });
  check('indices under prefix a', { indices: indices.map(entry => entry.index).sort() });
  results.ok = true;
} catch (error) {
  results.ok = false;
  results.error = { message: error?.message, stack: error?.stack?.split('\n').slice(0, 8) };
  process.exitCode = 1;
} finally {
  for (const { httpServer } of servers) {
    await new Promise(resolve => httpServer.close(() => resolve()));
  }
  const client = apps[0]?.storage.getClient();
  if (client) {
    const owned = [];
    for (const prefix of Object.values(prefixes)) {
      const entries = await client.cat.indices({ index: `${prefix}*`, format: 'json', h: 'index' });
      owned.push(...entries.map(entry => entry.index));
    }
    if (owned.length > 0) await client.indices.delete({ index: owned });
    const left = [];
    for (const prefix of Object.values(prefixes)) {
      left.push(...(await client.cat.indices({ index: `${prefix}*`, format: 'json', h: 'index' })));
    }
    results.cleanup = { deletedIndices: owned.length, remaining: left.length };
  }
  for (const app of apps) {
    await app.observability.shutdown().catch(() => undefined);
    await app.storage.close();
  }
  await writeFile(process.env.RESULT_FILE ?? 'result.json', JSON.stringify(results, null, 2));
  console.log(JSON.stringify(results, null, 2));
}
