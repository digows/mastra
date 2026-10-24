/**
 * Span storage. Each span is a child document of its trace's parent document in the spans index, routed by
 * the trace, so trace-level filters such as `hasChildError` run as join queries. Trace status and branch
 * status are derived from the stored spans at query time, as in the in-memory adapter.
 */
import type { estypes } from '@elastic/elasticsearch';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type {
  BatchDeleteTracesArgs,
  CreateSpanRecord,
  GetRootSpanResponse,
  GetSpanResponse,
  GetSpansResponse,
  GetStructureResponse,
  GetTraceResponse,
  ListBranchesArgs,
  ListBranchesResponse,
  ListTracesArgs,
  ListTracesResponse,
  SpanRecord,
  UpdateSpanArgs,
} from '@mastra/core/storage';
import { listBranchesArgsSchema, listTracesArgsSchema, toTraceSpan } from '@mastra/core/storage';

import type { ObservabilityContext, QueryClause, StoredDocument } from './context';
import {
  BRANCH_SPANS,
  NO_PARENT_SPAN,
  ROOT_SPANS,
  SPAN_DOCUMENTS,
  branchFilterClauses,
  tenantScopeClauses,
  traceFilterClauses,
} from './filters';
import {
  DOC_KIND_FIELD,
  buildSpanDocumentId,
  buildTraceDocumentId,
  fromSpanDocument,
  toSpanDocument,
  toTraceDocument,
} from './mapping';
import { listDelta, pageDeltaCursor, writeWithCursors } from './polling';

/** Page size when reading every span of a trace. */
const TRACE_PAGE_SIZE = 1000;

/** Order of spans within a trace, as the in-memory adapter returns them. */
const TRACE_ORDER: estypes.SortCombinations[] = [{ startedAt: 'asc' }, { spanId: 'asc' }];

export async function createSpans(context: ObservabilityContext, records: CreateSpanRecord[]): Promise<void> {
  if (records.length === 0) {
    return;
  }
  for (const record of records) {
    validateCreateSpan(record);
  }

  await context.ensure('spans');
  const index = context.indexes.spans;
  const now = new Date();
  const traceIds = [...new Set(records.map(record => record.traceId))];
  const traceDocuments = traceIds.flatMap(traceId => [
    { index: { _index: index, _id: buildTraceDocumentId(traceId) } },
    toTraceDocument(traceId),
  ]);
  const writes = records.map(record => ({
    index,
    id: buildSpanDocumentId(record.traceId, record.spanId),
    routing: buildTraceDocumentId(record.traceId),
    document: toSpanDocument({ ...record, createdAt: now, updatedAt: now } as SpanRecord),
  }));
  await writeWithCursors(context, 'BATCH_CREATE_SPANS', 'spans', writes, traceDocuments);
}

function validateCreateSpan(record: { traceId: string; spanId: string }): void {
  if (!record.spanId) {
    throw new MastraError({
      id: 'OBSERVABILITY_SPAN_ID_REQUIRED',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.SYSTEM,
      text: 'Span ID is required for creating a span',
    });
  }
  if (!record.traceId) {
    throw new MastraError({
      id: 'OBSERVABILITY_TRACE_ID_REQUIRED',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.SYSTEM,
      text: 'Trace ID is required for creating a span',
    });
  }
}

/**
 * Merges `updates` into a stored span, like the in-memory adapter. The write is conditional on the span not
 * having changed since it was read, and is retried otherwise, so concurrent updates keep each other's fields.
 */
export async function updateSpan(context: ObservabilityContext, args: UpdateSpanArgs): Promise<void> {
  const { traceId, spanId, updates } = args;
  await context.ensure('spans');
  const index = context.indexes.spans;
  const id = buildSpanDocumentId(traceId, spanId);
  const routing = buildTraceDocumentId(traceId);

  await context.updateWithRetry('UPDATE_SPAN', async () => {
    const existing = await context.getDocument('GET_SPAN', index, id, routing);
    if (!existing) {
      throw await missingSpanError(context, traceId);
    }
    const stored = existing._source as StoredDocument;
    const document = {
      ...toSpanDocument({ ...fromSpanDocument(stored), ...updates, updatedAt: new Date() } as SpanRecord),
      cursorId: stored.cursorId,
      cursorLeaseId: stored.cursorLeaseId,
    };
    await context.client.index({
      index,
      id,
      routing,
      document,
      if_seq_no: existing._seq_no,
      if_primary_term: existing._primary_term,
      refresh: true,
    });
  });
}

async function missingSpanError(context: ObservabilityContext, traceId: string): Promise<MastraError> {
  const { count } = await context.wrap('CHECK_TRACE', () =>
    context.client.count({
      index: context.indexes.spans,
      query: { bool: { filter: [SPAN_DOCUMENTS, { term: { traceId } }] } },
    }),
  );
  return new MastraError({
    id: 'OBSERVABILITY_UPDATE_SPAN_NOT_FOUND',
    domain: ErrorDomain.MASTRA_OBSERVABILITY,
    category: ErrorCategory.SYSTEM,
    text: count === 0 ? 'Trace not found for span update' : 'Span not found for update',
  });
}

/**
 * Deletes the traces' spans and the logs, metrics, scores, and feedback linked to them, within the optional
 * tenant scope. A trace parent document is removed once none of its spans remain.
 */
export async function deleteTraces(context: ObservabilityContext, args: BatchDeleteTracesArgs): Promise<void> {
  if (args.traceIds.length === 0) {
    return;
  }
  await context.ensure('spans', 'logs', 'metrics', 'scores', 'feedback');
  const traces: QueryClause = { terms: { traceId: args.traceIds } };
  const scope = tenantScopeClauses(args);

  await context.deleteWhere('BATCH_DELETE_TRACES', context.indexes.spans, [SPAN_DOCUMENTS, traces, ...scope]);
  await context.deleteWhere('BATCH_DELETE_TRACES', context.indexes.spans, [
    { term: { [DOC_KIND_FIELD]: 'trace' } },
    traces,
    { bool: { must_not: [{ has_child: { type: 'span', query: { match_all: {} } } }] } },
  ]);
  for (const kind of ['logs', 'metrics', 'scores', 'feedback'] as const) {
    await context.deleteWhere('BATCH_DELETE_TRACES', context.indexes[kind], [traces, ...scope]);
  }
}

export async function getSpan(
  context: ObservabilityContext,
  traceId: string,
  spanId: string,
): Promise<GetSpanResponse | null> {
  await context.ensure('spans');
  const document = await context.getDocument(
    'GET_SPAN',
    context.indexes.spans,
    buildSpanDocumentId(traceId, spanId),
    buildTraceDocumentId(traceId),
  );
  return document ? { span: fromSpanDocument(document._source as StoredDocument) } : null;
}

export async function getSpans(
  context: ObservabilityContext,
  traceId: string,
  spanIds: string[],
): Promise<GetSpansResponse> {
  const spans = await searchTraceSpans(context, traceId, [{ terms: { spanId: spanIds } }]);
  return { traceId, spans };
}

/** The trace's root span; with several, the earliest. */
export async function getRootSpan(context: ObservabilityContext, traceId: string): Promise<GetRootSpanResponse | null> {
  const [root] = await searchTraceSpans(context, traceId, [NO_PARENT_SPAN], 1);
  return root ? { span: root } : null;
}

export async function getTrace(context: ObservabilityContext, traceId: string): Promise<GetTraceResponse | null> {
  const spans = await searchTraceSpans(context, traceId, []);
  return spans.length > 0 ? { traceId, spans } : null;
}

/** Same projection as the in-memory adapter: no input, output, attributes, metadata, tags, or links. */
export async function getTraceLight(
  context: ObservabilityContext,
  traceId: string,
): Promise<GetStructureResponse | null> {
  const trace = await getTrace(context, traceId);
  if (!trace) {
    return null;
  }
  return {
    traceId,
    spans: trace.spans.map(span => ({
      traceId: span.traceId,
      spanId: span.spanId,
      parentSpanId: span.parentSpanId,
      name: span.name,
      spanType: span.spanType,
      isEvent: span.isEvent,
      startedAt: span.startedAt,
      endedAt: span.endedAt,
      error: span.error,
      entityType: span.entityType,
      entityId: span.entityId,
      entityName: span.entityName,
      createdAt: span.createdAt,
      updatedAt: span.updatedAt,
    })),
  };
}

/**
 * Spans of one trace in trace order, paged with `search_after` so a trace larger than one page is never
 * truncated. A `limit` returns the first page only.
 */
async function searchTraceSpans(
  context: ObservabilityContext,
  traceId: string,
  extraClauses: QueryClause[],
  limit?: number,
): Promise<SpanRecord[]> {
  await context.ensure('spans');
  const spans: SpanRecord[] = [];
  const size = limit ?? TRACE_PAGE_SIZE;
  let searchAfter: estypes.SortResults | undefined;

  for (;;) {
    const response = await context.wrap('SEARCH_TRACE_SPANS', () =>
      context.client.search<StoredDocument>({
        index: context.indexes.spans,
        routing: buildTraceDocumentId(traceId),
        query: { bool: { filter: [SPAN_DOCUMENTS, { term: { traceId } }, ...extraClauses] } },
        sort: TRACE_ORDER,
        size,
        ...(searchAfter ? { search_after: searchAfter } : {}),
      }),
    );
    const hits = response.hits.hits;
    spans.push(...hits.map(hit => fromSpanDocument(hit._source as StoredDocument)));
    if (limit !== undefined || hits.length < size) {
      return spans;
    }
    searchAfter = hits.at(-1)!.sort;
  }
}

/** Sort for trace and branch pages: null `endedAt` comes first for DESC and last for ASC, as in-memory. */
function listSort(orderBy: { field: 'startedAt' | 'endedAt'; direction: 'ASC' | 'DESC' }): estypes.Sort {
  const order = orderBy.direction === 'ASC' ? 'asc' : 'desc';
  const missing = orderBy.field === 'endedAt' && order === 'desc' ? '_first' : '_last';
  return [{ [orderBy.field]: { order, missing } }, { traceId: 'asc' }, { spanId: 'asc' }];
}

export async function listTraces(context: ObservabilityContext, args: ListTracesArgs): Promise<ListTracesResponse> {
  const { mode, filters, pagination, orderBy, after, limit } = listTracesArgsSchema.parse(args);
  await context.ensure('spans');
  const query = {
    stream: 'spans' as const,
    index: context.indexes.spans,
    clauses: traceFilterClauses(filters ?? {}),
    streamClauses: ROOT_SPANS,
  };

  if (mode === 'delta') {
    const window = await listDelta(context, query, after, limit);
    return {
      spans: window.documents.map(document => toTraceSpan(fromSpanDocument(document))),
      delta: window.delta,
      deltaCursor: window.deltaCursor,
    };
  }

  const { documents, page } = await context.searchPage(
    'LIST_TRACES',
    query.index,
    query.clauses,
    listSort(orderBy),
    pagination,
  );
  return {
    spans: documents.map(document => toTraceSpan(fromSpanDocument(document))),
    pagination: page,
    ...(await pageDeltaCursor(context, { ...query, fallBackToStreamHead: true })),
  };
}

export async function listBranches(
  context: ObservabilityContext,
  args: ListBranchesArgs,
): Promise<ListBranchesResponse> {
  const { mode, filters, pagination, orderBy, after, limit } = listBranchesArgsSchema.parse(args);
  await context.ensure('spans');
  const query = {
    stream: 'spans' as const,
    index: context.indexes.spans,
    clauses: branchFilterClauses(filters ?? {}),
    streamClauses: BRANCH_SPANS,
  };

  if (mode === 'delta') {
    const window = await listDelta(context, query, after, limit);
    return {
      branches: window.documents.map(document => toTraceSpan(fromSpanDocument(document))),
      delta: window.delta,
      deltaCursor: window.deltaCursor,
    };
  }

  const { documents, page } = await context.searchPage(
    'LIST_BRANCHES',
    query.index,
    query.clauses,
    listSort(orderBy),
    pagination,
  );
  return {
    branches: documents.map(document => toTraceSpan(fromSpanDocument(document))),
    pagination: page,
    ...(await pageDeltaCursor(context, { ...query, fallBackToStreamHead: true })),
  };
}
