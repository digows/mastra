import type { estypes } from '@elastic/elasticsearch';
import type { SpanRecord } from '@mastra/core/storage';

/**
 * Keyword fields carried by every observability signal (spans, logs, metrics, scores, feedback).
 * They back exact-match filters, group-by, and discovery.
 */
export const CONTEXT_KEYWORD_FIELDS = [
  'entityType',
  'entityId',
  'entityName',
  'entityVersionId',
  'parentEntityType',
  'parentEntityId',
  'parentEntityName',
  'parentEntityVersionId',
  'rootEntityType',
  'rootEntityId',
  'rootEntityName',
  'rootEntityVersionId',
  'experimentId',
  'userId',
  'organizationId',
  'resourceId',
  'runId',
  'sessionId',
  'threadId',
  'requestId',
  'environment',
  'serviceName',
  'executionSource',
] as const;

/** Keyword mappings for {@link CONTEXT_KEYWORD_FIELDS}. */
export function contextKeywordProperties(): Record<string, estypes.MappingProperty> {
  return Object.fromEntries(CONTEXT_KEYWORD_FIELDS.map(field => [field, { type: 'keyword' }]));
}

/**
 * Join relation inside the spans index. Every trace has one `trace` parent document, and every span is a
 * `span` child routed to it. `has_parent` / `has_child` queries then answer trace-level questions, such as
 * "does any child span of this trace have an error", inside Elasticsearch without collecting trace ids.
 */
export const TRACE_JOIN_FIELD = 'traceJoin';

/** Distinguishes trace parent documents from span documents in the spans index. */
export const DOC_KIND_FIELD = 'docKind';

/**
 * Span fields used by filters, sorting, or derived status are mapped explicitly. Everything else is kept
 * in `_source` only (`dynamic: false`, `enabled: false` for payloads), so arbitrary attributes, input, and
 * output never expand the mapping.
 */
export const OBSERVABILITY_SPANS_MAPPING: estypes.MappingTypeMapping = {
  dynamic: false,
  properties: {
    [DOC_KIND_FIELD]: { type: 'keyword' },
    [TRACE_JOIN_FIELD]: { type: 'join', relations: { trace: 'span' } },
    cursorId: { type: 'long' },
    traceId: { type: 'keyword' },
    spanId: { type: 'keyword' },
    parentSpanId: { type: 'keyword' },
    name: { type: 'keyword' },
    spanType: { type: 'keyword' },
    isEvent: { type: 'boolean' },
    startedAt: { type: 'date' },
    endedAt: { type: 'date' },
    createdAt: { type: 'date', index: false },
    updatedAt: { type: 'date', index: false },
    hasError: { type: 'boolean' },
    source: { type: 'keyword' },
    ...contextKeywordProperties(),
    tags: { type: 'keyword' },
    hasMetadata: { type: 'boolean' },
    hasScope: { type: 'boolean' },
    // Exact `[key, canonical value]` tokens for metadata and scope partial-match filters.
    metadataFilters: { type: 'keyword' },
    scopeFilters: { type: 'keyword' },
    attributes: { type: 'object', enabled: false },
    input: { type: 'object', enabled: false },
    output: { type: 'object', enabled: false },
    error: { type: 'object', enabled: false },
    links: { type: 'object', enabled: false },
    requestContext: { type: 'object', enabled: false },
  },
};

/** Date fields returned to callers as `Date` objects. */
const SPAN_DATE_FIELDS = ['startedAt', 'endedAt', 'createdAt', 'updatedAt'] as const;

/** Derived document fields that are indexed for filters and removed on read. */
const SPAN_DERIVED_FIELDS = [
  DOC_KIND_FIELD,
  TRACE_JOIN_FIELD,
  'cursorId',
  'cursorLeaseId',
  'hasError',
  'hasMetadata',
  'hasScope',
  'metadataFilters',
  'scopeFilters',
] as const;

/** Stable Elasticsearch document id for a span, unique per trace and span identifier. */
export function buildSpanDocumentId(traceId: string, spanId: string): string {
  return JSON.stringify([traceId, spanId]);
}

/**
 * Document id of the trace parent document. Spans are routed by this value so a trace and all of its spans
 * live on one shard, which the join relation requires.
 */
export function buildTraceDocumentId(traceId: string): string {
  return JSON.stringify([traceId]);
}

/** The trace parent document. It holds no span data; spans reference it through {@link TRACE_JOIN_FIELD}. */
export function toTraceDocument(traceId: string): Record<string, unknown> {
  return { [DOC_KIND_FIELD]: 'trace', [TRACE_JOIN_FIELD]: 'trace', traceId };
}

/**
 * JSON with object keys sorted at every level, so structurally equal values produce identical text.
 * Types stay distinct: `1`, `"1"`, `true`, `null`, and arrays all encode differently.
 */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortObjectKeys(value));
}

function sortObjectKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortObjectKeys);
  }
  if (value !== null && typeof value === 'object' && !(value instanceof Date)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map(key => [key, sortObjectKeys((value as Record<string, unknown>)[key])]),
    );
  }
  return value;
}

/** Exact token for one filterable `key: value` pair. Keys are not used as paths, so dots and nesting are safe. */
export function buildFilterToken(key: string, value: unknown): string {
  return JSON.stringify([key, canonicalJson(value)]);
}

/** Tokens for every own key of a metadata or scope object. `undefined` values are absent, as in JSON storage. */
export function buildFilterTokens(values: Record<string, unknown> | null | undefined): string[] {
  if (values == null) {
    return [];
  }
  return Object.entries(values)
    .filter(([, value]) => value !== undefined)
    .map(([key, value]) => buildFilterToken(key, value));
}

/**
 * Converts a span record to the stored span document. Derived fields make root-span status, child-error,
 * and metadata/scope partial-match filters answerable inside Elasticsearch.
 */
export function toSpanDocument(span: SpanRecord): Record<string, unknown> {
  return {
    ...span,
    [DOC_KIND_FIELD]: 'span',
    [TRACE_JOIN_FIELD]: { name: 'span', parent: buildTraceDocumentId(span.traceId) },
    hasError: span.error != null,
    hasMetadata: span.metadata != null,
    hasScope: span.scope != null,
    metadataFilters: buildFilterTokens(span.metadata),
    scopeFilters: buildFilterTokens(span.scope),
  };
}

/** Restores `Date` fields and removes derived fields from a stored span document. */
export function fromSpanDocument(document: Record<string, unknown>): SpanRecord {
  return restoreDocument(document, SPAN_DERIVED_FIELDS, SPAN_DATE_FIELDS) as SpanRecord;
}

/** Copies a stored document, dropping `derivedFields` and turning ISO strings in `dateFields` back into dates. */
export function restoreDocument(
  document: Record<string, unknown>,
  derivedFields: readonly string[],
  dateFields: readonly string[],
): Record<string, unknown> {
  const record: Record<string, unknown> = { ...document };
  for (const field of derivedFields) {
    delete record[field];
  }
  for (const field of dateFields) {
    const value = record[field];
    if (typeof value === 'string') {
      record[field] = new Date(value);
    }
  }
  return record;
}
