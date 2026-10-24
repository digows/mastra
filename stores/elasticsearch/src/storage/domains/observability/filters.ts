/**
 * Elasticsearch query clauses for the observability list and OLAP filters. Each builder mirrors the
 * in-memory adapter's matcher for the same signal, so the same filter selects the same records.
 */
import type {
  FeedbackFilter,
  LogsFilter,
  MetricsFilter,
  ScoresFilter,
  listBranchesArgsSchema,
  listTracesArgsSchema,
} from '@mastra/core/storage';
import { BRANCH_SPAN_TYPES, TraceStatus } from '@mastra/core/storage';

import type { QueryClause } from './context';
import { DOC_KIND_FIELD, buildFilterToken } from './mapping';
import { buildLabelToken } from './signal-mapping';

/** Trace and branch filters after the core list schemas parsed them (dates coerced). */
export type TraceFilters = NonNullable<ReturnType<typeof listTracesArgsSchema.parse>['filters']>;
export type BranchFilters = NonNullable<ReturnType<typeof listBranchesArgsSchema.parse>['filters']>;

/** A date range filter as the core schemas parse it. */
export type DateRangeFilter = { start?: Date; end?: Date; startExclusive?: boolean; endExclusive?: boolean };

/** Span documents only, excluding the trace parent documents of the join relation. */
export const SPAN_DOCUMENTS: QueryClause = { term: { [DOC_KIND_FIELD]: 'span' } };

/** Spans without a parent span. */
export const NO_PARENT_SPAN: QueryClause = { bool: { must_not: [{ exists: { field: 'parentSpanId' } }] } };

/** Root spans: span documents without a parent span. */
export const ROOT_SPANS: QueryClause[] = [SPAN_DOCUMENTS, NO_PARENT_SPAN];

/** Spans of the types `listBranches` lists. */
export const BRANCH_SPANS: QueryClause[] = [SPAN_DOCUMENTS, { terms: { spanType: [...BRANCH_SPAN_TYPES] } }];

/** Exact-match fields every signal filter accepts (`commonFilterFields` without timestamp and tags). */
const COMMON_TERM_FIELDS = [
  'traceId',
  'spanId',
  'entityType',
  'entityName',
  'entityVersionId',
  'parentEntityType',
  'parentEntityName',
  'parentEntityVersionId',
  'rootEntityType',
  'rootEntityName',
  'rootEntityVersionId',
  'userId',
  'organizationId',
  'experimentId',
  'serviceName',
  'environment',
  'resourceId',
  'runId',
  'sessionId',
  'threadId',
  'requestId',
] as const;

/** Exact-match fields of trace filters, matched against the root span. */
const TRACE_TERM_FIELDS = [
  'traceId',
  'spanType',
  'entityType',
  'entityId',
  'entityName',
  'entityVersionId',
  'experimentId',
  'userId',
  'organizationId',
  'resourceId',
  'runId',
  'sessionId',
  'threadId',
  'requestId',
  'environment',
  'source',
  'serviceName',
] as const;

/** Exact-match fields of branch filters, matched against the branch anchor span itself. */
const BRANCH_TERM_FIELDS = [
  'traceId',
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
  'source',
  'serviceName',
] as const;

/** `value === null` matches records without the field; `undefined` adds no clause. */
export function pushExact(clauses: QueryClause[], field: string, value: unknown): void {
  if (value === undefined) {
    return;
  }
  if (value === null) {
    clauses.push({ bool: { must_not: [{ exists: { field } }] } });
    return;
  }
  clauses.push(Array.isArray(value) ? { terms: { [field]: value } } : { term: { [field]: value } });
}

function pushExactFields(clauses: QueryClause[], fields: readonly string[], filters: Record<string, unknown>): void {
  for (const field of fields) {
    pushExact(clauses, field, filters[field]);
  }
}

/**
 * A date range. `honorExclusive` follows the in-memory matcher of each signal: traces, logs, and metrics
 * honor `startExclusive`/`endExclusive`; branches, scores, and feedback compare inclusively.
 */
export function pushDateRange(
  clauses: QueryClause[],
  field: string,
  range: DateRangeFilter | undefined,
  honorExclusive: boolean,
): void {
  if (!range) {
    return;
  }
  const bounds: Record<string, string> = {};
  if (range.start) {
    bounds[honorExclusive && range.startExclusive ? 'gt' : 'gte'] = range.start.toISOString();
  }
  if (range.end) {
    bounds[honorExclusive && range.endExclusive ? 'lt' : 'lte'] = range.end.toISOString();
  }
  if (Object.keys(bounds).length > 0) {
    clauses.push({ range: { [field]: bounds } });
  }
}

/** Records carrying every requested tag. */
export function pushAllTags(clauses: QueryClause[], tags: readonly string[] | null | undefined): void {
  for (const tag of tags ?? []) {
    clauses.push({ term: { tags: tag } });
  }
}

/**
 * Partial-match filters over `metadata` or `scope`, matching `jsonValueEquals` in the in-memory adapter:
 * every requested key must hold a deeply equal, type-exact value. Spans also require the object itself to
 * be present (`presenceField`), so `{}` only matches spans that carry the object; scores treat `{}` as no
 * filter.
 */
export function pushJsonMatch(
  clauses: QueryClause[],
  tokenField: string,
  values: Record<string, unknown> | null | undefined,
  presenceField?: string,
): void {
  if (values == null) {
    return;
  }
  if (presenceField) {
    clauses.push({ term: { [presenceField]: true } });
  }
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) {
      clauses.push({ term: { [tokenField]: buildFilterToken(key, value) } });
    }
  }
}

/** Status derived from one span: an error wins, then a missing end time means running. */
export function statusClause(status: TraceStatus): QueryClause {
  switch (status) {
    case TraceStatus.ERROR:
      return { term: { hasError: true } };
    case TraceStatus.RUNNING:
      return { bool: { filter: [{ term: { hasError: false } }], must_not: [{ exists: { field: 'endedAt' } }] } };
    case TraceStatus.SUCCESS:
      return { bool: { filter: [{ term: { hasError: false } }, { exists: { field: 'endedAt' } }] } };
  }
}

/**
 * Root spans whose trace has (or has not) a span with an error, matching the core contract ("true if any span
 * in the trace encountered an error") and the in-memory adapter: the root span's own error counts. Runs as a
 * join query inside Elasticsearch: the root's trace parent document must have a span child with an error.
 */
export function childErrorClause(hasChildError: boolean): QueryClause {
  const tracesWithChildError: QueryClause = {
    has_parent: {
      parent_type: 'trace',
      query: {
        has_child: {
          type: 'span',
          query: { term: { hasError: true } },
        },
      },
    },
  };
  return hasChildError ? tracesWithChildError : { bool: { must_not: [tracesWithChildError] } };
}

/** `listTraces` filters, matched against the root span of each trace. */
export function traceFilterClauses(filters: Partial<TraceFilters>): QueryClause[] {
  const clauses: QueryClause[] = [...ROOT_SPANS];
  pushDateRange(clauses, 'startedAt', filters.startedAt, true);
  if (filters.endedAt) {
    // An endedAt filter never matches a running root span.
    clauses.push({ exists: { field: 'endedAt' } });
    pushDateRange(clauses, 'endedAt', filters.endedAt, true);
  }
  pushExactFields(clauses, TRACE_TERM_FIELDS, filters);
  pushAllTags(clauses, filters.tags);
  pushJsonMatch(clauses, 'metadataFilters', filters.metadata, 'hasMetadata');
  pushJsonMatch(clauses, 'scopeFilters', filters.scope, 'hasScope');
  if (filters.status !== undefined) {
    clauses.push(statusClause(filters.status));
  }
  if (filters.hasChildError !== undefined) {
    clauses.push(childErrorClause(filters.hasChildError));
  }
  return clauses;
}

/**
 * `listBranches` filters. Unlike trace filters, every predicate applies to the branch anchor span itself,
 * and status comes from that span alone.
 */
export function branchFilterClauses(filters: Partial<BranchFilters>): QueryClause[] {
  const clauses: QueryClause[] = [SPAN_DOCUMENTS];
  if (filters.spanType === undefined) {
    clauses.push({ terms: { spanType: [...BRANCH_SPAN_TYPES] } });
  } else if ((BRANCH_SPAN_TYPES as readonly string[]).includes(filters.spanType)) {
    clauses.push({ term: { spanType: filters.spanType } });
  } else {
    // Not a branch span type: nothing can match.
    clauses.push({ bool: { must_not: [{ match_all: {} }] } });
  }
  pushDateRange(clauses, 'startedAt', filters.startedAt, false);
  if (filters.endedAt) {
    clauses.push({ exists: { field: 'endedAt' } });
    pushDateRange(clauses, 'endedAt', filters.endedAt, false);
  }
  pushExactFields(clauses, BRANCH_TERM_FIELDS, filters);
  pushAllTags(clauses, filters.tags);
  pushJsonMatch(clauses, 'metadataFilters', filters.metadata, 'hasMetadata');
  pushJsonMatch(clauses, 'scopeFilters', filters.scope, 'hasScope');
  if (filters.status !== undefined) {
    clauses.push(statusClause(filters.status));
  }
  return clauses;
}

/** `executionSource` and the deprecated `source` both compare against `executionSource ?? source`. */
function pushExecutionSource(clauses: QueryClause[], filters: { executionSource?: string; source?: string }): void {
  pushExact(clauses, 'effectiveExecutionSource', filters.executionSource);
  pushExact(clauses, 'effectiveExecutionSource', filters.source);
}

export function logFilterClauses(filters: Partial<LogsFilter>): QueryClause[] {
  const clauses: QueryClause[] = [];
  pushDateRange(clauses, 'timestamp', filters.timestamp, true);
  pushExact(clauses, 'level', filters.level);
  pushExactFields(clauses, COMMON_TERM_FIELDS, filters);
  pushExecutionSource(clauses, filters);
  pushAllTags(clauses, filters.tags);
  return clauses;
}

export function metricFilterClauses(filters: Partial<MetricsFilter>): QueryClause[] {
  const clauses: QueryClause[] = [];
  pushDateRange(clauses, 'timestamp', filters.timestamp, true);
  pushExact(clauses, 'name', filters.name);
  pushExactFields(clauses, COMMON_TERM_FIELDS, filters);
  pushExact(clauses, 'traceId', filters.traceIds);
  pushExact(clauses, 'experimentId', filters.experimentIds);
  pushExact(clauses, 'provider', filters.provider);
  pushExact(clauses, 'model', filters.model);
  pushExact(clauses, 'costUnit', filters.costUnit);
  pushExecutionSource(clauses, filters);
  pushAllTags(clauses, filters.tags);
  for (const [key, value] of Object.entries(filters.labels ?? {})) {
    clauses.push({ term: { labelTokens: buildLabelToken(key, value) } });
  }
  return clauses;
}

/** Score filters. An empty `metadata` object is no filter, matching the in-memory `scoreMatchesFilters`. */
export function scoreFilterClauses(filters: Partial<ScoresFilter>): QueryClause[] {
  const clauses: QueryClause[] = [];
  pushDateRange(clauses, 'timestamp', filters.timestamp, false);
  pushExactFields(clauses, COMMON_TERM_FIELDS, filters);
  pushExact(clauses, 'executionSource', filters.executionSource);
  pushExact(clauses, 'scorerId', filters.scorerId);
  pushExact(clauses, 'scoreSource', filters.scoreSource);
  pushExact(clauses, 'scoreSource', filters.source);
  pushAllTags(clauses, filters.tags);
  pushJsonMatch(clauses, 'metadataFilters', filters.metadata);
  return clauses;
}

export function feedbackFilterClauses(filters: Partial<FeedbackFilter>): QueryClause[] {
  const clauses: QueryClause[] = [];
  pushDateRange(clauses, 'timestamp', filters.timestamp, false);
  pushExactFields(clauses, COMMON_TERM_FIELDS, filters);
  pushExact(clauses, 'executionSource', filters.executionSource);
  pushExact(clauses, 'feedbackType', filters.feedbackType);
  pushExact(clauses, 'feedbackSource', filters.feedbackSource ?? filters.source);
  pushExact(clauses, 'feedbackUserId', filters.feedbackUserId);
  pushExact(clauses, 'reviewStatus', filters.reviewStatus);
  pushAllTags(clauses, filters.tags);
  return clauses;
}

/** Optional tenant scope of deletes. */
export function tenantScopeClauses(scope: { organizationId?: string; resourceId?: string }): QueryClause[] {
  const clauses: QueryClause[] = [];
  pushExact(clauses, 'organizationId', scope.organizationId);
  pushExact(clauses, 'resourceId', scope.resourceId);
  return clauses;
}
