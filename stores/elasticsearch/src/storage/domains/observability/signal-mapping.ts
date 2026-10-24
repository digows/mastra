import type { estypes } from '@elastic/elasticsearch';
import type { FeedbackRecord, LogRecord, MetricRecord, ScoreRecord } from '@mastra/core/storage';

import { buildFilterTokens, contextKeywordProperties, restoreDocument } from './mapping';

/**
 * Index mappings for the append-only observability signals. Each signal has one document per record id
 * (`logId`, `metricId`, `scoreId`, `feedbackId`). Fields used by filters, sorting, group-by, and
 * aggregations are mapped; payloads such as `message`, `data`, `labels`, `comment`, and `metadata` stay in
 * `_source` only (`dynamic: false`).
 */
function signalMapping(properties: Record<string, estypes.MappingProperty>): estypes.MappingTypeMapping {
  return {
    dynamic: false,
    properties: {
      cursorId: { type: 'long' },
      timestamp: { type: 'date' },
      traceId: { type: 'keyword' },
      spanId: { type: 'keyword' },
      ...contextKeywordProperties(),
      tags: { type: 'keyword' },
      ...properties,
    },
  };
}

export const OBSERVABILITY_LOGS_MAPPING = signalMapping({
  logId: { type: 'keyword' },
  level: { type: 'keyword' },
  source: { type: 'keyword' },
  effectiveExecutionSource: { type: 'keyword' },
});

export const OBSERVABILITY_METRICS_MAPPING = signalMapping({
  metricId: { type: 'keyword' },
  name: { type: 'keyword' },
  value: { type: 'double' },
  provider: { type: 'keyword' },
  model: { type: 'keyword' },
  estimatedCost: { type: 'double' },
  costUnit: { type: 'keyword' },
  source: { type: 'keyword' },
  effectiveExecutionSource: { type: 'keyword' },
  // `key\u0000value` per label: exact label filters, and the source of per-key group-by runtime fields.
  labelTokens: { type: 'keyword' },
  labelKeys: { type: 'keyword' },
});

export const OBSERVABILITY_SCORES_MAPPING = signalMapping({
  scoreId: { type: 'keyword' },
  scorerId: { type: 'keyword' },
  scorerName: { type: 'keyword' },
  scorerVersion: { type: 'keyword' },
  scoreSource: { type: 'keyword' },
  score: { type: 'double' },
  scoreTraceId: { type: 'keyword' },
  metadataFilters: { type: 'keyword' },
});

export const OBSERVABILITY_FEEDBACK_MAPPING = signalMapping({
  feedbackId: { type: 'keyword' },
  feedbackType: { type: 'keyword' },
  feedbackSource: { type: 'keyword' },
  feedbackUserId: { type: 'keyword' },
  reviewStatus: { type: 'keyword' },
  // Numeric projection of `value` for OLAP. Non-numeric feedback leaves this unset and is excluded.
  numericValue: { type: 'double' },
});

const DATE_FIELDS = ['timestamp'] as const;

/** Separates a label key from its value inside a label token. Label keys never contain it in practice. */
export const LABEL_TOKEN_SEPARATOR = '\u0000';

/** Exact token for one metric label. */
export function buildLabelToken(key: string, value: string): string {
  return `${key}${LABEL_TOKEN_SEPARATOR}${value}`;
}

/** `executionSource`, falling back to the deprecated `source`, as the in-memory adapter compares it. */
function effectiveExecutionSource(record: Record<string, unknown>): string | null {
  return (record.executionSource ?? record.source ?? null) as string | null;
}

export function toLogDocument(log: Record<string, unknown>): Record<string, unknown> {
  return { ...log, effectiveExecutionSource: effectiveExecutionSource(log) };
}

export function fromLogDocument(document: Record<string, unknown>): LogRecord {
  return restoreDocument(document, ['cursorId', 'cursorLeaseId', 'effectiveExecutionSource'], DATE_FIELDS) as LogRecord;
}

export function toMetricDocument(metric: Record<string, unknown>): Record<string, unknown> {
  const labels = (metric.labels ?? {}) as Record<string, string>;
  return {
    ...metric,
    labels,
    effectiveExecutionSource: effectiveExecutionSource(metric),
    labelTokens: Object.entries(labels).map(([key, value]) => buildLabelToken(key, value)),
    labelKeys: Object.keys(labels),
  };
}

export function fromMetricDocument(document: Record<string, unknown>): MetricRecord {
  return restoreDocument(
    document,
    ['cursorId', 'cursorLeaseId', 'effectiveExecutionSource', 'labelTokens', 'labelKeys'],
    DATE_FIELDS,
  ) as MetricRecord;
}

/** Applies the in-memory default: `scoreSource` and `source` both take `scoreSource ?? source ?? null`. */
export function toScoreDocument(score: Record<string, unknown>): Record<string, unknown> {
  const scoreSource = (score.scoreSource ?? score.source ?? null) as string | null;
  return {
    ...score,
    scoreSource,
    source: scoreSource,
    metadataFilters: buildFilterTokens(score.metadata as Record<string, unknown> | null | undefined),
  };
}

export function fromScoreDocument(document: Record<string, unknown>): ScoreRecord {
  return restoreDocument(document, ['cursorId', 'cursorLeaseId', 'metadataFilters'], DATE_FIELDS) as ScoreRecord;
}

/**
 * The numeric reading of a feedback value, matching the in-memory adapter: finite numbers as-is, and
 * strings that parse to a finite number after trimming. Anything else has no numeric value.
 */
export function feedbackNumericValue(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      return null;
    }
    const numeric = Number(trimmed);
    return Number.isFinite(numeric) ? numeric : null;
  }
  return null;
}

/**
 * Applies the in-memory defaults: `feedbackSource` and `source` fall back to `''`, `feedbackUserId` falls
 * back to the deprecated `userId` or to `metadata.userId`, and `reviewStatus` defaults to `needs-review`.
 */
export function toFeedbackDocument(feedback: Record<string, unknown>): Record<string, unknown> {
  const feedbackSource = (feedback.feedbackSource ?? feedback.source ?? '') as string;
  const metadata = feedback.metadata as Record<string, unknown> | null | undefined;
  const metadataUserId = typeof metadata?.userId === 'string' ? metadata.userId : null;
  return {
    ...feedback,
    feedbackSource,
    source: feedbackSource,
    feedbackUserId: (feedback.feedbackUserId ?? feedback.userId ?? metadataUserId) as string | null,
    reviewStatus: feedback.reviewStatus ?? 'needs-review',
    numericValue: feedbackNumericValue(feedback.value),
  };
}

export function fromFeedbackDocument(document: Record<string, unknown>): FeedbackRecord {
  return restoreDocument(document, ['cursorId', 'cursorLeaseId', 'numericValue'], DATE_FIELDS) as FeedbackRecord;
}
