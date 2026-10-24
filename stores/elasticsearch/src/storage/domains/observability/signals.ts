/**
 * Writes, lists, and deletes for the append-only signals: logs, metrics, scores, and feedback. Each record
 * is one document keyed by its id, so re-writing an id replaces the record (keeping its delta cursor) instead
 * of duplicating it.
 */
import type { estypes } from '@elastic/elasticsearch';

import type { ObservabilityContext, PageArgs, PageInfo, QueryClause, StoredDocument } from './context';
import { assertRecordId } from './context';
import { tenantScopeClauses } from './filters';
import type { CursorStream } from './polling';
import { listDelta, pageDeltaCursor, writeWithCursors } from './polling';
import {
  fromFeedbackDocument,
  fromLogDocument,
  fromMetricDocument,
  fromScoreDocument,
  toFeedbackDocument,
  toLogDocument,
  toMetricDocument,
  toScoreDocument,
} from './signal-mapping';

export type SignalKind = Extract<CursorStream, 'logs' | 'metrics' | 'scores' | 'feedback'>;

type SignalDefinition<TRecord> = {
  idField: string;
  toDocument: (record: Record<string, unknown>) => Record<string, unknown>;
  fromDocument: (document: StoredDocument) => TRecord;
};

const SIGNALS = {
  logs: { idField: 'logId', toDocument: toLogDocument, fromDocument: fromLogDocument },
  metrics: { idField: 'metricId', toDocument: toMetricDocument, fromDocument: fromMetricDocument },
  scores: { idField: 'scoreId', toDocument: toScoreDocument, fromDocument: fromScoreDocument },
  feedback: { idField: 'feedbackId', toDocument: toFeedbackDocument, fromDocument: fromFeedbackDocument },
} satisfies Record<SignalKind, SignalDefinition<unknown>>;

type SignalRecord<TKind extends SignalKind> = ReturnType<(typeof SIGNALS)[TKind]['fromDocument']>;

/** Writes records, assigning a delta cursor to new ids and keeping it for re-written ids. */
export async function writeSignals(
  context: ObservabilityContext,
  kind: SignalKind,
  records: ReadonlyArray<object>,
): Promise<void> {
  if (records.length === 0) {
    return;
  }
  const definition = SIGNALS[kind];
  const writes = records.map(record => {
    const fields = record as Record<string, unknown>;
    const id = fields[definition.idField] as string | null | undefined;
    assertRecordId(id, definition.idField);
    return { index: context.indexes[kind], id, document: definition.toDocument(fields) };
  });
  await context.ensure(kind);
  await writeWithCursors(context, `BATCH_CREATE_${kind.toUpperCase()}`, kind, writes);
}

/** One page or one delta window of a signal, in the list response shape of the core contract. */
export type SignalList<TRecord> =
  | { records: TRecord[]; pagination: PageInfo; deltaCursor?: string }
  | { records: TRecord[]; delta: { limit: number; hasMore: boolean }; deltaCursor: string };

export async function listSignals<TKind extends SignalKind>(
  context: ObservabilityContext,
  kind: TKind,
  args: {
    mode: 'page' | 'delta';
    clauses: QueryClause[];
    sort: estypes.Sort;
    pagination: PageArgs;
    after?: string;
    limit: number;
  },
): Promise<SignalList<SignalRecord<TKind>>> {
  await context.ensure(kind);
  const definition = SIGNALS[kind];
  const index = context.indexes[kind];
  const fromDocument = (document: StoredDocument) => definition.fromDocument(document) as SignalRecord<TKind>;

  if (args.mode === 'delta') {
    const window = await listDelta(
      context,
      { stream: kind, index, clauses: args.clauses, streamClauses: [] },
      args.after,
      args.limit,
    );
    return { records: window.documents.map(fromDocument), delta: window.delta, deltaCursor: window.deltaCursor };
  }

  const { documents, page } = await context.searchPage(
    `LIST_${kind.toUpperCase()}`,
    index,
    args.clauses,
    args.sort,
    args.pagination,
  );
  return {
    records: documents.map(fromDocument),
    pagination: page,
    ...(await pageDeltaCursor(context, {
      stream: kind,
      index,
      clauses: args.clauses,
      streamClauses: [],
      fallBackToStreamHead: false,
    })),
  };
}

/** Deletes records by id, within the optional tenant scope. */
export async function deleteSignals(
  context: ObservabilityContext,
  kind: SignalKind,
  ids: string[],
  scope: { organizationId?: string; resourceId?: string },
): Promise<void> {
  if (ids.length === 0) {
    return;
  }
  await context.ensure(kind);
  await context.deleteWhere(`DELETE_${kind.toUpperCase()}`, context.indexes[kind], [
    { terms: { [SIGNALS[kind].idField]: ids } },
    ...tenantScopeClauses(scope),
  ]);
}

/** Reads one record by id. */
export async function getSignal<TKind extends SignalKind>(
  context: ObservabilityContext,
  kind: TKind,
  id: string,
): Promise<{ record: SignalRecord<TKind>; document: estypes.GetGetResult<StoredDocument> } | null> {
  await context.ensure(kind);
  const document = await context.getDocument(`GET_${kind.toUpperCase()}`, context.indexes[kind], id);
  if (!document) {
    return null;
  }
  return { record: SIGNALS[kind].fromDocument(document._source as StoredDocument) as SignalRecord<TKind>, document };
}
