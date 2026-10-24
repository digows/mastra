/**
 * Delta polling for the Elasticsearch observability domain.
 *
 * Cursor model
 * ------------
 * Every record gets a `cursorId` from a per-stream counter document in the `state` index when it is first
 * written, and remembers the lease (`cursorLeaseId`) it was published under. Re-writing the same record id
 * keeps its original cursor, so retried exports and later updates of a span are not emitted again.
 *
 * Cursor ids are allocated before the records become searchable, so a writer that allocated a lower block
 * can finish after one that allocated a higher block. Each allocation therefore registers a lease on its
 * block, and readers only return cursor ids up to the safe horizon: just below the lowest leased block, or
 * the last allocated id when none is. This is the guarantee PostgresStoreVNext gets from `pg_snapshot_xmin`.
 *
 * A lease that outlives {@link CURSOR_LEASE_TTL_MS} stops holding the horizon back, so a writer that died
 * mid-write delays live views for at most that long. Expiry is a recorded transition, not a clock reading:
 * the reader or writer that sees an expired lease moves it to `abandoned` in the counter document before
 * reading past it. A writer that was only slow finds its lease abandoned when it releases, and republishes
 * its records under a new block above every cursor already handed out, so they are not skipped. A re-write of
 * a record still published under an abandoned lease also takes a new cursor instead of keeping the old one.
 */
import { randomUUID } from 'node:crypto';

import type { estypes } from '@elastic/elasticsearch';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { coreFeatures } from '@mastra/core/features';

import type { ObservabilityContext, QueryClause, StoredDocument } from './context';

export const OBSERVABILITY_DELTA_POLLING_FEATURE = 'observability-delta-polling';

/** How long an unreleased cursor block may hold back the read horizon. */
export const CURSOR_LEASE_TTL_MS = 60_000;

/** How long abandoned lease ids are remembered, so a later retry of their records takes a new cursor. */
const ABANDONED_RETENTION_MS = 24 * 60 * 60 * 1000;

/** Republish rounds before a writer that keeps losing its lease gives up and reports the write as failed. */
const REPUBLISH_ATTEMPTS = 5;

/** Record streams with their own cursor sequence. Traces and branches both read the `spans` stream. */
export type CursorStream = 'spans' | 'logs' | 'metrics' | 'scores' | 'feedback';

/** Stored counter state for one stream. */
export type CursorState = {
  next?: number;
  leases?: Record<string, { start: number; expiresAt: number }>;
  /** Lease ids readers or writers moved out of `leases` after they expired, with the time they did. */
  abandoned?: Record<string, number>;
};

/** A block of consecutive cursor ids held under one lease. */
type CursorBlock = { firstCursorId: number; leaseId: string; abandonedLeaseIds: string[] };

export function deltaPollingEnabled(): boolean {
  return coreFeatures.has(OBSERVABILITY_DELTA_POLLING_FEATURE);
}

export function assertDeltaPollingEnabled(): void {
  if (deltaPollingEnabled()) {
    return;
  }
  throw new MastraError({
    id: 'OBSERVABILITY_DELTA_POLLING_NOT_SUPPORTED',
    domain: ErrorDomain.MASTRA_OBSERVABILITY,
    category: ErrorCategory.SYSTEM,
    text: 'This storage provider does not support observability delta polling',
  });
}

export function encodeDeltaCursor(cursorId: number | null): string {
  return String(cursorId ?? 0);
}

export function decodeDeltaCursor(cursor: string): number {
  const cursorId = /^\d+$/.test(cursor) ? Number(cursor) : Number.NaN;
  if (!Number.isSafeInteger(cursorId)) {
    throw new MastraError({
      id: 'OBSERVABILITY_INVALID_DELTA_CURSOR',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.USER,
      text: 'Invalid observability delta cursor',
    });
  }
  return cursorId;
}

/** Highest cursor id that is safe to read: no lease still held covers it. */
export function safeHorizon(state: CursorState | null): number {
  const next = state?.next ?? 1;
  let lowestLeased = next;
  for (const lease of Object.values(state?.leases ?? {})) {
    lowestLeased = Math.min(lowestLeased, lease.start);
  }
  return lowestLeased - 1;
}

/** Whether any lease in `state` has expired at `now` and must be abandoned before reading past it. */
export function hasExpiredLease(state: CursorState | null, now: number): boolean {
  return Object.values(state?.leases ?? {}).some(lease => lease.expiresAt < now);
}

/** Moves expired leases to `abandoned` and forgets abandoned ids older than the retention. */
const ABANDON_EXPIRED = `
  long now = params.now;
  if (ctx._source.next == null) { ctx._source.next = 1L; }
  if (ctx._source.leases == null) { ctx._source.leases = new HashMap(); }
  if (ctx._source.abandoned == null) { ctx._source.abandoned = new HashMap(); }
  for (def entry : new ArrayList(ctx._source.leases.entrySet())) {
    if (((Number) entry.getValue().get('expiresAt')).longValue() < now) {
      ctx._source.abandoned.put(entry.getKey(), now);
      ctx._source.leases.remove(entry.getKey());
    }
  }
  ctx._source.abandoned.values().removeIf(at -> ((Number) at).longValue() < now - params.retention);
`;

const ALLOCATE_SCRIPT = `${ABANDON_EXPIRED}
  long start = ((Number) ctx._source.next).longValue();
  ctx._source.next = start + params.count;
  ctx._source.leases.put(params.leaseId, ['start': start, 'expiresAt': now + params.ttl]);
`;

/** Releases a lease. A lease that was abandoned meanwhile is gone, and the update reports `noop`. */
const RELEASE_SCRIPT = `
  if (ctx._source.leases == null || !ctx._source.leases.containsKey(params.leaseId)) {
    ctx.op = 'noop';
  } else {
    ctx._source.leases.remove(params.leaseId);
  }
`;

/**
 * Replaces a record with `params.doc` (which carries a freshly allocated cursor), keeping the record's existing
 * cursor unless it was published under an abandoned lease, which readers may already have passed.
 */
const UPSERT_SCRIPT = `
  Object existingCursorId = ctx._source.cursorId;
  Object existingLeaseId = ctx._source.cursorLeaseId;
  ctx._source.clear();
  ctx._source.putAll(params.doc);
  if (existingCursorId != null && !params.abandoned.contains(existingLeaseId)) {
    ctx._source.cursorId = existingCursorId;
    ctx._source.cursorLeaseId = existingLeaseId;
  }
`;

/** Moves a record still published under `fromLease` to a new cursor. Records re-written since are left alone. */
const REPUBLISH_SCRIPT = `
  if (ctx._source.cursorLeaseId != null && ctx._source.cursorLeaseId == params.fromLease) {
    ctx._source.cursorId = params.cursorId;
    ctx._source.cursorLeaseId = params.toLease;
  } else {
    ctx.op = 'noop';
  }
`;

function counterId(stream: CursorStream): string {
  return `cursor:${stream}`;
}

/** Allocates `count` consecutive cursor ids for `stream` under a new lease. */
async function allocateCursors(
  context: ObservabilityContext,
  stream: CursorStream,
  count: number,
): Promise<CursorBlock> {
  await context.ensure('state');
  const leaseId = randomUUID();
  const response = await context.wrap('ALLOCATE_CURSORS', () =>
    context.client.update<CursorState, CursorState, CursorState>({
      index: context.indexes.state,
      id: counterId(stream),
      script: {
        lang: 'painless',
        source: ALLOCATE_SCRIPT,
        params: { count, leaseId, now: Date.now(), ttl: CURSOR_LEASE_TTL_MS, retention: ABANDONED_RETENTION_MS },
      },
      scripted_upsert: true,
      upsert: {},
      retry_on_conflict: 20,
      _source: true,
    }),
  );
  const state = response.get?._source;
  if (typeof state?.next !== 'number') {
    throw new MastraError({
      id: 'OBSERVABILITY_ELASTICSEARCH_CURSOR_ALLOCATION_FAILED',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.THIRD_PARTY,
      text: 'Elasticsearch did not return the cursor counter after allocation',
    });
  }
  return { firstCursorId: state.next - count, leaseId, abandonedLeaseIds: Object.keys(state.abandoned ?? {}) };
}

/** Releases a lease. Returns `false` when the lease had already been abandoned. */
async function releaseCursors(context: ObservabilityContext, stream: CursorStream, leaseId: string): Promise<boolean> {
  const response = await context.wrap('RELEASE_CURSORS', () =>
    context.client.update({
      index: context.indexes.state,
      id: counterId(stream),
      script: { lang: 'painless', source: RELEASE_SCRIPT, params: { leaseId } },
      retry_on_conflict: 20,
    }),
  );
  return response.result === 'updated';
}

/**
 * The safe read horizon of `stream` right now. Expired leases are first abandoned in the counter document, so
 * their writers republish anything they still write instead of landing below a cursor already handed out.
 */
export async function readHorizon(context: ObservabilityContext, stream: CursorStream): Promise<number> {
  await context.ensure('state');
  const document = await context.getDocument('READ_CURSOR_HORIZON', context.indexes.state, counterId(stream));
  const state = (document?._source as CursorState | undefined) ?? null;
  if (!hasExpiredLease(state, Date.now())) {
    return safeHorizon(state);
  }
  const response = await context.wrap('ABANDON_EXPIRED_CURSORS', () =>
    context.client.update<CursorState, CursorState, CursorState>({
      index: context.indexes.state,
      id: counterId(stream),
      script: {
        lang: 'painless',
        source: ABANDON_EXPIRED,
        params: { now: Date.now(), retention: ABANDONED_RETENTION_MS },
      },
      retry_on_conflict: 20,
      _source: true,
    }),
  );
  return safeHorizon(response.get?._source ?? null);
}

async function maxCursorId(
  context: ObservabilityContext,
  index: string,
  clauses: QueryClause[],
  horizon: number,
): Promise<number | null> {
  const aggregations = await context.aggregate(
    'MAX_CURSOR_ID',
    index,
    [...clauses, { range: { cursorId: { lte: horizon } } }],
    { maxCursorId: { max: { field: 'cursorId' } } },
  );
  const value = (aggregations.maxCursorId as { value: number | null } | undefined)?.value;
  return typeof value === 'number' ? value : null;
}

/** What a list query reads for delta polling. */
export type DeltaQuery = {
  stream: CursorStream;
  index: string;
  /** The list's filters. */
  clauses: QueryClause[];
  /** Every record of the list's stream, used as the cursor when no record matches the filters. */
  streamClauses: QueryClause[];
};

/**
 * One delta window, matching the in-memory adapter: without `after` it returns no rows and a cursor at the
 * current head ("start from now"); with `after` it returns up to `limit` rows past the cursor, oldest first.
 */
export async function listDelta(
  context: ObservabilityContext,
  query: DeltaQuery,
  after: string | undefined,
  limit: number,
): Promise<{ documents: StoredDocument[]; delta: { limit: number; hasMore: boolean }; deltaCursor: string }> {
  assertDeltaPollingEnabled();
  const afterCursorId = after === undefined ? undefined : decodeDeltaCursor(after);
  const horizon = await readHorizon(context, query.stream);
  // The head of the filtered list, else of the whole stream: the cursor when the window has no rows.
  const headCursorId = async () =>
    (await maxCursorId(context, query.index, query.clauses, horizon)) ??
    (await maxCursorId(context, query.index, query.streamClauses, horizon));

  if (afterCursorId === undefined) {
    return { documents: [], delta: { limit, hasMore: false }, deltaCursor: encodeDeltaCursor(await headCursorId()) };
  }

  const response = await context.wrap('LIST_DELTA', () =>
    context.client.search<StoredDocument>({
      index: query.index,
      query: {
        bool: { filter: [...query.clauses, { range: { cursorId: { gt: afterCursorId, lte: horizon } } }] },
      },
      sort: [{ cursorId: { order: 'asc' } }] satisfies estypes.Sort,
      size: limit + 1,
    }),
  );
  const documents = response.hits.hits.map(hit => hit._source as StoredDocument);
  const visible = documents.slice(0, limit);
  const lastCursorId = visible.at(-1)?.cursorId;
  return {
    documents: visible,
    delta: { limit, hasMore: documents.length > limit },
    deltaCursor: encodeDeltaCursor(typeof lastCursorId === 'number' ? lastCursorId : await headCursorId()),
  };
}

/**
 * The cursor a page response hands out so a client can switch to delta polling from it. Traces and
 * branches fall back to the stream head when nothing matches; append-only signals fall back to `0`, as in
 * the in-memory adapter.
 */
export async function pageDeltaCursor(
  context: ObservabilityContext,
  query: DeltaQuery & { fallBackToStreamHead: boolean },
): Promise<{ deltaCursor?: string }> {
  if (!deltaPollingEnabled()) {
    return {};
  }
  const horizon = await readHorizon(context, query.stream);
  const matching = await maxCursorId(context, query.index, query.clauses, horizon);
  const cursorId =
    matching ??
    (query.fallBackToStreamHead ? await maxCursorId(context, query.index, query.streamClauses, horizon) : null);
  return { deltaCursor: encodeDeltaCursor(cursorId) };
}

/** One record to write with a cursor. */
export type CursorWrite = { index: string; id: string; routing?: string; document: Record<string, unknown> };

function bulkTarget(write: CursorWrite): Record<string, unknown> {
  return {
    _index: write.index,
    _id: write.id,
    ...(write.routing ? { routing: write.routing } : {}),
    retry_on_conflict: 5,
  };
}

function upsertOperations(writes: CursorWrite[], block: CursorBlock): estypes.BulkRequest['operations'] {
  return writes.flatMap((write, offset) => [
    { update: bulkTarget(write) },
    {
      script: {
        lang: 'painless',
        source: UPSERT_SCRIPT,
        params: {
          doc: { ...write.document, cursorId: block.firstCursorId + offset, cursorLeaseId: block.leaseId },
          abandoned: block.abandonedLeaseIds,
        },
      },
      upsert: {},
      scripted_upsert: true,
    },
  ]);
}

function republishOperations(
  writes: CursorWrite[],
  from: CursorBlock,
  to: CursorBlock,
): estypes.BulkRequest['operations'] {
  return writes.flatMap((write, offset) => [
    { update: bulkTarget(write) },
    {
      script: {
        lang: 'painless',
        source: REPUBLISH_SCRIPT,
        params: { fromLease: from.leaseId, toLease: to.leaseId, cursorId: to.firstCursorId + offset },
      },
      upsert: {},
      scripted_upsert: true,
    },
  ]);
}

/**
 * Writes records under a freshly leased cursor block, keeping existing cursors on re-writes. If the lease was
 * abandoned while the records were being written (the writer was slower than {@link CURSOR_LEASE_TTL_MS}),
 * the records are republished under a new block before the write is reported as done. `leadingOperations`
 * (for example trace parent documents) are sent first in the same bulk.
 */
export async function writeWithCursors(
  context: ObservabilityContext,
  operation: string,
  stream: CursorStream,
  writes: CursorWrite[],
  leadingOperations: estypes.BulkRequest['operations'] = [],
): Promise<void> {
  if (writes.length === 0) {
    return;
  }

  let block = await allocateCursors(context, stream, writes.length);
  await writeUnderLease(context, stream, block, () =>
    context.bulk(operation, [...(leadingOperations ?? []), ...(upsertOperations(writes, block) ?? [])]),
  );

  for (let attempt = 1; !(await releaseCursors(context, stream, block.leaseId)); attempt++) {
    if (attempt > REPUBLISH_ATTEMPTS) {
      throw new MastraError({
        id: 'OBSERVABILITY_ELASTICSEARCH_CURSOR_PUBLISH_FAILED',
        domain: ErrorDomain.MASTRA_OBSERVABILITY,
        category: ErrorCategory.THIRD_PARTY,
        text: `Records were stored but could not be published to delta polling within ${REPUBLISH_ATTEMPTS} attempts; retry the write`,
      });
    }
    const previous = block;
    block = await allocateCursors(context, stream, writes.length);
    await writeUnderLease(context, stream, block, () =>
      context.bulk(`${operation}_REPUBLISH`, republishOperations(writes, previous, block)),
    );
  }
}

/** Runs `write`, releasing the lease if it fails so readers are not held back until the lease expires. */
async function writeUnderLease(
  context: ObservabilityContext,
  stream: CursorStream,
  block: CursorBlock,
  write: () => Promise<void>,
): Promise<void> {
  try {
    await write();
  } catch (writeError) {
    // The write error is what the caller must see. If the release fails too, the lease expires on its own.
    await releaseCursors(context, stream, block.leaseId).catch(() => undefined);
    throw writeError;
  }
}
