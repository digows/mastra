import type { Client as ElasticSearchClient, estypes } from '@elastic/elasticsearch';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { createStorageErrorId } from '@mastra/core/storage';

import { validateIndexPrefix } from '../../db';
import { OBSERVABILITY_SPANS_MAPPING } from './mapping';
import {
  OBSERVABILITY_FEEDBACK_MAPPING,
  OBSERVABILITY_LOGS_MAPPING,
  OBSERVABILITY_METRICS_MAPPING,
  OBSERVABILITY_SCORES_MAPPING,
} from './signal-mapping';

export type QueryClause = estypes.QueryDslQueryContainer;
export type AggregationMap = Record<string, estypes.AggregationsAggregationContainer>;

/** Observability indices. `state` holds the delta-polling cursor counters. */
export type ObservabilityIndexKind = 'spans' | 'logs' | 'metrics' | 'scores' | 'feedback' | 'state';

const INDEX_MAPPINGS: Record<ObservabilityIndexKind, estypes.MappingTypeMapping> = {
  spans: OBSERVABILITY_SPANS_MAPPING,
  logs: OBSERVABILITY_LOGS_MAPPING,
  metrics: OBSERVABILITY_METRICS_MAPPING,
  scores: OBSERVABILITY_SCORES_MAPPING,
  feedback: OBSERVABILITY_FEEDBACK_MAPPING,
  state: { dynamic: false },
};

export const OBSERVABILITY_INDEX_KINDS = Object.keys(INDEX_MAPPINGS) as ObservabilityIndexKind[];

/** Pagination arguments after the core list schemas parsed them. */
export type PageArgs = { page: number; perPage: number };

/** Pagination envelope shared by every observability list response. */
export type PageInfo = { total: number; page: number; perPage: number; hasMore: boolean };

/** A stored document as returned by a search hit or a realtime get. */
export type StoredDocument = Record<string, unknown>;

/**
 * Shared Elasticsearch access for the observability domain: index names (with the store's prefix), lazy
 * index creation, error conversion, and the bulk/search helpers every signal uses.
 */
export class ObservabilityContext {
  readonly client: ElasticSearchClient;
  readonly indexes: Record<ObservabilityIndexKind, string>;
  private readonly ensured = new Map<ObservabilityIndexKind, Promise<void>>();

  constructor(client: ElasticSearchClient, indexPrefix: string | undefined) {
    this.client = client;
    const prefix = validateIndexPrefix(indexPrefix);
    this.indexes = Object.fromEntries(
      OBSERVABILITY_INDEX_KINDS.map(kind => [kind, `${prefix}mastra_observability_${kind}`]),
    ) as Record<ObservabilityIndexKind, string>;
  }

  /** Creates the given indices once per process, with their mapping, when they do not exist. */
  async ensure(...kinds: ObservabilityIndexKind[]): Promise<void> {
    await Promise.all(kinds.map(kind => this.ensureOne(kind)));
  }

  private ensureOne(kind: ObservabilityIndexKind): Promise<void> {
    let pending = this.ensured.get(kind);
    if (!pending) {
      pending = this.createIndexIfMissing(kind).catch(error => {
        this.ensured.delete(kind);
        throw error;
      });
      this.ensured.set(kind, pending);
    }
    return pending;
  }

  private async createIndexIfMissing(kind: ObservabilityIndexKind): Promise<void> {
    const index = this.indexes[kind];
    await this.wrap('ENSURE_INDEX', async () => {
      if (await this.client.indices.exists({ index })) {
        return;
      }
      try {
        await this.client.indices.create({ index, mappings: INDEX_MAPPINGS[kind] });
      } catch (error) {
        if (errorType(error) !== 'resource_already_exists_exception') {
          throw error;
        }
      }
    });
  }

  /** Runs an Elasticsearch call, converting transport failures into a Mastra observability error. */
  async wrap<T>(operation: string, call: () => Promise<T>): Promise<T> {
    try {
      return await call();
    } catch (error) {
      if (error instanceof MastraError) {
        throw error;
      }
      throw storageError(operation, error);
    }
  }

  /** Sends a bulk request with an immediate refresh and fails when any item failed. */
  async bulk(operation: string, operations: estypes.BulkRequest['operations']): Promise<void> {
    if (!operations || operations.length === 0) {
      return;
    }
    await this.wrap(operation, async () => {
      const response = await this.client.bulk({ operations, refresh: true });
      if (!response.errors) {
        return;
      }
      const failures = response.items.flatMap(item => Object.values(item)).filter(result => result?.error);
      throw new Error(
        `Bulk write reported ${failures.length} failures, first: ${JSON.stringify(failures[0]?.error ?? null)}`,
      );
    });
  }

  /** Deletes every document of `index` matching `clauses`, with an immediate refresh. */
  async deleteWhere(operation: string, index: string, clauses: QueryClause[]): Promise<void> {
    await this.wrap(operation, () =>
      this.client.deleteByQuery({ index, query: { bool: { filter: clauses } }, refresh: true, conflicts: 'proceed' }),
    );
  }

  /**
   * One page of documents matching `clauses` in `sort` order, with the exact total. Pages reach at most
   * {@link MAX_PAGE_WINDOW} results deep (Elasticsearch's `index.max_result_window`); deeper pages fail with
   * an explicit error instead of silently raising the cluster setting.
   */
  async searchPage(
    operation: string,
    index: string,
    clauses: QueryClause[],
    sort: estypes.Sort,
    pagination: PageArgs,
  ): Promise<{ documents: StoredDocument[]; page: PageInfo }> {
    const { page, perPage } = pagination;
    if ((page + 1) * perPage > MAX_PAGE_WINDOW) {
      throw new MastraError({
        id: 'OBSERVABILITY_ELASTICSEARCH_PAGE_TOO_DEEP',
        domain: ErrorDomain.MASTRA_OBSERVABILITY,
        category: ErrorCategory.USER,
        text: `Elasticsearch observability storage pages through the first ${MAX_PAGE_WINDOW} results; narrow the filters or the time range to reach older records`,
        details: { page, perPage },
      });
    }
    const response = await this.wrap(operation, () =>
      this.client.search<StoredDocument>({
        index,
        query: { bool: { filter: clauses } },
        sort,
        from: page * perPage,
        size: perPage,
        track_total_hits: true,
      }),
    );
    const total = totalHits(response.hits.total);
    return {
      documents: response.hits.hits.map(hit => hit._source as StoredDocument),
      page: { total, page, perPage, hasMore: (page + 1) * perPage < total },
    };
  }

  /** Runs a size-0 search for aggregations only. */
  async aggregate(
    operation: string,
    index: string | string[],
    clauses: QueryClause[],
    aggs: AggregationMap,
    runtimeMappings?: estypes.MappingRuntimeFields,
  ): Promise<Record<string, unknown>> {
    const response = await this.wrap(operation, () =>
      this.client.search({
        index,
        size: 0,
        query: { bool: { filter: clauses } },
        aggs,
        ...(runtimeMappings ? { runtime_mappings: runtimeMappings } : {}),
        ...(Array.isArray(index) ? { ignore_unavailable: true } : {}),
      }),
    );
    return (response.aggregations ?? {}) as Record<string, unknown>;
  }

  /** Realtime get of one document, or `null` when it does not exist. */
  async getDocument(
    operation: string,
    index: string,
    id: string,
    routing?: string,
  ): Promise<estypes.GetGetResult<StoredDocument> | null> {
    try {
      const response = await this.client.get<StoredDocument>({ index, id, ...(routing ? { routing } : {}) });
      return response.found ? response : null;
    } catch (error) {
      if (statusCode(error) === 404) {
        return null;
      }
      throw storageError(operation, error);
    }
  }

  /**
   * Runs a read-modify-write `attempt` (which writes with `if_seq_no`/`if_primary_term`), retrying it when
   * another writer changed the record in between. Concurrent updates of one record never lose each other's
   * fields.
   */
  async updateWithRetry<T>(operation: string, attempt: () => Promise<T>): Promise<T> {
    for (let tries = 1; ; tries++) {
      try {
        return await attempt();
      } catch (error) {
        if (tries < UPDATE_ATTEMPTS && isVersionConflict(error)) {
          continue;
        }
        throw error instanceof MastraError ? error : storageError(operation, error);
      }
    }
  }
}

/** Attempts for a read-modify-write update when another writer changes the record in between. */
const UPDATE_ATTEMPTS = 5;

/** Elasticsearch's default `index.max_result_window`: the deepest `from + size` a search may reach. */
export const MAX_PAGE_WINDOW = 10_000;

export function totalHits(total: number | estypes.SearchTotalHits | undefined): number {
  return typeof total === 'number' ? total : (total?.value ?? 0);
}

export function statusCode(error: unknown): number | undefined {
  return (error as { meta?: { statusCode?: number } })?.meta?.statusCode;
}

export function isVersionConflict(error: unknown): boolean {
  return statusCode(error) === 409 || errorType(error) === 'version_conflict_engine_exception';
}

function errorType(error: unknown): string | undefined {
  return (error as { meta?: { body?: { error?: { type?: string } } } })?.meta?.body?.error?.type;
}

export function storageError(operation: string, error: unknown): MastraError {
  return new MastraError(
    {
      id: createStorageErrorId('ELASTICSEARCH', operation, 'FAILED'),
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.THIRD_PARTY,
    },
    error,
  );
}

/** Rejects a record without its required id, matching the in-memory adapter. */
export function assertRecordId(recordId: string | null | undefined, field: string): asserts recordId is string {
  if (recordId == null) {
    throw new MastraError({
      id: 'OBSERVABILITY_MISSING_RECORD_ID',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.USER,
      text: `Observability record is missing required id field '${field}'`,
    });
  }
}
