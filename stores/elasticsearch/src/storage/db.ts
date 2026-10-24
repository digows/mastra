import type { Client as ElasticSearchClient } from '@elastic/elasticsearch';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { createStorageErrorId } from '@mastra/core/storage';
import type { TABLE_NAMES } from '@mastra/core/storage';

import { getKey, processRecord } from './domains/utils';

const SEARCH_PAGE_SIZE = 1000;

/**
 * Thin document-store layer over ElasticSearch.
 *
 * Each Mastra table maps to one ElasticSearch index (same name, already lowercase).
 * Records are stored as opaque JSON strings in a non-indexed `doc` field, with a
 * `key` keyword field (copy of `_id`) used for stable `search_after` pagination.
 *
 * ElasticSearch is near-real-time: all writes use `refresh: true` so
 * subsequent searches observe them, and point reads use `_get` by id (which is
 * real-time regardless of refresh).
 */
export class ElasticSearchDB {
  private client: ElasticSearchClient;
  private indexPrefix: string;
  private ensuredIndexes = new Set<string>();

  constructor({ client, indexPrefix }: ElasticSearchDomainConfig) {
    this.client = client;
    this.indexPrefix = validateIndexPrefix(indexPrefix);
  }

  /** Index that stores `tableName`, with the store's optional index prefix. */
  indexName(tableName: TABLE_NAMES): string {
    return `${this.indexPrefix}${tableName}`;
  }

  getClient(): ElasticSearchClient {
    return this.client;
  }

  async ensureIndex(tableName: TABLE_NAMES): Promise<void> {
    if (this.ensuredIndexes.has(tableName)) {
      return;
    }
    try {
      const exists = await this.client.indices.exists({ index: this.indexName(tableName) });
      if (!exists) {
        await this.client.indices.create({
          index: this.indexName(tableName),
          mappings: {
            dynamic: false,
            properties: {
              key: { type: 'keyword' },
              doc: { type: 'text', index: false },
            },
          },
        });
      }
      this.ensuredIndexes.add(tableName);
    } catch (error: any) {
      const message = error?.message || error?.toString();
      if (message && message.toLowerCase().includes('already exists')) {
        this.ensuredIndexes.add(tableName);
        return;
      }
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'ENSURE_INDEX', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async insert({ tableName, record }: { tableName: TABLE_NAMES; record: Record<string, unknown> }): Promise<void> {
    const { key, processedRecord } = processRecord(tableName, record);
    await this.set({ tableName, key, value: processedRecord });
  }

  async set({
    tableName,
    key,
    value,
  }: {
    tableName: TABLE_NAMES;
    key: string;
    value: Record<string, unknown>;
  }): Promise<void> {
    await this.ensureIndex(tableName);
    try {
      await this.client.index({
        index: this.indexName(tableName),
        id: key,
        document: { key, doc: JSON.stringify(value) },
        refresh: true,
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'INSERT', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async bulkSet({
    tableName,
    entries,
  }: {
    tableName: TABLE_NAMES;
    entries: Array<{ key: string; value: Record<string, unknown> }>;
  }): Promise<void> {
    if (entries.length === 0) {
      return;
    }
    await this.ensureIndex(tableName);
    try {
      const operations = entries.flatMap(({ key, value }) => [
        { index: { _index: this.indexName(tableName), _id: key } },
        { key, doc: JSON.stringify(value) },
      ]);
      const response = await this.client.bulk({ operations, refresh: true });
      if (response.errors) {
        const firstError = response.items.find(item => item.index?.error)?.index?.error;
        throw new Error(`Bulk write failed: ${firstError?.reason ?? 'unknown error'}`);
      }
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'BATCH_INSERT', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async get<R>({ tableName, keys }: { tableName: TABLE_NAMES; keys: Record<string, string> }): Promise<R | null> {
    const key = getKey(tableName, keys);
    return this.getByKey<R>({ tableName, key });
  }

  async getByKey<R>({ tableName, key }: { tableName: TABLE_NAMES; key: string }): Promise<R | null> {
    await this.ensureIndex(tableName);
    try {
      const response = await this.client.get<{ doc: string }>(
        { index: this.indexName(tableName), id: key },
        { ignore: [404] },
      );
      if (!response.found || !response._source?.doc) {
        return null;
      }
      return JSON.parse(response._source.doc) as R;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'LOAD', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  /**
   * Returns all documents in a table, parsed. Uses `search_after` on the `key`
   * field for stable deep pagination.
   */
  async listAll<R>({ tableName, keyPrefix }: { tableName: TABLE_NAMES; keyPrefix?: string }): Promise<R[]> {
    const entries = await this.listAllEntries<R>({ tableName, keyPrefix });
    return entries.map(entry => entry.value);
  }

  /**
   * Returns all `{ key, value }` entries in a table (optionally filtered by key
   * prefix), using `search_after` pagination.
   */
  async listAllEntries<R>({
    tableName,
    keyPrefix,
  }: {
    tableName: TABLE_NAMES;
    keyPrefix?: string;
  }): Promise<Array<{ key: string; value: R }>> {
    await this.ensureIndex(tableName);
    try {
      const results: Array<{ key: string; value: R }> = [];
      let searchAfter: Array<string | number> | undefined;

      while (true) {
        const response = await this.client.search<{ key: string; doc: string }>({
          index: this.indexName(tableName),
          size: SEARCH_PAGE_SIZE,
          query: keyPrefix ? { prefix: { key: keyPrefix } } : { match_all: {} },
          sort: [{ key: 'asc' }],
          ...(searchAfter ? { search_after: searchAfter } : {}),
        });

        const hits = response.hits.hits;
        for (const hit of hits) {
          if (hit._source?.doc) {
            results.push({ key: hit._source.key, value: JSON.parse(hit._source.doc) as R });
          }
        }

        if (hits.length < SEARCH_PAGE_SIZE) {
          break;
        }
        searchAfter = hits[hits.length - 1]!.sort as Array<string | number>;
      }

      return results;
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'SCAN', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async delete({ tableName, key }: { tableName: TABLE_NAMES; key: string }): Promise<void> {
    await this.ensureIndex(tableName);
    try {
      await this.client.delete({ index: this.indexName(tableName), id: key, refresh: true }, { ignore: [404] });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'DELETE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async deleteMany({ tableName, keys }: { tableName: TABLE_NAMES; keys: string[] }): Promise<void> {
    if (keys.length === 0) {
      return;
    }
    await this.ensureIndex(tableName);
    try {
      await this.client.deleteByQuery({
        index: this.indexName(tableName),
        query: { terms: { key: keys } },
        refresh: true,
        conflicts: 'proceed',
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'DELETE_MANY', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }

  async deleteData({ tableName, keyPrefix }: { tableName: TABLE_NAMES; keyPrefix?: string }): Promise<void> {
    await this.ensureIndex(tableName);
    try {
      await this.client.deleteByQuery({
        index: this.indexName(tableName),
        query: keyPrefix ? { prefix: { key: keyPrefix } } : { match_all: {} },
        refresh: true,
        conflicts: 'proceed',
      });
    } catch (error) {
      throw new MastraError(
        {
          id: createStorageErrorId('ELASTICSEARCH', 'CLEAR_TABLE', 'FAILED'),
          domain: ErrorDomain.STORAGE,
          category: ErrorCategory.THIRD_PARTY,
          details: { tableName },
        },
        error,
      );
    }
  }
}

export interface ElasticSearchDomainConfig {
  client: ElasticSearchClient;
  /** Prepended to every index name the domain creates or reads. Defaults to no prefix. */
  indexPrefix?: string;
}

/**
 * Elasticsearch index names must be lowercase and can't contain spaces or any of `\ / * ? " < > | , # :`.
 * A prefix also can't start with `-`, `_`, or `+`, which Elasticsearch rejects, or `.`, which it reserves
 * for hidden and system indices.
 */
const INDEX_PREFIX_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;

/** Returns the prefix to prepend to index names, rejecting one Elasticsearch would refuse. */
export function validateIndexPrefix(indexPrefix: string | undefined): string {
  if (indexPrefix === undefined || indexPrefix === '') {
    return '';
  }
  if (!INDEX_PREFIX_PATTERN.test(indexPrefix)) {
    throw new MastraError({
      id: 'ELASTIC_SEARCH_STORE_INVALID_INDEX_PREFIX',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.USER,
      text: `Invalid indexPrefix "${indexPrefix}": use lowercase letters, digits, "_", "-", or ".", starting with a letter or digit.`,
    });
  }
  return indexPrefix;
}
