/**
 * OLAP aggregations shared by scores, feedback, and metrics. Everything runs as Elasticsearch
 * aggregations: `stats` for sum/avg/min/max/count, `top_hits` for `last`, `cardinality` for
 * `count_distinct`, `date_histogram` for time series, `percentiles` for percentiles, and composite
 * aggregations (paged by `after_key`, so bounded by the number of groups, not documents) for group-by.
 */
import type { estypes } from '@elastic/elasticsearch';
import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';

import type { AggregationMap, ObservabilityContext, QueryClause } from './context';
import type { DateRangeFilter } from './filters';

/** Aggregation functions accepted by every OLAP query. */
export type OlapAggregation = 'sum' | 'avg' | 'min' | 'max' | 'count' | 'count_distinct' | 'last';

/** Comparison periods accepted by OLAP aggregate queries. */
export type OlapComparePeriod = 'previous_period' | 'previous_day' | 'previous_week';

/** Time bucket intervals accepted by time series and percentile queries. */
export type OlapInterval = '1m' | '5m' | '15m' | '1h' | '1d';

const INTERVAL_MS: Record<OlapInterval, number> = {
  '1m': 60_000,
  '5m': 300_000,
  '15m': 900_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

/** How far back the fixed comparison periods look. `previous_period` shifts by the range's own length. */
const FIXED_SHIFT_MS: Record<Exclude<OlapComparePeriod, 'previous_period'>, number> = {
  previous_day: 86_400_000,
  previous_week: 604_800_000,
};

/** Groups per page of composite aggregations. */
const COMPOSITE_PAGE_SIZE = 1000;

/** Values of a composite group key. `null` means the record has no value for that field. */
export type GroupKey = Record<string, string | null>;

/** One composite bucket, with its sub-aggregation results next to `key` and `doc_count`. */
export type CompositeBucket = { key: GroupKey; doc_count: number } & Record<string, unknown>;

/** One date-histogram bucket, with its sub-aggregation results next to `key`. */
export type TimelineBucket = { key: number; doc_count: number } & Record<string, unknown>;

/**
 * The previous range for period-over-period comparison, or `null` without a closed range, matching the
 * in-memory adapter. The exclusivity flags carry over unchanged.
 */
export function comparisonRange(comparePeriod: OlapComparePeriod, range: DateRangeFilter): DateRangeFilter | null {
  if (!range.start || !range.end) {
    return null;
  }
  const shift =
    comparePeriod === 'previous_period' ? range.end.getTime() - range.start.getTime() : FIXED_SHIFT_MS[comparePeriod];
  return {
    ...range,
    start: new Date(range.start.getTime() - shift),
    end: new Date(range.end.getTime() - shift),
  };
}

/** Change in percent from the previous value, or `null` when it cannot be computed. */
export function changePercent(value: number | null, previousValue: number | null): number | null {
  if (previousValue === null || previousValue === 0 || value === null) {
    return null;
  }
  return ((value - previousValue) / Math.abs(previousValue)) * 100;
}

/**
 * Sub-aggregations that compute `aggregation` over `valueField` in one bucket. `count_distinct` counts
 * distinct values of `distinctField` with Elasticsearch's HyperLogLog `cardinality`, which is close to exact
 * up to 40,000 distinct values and approximate above, like ClickHouse `uniq` and DuckDB `approx_count_distinct`.
 * `last` takes the value with the latest timestamp; ties have no defined winner, as in the other adapters.
 */
export function valueAggregations(
  aggregation: OlapAggregation,
  valueField: string,
  idField: string,
  distinctField?: string,
): AggregationMap {
  const aggregations: AggregationMap = { stats: { stats: { field: valueField } } };
  if (aggregation === 'last') {
    aggregations.last = {
      top_hits: {
        size: 1,
        sort: [{ timestamp: { order: 'desc' } }, { [idField]: { order: 'desc' } }],
        _source: { includes: [valueField] },
      },
    };
  }
  if (aggregation === 'count_distinct' && distinctField) {
    aggregations.distinct = { cardinality: { field: distinctField, precision_threshold: 40_000 } };
  }
  return aggregations;
}

type StatsResult = { count: number; sum: number; avg: number; min: number; max: number };

/**
 * Reads the value computed by {@link valueAggregations}. Like the in-memory adapter, an empty set has no
 * value, and `count_distinct` without a distinct field is 0.
 */
export function readValue(
  bucket: Record<string, unknown>,
  aggregation: OlapAggregation,
  valueField: string,
): number | null {
  if (aggregation === 'count_distinct') {
    return (bucket.distinct as { value: number } | undefined)?.value ?? 0;
  }
  const stats = bucket.stats as StatsResult | undefined;
  if (!stats || stats.count === 0) {
    return null;
  }
  switch (aggregation) {
    case 'sum':
      return stats.sum;
    case 'avg':
      return stats.avg;
    case 'min':
      return stats.min;
    case 'max':
      return stats.max;
    case 'count':
      return stats.count;
    case 'last': {
      const last = bucket.last as { hits: { hits: Array<{ _source: Record<string, unknown> }> } } | undefined;
      const value = last?.hits.hits[0]?._source[valueField];
      return typeof value === 'number' ? value : null;
    }
  }
}

/** A named date histogram over `timestamp`, with `subAggregations` inside each non-empty bucket. */
export function timelineAggregation(interval: OlapInterval, subAggregations: AggregationMap): AggregationMap {
  return {
    timeline: {
      date_histogram: { field: 'timestamp', fixed_interval: `${INTERVAL_MS[interval]}ms`, min_doc_count: 1 },
      aggs: subAggregations,
    },
  };
}

/** Buckets of a {@link timelineAggregation} result. */
export function timelineBuckets(container: Record<string, unknown> | undefined): TimelineBucket[] {
  return (container?.timeline as { buckets: TimelineBucket[] } | undefined)?.buckets ?? [];
}

/** Percentiles sub-aggregation for {@link timelineAggregation}. Percentiles are 0..1, as in the core contract. */
export function percentileAggregation(valueField: string, percentiles: number[]): AggregationMap {
  return { pct: { percentiles: { field: valueField, percents: percentiles.map(p => p * 100), keyed: false } } };
}

/** The value of `percentile` in one bucket, or 0 when the bucket has none (as the in-memory adapter). */
export function percentileValue(bucket: Record<string, unknown>, percentile: number): number {
  const pct = bucket.pct as { values: Array<{ key: number; value: number | null }> } | undefined;
  const match = pct?.values.find(entry => Math.abs(entry.key - percentile * 100) < 1e-9);
  return match?.value ?? 0;
}

/** A group-by field: a mapped keyword field, or a runtime field defined for the request. */
export type GroupField = { name: string; field: string };

/** Every composite bucket for `groupFields`, paged with `after_key`. */
export async function compositeBuckets(
  context: ObservabilityContext,
  index: string,
  clauses: QueryClause[],
  groupFields: GroupField[],
  subAggregations: AggregationMap,
  runtimeMappings?: estypes.MappingRuntimeFields,
): Promise<CompositeBucket[]> {
  // Positional source names: group names can be label keys with characters that are not safe as names.
  const sources = groupFields.map((group, position) => ({
    [sourceName(position)]: { terms: { field: group.field, missing_bucket: true } },
  }));
  const buckets: CompositeBucket[] = [];
  let after: GroupKey | undefined;

  for (;;) {
    const aggregations = await context.aggregate(
      'COMPOSITE_BUCKETS',
      index,
      clauses,
      {
        groups: {
          composite: { size: COMPOSITE_PAGE_SIZE, sources, ...(after ? { after } : {}) },
          aggs: subAggregations,
        },
      },
      runtimeMappings,
    );
    const page = aggregations.groups as { after_key?: GroupKey; buckets: CompositeBucket[] } | undefined;
    buckets.push(...(page?.buckets ?? []));
    if (!page?.after_key || page.buckets.length < COMPOSITE_PAGE_SIZE) {
      return buckets;
    }
    after = page.after_key;
  }
}

function sourceName(position: number): string {
  return `g${position}`;
}

/** Group values of a composite bucket, keyed by the requested group names. */
export function dimensionsOf(bucket: CompositeBucket, groupFields: GroupField[]): GroupKey {
  return Object.fromEntries(
    groupFields.map((group, position) => [group.name, bucket.key[sourceName(position)] ?? null]),
  );
}

/** Series name for a group: the values joined by `|`, empty for a missing value. */
export function seriesName(bucket: CompositeBucket, groupFields: GroupField[]): string {
  return groupFields.map((_, position) => bucket.key[sourceName(position)] ?? '').join('|');
}

/**
 * Resolves group-by names against mapped keyword fields. A name outside `groupable` fails explicitly
 * instead of falling back to reading documents.
 */
export function keywordGroupFields(groupBy: readonly string[], groupable: ReadonlySet<string>): GroupField[] {
  return groupBy.map(name => {
    if (!groupable.has(name)) {
      throw unsupportedGroupBy(name);
    }
    return { name, field: name };
  });
}

export function unsupportedGroupBy(name: string): MastraError {
  return new MastraError({
    id: 'OBSERVABILITY_ELASTICSEARCH_UNSUPPORTED_GROUP_BY',
    domain: ErrorDomain.MASTRA_OBSERVABILITY,
    category: ErrorCategory.USER,
    text: `Elasticsearch observability storage cannot group by "${name}"`,
  });
}

/** What score and feedback OLAP aggregate: one numeric field of one index. */
export type OlapTarget = {
  index: string;
  valueField: string;
  idField: string;
  groupable: ReadonlySet<string>;
};

export async function olapAggregate(
  context: ObservabilityContext,
  target: OlapTarget,
  clausesFor: (timestamp: DateRangeFilter | undefined) => QueryClause[],
  args: { aggregation: OlapAggregation; timestamp?: DateRangeFilter; comparePeriod?: OlapComparePeriod },
): Promise<{ value: number | null; previousValue?: number | null; changePercent?: number | null }> {
  const valueIn = async (timestamp: DateRangeFilter | undefined) =>
    readValue(
      await context.aggregate(
        'OLAP_AGGREGATE',
        target.index,
        clausesFor(timestamp),
        valueAggregations(args.aggregation, target.valueField, target.idField),
      ),
      args.aggregation,
      target.valueField,
    );

  const value = await valueIn(args.timestamp);
  const previousRange =
    args.comparePeriod && args.timestamp ? comparisonRange(args.comparePeriod, args.timestamp) : null;
  if (!previousRange) {
    return { value };
  }
  const previousValue = await valueIn(previousRange);
  return { value, previousValue, changePercent: changePercent(value, previousValue) };
}

export async function olapBreakdown(
  context: ObservabilityContext,
  target: OlapTarget,
  clauses: QueryClause[],
  args: { groupBy: readonly string[]; aggregation: OlapAggregation },
): Promise<{ groups: Array<{ dimensions: GroupKey; value: number }> }> {
  const groupFields = keywordGroupFields(args.groupBy, target.groupable);
  const buckets = await compositeBuckets(
    context,
    target.index,
    clauses,
    groupFields,
    valueAggregations(args.aggregation, target.valueField, target.idField),
  );
  const groups = buckets.map(bucket => ({
    dimensions: dimensionsOf(bucket, groupFields),
    value: readValue(bucket, args.aggregation, target.valueField) ?? 0,
  }));
  groups.sort((a, b) => b.value - a.value);
  return { groups };
}

export async function olapTimeSeries(
  context: ObservabilityContext,
  target: OlapTarget,
  clauses: QueryClause[],
  args: { interval: OlapInterval; aggregation: OlapAggregation; groupBy?: readonly string[]; name: string },
): Promise<{ series: Array<{ name: string; points: Array<{ timestamp: Date; value: number }> }> }> {
  const timeline = timelineAggregation(
    args.interval,
    valueAggregations(args.aggregation, target.valueField, target.idField),
  );
  const points = (container: Record<string, unknown> | undefined) =>
    timelineBuckets(container).map(bucket => ({
      timestamp: new Date(bucket.key),
      value: readValue(bucket, args.aggregation, target.valueField) ?? 0,
    }));

  if (args.groupBy && args.groupBy.length > 0) {
    const groupFields = keywordGroupFields(args.groupBy, target.groupable);
    const buckets = await compositeBuckets(context, target.index, clauses, groupFields, timeline);
    return { series: buckets.map(bucket => ({ name: seriesName(bucket, groupFields), points: points(bucket) })) };
  }

  const aggregations = await context.aggregate('OLAP_TIME_SERIES', target.index, clauses, timeline);
  return { series: [{ name: args.name, points: points(aggregations) }] };
}

/**
 * Percentile series. Elasticsearch computes percentiles with t-digest, an approximation, like ClickHouse
 * `quantiles`; PostgresStoreVNext and DuckDB interpolate exactly.
 */
export async function olapPercentiles(
  context: ObservabilityContext,
  target: OlapTarget,
  clauses: QueryClause[],
  args: { interval: OlapInterval; percentiles: number[] },
): Promise<{ series: Array<{ percentile: number; points: Array<{ timestamp: Date; value: number }> }> }> {
  const aggregations = await context.aggregate(
    'OLAP_PERCENTILES',
    target.index,
    clauses,
    timelineAggregation(args.interval, percentileAggregation(target.valueField, args.percentiles)),
  );
  const buckets = timelineBuckets(aggregations);
  return {
    series: args.percentiles.map(percentile => ({
      percentile,
      points: buckets.map(bucket => ({ timestamp: new Date(bucket.key), value: percentileValue(bucket, percentile) })),
    })),
  };
}
