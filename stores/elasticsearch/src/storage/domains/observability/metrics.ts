/**
 * Metric storage and OLAP. Metrics carry free-form string labels; a group-by name resolves to a metric
 * column when it is one, and to a label key otherwise, as in the ClickHouse adapter. Label values are read
 * per request through a keyword runtime field over the stored label tokens, so arbitrary label keys
 * (dots, dashes, spaces) never become mapping fields.
 */
import type { estypes } from '@elastic/elasticsearch';
import type {
  GetMetricAggregateArgs,
  GetMetricAggregateResponse,
  GetMetricBreakdownArgs,
  GetMetricBreakdownResponse,
  GetMetricPercentilesArgs,
  GetMetricPercentilesResponse,
  GetMetricTimeSeriesArgs,
  GetMetricTimeSeriesResponse,
  ListMetricsArgs,
  ListMetricsResponse,
  MetricsFilter,
} from '@mastra/core/storage';
import { listMetricsArgsSchema } from '@mastra/core/storage';

import type { AggregationMap, ObservabilityContext, QueryClause } from './context';
import type { DateRangeFilter } from './filters';
import { metricFilterClauses } from './filters';
import { CONTEXT_KEYWORD_FIELDS } from './mapping';
import type { CompositeBucket, GroupField, OlapAggregation } from './olap';
import {
  changePercent,
  comparisonRange,
  compositeBuckets,
  dimensionsOf,
  percentileAggregation,
  percentileValue,
  readValue,
  seriesName,
  timelineAggregation,
  timelineBuckets,
  unsupportedGroupBy,
  valueAggregations,
} from './olap';
import { LABEL_TOKEN_SEPARATOR } from './signal-mapping';
import { listSignals } from './signals';

/** Metric columns that group-by can use directly. */
const METRIC_GROUP_COLUMNS: ReadonlySet<string> = new Set([
  'name',
  'traceId',
  'spanId',
  'provider',
  'model',
  'costUnit',
  'source',
  ...CONTEXT_KEYWORD_FIELDS,
]);

/** Metric columns that are not group-by dimensions. Grouping by one is a caller error, not a label key. */
const METRIC_NON_GROUP_COLUMNS: ReadonlySet<string> = new Set([
  'metricId',
  'timestamp',
  'value',
  'estimatedCost',
  'labels',
  'metadata',
  'costMetadata',
  'tags',
]);

/** Emits the value of one label from the stored `key\u0000value` tokens. */
const LABEL_VALUE_SCRIPT = `
  String prefix = params.prefix;
  for (def token : doc['labelTokens']) {
    if (token.startsWith(prefix)) { emit(token.substring(prefix.length())); return; }
  }
`;

/** A keyword runtime field that exposes the value of label `key`. */
export function labelRuntimeField(key: string): estypes.MappingRuntimeField {
  return {
    type: 'keyword',
    script: { lang: 'painless', source: LABEL_VALUE_SCRIPT, params: { prefix: `${key}${LABEL_TOKEN_SEPARATOR}` } },
  };
}

type ResolvedGroups = { groupFields: GroupField[]; runtimeMappings?: estypes.MappingRuntimeFields };

function resolveGroupFields(groupBy: readonly string[]): ResolvedGroups {
  const runtimeMappings: estypes.MappingRuntimeFields = {};
  const groupFields = groupBy.map((name, position) => {
    if (METRIC_GROUP_COLUMNS.has(name)) {
      return { name, field: name };
    }
    if (METRIC_NON_GROUP_COLUMNS.has(name)) {
      throw unsupportedGroupBy(name);
    }
    const field = `group_label_${position}`;
    runtimeMappings[field] = labelRuntimeField(name);
    return { name, field };
  });
  return Object.keys(runtimeMappings).length > 0 ? { groupFields, runtimeMappings } : { groupFields };
}

/** Summed estimated cost and the shared cost unit of the same rows, as in the in-memory adapter. */
const COST_AGGREGATIONS: AggregationMap = {
  costSum: { sum: { field: 'estimatedCost' } },
  costCount: { value_count: { field: 'estimatedCost' } },
  costUnits: { terms: { field: 'costUnit', size: 2 } },
};

function readCost(bucket: Record<string, unknown>): { estimatedCost: number | null; costUnit: string | null } {
  const count = (bucket.costCount as { value: number } | undefined)?.value ?? 0;
  const units = (bucket.costUnits as { buckets: Array<{ key: string }> } | undefined)?.buckets ?? [];
  return {
    estimatedCost: count > 0 ? ((bucket.costSum as { value: number }).value ?? null) : null,
    costUnit: units.length === 1 ? units[0]!.key : null,
  };
}

function metricValueAggregations(aggregation: OlapAggregation, distinctColumn: string | undefined): AggregationMap {
  return { ...valueAggregations(aggregation, 'value', 'metricId', distinctColumn), ...COST_AGGREGATIONS };
}

/** Filters plus the queried metric names, optionally with another time range. */
function metricClauses(
  filters: Partial<MetricsFilter> | undefined,
  names: readonly string[],
  timestamp?: DateRangeFilter,
): QueryClause[] {
  return [...metricFilterClauses({ ...filters, ...(timestamp ? { timestamp } : {}) }), { terms: { name: [...names] } }];
}

export async function listMetrics(context: ObservabilityContext, args: ListMetricsArgs): Promise<ListMetricsResponse> {
  const { mode, filters, pagination, orderBy, after, limit } = listMetricsArgsSchema.parse(args);
  const direction = orderBy.direction === 'ASC' ? 'asc' : 'desc';
  const { records, ...rest } = await listSignals(context, 'metrics', {
    mode,
    clauses: metricFilterClauses(filters ?? {}),
    sort: [{ timestamp: { order: direction } }, { metricId: 'asc' }],
    pagination,
    after,
    limit,
  });
  return { metrics: records, ...rest };
}

export async function getMetricAggregate(
  context: ObservabilityContext,
  args: GetMetricAggregateArgs,
): Promise<GetMetricAggregateResponse> {
  await context.ensure('metrics');
  const read = async (timestamp?: DateRangeFilter) => {
    const aggregations = await context.aggregate(
      'METRIC_AGGREGATE',
      context.indexes.metrics,
      metricClauses(args.filters, args.name, timestamp),
      metricValueAggregations(args.aggregation, args.distinctColumn),
    );
    return { value: readValue(aggregations, args.aggregation, 'value'), ...readCost(aggregations) };
  };

  const current = await read();
  const previousRange =
    args.comparePeriod && args.filters?.timestamp ? comparisonRange(args.comparePeriod, args.filters.timestamp) : null;
  if (!previousRange) {
    return current;
  }
  const previous = await read(previousRange);
  return {
    ...current,
    previousValue: previous.value,
    previousEstimatedCost: previous.estimatedCost,
    changePercent: changePercent(current.value, previous.value),
    costChangePercent: changePercent(current.estimatedCost, previous.estimatedCost),
  };
}

/** `terms` order paths for a server-side top-K by the aggregated value. `last` has no orderable path. */
const ORDER_PATHS: Partial<Record<OlapAggregation, string>> = {
  sum: 'stats.sum',
  avg: 'stats.avg',
  min: 'stats.min',
  max: 'stats.max',
  count: 'stats.count',
  count_distinct: 'distinct',
};

/** Stands in for a missing group value in `terms`/`multi_terms`, which have no missing bucket of their own. */
const MISSING_GROUP_VALUE = '\u0000missing';

type MetricGroup = GetMetricBreakdownResponse['groups'][number];

/**
 * Groups ordered by the aggregated value. With `limit`, Elasticsearch ranks the groups (`terms` or
 * `multi_terms` ordered by the value), so high-cardinality group-bys stay server-side. Without a limit, or
 * for `last`, every group is read by composite paging and ordered here.
 */
export async function getMetricBreakdown(
  context: ObservabilityContext,
  args: GetMetricBreakdownArgs,
): Promise<GetMetricBreakdownResponse> {
  await context.ensure('metrics');
  const { groupFields, runtimeMappings } = resolveGroupFields(args.groupBy);
  const clauses = metricClauses(args.filters, args.name);
  const subAggregations = metricValueAggregations(args.aggregation, args.distinctColumn);
  const direction: estypes.SortOrder = args.orderDirection === 'ASC' ? 'asc' : 'desc';
  const orderPath = ORDER_PATHS[args.aggregation];

  const toGroup = (dimensions: MetricGroup['dimensions'], bucket: Record<string, unknown>): MetricGroup => ({
    dimensions,
    value: readValue(bucket, args.aggregation, 'value') ?? 0,
    ...readCost(bucket),
  });

  if (args.limit !== undefined && orderPath) {
    const order = { [orderPath]: direction };
    const ranking: estypes.AggregationsAggregationContainer =
      groupFields.length === 1
        ? { terms: { field: groupFields[0]!.field, size: args.limit, missing: MISSING_GROUP_VALUE, order } }
        : {
            multi_terms: {
              terms: groupFields.map(group => ({ field: group.field, missing: MISSING_GROUP_VALUE })),
              size: args.limit,
              order,
            },
          };
    const aggregations = await context.aggregate(
      'METRIC_BREAKDOWN',
      context.indexes.metrics,
      clauses,
      { groups: { ...ranking, aggs: subAggregations } },
      runtimeMappings,
    );
    const buckets = (aggregations.groups as { buckets: Array<{ key: string | string[] } & Record<string, unknown>> })
      .buckets;
    return {
      groups: buckets.map(bucket => {
        const values = Array.isArray(bucket.key) ? bucket.key : [bucket.key];
        const dimensions = Object.fromEntries(
          groupFields.map((group, position) => {
            const value = String(values[position]);
            return [group.name, value === MISSING_GROUP_VALUE ? null : value];
          }),
        );
        return toGroup(dimensions, bucket);
      }),
    };
  }

  const buckets = await compositeBuckets(
    context,
    context.indexes.metrics,
    clauses,
    groupFields,
    subAggregations,
    runtimeMappings,
  );
  const groups = buckets.map(bucket => toGroup(dimensionsOf(bucket, groupFields), bucket));
  const sign = direction === 'asc' ? 1 : -1;
  groups.sort((a, b) => (a.value - b.value) * sign);
  return { groups: args.limit === undefined ? groups : groups.slice(0, args.limit) };
}

export async function getMetricTimeSeries(
  context: ObservabilityContext,
  args: GetMetricTimeSeriesArgs,
): Promise<GetMetricTimeSeriesResponse> {
  await context.ensure('metrics');
  const clauses = metricClauses(args.filters, args.name);
  const timeline = timelineAggregation(args.interval, metricValueAggregations(args.aggregation, args.distinctColumn));
  const points = (container: Record<string, unknown>) =>
    timelineBuckets(container).map(bucket => ({
      timestamp: new Date(bucket.key),
      value: readValue(bucket, args.aggregation, 'value') ?? 0,
      estimatedCost: readCost(bucket).estimatedCost,
    }));

  if (args.groupBy && args.groupBy.length > 0) {
    const { groupFields, runtimeMappings } = resolveGroupFields(args.groupBy);
    // One series per distinct value tuple: values containing `|` keep separate series even when their joined
    // display names collide.
    const buckets: CompositeBucket[] = await compositeBuckets(
      context,
      context.indexes.metrics,
      clauses,
      groupFields,
      { ...timeline, ...COST_AGGREGATIONS },
      runtimeMappings,
    );
    return {
      series: buckets.map(bucket => ({
        name: seriesName(bucket, groupFields),
        costUnit: readCost(bucket).costUnit,
        points: points(bucket),
      })),
    };
  }

  const aggregations = await context.aggregate('METRIC_TIME_SERIES', context.indexes.metrics, clauses, {
    ...timeline,
    ...COST_AGGREGATIONS,
  });
  return {
    series: [{ name: args.name.join(','), costUnit: readCost(aggregations).costUnit, points: points(aggregations) }],
  };
}

/**
 * Percentile series of one metric. Elasticsearch percentiles are t-digest approximations, like ClickHouse
 * `quantiles`.
 */
export async function getMetricPercentiles(
  context: ObservabilityContext,
  args: GetMetricPercentilesArgs,
): Promise<GetMetricPercentilesResponse> {
  await context.ensure('metrics');
  const aggregations = await context.aggregate(
    'METRIC_PERCENTILES',
    context.indexes.metrics,
    metricClauses(args.filters, [args.name]),
    timelineAggregation(args.interval, percentileAggregation('value', args.percentiles)),
  );
  const buckets = timelineBuckets(aggregations);
  return {
    series: args.percentiles.map(percentile => ({
      percentile,
      points: buckets.map(bucket => ({ timestamp: new Date(bucket.key), value: percentileValue(bucket, percentile) })),
    })),
  };
}
