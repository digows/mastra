/**
 * Filter discovery: distinct values for the Studio filter pickers. Like the in-memory adapter, entity,
 * service, environment, and tag discovery reads spans, logs, and metrics; metric discovery reads metrics.
 * Values come from composite aggregations in key order, so they are sorted and bounded by the number of
 * distinct values, never by documents.
 */
import type { estypes } from '@elastic/elasticsearch';
import type {
  GetEntityNamesArgs,
  GetEntityNamesResponse,
  GetEntityTypesResponse,
  GetEnvironmentsResponse,
  GetMetricLabelKeysArgs,
  GetMetricLabelKeysResponse,
  GetMetricLabelValuesArgs,
  GetMetricLabelValuesResponse,
  GetMetricNamesArgs,
  GetMetricNamesResponse,
  GetServiceNamesResponse,
  GetTagsArgs,
  GetTagsResponse,
} from '@mastra/core/storage';
import { EntityType } from '@mastra/core/storage';

import type { ObservabilityContext, QueryClause } from './context';
import { labelRuntimeField } from './metrics';

const DISCOVERY_PAGE_SIZE = 1000;

/** Distinct values of `field` among documents matching `clauses`, in ascending order. */
async function distinctValues(
  context: ObservabilityContext,
  indices: string[],
  field: string,
  clauses: QueryClause[],
  options: { limit?: number; runtimeMappings?: estypes.MappingRuntimeFields } = {},
): Promise<string[]> {
  const values: string[] = [];
  let after: Record<string, string> | undefined;

  for (;;) {
    const size =
      options.limit === undefined ? DISCOVERY_PAGE_SIZE : Math.min(DISCOVERY_PAGE_SIZE, options.limit - values.length);
    const aggregations = await context.aggregate(
      'DISCOVERY',
      indices,
      clauses,
      { values: { composite: { size, sources: [{ value: { terms: { field } } }], ...(after ? { after } : {}) } } },
      options.runtimeMappings,
    );
    const page = aggregations.values as
      | { after_key?: Record<string, string>; buckets: Array<{ key: { value: string } }> }
      | undefined;
    values.push(...(page?.buckets ?? []).map(bucket => String(bucket.key.value)));
    const reachedLimit = options.limit !== undefined && values.length >= options.limit;
    if (reachedLimit || !page?.after_key || page.buckets.length < size) {
      return values;
    }
    after = page.after_key;
  }
}

/** Spans, logs, and metrics: the signals that carry shared observability context. */
function contextIndices(context: ObservabilityContext): string[] {
  return [context.indexes.spans, context.indexes.logs, context.indexes.metrics];
}

/** Empty strings are not offered as filter values, matching the in-memory adapter. */
function nonEmpty(values: string[]): string[] {
  return values.filter(value => value !== '');
}

function entityTypeClause(entityType: string | undefined): QueryClause[] {
  return entityType === undefined ? [] : [{ term: { entityType } }];
}

export async function getMetricNames(
  context: ObservabilityContext,
  args: GetMetricNamesArgs,
): Promise<GetMetricNamesResponse> {
  const clauses: QueryClause[] = args.prefix ? [{ prefix: { name: args.prefix } }] : [];
  return { names: await distinctValues(context, [context.indexes.metrics], 'name', clauses, { limit: args.limit }) };
}

export async function getMetricLabelKeys(
  context: ObservabilityContext,
  args: GetMetricLabelKeysArgs,
): Promise<GetMetricLabelKeysResponse> {
  return {
    keys: await distinctValues(context, [context.indexes.metrics], 'labelKeys', [{ term: { name: args.metricName } }]),
  };
}

export async function getMetricLabelValues(
  context: ObservabilityContext,
  args: GetMetricLabelValuesArgs,
): Promise<GetMetricLabelValuesResponse> {
  const runtimeMappings = { label_value: labelRuntimeField(args.labelKey) };
  const clauses: QueryClause[] = [
    { term: { name: args.metricName } },
    ...(args.prefix ? [{ prefix: { label_value: args.prefix } }] : []),
  ];
  return {
    values: await distinctValues(context, [context.indexes.metrics], 'label_value', clauses, {
      limit: args.limit,
      runtimeMappings,
    }),
  };
}

/** Distinct entity types, keeping only known `EntityType` values, as the in-memory adapter. */
export async function getEntityTypes(context: ObservabilityContext): Promise<GetEntityTypesResponse> {
  const known = new Set<string>(Object.values(EntityType));
  const values = await distinctValues(context, contextIndices(context), 'entityType', []);
  return { entityTypes: values.filter(value => known.has(value)) as EntityType[] };
}

export async function getEntityNames(
  context: ObservabilityContext,
  args: GetEntityNamesArgs,
): Promise<GetEntityNamesResponse> {
  return {
    names: nonEmpty(
      await distinctValues(context, contextIndices(context), 'entityName', entityTypeClause(args.entityType)),
    ),
  };
}

export async function getServiceNames(context: ObservabilityContext): Promise<GetServiceNamesResponse> {
  return { serviceNames: nonEmpty(await distinctValues(context, contextIndices(context), 'serviceName', [])) };
}

export async function getEnvironments(context: ObservabilityContext): Promise<GetEnvironmentsResponse> {
  return { environments: nonEmpty(await distinctValues(context, contextIndices(context), 'environment', [])) };
}

export async function getTags(context: ObservabilityContext, args: GetTagsArgs): Promise<GetTagsResponse> {
  return { tags: await distinctValues(context, contextIndices(context), 'tags', entityTypeClause(args.entityType)) };
}
