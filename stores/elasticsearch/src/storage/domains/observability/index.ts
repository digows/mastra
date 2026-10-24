import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type {
  BatchCreateFeedbackArgs,
  BatchCreateLogsArgs,
  BatchCreateMetricsArgs,
  BatchCreateScoresArgs,
  BatchCreateSpansArgs,
  BatchDeleteTracesArgs,
  BatchUpdateSpansArgs,
  CreateFeedbackArgs,
  CreateScoreArgs,
  CreateSpanArgs,
  DeleteFeedbackArgs,
  DeleteScoresArgs,
  FeedbackFilter,
  FeedbackRecord,
  GetEntityNamesArgs,
  GetEntityNamesResponse,
  GetEntityTypesArgs,
  GetEntityTypesResponse,
  GetEnvironmentsArgs,
  GetEnvironmentsResponse,
  GetFeedbackAggregateArgs,
  GetFeedbackAggregateResponse,
  GetFeedbackBreakdownArgs,
  GetFeedbackBreakdownResponse,
  GetFeedbackPercentilesArgs,
  GetFeedbackPercentilesResponse,
  GetFeedbackTimeSeriesArgs,
  GetFeedbackTimeSeriesResponse,
  GetMetricAggregateArgs,
  GetMetricAggregateResponse,
  GetMetricBreakdownArgs,
  GetMetricBreakdownResponse,
  GetMetricLabelKeysArgs,
  GetMetricLabelKeysResponse,
  GetMetricLabelValuesArgs,
  GetMetricLabelValuesResponse,
  GetMetricNamesArgs,
  GetMetricNamesResponse,
  GetMetricPercentilesArgs,
  GetMetricPercentilesResponse,
  GetMetricTimeSeriesArgs,
  GetMetricTimeSeriesResponse,
  GetRootSpanArgs,
  GetRootSpanResponse,
  GetScoreAggregateArgs,
  GetScoreAggregateResponse,
  GetScoreBreakdownArgs,
  GetScoreBreakdownResponse,
  GetScorePercentilesArgs,
  GetScorePercentilesResponse,
  GetScoreTimeSeriesArgs,
  GetScoreTimeSeriesResponse,
  GetServiceNamesArgs,
  GetServiceNamesResponse,
  GetSpanArgs,
  GetSpanResponse,
  GetSpansArgs,
  GetSpansResponse,
  GetStructureResponse,
  GetTagsArgs,
  GetTagsResponse,
  GetTraceArgs,
  GetTraceResponse,
  ListBranchesArgs,
  ListBranchesResponse,
  ListFeedbackArgs,
  ListFeedbackResponse,
  ListLogsArgs,
  ListLogsResponse,
  ListMetricsArgs,
  ListMetricsResponse,
  ListScoresArgs,
  ListScoresResponse,
  ListTracesArgs,
  ListTracesResponse,
  ObservabilityStorageFeature,
  ObservabilityStorageStrategy,
  ScoreRecord,
  ScoresFilter,
  UpdateFeedbackReviewStatusArgs,
  UpdateSpanArgs,
} from '@mastra/core/storage';
import {
  listFeedbackArgsSchema,
  listLogsArgsSchema,
  listScoresArgsSchema,
  ObservabilityStorage,
} from '@mastra/core/storage';

import type { ElasticSearchDomainConfig } from '../../db';
import type { QueryClause, StoredDocument } from './context';
import { OBSERVABILITY_INDEX_KINDS, ObservabilityContext } from './context';
import * as discovery from './discovery';
import type { DateRangeFilter } from './filters';
import { feedbackFilterClauses, logFilterClauses, scoreFilterClauses } from './filters';
import { CONTEXT_KEYWORD_FIELDS } from './mapping';
import * as metrics from './metrics';
import type { OlapTarget } from './olap';
import { olapAggregate, olapBreakdown, olapPercentiles, olapTimeSeries } from './olap';
import { deltaPollingEnabled } from './polling';
import { fromFeedbackDocument } from './signal-mapping';
import { deleteSignals, getSignal, listSignals, writeSignals } from './signals';
import * as spans from './spans';

/** Observability capabilities this adapter implements, declared through `getFeatures()`. */
const FEATURES = [
  'metrics',
  'logs',
  'feedback',
  'entity-type-discovery',
  'entity-name-discovery',
  'service-name-discovery',
  'environment-discovery',
  'tag-discovery',
  'metric-discovery',
] as const satisfies readonly ObservabilityStorageFeature[];

const SCORE_GROUPABLE_FIELDS: ReadonlySet<string> = new Set([
  'traceId',
  'spanId',
  'scorerId',
  'scorerName',
  'scorerVersion',
  'scoreSource',
  'scoreTraceId',
  ...CONTEXT_KEYWORD_FIELDS,
]);

const FEEDBACK_GROUPABLE_FIELDS: ReadonlySet<string> = new Set([
  'traceId',
  'spanId',
  'feedbackType',
  'feedbackSource',
  'feedbackUserId',
  'reviewStatus',
  ...CONTEXT_KEYWORD_FIELDS,
]);

/**
 * Observability storage on Elasticsearch: traces, spans, logs, metrics, scores, and feedback, each in its
 * own index (with the store's optional index prefix). The operations live in one module per signal; this
 * class binds them to the shared {@link ObservabilityContext}.
 */
export class ObservabilityElasticSearch extends ObservabilityStorage {
  private readonly context: ObservabilityContext;
  private readonly scoreTarget: OlapTarget;
  private readonly feedbackTarget: OlapTarget;

  constructor(config: ElasticSearchDomainConfig) {
    super();
    this.context = new ObservabilityContext(config.client, config.indexPrefix);
    this.scoreTarget = {
      index: this.context.indexes.scores,
      valueField: 'score',
      idField: 'scoreId',
      groupable: SCORE_GROUPABLE_FIELDS,
    };
    this.feedbackTarget = {
      index: this.context.indexes.feedback,
      valueField: 'numericValue',
      idField: 'feedbackId',
      groupable: FEEDBACK_GROUPABLE_FIELDS,
    };
  }

  public override get observabilityStrategy(): {
    preferred: ObservabilityStorageStrategy;
    supported: ObservabilityStorageStrategy[];
  } {
    return { preferred: 'batch-with-updates', supported: ['realtime', 'batch-with-updates'] };
  }

  public override getFeatures(): readonly ObservabilityStorageFeature[] {
    return deltaPollingEnabled() ? [...FEATURES, 'delta-polling'] : FEATURES;
  }

  public async init(): Promise<void> {
    await this.context.ensure(...OBSERVABILITY_INDEX_KINDS);
  }

  /**
   * Deletes every observability record. The delta cursor counters are kept, so cursors handed out before
   * the clear never skip records written after it.
   */
  public async dangerouslyClearAll(): Promise<void> {
    await this.context.ensure(...OBSERVABILITY_INDEX_KINDS);
    for (const kind of OBSERVABILITY_INDEX_KINDS) {
      if (kind !== 'state') {
        await this.context.deleteWhere('CLEAR_ALL', this.context.indexes[kind], [{ match_all: {} }]);
      }
    }
  }

  // ============================================================================
  // Traces and spans
  // ============================================================================

  public async createSpan(args: CreateSpanArgs): Promise<void> {
    await spans.createSpans(this.context, [args.span]);
  }

  public async batchCreateSpans(args: BatchCreateSpansArgs): Promise<void> {
    await spans.createSpans(this.context, args.records);
  }

  public async updateSpan(args: UpdateSpanArgs): Promise<void> {
    await spans.updateSpan(this.context, args);
  }

  /** Applies updates in order and stops at the first missing span, as the in-memory adapter does. */
  public async batchUpdateSpans(args: BatchUpdateSpansArgs): Promise<void> {
    for (const record of args.records) {
      await spans.updateSpan(this.context, record);
    }
  }

  public async batchDeleteTraces(args: BatchDeleteTracesArgs): Promise<void> {
    await spans.deleteTraces(this.context, args);
  }

  public async getSpan(args: GetSpanArgs): Promise<GetSpanResponse | null> {
    return spans.getSpan(this.context, args.traceId, args.spanId);
  }

  public async getSpans(args: GetSpansArgs): Promise<GetSpansResponse> {
    return spans.getSpans(this.context, args.traceId, args.spanIds);
  }

  public async getRootSpan(args: GetRootSpanArgs): Promise<GetRootSpanResponse | null> {
    return spans.getRootSpan(this.context, args.traceId);
  }

  public async getTrace(args: GetTraceArgs): Promise<GetTraceResponse | null> {
    return spans.getTrace(this.context, args.traceId);
  }

  public async getTraceLight(args: GetTraceArgs): Promise<GetStructureResponse | null> {
    return spans.getTraceLight(this.context, args.traceId);
  }

  public async listTraces(args: ListTracesArgs): Promise<ListTracesResponse> {
    return spans.listTraces(this.context, args);
  }

  public async listBranches(args: ListBranchesArgs): Promise<ListBranchesResponse> {
    return spans.listBranches(this.context, args);
  }

  // ============================================================================
  // Logs
  // ============================================================================

  public async batchCreateLogs(args: BatchCreateLogsArgs): Promise<void> {
    await writeSignals(this.context, 'logs', args.logs);
  }

  public async listLogs(args: ListLogsArgs): Promise<ListLogsResponse> {
    const { mode, filters, pagination, orderBy, after, limit } = listLogsArgsSchema.parse(args);
    const { records, ...rest } = await listSignals(this.context, 'logs', {
      mode,
      clauses: logFilterClauses(filters ?? {}),
      sort: [{ timestamp: { order: orderBy.direction === 'ASC' ? 'asc' : 'desc' } }, { logId: 'asc' }],
      pagination,
      after,
      limit,
    });
    return { logs: records, ...rest };
  }

  // ============================================================================
  // Metrics
  // ============================================================================

  public async batchCreateMetrics(args: BatchCreateMetricsArgs): Promise<void> {
    await writeSignals(this.context, 'metrics', args.metrics);
  }

  public async listMetrics(args: ListMetricsArgs): Promise<ListMetricsResponse> {
    return metrics.listMetrics(this.context, args);
  }

  public async getMetricAggregate(args: GetMetricAggregateArgs): Promise<GetMetricAggregateResponse> {
    return metrics.getMetricAggregate(this.context, args);
  }

  public async getMetricBreakdown(args: GetMetricBreakdownArgs): Promise<GetMetricBreakdownResponse> {
    return metrics.getMetricBreakdown(this.context, args);
  }

  public async getMetricTimeSeries(args: GetMetricTimeSeriesArgs): Promise<GetMetricTimeSeriesResponse> {
    return metrics.getMetricTimeSeries(this.context, args);
  }

  public async getMetricPercentiles(args: GetMetricPercentilesArgs): Promise<GetMetricPercentilesResponse> {
    return metrics.getMetricPercentiles(this.context, args);
  }

  // ============================================================================
  // Discovery
  // ============================================================================

  public async getMetricNames(args: GetMetricNamesArgs): Promise<GetMetricNamesResponse> {
    return discovery.getMetricNames(this.context, args);
  }

  public async getMetricLabelKeys(args: GetMetricLabelKeysArgs): Promise<GetMetricLabelKeysResponse> {
    return discovery.getMetricLabelKeys(this.context, args);
  }

  public async getMetricLabelValues(args: GetMetricLabelValuesArgs): Promise<GetMetricLabelValuesResponse> {
    return discovery.getMetricLabelValues(this.context, args);
  }

  public async getEntityTypes(_args: GetEntityTypesArgs): Promise<GetEntityTypesResponse> {
    return discovery.getEntityTypes(this.context);
  }

  public async getEntityNames(args: GetEntityNamesArgs): Promise<GetEntityNamesResponse> {
    return discovery.getEntityNames(this.context, args);
  }

  public async getServiceNames(_args: GetServiceNamesArgs): Promise<GetServiceNamesResponse> {
    return discovery.getServiceNames(this.context);
  }

  public async getEnvironments(_args: GetEnvironmentsArgs): Promise<GetEnvironmentsResponse> {
    return discovery.getEnvironments(this.context);
  }

  public async getTags(args: GetTagsArgs): Promise<GetTagsResponse> {
    return discovery.getTags(this.context, args);
  }

  // ============================================================================
  // Scores
  // ============================================================================

  public async createScore(args: CreateScoreArgs): Promise<void> {
    await writeSignals(this.context, 'scores', [args.score]);
  }

  public async batchCreateScores(args: BatchCreateScoresArgs): Promise<void> {
    await writeSignals(this.context, 'scores', args.scores);
  }

  public async deleteScores(args: DeleteScoresArgs): Promise<void> {
    await deleteSignals(this.context, 'scores', args.scoreIds, args);
  }

  public async listScores(args: ListScoresArgs): Promise<ListScoresResponse> {
    const { mode, filters, pagination, orderBy, after, limit } = listScoresArgsSchema.parse(args);
    const order = orderBy.direction === 'ASC' ? 'asc' : 'desc';
    const { records, ...rest } = await listSignals(this.context, 'scores', {
      mode,
      clauses: scoreFilterClauses(filters ?? {}),
      sort: [{ [orderBy.field]: { order } }, { scoreId: 'asc' }],
      pagination,
      after,
      limit,
    });
    return { scores: records, ...rest };
  }

  public async getScoreById(scoreId: string): Promise<ScoreRecord | null> {
    return (await getSignal(this.context, 'scores', scoreId))?.record ?? null;
  }

  /** Score filters plus the scorer and optional score source every score OLAP query is keyed by. */
  private scoreOlapClauses(
    filters: Partial<ScoresFilter> | undefined,
    scorerId: string,
    scoreSource: string | undefined,
    timestamp?: DateRangeFilter,
  ): QueryClause[] {
    return [
      ...scoreFilterClauses({ ...filters, ...(timestamp ? { timestamp } : {}) }),
      { term: { scorerId } },
      ...(scoreSource ? [{ term: { scoreSource } }] : []),
    ];
  }

  public async getScoreAggregate(args: GetScoreAggregateArgs): Promise<GetScoreAggregateResponse> {
    await this.context.ensure('scores');
    return olapAggregate(
      this.context,
      this.scoreTarget,
      timestamp => this.scoreOlapClauses(args.filters, args.scorerId, args.scoreSource, timestamp),
      { aggregation: args.aggregation, timestamp: args.filters?.timestamp, comparePeriod: args.comparePeriod },
    );
  }

  public async getScoreBreakdown(args: GetScoreBreakdownArgs): Promise<GetScoreBreakdownResponse> {
    await this.context.ensure('scores');
    return olapBreakdown(
      this.context,
      this.scoreTarget,
      this.scoreOlapClauses(args.filters, args.scorerId, args.scoreSource),
      args,
    );
  }

  public async getScoreTimeSeries(args: GetScoreTimeSeriesArgs): Promise<GetScoreTimeSeriesResponse> {
    await this.context.ensure('scores');
    return olapTimeSeries(
      this.context,
      this.scoreTarget,
      this.scoreOlapClauses(args.filters, args.scorerId, args.scoreSource),
      {
        ...args,
        name: args.scoreSource ? `${args.scorerId}|${args.scoreSource}` : args.scorerId,
      },
    );
  }

  public async getScorePercentiles(args: GetScorePercentilesArgs): Promise<GetScorePercentilesResponse> {
    await this.context.ensure('scores');
    return olapPercentiles(
      this.context,
      this.scoreTarget,
      this.scoreOlapClauses(args.filters, args.scorerId, args.scoreSource),
      args,
    );
  }

  // ============================================================================
  // Feedback
  // ============================================================================

  public async createFeedback(args: CreateFeedbackArgs): Promise<void> {
    await writeSignals(this.context, 'feedback', [args.feedback]);
  }

  public async batchCreateFeedback(args: BatchCreateFeedbackArgs): Promise<void> {
    await writeSignals(this.context, 'feedback', args.feedbacks);
  }

  public async deleteFeedback(args: DeleteFeedbackArgs): Promise<void> {
    await deleteSignals(this.context, 'feedback', args.feedbackIds, args);
  }

  public async listFeedback(args: ListFeedbackArgs): Promise<ListFeedbackResponse> {
    const { mode, filters, pagination, orderBy, after, limit } = listFeedbackArgsSchema.parse(args);
    const { records, ...rest } = await listSignals(this.context, 'feedback', {
      mode,
      clauses: feedbackFilterClauses(filters ?? {}),
      sort: [{ timestamp: { order: orderBy.direction === 'ASC' ? 'asc' : 'desc' } }, { feedbackId: 'asc' }],
      pagination,
      after,
      limit,
    });
    return { feedback: records, ...rest };
  }

  /** Changes the review status of one feedback record, keeping concurrent changes to it. */
  public async updateFeedbackReviewStatus(args: UpdateFeedbackReviewStatusArgs): Promise<FeedbackRecord> {
    const index = this.context.indexes.feedback;
    return this.context.updateWithRetry('UPDATE_FEEDBACK_REVIEW_STATUS', async () => {
      const existing = await getSignal(this.context, 'feedback', args.feedbackId);
      if (!existing) {
        throw new MastraError({
          id: 'OBSERVABILITY_UPDATE_FEEDBACK_REVIEW_STATUS_NOT_FOUND',
          domain: ErrorDomain.MASTRA_OBSERVABILITY,
          category: ErrorCategory.USER,
          text: 'Feedback record not found',
        });
      }
      const document: StoredDocument = {
        ...(existing.document._source as StoredDocument),
        reviewStatus: args.reviewStatus,
      };
      await this.context.client.index({
        index,
        id: args.feedbackId,
        document,
        if_seq_no: existing.document._seq_no,
        if_primary_term: existing.document._primary_term,
        refresh: true,
      });
      return fromFeedbackDocument(document);
    });
  }

  /** Feedback filters plus the feedback type and optional source; only numeric feedback is aggregated. */
  private feedbackOlapClauses(
    filters: Partial<FeedbackFilter> | undefined,
    feedbackType: string,
    feedbackSource: string | undefined,
    timestamp?: DateRangeFilter,
  ): QueryClause[] {
    return [
      ...feedbackFilterClauses({ ...filters, ...(timestamp ? { timestamp } : {}) }),
      { exists: { field: 'numericValue' } },
      { term: { feedbackType } },
      ...(feedbackSource ? [{ term: { feedbackSource } }] : []),
    ];
  }

  public async getFeedbackAggregate(args: GetFeedbackAggregateArgs): Promise<GetFeedbackAggregateResponse> {
    await this.context.ensure('feedback');
    return olapAggregate(
      this.context,
      this.feedbackTarget,
      timestamp => this.feedbackOlapClauses(args.filters, args.feedbackType, args.feedbackSource, timestamp),
      { aggregation: args.aggregation, timestamp: args.filters?.timestamp, comparePeriod: args.comparePeriod },
    );
  }

  public async getFeedbackBreakdown(args: GetFeedbackBreakdownArgs): Promise<GetFeedbackBreakdownResponse> {
    await this.context.ensure('feedback');
    return olapBreakdown(
      this.context,
      this.feedbackTarget,
      this.feedbackOlapClauses(args.filters, args.feedbackType, args.feedbackSource),
      args,
    );
  }

  public async getFeedbackTimeSeries(args: GetFeedbackTimeSeriesArgs): Promise<GetFeedbackTimeSeriesResponse> {
    await this.context.ensure('feedback');
    return olapTimeSeries(
      this.context,
      this.feedbackTarget,
      this.feedbackOlapClauses(args.filters, args.feedbackType, args.feedbackSource),
      { ...args, name: args.feedbackSource ? `${args.feedbackType}|${args.feedbackSource}` : args.feedbackType },
    );
  }

  public async getFeedbackPercentiles(args: GetFeedbackPercentilesArgs): Promise<GetFeedbackPercentilesResponse> {
    await this.context.ensure('feedback');
    return olapPercentiles(
      this.context,
      this.feedbackTarget,
      this.feedbackOlapClauses(args.filters, args.feedbackType, args.feedbackSource),
      args,
    );
  }
}
