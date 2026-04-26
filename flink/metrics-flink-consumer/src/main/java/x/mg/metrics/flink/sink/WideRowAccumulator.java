package x.mg.metrics.flink.sink;

import x.mg.metrics.flink.classify.MetricCategory;
import x.mg.metrics.flink.classify.MetricCategoryClassifier;
import x.mg.metrics.flink.classify.MetricMapping;
import x.mg.metrics.flink.model.*;

import java.io.Serializable;
import java.util.*;

public class WideRowAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, TaskMetricRow> taskRows = new HashMap<>();
    private final Map<String, StageMetricRow> stageRows = new HashMap<>();
    private final Map<String, JobMetricRow> jobRows = new HashMap<>();
    private final Map<String, JvmMemoryMetricRow> memoryRows = new HashMap<>();
    private final Map<String, JvmGcMetricRow> gcRows = new HashMap<>();
    private final Map<String, SqlQueryMetricRow> sqlQueryRows = new HashMap<>();
    private final Map<String, SqlTableIoMetricRow> sqlTableIoRows = new HashMap<>();
    private final Map<String, HiveQueryMetricRow> hiveQueryRows = new HashMap<>();
    private final Map<String, HiveTableIoMetricRow> hiveTableIoRows = new HashMap<>();
    private final Map<String, MrJobMetricRow> mrJobRows = new HashMap<>();
    private final Map<String, MrTaskMetricRow> mrTaskRows = new HashMap<>();

    // Unified wide-table accumulator (unified_metrics)
    private final Map<String, MetricEventRow> metricEventRows = new HashMap<>();

    private final List<HistogramBucket> taskBuckets = new ArrayList<>();
    private final List<HistogramBucket> stageBuckets = new ArrayList<>();
    private final List<HistogramBucket> jobBuckets = new ArrayList<>();

    // Governance: per-stage task metric accumulation (persists across flushes until stage completes)
    private final Map<String, StageTaskAccumulator> stageTaskAccumulators = new HashMap<>();
    private final Set<String> completedStages = new HashSet<>();

    private long totalSamplesAccepted = 0;
    private long totalBucketsAccepted = 0;
    private long totalSamplesSkipped = 0;

    public void accumulate(MetricSample sample) {
        MetricMapping mapping = MetricCategoryClassifier.classify(sample.getMetricName());
        if (mapping == null) {
            totalSamplesSkipped++;
            return;
        }

        // For histograms, only take the sum value; skip count
        if (mapping.isHistogram()) {
            String histType = sample.getLabels().get("_histogram");
            if ("count".equals(histType)) {
                totalSamplesSkipped++;
                return;
            }
        }

        MetricCategory cat = mapping.getCategory();
        String key = MetricCategoryClassifier.extractGroupKey(cat, sample.getLabels(), sample.getTimestampMs());

        switch (cat) {
            case TASK:
                taskRows.computeIfAbsent(key,
                    k -> TaskMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());

                // Also accumulate into stage-level running stats for governance
                String appId = sample.getLabels().get("spark.app.id");
                String stageIdStr = sample.getLabels().get("spark.stage.id");
                if (appId != null && stageIdStr != null) {
                    String stageKey = appId + "|" + stageIdStr;
                    StageTaskAccumulator stageAcc = stageTaskAccumulators.computeIfAbsent(
                        stageKey, k -> new StageTaskAccumulator(appId, Integer.parseInt(stageIdStr)));
                    stageAcc.accumulate(mapping.getColumnName(), sample.getValue());
                    stageAcc.updateTimestamp(sample.getTimestampMs());
                }
                break;
            case STAGE:
                stageRows.computeIfAbsent(key,
                    k -> StageMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());

                // Mark stage as completed for governance computation
                String sAppId = sample.getLabels().get("spark.app.id");
                String sStageIdStr = sample.getLabels().get("spark.stage.id");
                if (sAppId != null && sStageIdStr != null) {
                    completedStages.add(sAppId + "|" + sStageIdStr);
                }
                break;
            case JOB:
                jobRows.computeIfAbsent(key,
                    k -> JobMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case JVM_MEMORY:
                memoryRows.computeIfAbsent(key,
                    k -> JvmMemoryMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case JVM_GC:
                gcRows.computeIfAbsent(key,
                    k -> JvmGcMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case SQL_EXECUTION:
                sqlQueryRows.computeIfAbsent(key,
                    k -> SqlQueryMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case SQL_TABLE_IO:
                sqlTableIoRows.computeIfAbsent(key,
                    k -> SqlTableIoMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case HIVE_QUERY:
                hiveQueryRows.computeIfAbsent(key,
                    k -> HiveQueryMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case HIVE_TABLE_IO:
                hiveTableIoRows.computeIfAbsent(key,
                    k -> HiveTableIoMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case MR_JOB:
                mrJobRows.computeIfAbsent(key,
                    k -> MrJobMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            case MR_TASK:
                mrTaskRows.computeIfAbsent(key,
                    k -> MrTaskMetricRow.fromLabels(sample.getTimestampMs(), sample.getLabels()))
                    .setMetricColumn(mapping.getColumnName(), sample.getValue());
                break;
            default:
                totalSamplesSkipped++;
                return;
        }

        // Also accumulate into the unified wide-table (unified_metrics)
        String eventKey = MetricCategoryClassifier.extractGroupKey(cat, sample.getLabels(), sample.getTimestampMs());
        metricEventRows.computeIfAbsent(eventKey,
            k -> MetricEventRow.fromLabels(sample.getTimestampMs(), cat, sample.getLabels()))
            .setMetricColumn(mapping.getColumnName(), sample.getValue());

        totalSamplesAccepted++;
    }

    public void accumulateBucket(HistogramBucket bucket) {
        MetricMapping mapping = MetricCategoryClassifier.classify(bucket.getMetricName());
        if (mapping == null) {
            return;
        }

        switch (mapping.getCategory()) {
            case TASK:
                taskBuckets.add(bucket);
                break;
            case STAGE:
                stageBuckets.add(bucket);
                break;
            case JOB:
                jobBuckets.add(bucket);
                break;
            default:
                break;
        }
        totalBucketsAccepted++;
    }

    public FlushResult drain() {
        FlushResult result = new FlushResult();
        result.taskRows = new ArrayList<>(taskRows.values());
        result.stageRows = new ArrayList<>(stageRows.values());
        result.jobRows = new ArrayList<>(jobRows.values());
        result.memoryRows = new ArrayList<>(memoryRows.values());
        result.gcRows = new ArrayList<>(gcRows.values());
        result.sqlQueryRows = new ArrayList<>(sqlQueryRows.values());
        result.sqlTableIoRows = new ArrayList<>(sqlTableIoRows.values());
        result.hiveQueryRows = new ArrayList<>(hiveQueryRows.values());
        result.hiveTableIoRows = new ArrayList<>(hiveTableIoRows.values());
        result.mrJobRows = new ArrayList<>(mrJobRows.values());
        result.mrTaskRows = new ArrayList<>(mrTaskRows.values());
        result.taskBuckets = new ArrayList<>(taskBuckets);
        result.stageBuckets = new ArrayList<>(stageBuckets);
        result.jobBuckets = new ArrayList<>(jobBuckets);

        // Normalize and drain unified wide-table rows
        List<MetricEventRow> normalizedEventRows = new ArrayList<>(metricEventRows.values());
        for (MetricEventRow row : normalizedEventRows) {
            row.normalizeAggregatedMetrics();
        }
        result.metricEventRows = normalizedEventRows;

        // Compute governance for completed stages
        List<StageGovernanceRow> governanceRows = new ArrayList<>();
        for (String stageKey : completedStages) {
            StageTaskAccumulator acc = stageTaskAccumulators.remove(stageKey);
            if (acc != null && acc.getTaskCount() > 0) {
                // Find stage duration from stageRows being flushed
                Double stageDuration = findStageDuration(stageKey, result.stageRows);
                governanceRows.add(acc.toGovernanceRow(stageDuration));
            }
        }
        completedStages.clear();
        result.governanceRows = governanceRows;

        // Clear
        taskRows.clear();
        stageRows.clear();
        jobRows.clear();
        memoryRows.clear();
        gcRows.clear();
        sqlQueryRows.clear();
        sqlTableIoRows.clear();
        hiveQueryRows.clear();
        hiveTableIoRows.clear();
        mrJobRows.clear();
        mrTaskRows.clear();
        taskBuckets.clear();
        stageBuckets.clear();
        jobBuckets.clear();
        metricEventRows.clear();

        return result;
    }

    private Double findStageDuration(String stageKey, List<StageMetricRow> stageRows) {
        String[] parts = stageKey.split("\\|");
        if (parts.length != 2) return null;
        String appId = parts[0];
        int stageId;
        try { stageId = Integer.parseInt(parts[1]); } catch (NumberFormatException e) { return null; }

        for (StageMetricRow r : stageRows) {
            if (appId.equals(r.getAppId()) && stageId == r.getStageId()) {
                return r.getDurationMs();
            }
        }
        return null;
    }

    public int pendingCount() {
        return taskRows.size() + stageRows.size() + jobRows.size()
             + memoryRows.size() + gcRows.size()
             + sqlQueryRows.size() + sqlTableIoRows.size()
             + hiveQueryRows.size() + hiveTableIoRows.size()
             + mrJobRows.size() + mrTaskRows.size()
             + taskBuckets.size() + stageBuckets.size() + jobBuckets.size();
    }

    public long getTotalSamplesAccepted() { return totalSamplesAccepted; }
    public long getTotalBucketsAccepted() { return totalBucketsAccepted; }
    public long getTotalSamplesSkipped() { return totalSamplesSkipped; }

    public static class FlushResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<TaskMetricRow> taskRows;
        public List<StageMetricRow> stageRows;
        public List<JobMetricRow> jobRows;
        public List<JvmMemoryMetricRow> memoryRows;
        public List<JvmGcMetricRow> gcRows;
        public List<SqlQueryMetricRow> sqlQueryRows;
        public List<SqlTableIoMetricRow> sqlTableIoRows;
        public List<HiveQueryMetricRow> hiveQueryRows;
        public List<HiveTableIoMetricRow> hiveTableIoRows;
        public List<MrJobMetricRow> mrJobRows;
        public List<MrTaskMetricRow> mrTaskRows;
        public List<HistogramBucket> taskBuckets;
        public List<HistogramBucket> stageBuckets;
        public List<HistogramBucket> jobBuckets;
        public List<StageGovernanceRow> governanceRows;
        public List<MetricEventRow> metricEventRows;

        public int totalCount() {
            int total = taskRows.size() + stageRows.size() + jobRows.size()
                 + memoryRows.size() + gcRows.size()
                 + sqlQueryRows.size() + sqlTableIoRows.size()
                 + hiveQueryRows.size() + hiveTableIoRows.size()
                 + mrJobRows.size() + mrTaskRows.size()
                 + taskBuckets.size() + stageBuckets.size() + jobBuckets.size();
            if (governanceRows != null) total += governanceRows.size();
            if (metricEventRows != null) total += metricEventRows.size();
            return total;
        }
    }
}
