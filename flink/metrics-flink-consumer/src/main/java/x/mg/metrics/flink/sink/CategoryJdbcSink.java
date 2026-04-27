package x.mg.metrics.flink.sink;

import x.mg.metrics.flink.classify.MetricCategory;
import x.mg.metrics.flink.classify.MetricCategoryClassifier;
import x.mg.metrics.flink.classify.MetricMapping;
import x.mg.metrics.flink.config.FlinkConsumerConfig;
import x.mg.metrics.flink.model.*;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CategoryJdbcSink {
    private static final Logger LOG = Logger.getLogger(CategoryJdbcSink.class.getName());
    private static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneId.of("UTC"));

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final int batchSize;
    private final long flushIntervalMs;
    private final boolean isClickHouse;

    private Connection connection;
    private WideRowAccumulator accumulator;

    public CategoryJdbcSink(FlinkConsumerConfig config) {
        this(config.getJdbcUrl(), config.getJdbcUser(), config.getJdbcPassword(),
             config.getBatchSize(), config.getFlushIntervalMs(),
             "clickhouse".equalsIgnoreCase(config.getSinkType()));
    }

    public CategoryJdbcSink(String jdbcUrl, String jdbcUser, String jdbcPassword,
                            int batchSize, long flushIntervalMs, boolean isClickHouse) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.isClickHouse = isClickHouse;
    }

    public void open() throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        if (!isClickHouse) {
            connection.setAutoCommit(false);
        }
        accumulator = new WideRowAccumulator();

        SchemaManager schemaManager = new SchemaManager(isClickHouse);
        schemaManager.createTables(connection);
        LOG.info("CategoryJdbcSink opened (" + (isClickHouse ? "clickhouse" : "mysql") + "): " + jdbcUrl);
    }

    public void invoke(MetricSample sample) {
        accumulator.accumulate(sample);
        if (accumulator.pendingCount() >= batchSize) {
            try { flush(); } catch (Exception e) {
                LOG.log(Level.WARNING, "Flush on batch size failed", e);
            }
        }
    }

    public void invoke(HistogramBucket bucket) {
        accumulator.accumulateBucket(bucket);
        if (accumulator.pendingCount() >= batchSize) {
            try { flush(); } catch (Exception e) {
                LOG.log(Level.WARNING, "Flush on batch size failed", e);
            }
        }
    }

    public synchronized void flush() throws Exception {
        WideRowAccumulator.FlushResult result = accumulator.drain();
        if (result.totalCount() == 0) return;

        int flushed = 0;

        if (!result.taskRows.isEmpty()) {
            flushed += insertTaskMetrics(result.taskRows);
        }
        if (!result.stageRows.isEmpty()) {
            flushed += insertStageMetrics(result.stageRows);
        }
        if (!result.jobRows.isEmpty()) {
            flushed += insertJobMetrics(result.jobRows);
        }
        if (!result.memoryRows.isEmpty()) {
            flushed += insertJvmMemoryMetrics(result.memoryRows);
        }
        if (!result.gcRows.isEmpty()) {
            flushed += insertJvmGcMetrics(result.gcRows);
        }
        if (result.governanceRows != null && !result.governanceRows.isEmpty()) {
            flushed += insertStageGovernance(result.governanceRows);
        }
        if (result.sqlQueryRows != null && !result.sqlQueryRows.isEmpty()) {
            flushed += insertSqlQueryMetrics(result.sqlQueryRows);
        }
        if (result.sqlTableIoRows != null && !result.sqlTableIoRows.isEmpty()) {
            flushed += insertSqlTableIoMetrics(result.sqlTableIoRows);
        }
        if (result.hiveQueryRows != null && !result.hiveQueryRows.isEmpty()) {
            flushed += insertHiveQueryMetrics(result.hiveQueryRows);
        }
        if (result.hiveTableIoRows != null && !result.hiveTableIoRows.isEmpty()) {
            flushed += insertHiveTableIoMetrics(result.hiveTableIoRows);
        }
        if (result.mrJobRows != null && !result.mrJobRows.isEmpty()) {
            flushed += insertMrJobMetrics(result.mrJobRows);
        }
        if (result.mrTaskRows != null && !result.mrTaskRows.isEmpty()) {
            flushed += insertMrTaskMetrics(result.mrTaskRows);
        }
        if (result.metricEventRows != null && !result.metricEventRows.isEmpty()) {
            flushed += insertMetricEvents(result.metricEventRows);
        }
        if (!result.taskBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_task_histogram", result.taskBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, metric_name, bucket_le, bucket_count",
                this::bindTaskBucket);
        }
        if (!result.stageBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_stage_histogram", result.stageBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, metric_name, bucket_le, bucket_count",
                this::bindStageBucket);
        }
        if (!result.jobBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_job_histogram", result.jobBuckets,
                "timestamp_ms, app_id, job_id, job_success, metric_name, bucket_le, bucket_count",
                this::bindJobBucket);
        }

        LOG.info("Flushed " + flushed + " rows (task=" + result.taskRows.size()
            + " stage=" + result.stageRows.size()
            + " job=" + result.jobRows.size()
            + " mem=" + result.memoryRows.size()
            + " gc=" + result.gcRows.size()
            + " sql=" + (result.sqlQueryRows != null ? result.sqlQueryRows.size() : 0)
            + " sql_tbl=" + (result.sqlTableIoRows != null ? result.sqlTableIoRows.size() : 0)
            + " hive_q=" + (result.hiveQueryRows != null ? result.hiveQueryRows.size() : 0)
            + " hive_tbl=" + (result.hiveTableIoRows != null ? result.hiveTableIoRows.size() : 0)
            + " mr_job=" + (result.mrJobRows != null ? result.mrJobRows.size() : 0)
            + " mr_task=" + (result.mrTaskRows != null ? result.mrTaskRows.size() : 0)
            + " unified_metrics=" + (result.metricEventRows != null ? result.metricEventRows.size() : 0)
            + " buckets=" + (result.taskBuckets.size() + result.stageBuckets.size() + result.jobBuckets.size())
            + ") | total accepted: " + accumulator.getTotalSamplesAccepted() + " samples, "
            + accumulator.getTotalBucketsAccepted() + " buckets, "
            + accumulator.getTotalSamplesSkipped() + " skipped");
    }

    public int writeFlushResult(WideRowAccumulator.FlushResult result) throws Exception {
        if (result.totalCount() == 0) return 0;

        int flushed = 0;
        if (!result.taskRows.isEmpty()) flushed += insertTaskMetrics(result.taskRows);
        if (!result.stageRows.isEmpty()) flushed += insertStageMetrics(result.stageRows);
        if (!result.jobRows.isEmpty()) flushed += insertJobMetrics(result.jobRows);
        if (!result.memoryRows.isEmpty()) flushed += insertJvmMemoryMetrics(result.memoryRows);
        if (!result.gcRows.isEmpty()) flushed += insertJvmGcMetrics(result.gcRows);
        if (result.governanceRows != null && !result.governanceRows.isEmpty()) flushed += insertStageGovernance(result.governanceRows);
        if (result.sqlQueryRows != null && !result.sqlQueryRows.isEmpty()) flushed += insertSqlQueryMetrics(result.sqlQueryRows);
        if (result.sqlTableIoRows != null && !result.sqlTableIoRows.isEmpty()) flushed += insertSqlTableIoMetrics(result.sqlTableIoRows);
        if (result.hiveQueryRows != null && !result.hiveQueryRows.isEmpty()) flushed += insertHiveQueryMetrics(result.hiveQueryRows);
        if (result.hiveTableIoRows != null && !result.hiveTableIoRows.isEmpty()) flushed += insertHiveTableIoMetrics(result.hiveTableIoRows);
        if (result.mrJobRows != null && !result.mrJobRows.isEmpty()) flushed += insertMrJobMetrics(result.mrJobRows);
        if (result.mrTaskRows != null && !result.mrTaskRows.isEmpty()) flushed += insertMrTaskMetrics(result.mrTaskRows);
        if (result.metricEventRows != null && !result.metricEventRows.isEmpty()) flushed += insertMetricEvents(result.metricEventRows);
        if (!result.taskBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_task_histogram", result.taskBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, metric_name, bucket_le, bucket_count",
                this::bindTaskBucket);
        }
        if (!result.stageBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_stage_histogram", result.stageBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, metric_name, bucket_le, bucket_count",
                this::bindStageBucket);
        }
        if (!result.jobBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("spark_job_histogram", result.jobBuckets,
                "timestamp_ms, app_id, job_id, job_success, metric_name, bucket_le, bucket_count",
                this::bindJobBucket);
        }

        LOG.info("Flushed " + flushed + " rows via writeFlushResult");
        return flushed;
    }

    public void close() {
        try { flush(); } catch (Exception e) {
            LOG.log(Level.WARNING, "Final flush failed", e);
        }
        try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
        LOG.info("CategoryJdbcSink closed");
    }

    private int insertTaskMetrics(List<TaskMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_task_metrics (timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, " +
            "task_host, task_locality, task_speculative, app_name, user_name, queue, duration_ms, io_bytes_read, io_bytes_written, " +
            "io_records_read, io_records_written, shuffle_bytes_read, shuffle_bytes_written, " +
            "shuffle_fetch_wait_time_ms, disk_bytes_spilled, memory_bytes_spilled, executor_run_time_ms, " +
            "executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, result_serialization_time_ms, " +
            "jvm_gc_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, " +
            "shuffle_local_blocks_fetched, shuffle_records_read, shuffle_remote_bytes_read_to_disk, " +
            "shuffle_remote_reqs_duration_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setInt(i++, r.getStageId());
            ps.setLong(i++, r.getTaskId());
            ps.setString(i++, r.getTaskSuccess());
            ps.setString(i++, r.getTaskHost());
            ps.setString(i++, r.getTaskLocality());
            ps.setString(i++, r.getTaskSpeculative());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getIoBytesRead());
            setDouble(ps, i++, r.getIoBytesWritten());
            setDouble(ps, i++, r.getIoRecordsRead());
            setDouble(ps, i++, r.getIoRecordsWritten());
            setDouble(ps, i++, r.getShuffleBytesRead());
            setDouble(ps, i++, r.getShuffleBytesWritten());
            setDouble(ps, i++, r.getShuffleFetchWaitTimeMs());
            setDouble(ps, i++, r.getDiskBytesSpilled());
            setDouble(ps, i++, r.getMemoryBytesSpilled());
            setDouble(ps, i++, r.getExecutorRunTimeMs());
            setDouble(ps, i++, r.getExecutorCpuTimeNs());
            setDouble(ps, i++, r.getDeserializeTimeMs());
            setDouble(ps, i++, r.getDeserializeCpuTimeNs());
            setDouble(ps, i++, r.getResultSerializationTimeMs());
            setDouble(ps, i++, r.getJvmGcTimeMs());
            setDouble(ps, i++, r.getSchedulerDelayMs());
            setDouble(ps, i++, r.getResultSizeBytes());
            setDouble(ps, i++, r.getPeakExecutionMemoryBytes());
            setDouble(ps, i++, r.getShuffleLocalBlocksFetched());
            setDouble(ps, i++, r.getShuffleRecordsRead());
            setDouble(ps, i++, r.getShuffleRemoteBytesReadToDisk());
            setDouble(ps, i++, r.getShuffleRemoteReqsDurationMs());
        });
    }

    private int insertStageMetrics(List<StageMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_stage_metrics (timestamp_ms, app_id, executor_id, stage_id, " +
            "app_name, user_name, queue, duration_ms, num_tasks, executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms, " +
            "peak_execution_memory_bytes, io_bytes_read, io_bytes_written) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setInt(i++, r.getStageId());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getNumTasks());
            setDouble(ps, i++, r.getExecutorRunTimeMs());
            setDouble(ps, i++, r.getExecutorCpuTimeNs());
            setDouble(ps, i++, r.getJvmGcTimeMs());
            setDouble(ps, i++, r.getPeakExecutionMemoryBytes());
            setDouble(ps, i++, r.getIoBytesRead());
            setDouble(ps, i++, r.getIoBytesWritten());
        });
    }

    private int insertJobMetrics(List<JobMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_job_metrics (timestamp_ms, app_id, job_id, job_success, " +
            "app_name, user_name, queue, duration_ms, num_stages) VALUES (?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setInt(i++, r.getJobId());
            ps.setString(i++, r.getJobSuccess());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getNumStages());
        });
    }

    private int insertJvmMemoryMetrics(List<JvmMemoryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_jvm_memory (timestamp_ms, app_id, executor_id, " +
            "app_name, user_name, queue, heap_used, non_heap_used) VALUES (?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getHeapUsed());
            setDouble(ps, i++, r.getNonHeapUsed());
        });
    }

    private int insertJvmGcMetrics(List<JvmGcMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_jvm_gc (timestamp_ms, app_id, executor_id, gc_name, " +
            "app_name, user_name, queue, gc_count, gc_time_ms) VALUES (?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setString(i++, r.getGcName());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getGcCount());
            setDouble(ps, i++, r.getGcTimeMs());
        });
    }

    private int insertSqlQueryMetrics(List<SqlQueryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_sql_metrics (timestamp_ms, app_id, execution_id, " +
            "app_name, user_name, queue, duration_ms, shuffle_bytes_read, shuffle_bytes_written, join_count, query_text) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutionId());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getShuffleBytesRead());
            setDouble(ps, i++, r.getShuffleBytesWritten());
            setDouble(ps, i++, r.getJoinCount());
            ps.setString(i++, r.getQueryText());
        });
    }

    private int insertSqlTableIoMetrics(List<SqlTableIoMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_sql_table (timestamp_ms, app_id, execution_id, " +
            "table_name, operation, app_name, user_name, queue, bytes, `rows`, files_read, time_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutionId());
            ps.setString(i++, r.getTableName());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getBytes());
            setDouble(ps, i++, r.getRows());
            setDouble(ps, i++, r.getFilesRead());
            setDouble(ps, i++, r.getTimeMs());
        });
    }

    private interface BucketBinder {
        void bind(PreparedStatement ps, HistogramBucket b) throws SQLException;
    }

    private int insertHistogramBuckets(String table, List<HistogramBucket> buckets,
                                        String columns, BucketBinder binder) throws SQLException {
        int colCount = columns.split(",").length;
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            if (i > 0) placeholders.append(",");
            placeholders.append("?");
        }
        String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, buckets, (ps, b) -> binder.bind(ps, b));
    }

    private void bindTaskBucket(PreparedStatement ps, HistogramBucket b) throws SQLException {
        int i = 1;
        setTimestamp(ps, i++, b.getTimestampMs());
        ps.setString(i++, b.getLabels().getOrDefault("spark.app.id", "unknown"));
        ps.setString(i++, b.getLabels().getOrDefault("spark.executor.id", "unknown"));
        ps.setInt(i++, parseIntSafe(b.getLabels().get("spark.stage.id")));
        ps.setLong(i++, parseLongSafe(b.getLabels().get("spark.task.id")));
        ps.setString(i++, b.getLabels().get("spark.task.success"));
        ps.setString(i++, b.getMetricName());
        double le = b.getBucketLe();
        ps.setDouble(i++, Double.isInfinite(le) ? Double.MAX_VALUE : le);
        ps.setLong(i, b.getBucketCount());
    }

    private void bindStageBucket(PreparedStatement ps, HistogramBucket b) throws SQLException {
        int i = 1;
        setTimestamp(ps, i++, b.getTimestampMs());
        ps.setString(i++, b.getLabels().getOrDefault("spark.app.id", "unknown"));
        ps.setString(i++, b.getLabels().getOrDefault("spark.executor.id", "unknown"));
        ps.setInt(i++, parseIntSafe(b.getLabels().get("spark.stage.id")));
        ps.setString(i++, b.getMetricName());
        double le = b.getBucketLe();
        ps.setDouble(i++, Double.isInfinite(le) ? Double.MAX_VALUE : le);
        ps.setLong(i, b.getBucketCount());
    }

    private void bindJobBucket(PreparedStatement ps, HistogramBucket b) throws SQLException {
        int i = 1;
        setTimestamp(ps, i++, b.getTimestampMs());
        ps.setString(i++, b.getLabels().getOrDefault("spark.app.id", "unknown"));
        ps.setInt(i++, parseIntSafe(b.getLabels().get("spark.job.id")));
        ps.setString(i++, b.getLabels().get("spark.job.success"));
        ps.setString(i++, b.getMetricName());
        double le = b.getBucketLe();
        ps.setDouble(i++, Double.isInfinite(le) ? Double.MAX_VALUE : le);
        ps.setLong(i, b.getBucketCount());
    }

    private int insertHiveQueryMetrics(List<HiveQueryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO hive_query_metrics (timestamp_ms, query_id, operation, user_name, " +
            "app_name, queue, success, duration_ms, success_count, failure_count, input_bytes, output_bytes, " +
            "input_rows, output_rows, execution_engine, query_text) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getQueryId());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getQueue());
            ps.setString(i++, r.getSuccess());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getSuccessCount());
            setDouble(ps, i++, r.getFailureCount());
            setDouble(ps, i++, r.getInputBytes());
            setDouble(ps, i++, r.getOutputBytes());
            setDouble(ps, i++, r.getInputRows());
            setDouble(ps, i++, r.getOutputRows());
            ps.setString(i++, r.getExecutionEngine());
            ps.setString(i++, r.getQueryText());
        });
    }

    private int insertHiveTableIoMetrics(List<HiveTableIoMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO hive_query_table (timestamp_ms, query_id, table_name, " +
            "table_type, operation, user_name, app_name, input_table_count, output_table_count, execution_engine, " +
            "bytes, `rows`, files_read, time_ms, queue) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getQueryId());
            ps.setString(i++, r.getTableName());
            ps.setString(i++, r.getTableType());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getAppName());
            setDouble(ps, i++, r.getInputTableCount());
            setDouble(ps, i++, r.getOutputTableCount());
            ps.setString(i++, r.getExecutionEngine());
            setDouble(ps, i++, r.getBytes());
            setDouble(ps, i++, r.getRows());
            setDouble(ps, i++, r.getFilesRead());
            setDouble(ps, i++, r.getTimeMs());
            ps.setString(i++, r.getQueue());
        });
    }

    private int insertMrJobMetrics(List<MrJobMetricRow> rows) throws SQLException {
        // Use UPSERT to avoid duplicate rows for the same job
        String sql = "INSERT INTO mr_job_metrics (timestamp_ms, job_id, job_name, user_name, app_name, state, queue, " +
            "hdfs_bytes_read, hdfs_bytes_written, file_bytes_read, file_bytes_written, " +
            "map_input_records, map_output_records, map_output_bytes, " +
            "reduce_input_records, reduce_output_records, reduce_shuffle_bytes, spilled_records, " +
            "cpu_time_ms, gc_time_ms, physical_memory_bytes, virtual_memory_bytes, committed_heap_bytes, " +
            "maps_duration_ms, reduces_duration_ms, elapsed_time_ms, launched_maps, launched_reduces, " +
            "start_time_ms, finish_time_ms) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "ON DUPLICATE KEY UPDATE " +
            "timestamp_ms=VALUES(timestamp_ms), job_name=VALUES(job_name), user_name=VALUES(user_name), " +
            "app_name=VALUES(app_name), state=VALUES(state), queue=VALUES(queue), " +
            "hdfs_bytes_read=IFNULL(VALUES(hdfs_bytes_read), hdfs_bytes_read), " +
            "hdfs_bytes_written=IFNULL(VALUES(hdfs_bytes_written), hdfs_bytes_written), " +
            "file_bytes_read=IFNULL(VALUES(file_bytes_read), file_bytes_read), " +
            "file_bytes_written=IFNULL(VALUES(file_bytes_written), file_bytes_written), " +
            "map_input_records=IFNULL(VALUES(map_input_records), map_input_records), " +
            "map_output_records=IFNULL(VALUES(map_output_records), map_output_records), " +
            "map_output_bytes=IFNULL(VALUES(map_output_bytes), map_output_bytes), " +
            "reduce_input_records=IFNULL(VALUES(reduce_input_records), reduce_input_records), " +
            "reduce_output_records=IFNULL(VALUES(reduce_output_records), reduce_output_records), " +
            "reduce_shuffle_bytes=IFNULL(VALUES(reduce_shuffle_bytes), reduce_shuffle_bytes), " +
            "spilled_records=IFNULL(VALUES(spilled_records), spilled_records), " +
            "cpu_time_ms=IFNULL(VALUES(cpu_time_ms), cpu_time_ms), " +
            "gc_time_ms=IFNULL(VALUES(gc_time_ms), gc_time_ms), " +
            "physical_memory_bytes=IFNULL(VALUES(physical_memory_bytes), physical_memory_bytes), " +
            "virtual_memory_bytes=IFNULL(VALUES(virtual_memory_bytes), virtual_memory_bytes), " +
            "committed_heap_bytes=IFNULL(VALUES(committed_heap_bytes), committed_heap_bytes), " +
            "maps_duration_ms=IFNULL(VALUES(maps_duration_ms), maps_duration_ms), " +
            "reduces_duration_ms=IFNULL(VALUES(reduces_duration_ms), reduces_duration_ms), " +
            "elapsed_time_ms=IFNULL(VALUES(elapsed_time_ms), elapsed_time_ms), " +
            "launched_maps=IFNULL(VALUES(launched_maps), launched_maps), " +
            "launched_reduces=IFNULL(VALUES(launched_reduces), launched_reduces), " +
            "start_time_ms=IFNULL(VALUES(start_time_ms), start_time_ms), " +
            "finish_time_ms=IFNULL(VALUES(finish_time_ms), finish_time_ms)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (MrJobMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getJobId());
            ps.setString(i++, r.getJobName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getState());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getHdfsBytesRead());
            setDouble(ps, i++, r.getHdfsBytesWritten());
            setDouble(ps, i++, r.getFileBytesRead());
            setDouble(ps, i++, r.getFileBytesWritten());
            setDouble(ps, i++, r.getMapInputRecords());
            setDouble(ps, i++, r.getMapOutputRecords());
            setDouble(ps, i++, r.getMapOutputBytes());
            setDouble(ps, i++, r.getReduceInputRecords());
            setDouble(ps, i++, r.getReduceOutputRecords());
            setDouble(ps, i++, r.getReduceShuffleBytes());
            setDouble(ps, i++, r.getSpilledRecords());
            setDouble(ps, i++, r.getCpuTimeMs());
            setDouble(ps, i++, r.getGcTimeMs());
            setDouble(ps, i++, r.getPhysicalMemoryBytes());
            setDouble(ps, i++, r.getVirtualMemoryBytes());
            setDouble(ps, i++, r.getCommittedHeapBytes());
            setDouble(ps, i++, r.getMapsDurationMs());
            setDouble(ps, i++, r.getReducesDurationMs());
            setDouble(ps, i++, r.getElapsedTimeMs());
            setDouble(ps, i++, r.getLaunchedMaps());
            setDouble(ps, i++, r.getLaunchedReduces());
            if (r.getStartTimeMs() > 0) ps.setLong(i++, r.getStartTimeMs()); else ps.setNull(i++, java.sql.Types.BIGINT);
            if (r.getFinishTimeMs() > 0) ps.setLong(i++, r.getFinishTimeMs()); else ps.setNull(i++, java.sql.Types.BIGINT);
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertMrTaskMetrics(List<MrTaskMetricRow> rows) throws SQLException {
        // Use UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) to merge metrics for the same task
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO mr_task_metrics (timestamp_ms, task_id, task_type, job_id, job_name, user_name, state, queue, " +
            "hdfs_bytes_read, hdfs_bytes_written, file_bytes_read, file_bytes_written, " +
            "map_input_records, map_output_records, map_output_bytes, " +
            "reduce_input_records, reduce_output_records, reduce_shuffle_bytes, spilled_records, " +
            "cpu_time_ms, gc_time_ms, " +
            "duration_ms, success_count, failure_count, " +
            "hdfs_read_ops, hdfs_write_ops, hdfs_large_read_ops) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "ON DUPLICATE KEY UPDATE ");

        // Build the UPDATE clause - only update non-null values
        String[] columns = {
            "timestamp_ms=VALUES(timestamp_ms)",
            "task_type=VALUES(task_type)", "job_id=VALUES(job_id)", "job_name=VALUES(job_name)",
            "user_name=VALUES(user_name)", "state=VALUES(state)", "queue=VALUES(queue)",
            "hdfs_bytes_read=IFNULL(VALUES(hdfs_bytes_read), hdfs_bytes_read)",
            "hdfs_bytes_written=IFNULL(VALUES(hdfs_bytes_written), hdfs_bytes_written)",
            "file_bytes_read=IFNULL(VALUES(file_bytes_read), file_bytes_read)",
            "file_bytes_written=IFNULL(VALUES(file_bytes_written), file_bytes_written)",
            "map_input_records=IFNULL(VALUES(map_input_records), map_input_records)",
            "map_output_records=IFNULL(VALUES(map_output_records), map_output_records)",
            "map_output_bytes=IFNULL(VALUES(map_output_bytes), map_output_bytes)",
            "reduce_input_records=IFNULL(VALUES(reduce_input_records), reduce_input_records)",
            "reduce_output_records=IFNULL(VALUES(reduce_output_records), reduce_output_records)",
            "reduce_shuffle_bytes=IFNULL(VALUES(reduce_shuffle_bytes), reduce_shuffle_bytes)",
            "spilled_records=IFNULL(VALUES(spilled_records), spilled_records)",
            "cpu_time_ms=IFNULL(VALUES(cpu_time_ms), cpu_time_ms)",
            "gc_time_ms=IFNULL(VALUES(gc_time_ms), gc_time_ms)",
            "duration_ms=IFNULL(VALUES(duration_ms), duration_ms)",
            "success_count=IFNULL(VALUES(success_count), success_count)",
            "failure_count=IFNULL(VALUES(failure_count), failure_count)",
            "hdfs_read_ops=IFNULL(VALUES(hdfs_read_ops), hdfs_read_ops)",
            "hdfs_write_ops=IFNULL(VALUES(hdfs_write_ops), hdfs_write_ops)",
            "hdfs_large_read_ops=IFNULL(VALUES(hdfs_large_read_ops), hdfs_large_read_ops)"
        };
        sqlBuilder.append(String.join(", ", columns));

        String sql = sqlBuilder.toString();
        PreparedStatement ps = connection.prepareStatement(sql);
        for (MrTaskMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getTaskId());
            ps.setString(i++, r.getTaskType());
            ps.setString(i++, r.getJobId());
            ps.setString(i++, r.getJobName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getState());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getHdfsBytesRead());
            setDouble(ps, i++, r.getHdfsBytesWritten());
            setDouble(ps, i++, r.getFileBytesRead());
            setDouble(ps, i++, r.getFileBytesWritten());
            setDouble(ps, i++, r.getMapInputRecords());
            setDouble(ps, i++, r.getMapOutputRecords());
            setDouble(ps, i++, r.getMapOutputBytes());
            setDouble(ps, i++, r.getReduceInputRecords());
            setDouble(ps, i++, r.getReduceOutputRecords());
            setDouble(ps, i++, r.getReduceShuffleBytes());
            setDouble(ps, i++, r.getSpilledRecords());
            setDouble(ps, i++, r.getCpuTimeMs());
            setDouble(ps, i++, r.getGcTimeMs());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getSuccessCount());
            setDouble(ps, i++, r.getFailureCount());
            setDouble(ps, i++, r.getHdfsReadOps());
            setDouble(ps, i++, r.getHdfsWriteOps());
            setDouble(ps, i++, r.getHdfsLargeReadOps());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    // ==================== Metric Events (wide table) ====================

    private int insertMetricEvents(List<MetricEventRow> rows) throws SQLException {
        String columns = "timestamp_ms, event_type, engine, status, app_id, app_name, user_name, queue, "
            + "duration_ms, bytes_read, bytes_written, shuffle_bytes_read, shuffle_bytes_written, "
            + "cpu_time_ms, gc_time_ms, bytes_spilled, "
            + "executor_id, stage_id, task_id, task_host, task_locality, task_speculative, "
            + "executor_run_time_ms, executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, "
            + "result_serialization_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, "
            + "shuffle_local_blocks_fetched, shuffle_records_read, "
            + "shuffle_remote_bytes_read_to_disk, shuffle_remote_reqs_duration_ms, "
            + "disk_bytes_spilled, shuffle_fetch_wait_time_ms, num_tasks, num_stages, "
            + "execution_id, join_count, "
            + "table_name, table_operation, bytes, `rows`, files_read, time_ms, "
            + "heap_used, non_heap_used, gc_name, gc_count, "
            + "job_id, job_name, task_type, map_output_bytes, "
            + "physical_memory_bytes, virtual_memory_bytes, committed_heap_bytes, "
            + "maps_duration_ms, reduces_duration_ms, launched_maps, launched_reduces, "
            + "start_time_ms, finish_time_ms, "
            + "hdfs_bytes_read, hdfs_bytes_written, file_bytes_read, file_bytes_written, "
            + "map_input_records, map_output_records, reduce_input_records, reduce_output_records, "
            + "reduce_shuffle_bytes, spilled_records, "
            + "hdfs_read_ops, hdfs_write_ops, hdfs_large_read_ops, "
            + "operation, table_type, execution_engine, success_count, failure_count, "
            + "input_rows, output_rows, records_read, records_written, query_text";

        int colCount = columns.split(",").length;
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            if (i > 0) placeholders.append(",");
            placeholders.append("?");
        }

        String sql = "INSERT INTO unified_metrics (" + columns + ") VALUES (" + placeholders + ")";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getEventType());
            ps.setString(i++, r.getEngine());
            ps.setString(i++, r.getStatus());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getBytesRead());
            setDouble(ps, i++, r.getBytesWritten());
            setDouble(ps, i++, r.getShuffleBytesRead());
            setDouble(ps, i++, r.getShuffleBytesWritten());
            setDouble(ps, i++, r.getCpuTimeMs());
            setDouble(ps, i++, r.getGcTimeMs());
            setDouble(ps, i++, r.getBytesSpilled());
            ps.setString(i++, r.getExecutorId());
            setIntNullable(ps, i++, r.getStageId());
            ps.setString(i++, r.getTaskId());
            ps.setString(i++, r.getTaskHost());
            ps.setString(i++, r.getTaskLocality());
            ps.setString(i++, r.getTaskSpeculative());
            setDouble(ps, i++, r.getExecutorRunTimeMs());
            setDouble(ps, i++, r.getExecutorCpuTimeNs());
            setDouble(ps, i++, r.getDeserializeTimeMs());
            setDouble(ps, i++, r.getDeserializeCpuTimeNs());
            setDouble(ps, i++, r.getResultSerializationTimeMs());
            setDouble(ps, i++, r.getSchedulerDelayMs());
            setDouble(ps, i++, r.getResultSizeBytes());
            setDouble(ps, i++, r.getPeakExecutionMemoryBytes());
            setDouble(ps, i++, r.getShuffleLocalBlocksFetched());
            setDouble(ps, i++, r.getShuffleRecordsRead());
            setDouble(ps, i++, r.getShuffleRemoteBytesReadToDisk());
            setDouble(ps, i++, r.getShuffleRemoteReqsDurationMs());
            setDouble(ps, i++, r.getDiskBytesSpilled());
            setDouble(ps, i++, r.getShuffleFetchWaitTimeMs());
            setDouble(ps, i++, r.getNumTasks());
            setDouble(ps, i++, r.getNumStages());
            ps.setString(i++, r.getExecutionId());
            setDouble(ps, i++, r.getJoinCount());
            ps.setString(i++, r.getTableName());
            ps.setString(i++, r.getTableOperation());
            setDouble(ps, i++, r.getBytes());
            setDouble(ps, i++, r.getRows());
            setDouble(ps, i++, r.getFilesRead());
            setDouble(ps, i++, r.getTimeMs());
            setDouble(ps, i++, r.getHeapUsed());
            setDouble(ps, i++, r.getNonHeapUsed());
            ps.setString(i++, r.getGcName());
            setDouble(ps, i++, r.getGcCount());
            ps.setString(i++, r.getJobId());
            ps.setString(i++, r.getJobName());
            ps.setString(i++, r.getTaskType());
            setDouble(ps, i++, r.getMapOutputBytes());
            setDouble(ps, i++, r.getPhysicalMemoryBytes());
            setDouble(ps, i++, r.getVirtualMemoryBytes());
            setDouble(ps, i++, r.getCommittedHeapBytes());
            setDouble(ps, i++, r.getMapsDurationMs());
            setDouble(ps, i++, r.getReducesDurationMs());
            setDouble(ps, i++, r.getLaunchedMaps());
            setDouble(ps, i++, r.getLaunchedReduces());
            setLongNullable(ps, i++, r.getStartTimeMs());
            setLongNullable(ps, i++, r.getFinishTimeMs());
            setDouble(ps, i++, r.getHdfsBytesRead());
            setDouble(ps, i++, r.getHdfsBytesWritten());
            setDouble(ps, i++, r.getFileBytesRead());
            setDouble(ps, i++, r.getFileBytesWritten());
            setDouble(ps, i++, r.getMapInputRecords());
            setDouble(ps, i++, r.getMapOutputRecords());
            setDouble(ps, i++, r.getReduceInputRecords());
            setDouble(ps, i++, r.getReduceOutputRecords());
            setDouble(ps, i++, r.getReduceShuffleBytes());
            setDouble(ps, i++, r.getSpilledRecords());
            setDouble(ps, i++, r.getHdfsReadOps());
            setDouble(ps, i++, r.getHdfsWriteOps());
            setDouble(ps, i++, r.getHdfsLargeReadOps());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getTableType());
            ps.setString(i++, r.getExecutionEngine());
            setDouble(ps, i++, r.getSuccessCount());
            setDouble(ps, i++, r.getFailureCount());
            setDouble(ps, i++, r.getInputRows());
            setDouble(ps, i++, r.getOutputRows());
            setDouble(ps, i++, r.getRecordsRead());
            setDouble(ps, i++, r.getRecordsWritten());
            ps.setString(i++, r.getQueryText());
        });
    }

    // ==================== Helpers ====================

    private int insertStageGovernance(List<StageGovernanceRow> rows) throws SQLException {
        String sql = "INSERT INTO spark_stage_skew (timestamp_ms, app_id, stage_id, task_count, " +
            "stage_duration_ms, avg_task_duration_ms, max_task_duration_ms, min_task_duration_ms, " +
            "duration_skew_ratio, total_bytes_read, total_bytes_written, total_shuffle_bytes_read, " +
            "total_shuffle_bytes_written, total_records_read, total_records_written, " +
            "io_read_skew_ratio, io_write_skew_ratio, shuffle_read_skew_ratio, " +
            "avg_output_bytes_per_task, avg_output_records_per_task, small_output_task_count, " +
            "cpu_efficiency, gc_overhead_ratio, shuffle_wait_ratio, spill_ratio, " +
            "deserialize_overhead, scheduler_delay_ratio, max_peak_memory_bytes, total_memory_spilled) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        return new BatchInserter(connection, isClickHouse).executeBatch(sql, rows, (ps, r) -> {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setInt(i++, r.getStageId());
            ps.setInt(i++, r.getTaskCount());
            setDouble(ps, i++, r.getStageDurationMs());
            setDouble(ps, i++, r.getAvgTaskDurationMs());
            setDouble(ps, i++, r.getMaxTaskDurationMs());
            setDouble(ps, i++, r.getMinTaskDurationMs());
            setDouble(ps, i++, r.getDurationSkewRatio());
            setDouble(ps, i++, r.getTotalBytesRead());
            setDouble(ps, i++, r.getTotalBytesWritten());
            setDouble(ps, i++, r.getTotalShuffleBytesRead());
            setDouble(ps, i++, r.getTotalShuffleBytesWritten());
            setDouble(ps, i++, r.getTotalRecordsRead());
            setDouble(ps, i++, r.getTotalRecordsWritten());
            setDouble(ps, i++, r.getIoReadSkewRatio());
            setDouble(ps, i++, r.getIoWriteSkewRatio());
            setDouble(ps, i++, r.getShuffleReadSkewRatio());
            setDouble(ps, i++, r.getAvgOutputBytesPerTask());
            setDouble(ps, i++, r.getAvgOutputRecordsPerTask());
            ps.setInt(i++, r.getSmallOutputTaskCount());
            setDouble(ps, i++, r.getCpuEfficiency());
            setDouble(ps, i++, r.getGcOverheadRatio());
            setDouble(ps, i++, r.getShuffleWaitRatio());
            setDouble(ps, i++, r.getSpillRatio());
            setDouble(ps, i++, r.getDeserializeOverhead());
            setDouble(ps, i++, r.getSchedulerDelayRatio());
            setDouble(ps, i++, r.getMaxPeakMemoryBytes());
            setDouble(ps, i++, r.getTotalMemorySpilled());
        });
    }

    private void setTimestamp(PreparedStatement ps, int idx, long epochMs) throws SQLException {
        if (isClickHouse) {
            ps.setString(idx, TS_FORMATTER.format(Instant.ofEpochMilli(epochMs)));
        } else {
            ps.setLong(idx, epochMs);
        }
    }

    private void setDouble(PreparedStatement ps, int idx, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.DOUBLE);
        } else {
            ps.setDouble(idx, value);
        }
    }

    private static int parseIntSafe(String s) {
        try { return s != null ? Integer.parseInt(s) : 0; }
        catch (NumberFormatException e) { return 0; }
    }

    private static long parseLongSafe(String s) {
        try { return s != null ? Long.parseLong(s) : 0L; }
        catch (NumberFormatException e) { return 0L; }
    }

    private static void setIntNullable(PreparedStatement ps, int idx, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.INTEGER);
        } else {
            ps.setInt(idx, value);
        }
    }

    private static void setLongNullable(PreparedStatement ps, int idx, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.BIGINT);
        } else {
            ps.setLong(idx, value);
        }
    }
}
