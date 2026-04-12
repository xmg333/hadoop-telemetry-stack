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
        this.jdbcUrl = config.getJdbcUrl();
        this.jdbcUser = config.getJdbcUser();
        this.jdbcPassword = config.getJdbcPassword();
        this.batchSize = config.getBatchSize();
        this.flushIntervalMs = config.getFlushIntervalMs();
        this.isClickHouse = "clickhouse".equalsIgnoreCase(config.getSinkType());
    }

    public void open() throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        if (!isClickHouse) {
            connection.setAutoCommit(false);
        }
        accumulator = new WideRowAccumulator();

        createTables();
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
        if (!result.taskBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("task_histogram_buckets", result.taskBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, metric_name, bucket_le, bucket_count",
                this::bindTaskBucket);
        }
        if (!result.stageBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("stage_histogram_buckets", result.stageBuckets,
                "timestamp_ms, app_id, executor_id, stage_id, metric_name, bucket_le, bucket_count",
                this::bindStageBucket);
        }
        if (!result.jobBuckets.isEmpty()) {
            flushed += insertHistogramBuckets("job_histogram_buckets", result.jobBuckets,
                "timestamp_ms, app_id, job_id, job_success, metric_name, bucket_le, bucket_count",
                this::bindJobBucket);
        }

        LOG.info("Flushed " + flushed + " rows (task=" + result.taskRows.size()
            + " stage=" + result.stageRows.size()
            + " job=" + result.jobRows.size()
            + " mem=" + result.memoryRows.size()
            + " gc=" + result.gcRows.size()
            + " buckets=" + (result.taskBuckets.size() + result.stageBuckets.size() + result.jobBuckets.size())
            + ") | total accepted: " + accumulator.getTotalSamplesAccepted() + " samples, "
            + accumulator.getTotalBucketsAccepted() + " buckets, "
            + accumulator.getTotalSamplesSkipped() + " skipped");
    }

    public void close() {
        try { flush(); } catch (Exception e) {
            LOG.log(Level.WARNING, "Final flush failed", e);
        }
        try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
        LOG.info("CategoryJdbcSink closed");
    }

    // ==================== Table creation ====================

    private void createTables() throws Exception {
        Statement stmt = connection.createStatement();
        if (isClickHouse) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS metrics_db");
            stmt.execute("USE metrics_db");
            createClickHouseTables(stmt);
        } else {
            createMySqlTables(stmt);
            connection.commit();
        }
        stmt.close();
    }

    private void createMySqlTables(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS task_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "task_id BIGINT NOT NULL, " +
            "task_success VARCHAR(16), " +
            "task_host VARCHAR(255), " +
            "task_locality VARCHAR(64), " +
            "task_speculative VARCHAR(16), " +
            "duration_ms DOUBLE, " +
            "io_bytes_read DOUBLE, " +
            "io_bytes_written DOUBLE, " +
            "io_records_read DOUBLE, " +
            "io_records_written DOUBLE, " +
            "shuffle_bytes_read DOUBLE, " +
            "shuffle_bytes_written DOUBLE, " +
            "shuffle_fetch_wait_time_ms DOUBLE, " +
            "disk_bytes_spilled DOUBLE, " +
            "memory_bytes_spilled DOUBLE, " +
            "executor_run_time_ms DOUBLE, " +
            "executor_cpu_time_ns DOUBLE, " +
            "deserialize_time_ms DOUBLE, " +
            "deserialize_cpu_time_ns DOUBLE, " +
            "result_serialization_time_ms DOUBLE, " +
            "jvm_gc_time_ms DOUBLE, " +
            "scheduler_delay_ms DOUBLE, " +
            "result_size_bytes DOUBLE, " +
            "peak_execution_memory_bytes DOUBLE, " +
            "shuffle_local_blocks_fetched DOUBLE, " +
            "shuffle_records_read DOUBLE, " +
            "shuffle_remote_bytes_read_to_disk DOUBLE, " +
            "shuffle_remote_reqs_duration_ms DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms), " +
            "INDEX idx_stage (app_id, stage_id))");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "duration_ms DOUBLE, " +
            "num_tasks DOUBLE, " +
            "executor_run_time_ms DOUBLE, " +
            "executor_cpu_time_ns DOUBLE, " +
            "jvm_gc_time_ms DOUBLE, " +
            "peak_execution_memory_bytes DOUBLE, " +
            "io_bytes_read DOUBLE, " +
            "io_bytes_written DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS job_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "job_id INT NOT NULL, " +
            "job_success VARCHAR(16), " +
            "duration_ms DOUBLE, " +
            "num_stages DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_memory_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "heap_used DOUBLE, " +
            "non_heap_used DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_gc_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "gc_name VARCHAR(128), " +
            "gc_count DOUBLE, " +
            "gc_time_ms DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS task_histogram_buckets (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "task_id BIGINT NOT NULL, " +
            "task_success VARCHAR(16), " +
            "metric_name VARCHAR(255) NOT NULL, " +
            "bucket_le DOUBLE NOT NULL, " +
            "bucket_count BIGINT NOT NULL, " +
            "INDEX idx_metric_time (metric_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_histogram_buckets (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "metric_name VARCHAR(255) NOT NULL, " +
            "bucket_le DOUBLE NOT NULL, " +
            "bucket_count BIGINT NOT NULL, " +
            "INDEX idx_metric_time (metric_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS job_histogram_buckets (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "job_id INT NOT NULL, " +
            "job_success VARCHAR(16), " +
            "metric_name VARCHAR(255) NOT NULL, " +
            "bucket_le DOUBLE NOT NULL, " +
            "bucket_count BIGINT NOT NULL, " +
            "INDEX idx_metric_time (metric_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_governance (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "task_count INT NOT NULL, " +
            "stage_duration_ms DOUBLE, " +
            "avg_task_duration_ms DOUBLE, " +
            "max_task_duration_ms DOUBLE, " +
            "min_task_duration_ms DOUBLE, " +
            "duration_skew_ratio DOUBLE, " +
            "total_bytes_read DOUBLE, " +
            "total_bytes_written DOUBLE, " +
            "total_shuffle_bytes_read DOUBLE, " +
            "total_shuffle_bytes_written DOUBLE, " +
            "total_records_read DOUBLE, " +
            "total_records_written DOUBLE, " +
            "io_read_skew_ratio DOUBLE, " +
            "io_write_skew_ratio DOUBLE, " +
            "shuffle_read_skew_ratio DOUBLE, " +
            "avg_output_bytes_per_task DOUBLE, " +
            "avg_output_records_per_task DOUBLE, " +
            "small_output_task_count INT, " +
            "cpu_efficiency DOUBLE, " +
            "gc_overhead_ratio DOUBLE, " +
            "shuffle_wait_ratio DOUBLE, " +
            "spill_ratio DOUBLE, " +
            "deserialize_overhead DOUBLE, " +
            "scheduler_delay_ratio DOUBLE, " +
            "max_peak_memory_bytes DOUBLE, " +
            "total_memory_spilled DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms), " +
            "INDEX idx_stage (app_id, stage_id))");
    }

    private void createClickHouseTables(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS task_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "stage_id Int32, " +
            "task_id Int64, " +
            "task_success LowCardinality(String), " +
            "task_host Nullable(String), " +
            "task_locality Nullable(LowCardinality(String)), " +
            "task_speculative Nullable(LowCardinality(String)), " +
            "duration_ms Nullable(Float64), " +
            "io_bytes_read Nullable(Float64), " +
            "io_bytes_written Nullable(Float64), " +
            "io_records_read Nullable(Float64), " +
            "io_records_written Nullable(Float64), " +
            "shuffle_bytes_read Nullable(Float64), " +
            "shuffle_bytes_written Nullable(Float64), " +
            "shuffle_fetch_wait_time_ms Nullable(Float64), " +
            "disk_bytes_spilled Nullable(Float64), " +
            "memory_bytes_spilled Nullable(Float64), " +
            "executor_run_time_ms Nullable(Float64), " +
            "executor_cpu_time_ns Nullable(Float64), " +
            "deserialize_time_ms Nullable(Float64), " +
            "deserialize_cpu_time_ns Nullable(Float64), " +
            "result_serialization_time_ms Nullable(Float64), " +
            "jvm_gc_time_ms Nullable(Float64), " +
            "scheduler_delay_ms Nullable(Float64), " +
            "result_size_bytes Nullable(Float64), " +
            "peak_execution_memory_bytes Nullable(Float64), " +
            "shuffle_local_blocks_fetched Nullable(Float64), " +
            "shuffle_records_read Nullable(Float64), " +
            "shuffle_remote_bytes_read_to_disk Nullable(Float64), " +
            "shuffle_remote_reqs_duration_ms Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "stage_id Int32, " +
            "duration_ms Nullable(Float64), " +
            "num_tasks Nullable(Float64), " +
            "executor_run_time_ms Nullable(Float64), " +
            "executor_cpu_time_ns Nullable(Float64), " +
            "jvm_gc_time_ms Nullable(Float64), " +
            "peak_execution_memory_bytes Nullable(Float64), " +
            "io_bytes_read Nullable(Float64), " +
            "io_bytes_written Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS job_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "job_id Int32, " +
            "job_success LowCardinality(String), " +
            "duration_ms Nullable(Float64), " +
            "num_stages Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_memory_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "heap_used Nullable(Float64), " +
            "non_heap_used Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_gc_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "gc_name LowCardinality(String), " +
            "gc_count Nullable(Float64), " +
            "gc_time_ms Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS task_histogram_buckets (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "stage_id Int32, " +
            "task_id Int64, " +
            "task_success LowCardinality(String), " +
            "metric_name String, " +
            "bucket_le Float64, " +
            "bucket_count UInt64" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (metric_name, timestamp_ms, bucket_le)");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_histogram_buckets (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "stage_id Int32, " +
            "metric_name String, " +
            "bucket_le Float64, " +
            "bucket_count UInt64" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (metric_name, timestamp_ms, bucket_le)");

        stmt.execute("CREATE TABLE IF NOT EXISTS job_histogram_buckets (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "job_id Int32, " +
            "job_success LowCardinality(String), " +
            "metric_name String, " +
            "bucket_le Float64, " +
            "bucket_count UInt64" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (metric_name, timestamp_ms, bucket_le)");

        stmt.execute("CREATE TABLE IF NOT EXISTS stage_governance (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "stage_id Int32, " +
            "task_count Int32, " +
            "stage_duration_ms Nullable(Float64), " +
            "avg_task_duration_ms Nullable(Float64), " +
            "max_task_duration_ms Nullable(Float64), " +
            "min_task_duration_ms Nullable(Float64), " +
            "duration_skew_ratio Nullable(Float64), " +
            "total_bytes_read Nullable(Float64), " +
            "total_bytes_written Nullable(Float64), " +
            "total_shuffle_bytes_read Nullable(Float64), " +
            "total_shuffle_bytes_written Nullable(Float64), " +
            "total_records_read Nullable(Float64), " +
            "total_records_written Nullable(Float64), " +
            "io_read_skew_ratio Nullable(Float64), " +
            "io_write_skew_ratio Nullable(Float64), " +
            "shuffle_read_skew_ratio Nullable(Float64), " +
            "avg_output_bytes_per_task Nullable(Float64), " +
            "avg_output_records_per_task Nullable(Float64), " +
            "small_output_task_count Int32, " +
            "cpu_efficiency Nullable(Float64), " +
            "gc_overhead_ratio Nullable(Float64), " +
            "shuffle_wait_ratio Nullable(Float64), " +
            "spill_ratio Nullable(Float64), " +
            "deserialize_overhead Nullable(Float64), " +
            "scheduler_delay_ratio Nullable(Float64), " +
            "max_peak_memory_bytes Nullable(Float64), " +
            "total_memory_spilled Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");
    }

    private int insertTaskMetrics(List<TaskMetricRow> rows) throws SQLException {
        String sql = isClickHouse
            ? "INSERT INTO task_metrics (timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, " +
              "task_host, task_locality, task_speculative, duration_ms, io_bytes_read, io_bytes_written, " +
              "io_records_read, io_records_written, shuffle_bytes_read, shuffle_bytes_written, " +
              "shuffle_fetch_wait_time_ms, disk_bytes_spilled, memory_bytes_spilled, executor_run_time_ms, " +
              "executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, result_serialization_time_ms, " +
              "jvm_gc_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, " +
              "shuffle_local_blocks_fetched, shuffle_records_read, shuffle_remote_bytes_read_to_disk, " +
              "shuffle_remote_reqs_duration_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            : "INSERT INTO task_metrics (timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, " +
              "task_host, task_locality, task_speculative, duration_ms, io_bytes_read, io_bytes_written, " +
              "io_records_read, io_records_written, shuffle_bytes_read, shuffle_bytes_written, " +
              "shuffle_fetch_wait_time_ms, disk_bytes_spilled, memory_bytes_spilled, executor_run_time_ms, " +
              "executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, result_serialization_time_ms, " +
              "jvm_gc_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, " +
              "shuffle_local_blocks_fetched, shuffle_records_read, shuffle_remote_bytes_read_to_disk, " +
              "shuffle_remote_reqs_duration_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement ps = connection.prepareStatement(sql);
        for (TaskMetricRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertStageMetrics(List<StageMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO stage_metrics (timestamp_ms, app_id, executor_id, stage_id, " +
            "duration_ms, num_tasks, executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms, " +
            "peak_execution_memory_bytes, io_bytes_read, io_bytes_written) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (StageMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setInt(i++, r.getStageId());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getNumTasks());
            setDouble(ps, i++, r.getExecutorRunTimeMs());
            setDouble(ps, i++, r.getExecutorCpuTimeNs());
            setDouble(ps, i++, r.getJvmGcTimeMs());
            setDouble(ps, i++, r.getPeakExecutionMemoryBytes());
            setDouble(ps, i++, r.getIoBytesRead());
            setDouble(ps, i++, r.getIoBytesWritten());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertJobMetrics(List<JobMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO job_metrics (timestamp_ms, app_id, job_id, job_success, " +
            "duration_ms, num_stages) VALUES (?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JobMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setInt(i++, r.getJobId());
            ps.setString(i++, r.getJobSuccess());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getNumStages());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertJvmMemoryMetrics(List<JvmMemoryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO jvm_memory_metrics (timestamp_ms, app_id, executor_id, " +
            "heap_used, non_heap_used) VALUES (?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JvmMemoryMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            setDouble(ps, i++, r.getHeapUsed());
            setDouble(ps, i++, r.getNonHeapUsed());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertJvmGcMetrics(List<JvmGcMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO jvm_gc_metrics (timestamp_ms, app_id, executor_id, gc_name, " +
            "gc_count, gc_time_ms) VALUES (?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JvmGcMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setString(i++, r.getGcName());
            setDouble(ps, i++, r.getGcCount());
            setDouble(ps, i++, r.getGcTimeMs());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private interface BucketBinder {
        void bind(PreparedStatement ps, HistogramBucket b) throws SQLException;
    }

    private int insertHistogramBuckets(String table, List<HistogramBucket> buckets,
                                        String columns, BucketBinder binder) throws SQLException {
        String sql = "INSERT INTO " + table + " (" + columns + ") VALUES ("
            + columns.replaceAll("[^,]", "?").replace(",", "?,") .substring(1);
        // Build proper placeholders
        int colCount = columns.split(",").length;
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            if (i > 0) placeholders.append(",");
            placeholders.append("?");
        }
        sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";

        PreparedStatement ps = connection.prepareStatement(sql);
        for (HistogramBucket b : buckets) {
            binder.bind(ps, b);
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return buckets.size();
    }

    private void bindTaskBucket(PreparedStatement ps, HistogramBucket b) throws SQLException {
        int i = 1;
        setTimestamp(ps, i++, b.getTimestampMs());
        ps.setString(i++, b.getLabels().get("spark.app.id"));
        ps.setString(i++, b.getLabels().get("spark.executor.id"));
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
        ps.setString(i++, b.getLabels().get("spark.app.id"));
        ps.setString(i++, b.getLabels().get("spark.executor.id"));
        ps.setInt(i++, parseIntSafe(b.getLabels().get("spark.stage.id")));
        ps.setString(i++, b.getMetricName());
        double le = b.getBucketLe();
        ps.setDouble(i++, Double.isInfinite(le) ? Double.MAX_VALUE : le);
        ps.setLong(i, b.getBucketCount());
    }

    private void bindJobBucket(PreparedStatement ps, HistogramBucket b) throws SQLException {
        int i = 1;
        setTimestamp(ps, i++, b.getTimestampMs());
        ps.setString(i++, b.getLabels().get("spark.app.id"));
        ps.setInt(i++, parseIntSafe(b.getLabels().get("spark.job.id")));
        ps.setString(i++, b.getLabels().get("spark.job.success"));
        ps.setString(i++, b.getMetricName());
        double le = b.getBucketLe();
        ps.setDouble(i++, Double.isInfinite(le) ? Double.MAX_VALUE : le);
        ps.setLong(i, b.getBucketCount());
    }

    // ==================== Helpers ====================

    private int insertStageGovernance(List<StageGovernanceRow> rows) throws SQLException {
        String sql = "INSERT INTO stage_governance (timestamp_ms, app_id, stage_id, task_count, " +
            "stage_duration_ms, avg_task_duration_ms, max_task_duration_ms, min_task_duration_ms, " +
            "duration_skew_ratio, total_bytes_read, total_bytes_written, total_shuffle_bytes_read, " +
            "total_shuffle_bytes_written, total_records_read, total_records_written, " +
            "io_read_skew_ratio, io_write_skew_ratio, shuffle_read_skew_ratio, " +
            "avg_output_bytes_per_task, avg_output_records_per_task, small_output_task_count, " +
            "cpu_efficiency, gc_overhead_ratio, shuffle_wait_ratio, spill_ratio, " +
            "deserialize_overhead, scheduler_delay_ratio, max_peak_memory_bytes, total_memory_spilled) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (StageGovernanceRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
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
}
