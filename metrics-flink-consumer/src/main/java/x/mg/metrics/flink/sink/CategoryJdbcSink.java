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
            + " sql=" + (result.sqlQueryRows != null ? result.sqlQueryRows.size() : 0)
            + " sql_tbl=" + (result.sqlTableIoRows != null ? result.sqlTableIoRows.size() : 0)
            + " hive_q=" + (result.hiveQueryRows != null ? result.hiveQueryRows.size() : 0)
            + " hive_tbl=" + (result.hiveTableIoRows != null ? result.hiveTableIoRows.size() : 0)
            + " mr_job=" + (result.mrJobRows != null ? result.mrJobRows.size() : 0)
            + " mr_task=" + (result.mrTaskRows != null ? result.mrTaskRows.size() : 0)
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
            migrateClickHouseSchema(stmt);
        } else {
            createMySqlTables(stmt);
            migrateMySqlSchema(stmt);
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
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
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
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
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
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "duration_ms DOUBLE, " +
            "num_stages DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_memory_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "heap_used DOUBLE, " +
            "non_heap_used DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_gc_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "gc_name VARCHAR(128), " +
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS sql_query_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "execution_id VARCHAR(255) NOT NULL, " +
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "duration_ms DOUBLE, " +
            "shuffle_bytes_read DOUBLE, " +
            "shuffle_bytes_written DOUBLE, " +
            "join_count DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS sql_query_table_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "execution_id VARCHAR(255) NOT NULL, " +
            "table_name VARCHAR(512) NOT NULL, " +
            "operation VARCHAR(32) NOT NULL, " +
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "bytes DOUBLE, " +
            "`rows` DOUBLE, " +
            "files_read DOUBLE, " +
            "time_ms DOUBLE, " +
            "INDEX idx_app_time (app_id, timestamp_ms), " +
            "INDEX idx_table (table_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_query_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "query_id VARCHAR(255) NOT NULL, " +
            "operation VARCHAR(64), " +
            "user_name VARCHAR(255), " +
            "success VARCHAR(16), " +
            "duration_ms DOUBLE, " +
            "success_count DOUBLE, " +
            "failure_count DOUBLE, " +
            "input_bytes DOUBLE, " +
            "output_bytes DOUBLE, " +
            "input_rows DOUBLE, " +
            "output_rows DOUBLE, " +
            "execution_engine VARCHAR(32), " +
            "INDEX idx_query_time (query_id, timestamp_ms), " +
            "INDEX idx_operation_time (operation, timestamp_ms), " +
            "INDEX idx_engine_time (execution_engine, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_table_io_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "query_id VARCHAR(255) NOT NULL, " +
            "table_name VARCHAR(512) NOT NULL, " +
            "table_type VARCHAR(16) NOT NULL, " +
            "operation VARCHAR(64), " +
            "user_name VARCHAR(255), " +
            "input_table_count DOUBLE, " +
            "output_table_count DOUBLE, " +
            "execution_engine VARCHAR(32), " +
            "INDEX idx_query_time (query_id, timestamp_ms), " +
            "INDEX idx_table_time (table_name, timestamp_ms), " +
            "INDEX idx_engine_time (execution_engine, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_job_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "job_id VARCHAR(255) NOT NULL, " +
            "job_name VARCHAR(512), " +
            "user_name VARCHAR(255), " +
            "state VARCHAR(32), " +
            "queue VARCHAR(255), " +
            "hdfs_bytes_read DOUBLE, " +
            "hdfs_bytes_written DOUBLE, " +
            "file_bytes_read DOUBLE, " +
            "file_bytes_written DOUBLE, " +
            "map_input_records DOUBLE, " +
            "map_output_records DOUBLE, " +
            "map_output_bytes DOUBLE, " +
            "reduce_input_records DOUBLE, " +
            "reduce_output_records DOUBLE, " +
            "reduce_shuffle_bytes DOUBLE, " +
            "spilled_records DOUBLE, " +
            "cpu_time_ms DOUBLE, " +
            "gc_time_ms DOUBLE, " +
            "physical_memory_bytes DOUBLE, " +
            "virtual_memory_bytes DOUBLE, " +
            "committed_heap_bytes DOUBLE, " +
            "maps_duration_ms DOUBLE, " +
            "reduces_duration_ms DOUBLE, " +
            "elapsed_time_ms DOUBLE, " +
            "launched_maps DOUBLE, " +
            "launched_reduces DOUBLE, " +
            "start_time_ms BIGINT, " +
            "finish_time_ms BIGINT, " +
            "INDEX idx_job_time (job_id, timestamp_ms), " +
            "INDEX idx_state_time (state, timestamp_ms), " +
            "INDEX idx_user_time (user_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_task_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "task_id VARCHAR(255) NOT NULL, " +
            "task_type VARCHAR(32), " +
            "job_id VARCHAR(255) NOT NULL, " +
            "job_name VARCHAR(512), " +
            "user_name VARCHAR(255), " +
            "state VARCHAR(32), " +
            "queue VARCHAR(255), " +
            "hdfs_bytes_read DOUBLE, " +
            "hdfs_bytes_written DOUBLE, " +
            "file_bytes_read DOUBLE, " +
            "file_bytes_written DOUBLE, " +
            "map_input_records DOUBLE, " +
            "map_output_records DOUBLE, " +
            "map_output_bytes DOUBLE, " +
            "reduce_input_records DOUBLE, " +
            "reduce_output_records DOUBLE, " +
            "reduce_shuffle_bytes DOUBLE, " +
            "spilled_records DOUBLE, " +
            "cpu_time_ms DOUBLE, " +
            "gc_time_ms DOUBLE, " +
            "duration_ms DOUBLE, " +
            "success_count DOUBLE, " +
            "failure_count DOUBLE, " +
            "hdfs_read_ops DOUBLE, " +
            "hdfs_write_ops DOUBLE, " +
            "hdfs_large_read_ops DOUBLE, " +
            "file_read_ops DOUBLE, " +
            "file_write_ops DOUBLE, " +
            "file_large_read_ops DOUBLE, " +
            "INDEX idx_task_time (task_id, timestamp_ms), " +
            "INDEX idx_job_time (job_id, timestamp_ms), " +
            "INDEX idx_type_time (task_type, timestamp_ms))");
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
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
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
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
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
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
            "duration_ms Nullable(Float64), " +
            "num_stages Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS jvm_memory_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "executor_id LowCardinality(String), " +
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
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
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS sql_query_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "execution_id String, " +
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
            "duration_ms Nullable(Float64), " +
            "shuffle_bytes_read Nullable(Float64), " +
            "shuffle_bytes_written Nullable(Float64), " +
            "join_count Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS sql_query_table_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "execution_id String, " +
            "table_name String, " +
            "operation LowCardinality(String), " +
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
            "bytes Nullable(Float64), " +
            "rows Nullable(Float64), " +
            "files_read Nullable(Float64), " +
            "time_ms Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_query_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "query_id String, " +
            "operation LowCardinality(Nullable(String)), " +
            "user_name Nullable(String), " +
            "success LowCardinality(Nullable(String)), " +
            "duration_ms Nullable(Float64), " +
            "success_count Nullable(Float64), " +
            "failure_count Nullable(Float64), " +
            "input_bytes Nullable(Float64), " +
            "output_bytes Nullable(Float64), " +
            "input_rows Nullable(Float64), " +
            "output_rows Nullable(Float64), " +
            "execution_engine LowCardinality(Nullable(String))" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (query_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_table_io_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "query_id String, " +
            "table_name String, " +
            "table_type LowCardinality(String), " +
            "operation LowCardinality(Nullable(String)), " +
            "user_name Nullable(String), " +
            "input_table_count Nullable(Float64), " +
            "output_table_count Nullable(Float64), " +
            "execution_engine LowCardinality(Nullable(String))" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (table_name, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_job_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "job_id String, " +
            "job_name Nullable(String), " +
            "user_name Nullable(String), " +
            "state LowCardinality(Nullable(String)), " +
            "queue Nullable(String), " +
            "hdfs_bytes_read Nullable(Float64), " +
            "hdfs_bytes_written Nullable(Float64), " +
            "file_bytes_read Nullable(Float64), " +
            "file_bytes_written Nullable(Float64), " +
            "map_input_records Nullable(Float64), " +
            "map_output_records Nullable(Float64), " +
            "map_output_bytes Nullable(Float64), " +
            "reduce_input_records Nullable(Float64), " +
            "reduce_output_records Nullable(Float64), " +
            "reduce_shuffle_bytes Nullable(Float64), " +
            "spilled_records Nullable(Float64), " +
            "cpu_time_ms Nullable(Float64), " +
            "gc_time_ms Nullable(Float64), " +
            "physical_memory_bytes Nullable(Float64), " +
            "virtual_memory_bytes Nullable(Float64), " +
            "committed_heap_bytes Nullable(Float64), " +
            "maps_duration_ms Nullable(Float64), " +
            "reduces_duration_ms Nullable(Float64), " +
            "elapsed_time_ms Nullable(Float64), " +
            "launched_maps Nullable(Float64), " +
            "launched_reduces Nullable(Float64), " +
            "start_time_ms Nullable(Int64), " +
            "finish_time_ms Nullable(Int64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (job_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_task_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "task_id String, " +
            "task_type LowCardinality(Nullable(String)), " +
            "job_id String, " +
            "job_name Nullable(String), " +
            "user_name Nullable(String), " +
            "state LowCardinality(Nullable(String)), " +
            "queue Nullable(String), " +
            "hdfs_bytes_read Nullable(Float64), " +
            "hdfs_bytes_written Nullable(Float64), " +
            "file_bytes_read Nullable(Float64), " +
            "file_bytes_written Nullable(Float64), " +
            "map_input_records Nullable(Float64), " +
            "map_output_records Nullable(Float64), " +
            "map_output_bytes Nullable(Float64), " +
            "reduce_input_records Nullable(Float64), " +
            "reduce_output_records Nullable(Float64), " +
            "reduce_shuffle_bytes Nullable(Float64), " +
            "spilled_records Nullable(Float64), " +
            "cpu_time_ms Nullable(Float64), " +
            "gc_time_ms Nullable(Float64), " +
            "duration_ms Nullable(Float64), " +
            "success_count Nullable(Float64), " +
            "failure_count Nullable(Float64), " +
            "hdfs_read_ops Nullable(Float64), " +
            "hdfs_write_ops Nullable(Float64), " +
            "hdfs_large_read_ops Nullable(Float64), " +
            "file_read_ops Nullable(Float64), " +
            "file_write_ops Nullable(Float64), " +
            "file_large_read_ops Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (job_id, timestamp_ms)");
    }

    private int insertTaskMetrics(List<TaskMetricRow> rows) throws SQLException {
        String sql = isClickHouse
            ? "INSERT INTO task_metrics (timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, " +
              "task_host, task_locality, task_speculative, app_name, user_name, queue, duration_ms, io_bytes_read, io_bytes_written, " +
              "io_records_read, io_records_written, shuffle_bytes_read, shuffle_bytes_written, " +
              "shuffle_fetch_wait_time_ms, disk_bytes_spilled, memory_bytes_spilled, executor_run_time_ms, " +
              "executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, result_serialization_time_ms, " +
              "jvm_gc_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, " +
              "shuffle_local_blocks_fetched, shuffle_records_read, shuffle_remote_bytes_read_to_disk, " +
              "shuffle_remote_reqs_duration_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            : "INSERT INTO task_metrics (timestamp_ms, app_id, executor_id, stage_id, task_id, task_success, " +
              "task_host, task_locality, task_speculative, app_name, user_name, queue, duration_ms, io_bytes_read, io_bytes_written, " +
              "io_records_read, io_records_written, shuffle_bytes_read, shuffle_bytes_written, " +
              "shuffle_fetch_wait_time_ms, disk_bytes_spilled, memory_bytes_spilled, executor_run_time_ms, " +
              "executor_cpu_time_ns, deserialize_time_ms, deserialize_cpu_time_ns, result_serialization_time_ms, " +
              "jvm_gc_time_ms, scheduler_delay_ms, result_size_bytes, peak_execution_memory_bytes, " +
              "shuffle_local_blocks_fetched, shuffle_records_read, shuffle_remote_bytes_read_to_disk, " +
              "shuffle_remote_reqs_duration_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertStageMetrics(List<StageMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO stage_metrics (timestamp_ms, app_id, executor_id, stage_id, " +
            "app_name, user_name, queue, duration_ms, num_tasks, executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms, " +
            "peak_execution_memory_bytes, io_bytes_read, io_bytes_written) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (StageMetricRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertJobMetrics(List<JobMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO job_metrics (timestamp_ms, app_id, job_id, job_success, " +
            "app_name, user_name, queue, duration_ms, num_stages) VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JobMetricRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertJvmMemoryMetrics(List<JvmMemoryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO jvm_memory_metrics (timestamp_ms, app_id, executor_id, " +
            "app_name, user_name, queue, heap_used, non_heap_used) VALUES (?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JvmMemoryMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getAppId());
            ps.setString(i++, r.getExecutorId());
            ps.setString(i++, r.getAppName());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getQueue());
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
            "app_name, user_name, queue, gc_count, gc_time_ms) VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (JvmGcMetricRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertSqlQueryMetrics(List<SqlQueryMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO sql_query_metrics (timestamp_ms, app_id, execution_id, " +
            "app_name, user_name, queue, duration_ms, shuffle_bytes_read, shuffle_bytes_written, join_count) VALUES (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (SqlQueryMetricRow r : rows) {
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
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertSqlTableIoMetrics(List<SqlTableIoMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO sql_query_table_metrics (timestamp_ms, app_id, execution_id, " +
            "table_name, operation, app_name, user_name, queue, bytes, `rows`, files_read, time_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (SqlTableIoMetricRow r : rows) {
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
            "success, duration_ms, success_count, failure_count, input_bytes, output_bytes, " +
            "input_rows, output_rows, execution_engine) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (HiveQueryMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getQueryId());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getUserName());
            ps.setString(i++, r.getSuccess());
            setDouble(ps, i++, r.getDurationMs());
            setDouble(ps, i++, r.getSuccessCount());
            setDouble(ps, i++, r.getFailureCount());
            setDouble(ps, i++, r.getInputBytes());
            setDouble(ps, i++, r.getOutputBytes());
            setDouble(ps, i++, r.getInputRows());
            setDouble(ps, i++, r.getOutputRows());
            ps.setString(i++, r.getExecutionEngine());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertHiveTableIoMetrics(List<HiveTableIoMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO hive_table_io_metrics (timestamp_ms, query_id, table_name, " +
            "table_type, operation, user_name, input_table_count, output_table_count, execution_engine) " +
            "VALUES (?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (HiveTableIoMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
            ps.setString(i++, r.getQueryId());
            ps.setString(i++, r.getTableName());
            ps.setString(i++, r.getTableType());
            ps.setString(i++, r.getOperation());
            ps.setString(i++, r.getUserName());
            setDouble(ps, i++, r.getInputTableCount());
            setDouble(ps, i++, r.getOutputTableCount());
            ps.setString(i++, r.getExecutionEngine());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    private int insertMrJobMetrics(List<MrJobMetricRow> rows) throws SQLException {
        String sql = "INSERT INTO mr_job_metrics (timestamp_ms, job_id, job_name, user_name, state, queue, " +
            "hdfs_bytes_read, hdfs_bytes_written, file_bytes_read, file_bytes_written, " +
            "map_input_records, map_output_records, map_output_bytes, " +
            "reduce_input_records, reduce_output_records, reduce_shuffle_bytes, spilled_records, " +
            "cpu_time_ms, gc_time_ms, physical_memory_bytes, virtual_memory_bytes, committed_heap_bytes, " +
            "maps_duration_ms, reduces_duration_ms, elapsed_time_ms, launched_maps, launched_reduces, " +
            "start_time_ms, finish_time_ms) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (MrJobMetricRow r : rows) {
            int i = 1;
            setTimestamp(ps, i++, r.getTimestampMs());
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
        String sql = "INSERT INTO mr_task_metrics (timestamp_ms, task_id, task_type, job_id, job_name, user_name, state, queue, " +
            "hdfs_bytes_read, hdfs_bytes_written, file_bytes_read, file_bytes_written, " +
            "map_input_records, map_output_records, map_output_bytes, " +
            "reduce_input_records, reduce_output_records, reduce_shuffle_bytes, spilled_records, " +
            "cpu_time_ms, gc_time_ms, " +
            "duration_ms, success_count, failure_count, " +
            "hdfs_read_ops, hdfs_write_ops, hdfs_large_read_ops, " +
            "file_read_ops, file_write_ops, file_large_read_ops) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
            setDouble(ps, i++, r.getFileReadOps());
            setDouble(ps, i++, r.getFileWriteOps());
            setDouble(ps, i++, r.getFileLargeReadOps());
            ps.addBatch();
        }
        ps.executeBatch();
        if (!isClickHouse) connection.commit();
        ps.close();
        return rows.size();
    }

    // ==================== Schema Migration ====================

    private void migrateMySqlSchema(Statement stmt) {
        // v2: Add execution_engine column to hive tables (if not present)
        String[] hiveMigrations = {
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32)",
            "ALTER TABLE hive_table_io_metrics ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32)"
        };
        for (String sql : hiveMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // Add index if not exists (MySQL 8.0+ supports IF NOT EXISTS for indexes)
        String[] indexMigrations = {
            "ALTER TABLE hive_query_metrics ADD INDEX IF NOT EXISTS idx_engine_time (execution_engine, timestamp_ms)",
            "ALTER TABLE hive_table_io_metrics ADD INDEX IF NOT EXISTS idx_engine_time (execution_engine, timestamp_ms)"
        };
        for (String sql : indexMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Index migration skipped: " + e.getMessage());
            }
        }
        // v3: Add new MR task metric columns
        String[] mrTaskMigrations = {
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS duration_ms DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS success_count DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS failure_count DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_read_ops DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_write_ops DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_large_read_ops DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_read_ops DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_write_ops DOUBLE",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_large_read_ops DOUBLE"
        };
        for (String sql : mrTaskMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v4: Add start/finish time to mr_job_metrics
        String[] mrJobTimeMigrations = {
            "ALTER TABLE mr_job_metrics ADD COLUMN IF NOT EXISTS start_time_ms BIGINT",
            "ALTER TABLE mr_job_metrics ADD COLUMN IF NOT EXISTS finish_time_ms BIGINT"
        };
        for (String sql : mrJobTimeMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v5: Add app_name, user_name, queue to Spark tables; queue to mr_task_metrics
        String[] sparkUserQueueMigrations = {
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)"
        };
        for (String sql : sparkUserQueueMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
    }

    private void migrateClickHouseSchema(Statement stmt) {
        // v2: Add execution_engine column to hive tables (if not present)
        String[] migrations = {
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String))",
            "ALTER TABLE hive_table_io_metrics ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String))"
        };
        for (String sql : migrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v3: Add new MR task metric columns
        String[] mrTaskChMigrations = {
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS duration_ms Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS success_count Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS failure_count Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_read_ops Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_write_ops Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_large_read_ops Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_read_ops Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_write_ops Nullable(Float64)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS file_large_read_ops Nullable(Float64)"
        };
        for (String sql : mrTaskChMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v4: Add start/finish time to mr_job_metrics
        String[] mrJobTimeChMigrations = {
            "ALTER TABLE mr_job_metrics ADD COLUMN IF NOT EXISTS start_time_ms Nullable(Int64)",
            "ALTER TABLE mr_job_metrics ADD COLUMN IF NOT EXISTS finish_time_ms Nullable(Int64)"
        };
        for (String sql : mrJobTimeChMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v5: Add app_name, user_name, queue to Spark tables; queue to mr_task_metrics
        String[] sparkUserQueueChMigrations = {
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)"
        };
        for (String sql : sparkUserQueueChMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
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
