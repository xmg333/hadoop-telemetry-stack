package x.mg.metrics.flink.sink;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

class SchemaManager {

    private static final Logger LOG = Logger.getLogger(SchemaManager.class.getName());

    private final boolean isClickHouse;

    SchemaManager(boolean isClickHouse) {
        this.isClickHouse = isClickHouse;
    }

    void createTables(Connection connection) throws Exception {
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
        stmt.execute("CREATE TABLE IF NOT EXISTS spark_task_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_job_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_jvm_memory (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_jvm_gc (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_task_histogram (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_histogram (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "executor_id VARCHAR(64) NOT NULL, " +
            "stage_id INT NOT NULL, " +
            "metric_name VARCHAR(255) NOT NULL, " +
            "bucket_le DOUBLE NOT NULL, " +
            "bucket_count BIGINT NOT NULL, " +
            "INDEX idx_metric_time (metric_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_job_histogram (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "app_id VARCHAR(255) NOT NULL, " +
            "job_id INT NOT NULL, " +
            "job_success VARCHAR(16), " +
            "metric_name VARCHAR(255) NOT NULL, " +
            "bucket_le DOUBLE NOT NULL, " +
            "bucket_count BIGINT NOT NULL, " +
            "INDEX idx_metric_time (metric_name, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_skew (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_sql_metrics (" +
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
            "query_text TEXT, " +
            "INDEX idx_app_time (app_id, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_sql_table (" +
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
            "app_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "success VARCHAR(16), " +
            "duration_ms DOUBLE, " +
            "success_count DOUBLE, " +
            "failure_count DOUBLE, " +
            "input_bytes DOUBLE, " +
            "output_bytes DOUBLE, " +
            "input_rows DOUBLE, " +
            "output_rows DOUBLE, " +
            "execution_engine VARCHAR(32), " +
            "query_text TEXT, " +
            "INDEX idx_query_time (query_id, timestamp_ms), " +
            "INDEX idx_operation_time (operation, timestamp_ms), " +
            "INDEX idx_engine_time (execution_engine, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_query_table (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "query_id VARCHAR(255) NOT NULL, " +
            "table_name VARCHAR(512) NOT NULL, " +
            "table_type VARCHAR(16) NOT NULL, " +
            "operation VARCHAR(64), " +
            "user_name VARCHAR(255), " +
            "app_name VARCHAR(255), " +
            "input_table_count DOUBLE, " +
            "output_table_count DOUBLE, " +
            "execution_engine VARCHAR(32), " +
            "bytes DOUBLE, " +
            "`rows` DOUBLE, " +
            "files_read DOUBLE, " +
            "time_ms DOUBLE, " +
            "queue VARCHAR(255), " +
            "INDEX idx_query_time (query_id, timestamp_ms), " +
            "INDEX idx_table_time (table_name, timestamp_ms), " +
            "INDEX idx_engine_time (execution_engine, timestamp_ms))");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_job_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "job_id VARCHAR(255) NOT NULL, " +
            "job_name VARCHAR(512), " +
            "user_name VARCHAR(255), " +
            "app_name VARCHAR(255), " +
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
            "INDEX idx_task_time (task_id, timestamp_ms), " +
            "INDEX idx_job_time (job_id, timestamp_ms), " +
            "INDEX idx_type_time (task_type, timestamp_ms))");

        // Unified wide table for cross-engine analytics
        stmt.execute("CREATE TABLE IF NOT EXISTS unified_metrics (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "timestamp_ms BIGINT NOT NULL, " +
            "event_type VARCHAR(32) NOT NULL, " +
            "engine VARCHAR(16) NOT NULL, " +
            "status VARCHAR(16), " +
            "app_id VARCHAR(255), " +
            "app_name VARCHAR(255), " +
            "user_name VARCHAR(255), " +
            "queue VARCHAR(255), " +
            "duration_ms DOUBLE, " +
            "bytes_read DOUBLE, " +
            "bytes_written DOUBLE, " +
            "shuffle_bytes_read DOUBLE, " +
            "shuffle_bytes_written DOUBLE, " +
            "cpu_time_ms DOUBLE, " +
            "gc_time_ms DOUBLE, " +
            "bytes_spilled DOUBLE, " +
            "executor_id VARCHAR(64), " +
            "stage_id INT, " +
            "task_id VARCHAR(255), " +
            "task_host VARCHAR(255), " +
            "task_locality VARCHAR(64), " +
            "task_speculative VARCHAR(16), " +
            "executor_run_time_ms DOUBLE, " +
            "executor_cpu_time_ns DOUBLE, " +
            "deserialize_time_ms DOUBLE, " +
            "deserialize_cpu_time_ns DOUBLE, " +
            "result_serialization_time_ms DOUBLE, " +
            "scheduler_delay_ms DOUBLE, " +
            "result_size_bytes DOUBLE, " +
            "peak_execution_memory_bytes DOUBLE, " +
            "shuffle_local_blocks_fetched DOUBLE, " +
            "shuffle_records_read DOUBLE, " +
            "shuffle_remote_bytes_read_to_disk DOUBLE, " +
            "shuffle_remote_reqs_duration_ms DOUBLE, " +
            "disk_bytes_spilled DOUBLE, " +
            "shuffle_fetch_wait_time_ms DOUBLE, " +
            "num_tasks DOUBLE, " +
            "num_stages DOUBLE, " +
            "execution_id VARCHAR(255), " +
            "join_count DOUBLE, " +
            "table_name VARCHAR(512), " +
            "table_operation VARCHAR(64), " +
            "bytes DOUBLE, " +
            "`rows` DOUBLE, " +
            "files_read DOUBLE, " +
            "time_ms DOUBLE, " +
            "heap_used DOUBLE, " +
            "non_heap_used DOUBLE, " +
            "gc_name VARCHAR(128), " +
            "gc_count DOUBLE, " +
            "job_id VARCHAR(255), " +
            "job_name VARCHAR(512), " +
            "task_type VARCHAR(32), " +
            "map_output_bytes DOUBLE, " +
            "physical_memory_bytes DOUBLE, " +
            "virtual_memory_bytes DOUBLE, " +
            "committed_heap_bytes DOUBLE, " +
            "maps_duration_ms DOUBLE, " +
            "reduces_duration_ms DOUBLE, " +
            "launched_maps DOUBLE, " +
            "launched_reduces DOUBLE, " +
            "start_time_ms BIGINT, " +
            "finish_time_ms BIGINT, " +
            "hdfs_bytes_read DOUBLE, " +
            "hdfs_bytes_written DOUBLE, " +
            "file_bytes_read DOUBLE, " +
            "file_bytes_written DOUBLE, " +
            "map_input_records DOUBLE, " +
            "map_output_records DOUBLE, " +
            "reduce_input_records DOUBLE, " +
            "reduce_output_records DOUBLE, " +
            "reduce_shuffle_bytes DOUBLE, " +
            "spilled_records DOUBLE, " +
            "hdfs_read_ops DOUBLE, " +
            "hdfs_write_ops DOUBLE, " +
            "hdfs_large_read_ops DOUBLE, " +
            "operation VARCHAR(64), " +
            "table_type VARCHAR(16), " +
            "execution_engine VARCHAR(32), " +
            "success_count DOUBLE, " +
            "failure_count DOUBLE, " +
            "input_rows DOUBLE, " +
            "output_rows DOUBLE, " +
            "records_read DOUBLE, " +
            "records_written DOUBLE, " +
            "query_text TEXT, " +
            "INDEX idx_engine_type_time (engine, event_type, timestamp_ms), " +
            "INDEX idx_app_time (app_id, timestamp_ms), " +
            "INDEX idx_user_time (user_name, timestamp_ms), " +
            "INDEX idx_queue_time (queue, timestamp_ms), " +
            "INDEX idx_status_time (status, timestamp_ms))");
    }

    private void createClickHouseTables(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS spark_task_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_job_metrics (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_jvm_memory (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_jvm_gc (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_task_histogram (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_histogram (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_job_histogram (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_stage_skew (" +
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

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_sql_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "app_id String, " +
            "execution_id String, " +
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
            "duration_ms Nullable(Float64), " +
            "shuffle_bytes_read Nullable(Float64), " +
            "shuffle_bytes_written Nullable(Float64), " +
            "join_count Nullable(Float64), " +
            "query_text Nullable(String)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (app_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS spark_sql_table (" +
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
            "app_name Nullable(String), " +
            "queue Nullable(String), " +
            "success LowCardinality(Nullable(String)), " +
            "duration_ms Nullable(Float64), " +
            "success_count Nullable(Float64), " +
            "failure_count Nullable(Float64), " +
            "input_bytes Nullable(Float64), " +
            "output_bytes Nullable(Float64), " +
            "input_rows Nullable(Float64), " +
            "output_rows Nullable(Float64), " +
            "execution_engine LowCardinality(Nullable(String)), " +
            "query_text Nullable(String)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (query_id, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS hive_query_table (" +
            "timestamp_ms DateTime64(3), " +
            "query_id String, " +
            "table_name String, " +
            "table_type LowCardinality(String), " +
            "operation LowCardinality(Nullable(String)), " +
            "user_name Nullable(String), " +
            "app_name Nullable(String), " +
            "input_table_count Nullable(Float64), " +
            "output_table_count Nullable(Float64), " +
            "execution_engine LowCardinality(Nullable(String)), " +
            "bytes Nullable(Float64), " +
            "rows Nullable(Float64), " +
            "files_read Nullable(Float64), " +
            "time_ms Nullable(Float64), " +
            "queue Nullable(String)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (table_name, timestamp_ms)");

        stmt.execute("CREATE TABLE IF NOT EXISTS mr_job_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "job_id String, " +
            "job_name Nullable(String), " +
            "user_name Nullable(String), " +
            "app_name Nullable(String), " +
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
            "hdfs_large_read_ops Nullable(Float64)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (job_id, timestamp_ms)");

        // Unified wide table for cross-engine analytics (ClickHouse)
        stmt.execute("CREATE TABLE IF NOT EXISTS unified_metrics (" +
            "timestamp_ms DateTime64(3), " +
            "event_type LowCardinality(String), " +
            "engine LowCardinality(String), " +
            "status Nullable(LowCardinality(String)), " +
            "app_id Nullable(String), " +
            "app_name Nullable(String), " +
            "user_name Nullable(String), " +
            "queue Nullable(String), " +
            "duration_ms Nullable(Float64), " +
            "bytes_read Nullable(Float64), " +
            "bytes_written Nullable(Float64), " +
            "shuffle_bytes_read Nullable(Float64), " +
            "shuffle_bytes_written Nullable(Float64), " +
            "cpu_time_ms Nullable(Float64), " +
            "gc_time_ms Nullable(Float64), " +
            "bytes_spilled Nullable(Float64), " +
            "executor_id Nullable(LowCardinality(String)), " +
            "stage_id Nullable(Int32), " +
            "task_id Nullable(String), " +
            "task_host Nullable(String), " +
            "task_locality Nullable(LowCardinality(String)), " +
            "task_speculative Nullable(LowCardinality(String)), " +
            "executor_run_time_ms Nullable(Float64), " +
            "executor_cpu_time_ns Nullable(Float64), " +
            "deserialize_time_ms Nullable(Float64), " +
            "deserialize_cpu_time_ns Nullable(Float64), " +
            "result_serialization_time_ms Nullable(Float64), " +
            "scheduler_delay_ms Nullable(Float64), " +
            "result_size_bytes Nullable(Float64), " +
            "peak_execution_memory_bytes Nullable(Float64), " +
            "shuffle_local_blocks_fetched Nullable(Float64), " +
            "shuffle_records_read Nullable(Float64), " +
            "shuffle_remote_bytes_read_to_disk Nullable(Float64), " +
            "shuffle_remote_reqs_duration_ms Nullable(Float64), " +
            "disk_bytes_spilled Nullable(Float64), " +
            "shuffle_fetch_wait_time_ms Nullable(Float64), " +
            "num_tasks Nullable(Float64), " +
            "num_stages Nullable(Float64), " +
            "execution_id Nullable(String), " +
            "join_count Nullable(Float64), " +
            "table_name Nullable(String), " +
            "table_operation Nullable(LowCardinality(String)), " +
            "bytes Nullable(Float64), " +
            "rows Nullable(Float64), " +
            "files_read Nullable(Float64), " +
            "time_ms Nullable(Float64), " +
            "heap_used Nullable(Float64), " +
            "non_heap_used Nullable(Float64), " +
            "gc_name Nullable(LowCardinality(String)), " +
            "gc_count Nullable(Float64), " +
            "job_id Nullable(String), " +
            "job_name Nullable(String), " +
            "task_type Nullable(LowCardinality(String)), " +
            "map_output_bytes Nullable(Float64), " +
            "physical_memory_bytes Nullable(Float64), " +
            "virtual_memory_bytes Nullable(Float64), " +
            "committed_heap_bytes Nullable(Float64), " +
            "maps_duration_ms Nullable(Float64), " +
            "reduces_duration_ms Nullable(Float64), " +
            "launched_maps Nullable(Float64), " +
            "launched_reduces Nullable(Float64), " +
            "start_time_ms Nullable(Int64), " +
            "finish_time_ms Nullable(Int64), " +
            "hdfs_bytes_read Nullable(Float64), " +
            "hdfs_bytes_written Nullable(Float64), " +
            "file_bytes_read Nullable(Float64), " +
            "file_bytes_written Nullable(Float64), " +
            "map_input_records Nullable(Float64), " +
            "map_output_records Nullable(Float64), " +
            "reduce_input_records Nullable(Float64), " +
            "reduce_output_records Nullable(Float64), " +
            "reduce_shuffle_bytes Nullable(Float64), " +
            "spilled_records Nullable(Float64), " +
            "hdfs_read_ops Nullable(Float64), " +
            "hdfs_write_ops Nullable(Float64), " +
            "hdfs_large_read_ops Nullable(Float64), " +
            "operation Nullable(LowCardinality(String)), " +
            "table_type Nullable(LowCardinality(String)), " +
            "execution_engine Nullable(LowCardinality(String)), " +
            "success_count Nullable(Float64), " +
            "failure_count Nullable(Float64), " +
            "input_rows Nullable(Float64), " +
            "output_rows Nullable(Float64), " +
            "records_read Nullable(Float64), " +
            "records_written Nullable(Float64), " +
            "query_text Nullable(String)" +
            ") ENGINE = MergeTree() " +
            "PARTITION BY toYYYYMM(timestamp_ms) " +
            "ORDER BY (engine, event_type, timestamp_ms, app_id)");
    }

    // ==================== Schema Migration ====================

    private void migrateMySqlSchema(Statement stmt) {
        // v2: Add execution_engine column to hive tables (if not present)
        String[] hiveMigrations = {
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32)",
            "ALTER TABLE hive_query_table ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32)"
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
            "ALTER TABLE hive_query_table ADD INDEX IF NOT EXISTS idx_engine_time (execution_engine, timestamp_ms)"
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
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_large_read_ops DOUBLE"
        };
        for (String sql : mrTaskMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v5: Add unique constraint on task_id for mr_task_metrics to enable UPSERT
        String[] mrTaskUniqueMigrations = {
            "ALTER TABLE mr_task_metrics ADD UNIQUE INDEX IF NOT EXISTS idx_unique_task_id (task_id)"
        };
        for (String sql : mrTaskUniqueMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Unique index migration skipped: " + e.getMessage());
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
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS app_name VARCHAR(255)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS user_name VARCHAR(255)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS queue VARCHAR(255)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255)"
        };
        for (String sql : sparkUserQueueMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v6: Add query_text column
        String[] queryTextMigrations = {
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS query_text TEXT",
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS query_text TEXT",
            "ALTER TABLE unified_metrics ADD COLUMN IF NOT EXISTS query_text TEXT"
        };
        for (String sql : queryTextMigrations) {
            try { stmt.execute(sql); } catch (SQLException e) { LOG.log(Level.FINE, "Migration skipped: " + e.getMessage()); }
        }
    }

    private void migrateClickHouseSchema(Statement stmt) {
        // v2: Add execution_engine column to hive tables (if not present)
        String[] migrations = {
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String))",
            "ALTER TABLE hive_query_table ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String))"
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
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS hdfs_large_read_ops Nullable(Float64)"
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
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_stage_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_job_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_jvm_memory ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_jvm_gc ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS app_name Nullable(String)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS user_name Nullable(String)",
            "ALTER TABLE spark_sql_table ADD COLUMN IF NOT EXISTS queue Nullable(String)",
            "ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String)"
        };
        for (String sql : sparkUserQueueChMigrations) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                LOG.log(Level.FINE, "Migration skipped (column likely exists): " + e.getMessage());
            }
        }
        // v6: Add query_text column
        String[] queryTextChMigrations = {
            "ALTER TABLE spark_sql_metrics ADD COLUMN IF NOT EXISTS query_text Nullable(String)",
            "ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS query_text Nullable(String)",
            "ALTER TABLE unified_metrics ADD COLUMN IF NOT EXISTS query_text Nullable(String)"
        };
        for (String sql : queryTextChMigrations) {
            try { stmt.execute(sql); } catch (SQLException e) { LOG.log(Level.FINE, "Migration skipped: " + e.getMessage()); }
        }
    }
}
