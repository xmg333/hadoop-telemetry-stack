-- V5 Migration: Add unified metric_events wide table for cross-engine analytics
-- This table consolidates data from all 11 existing tables into a single wide table
-- with engine (SPARK/MR/HIVE) and event_type as discriminators.

-- ==================== MySQL ====================

CREATE TABLE IF NOT EXISTS metric_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    event_type VARCHAR(32) NOT NULL COMMENT 'TASK|STAGE|JOB|JVM_MEMORY|JVM_GC|SQL_QUERY|SQL_TABLE_IO|HIVE_QUERY|HIVE_TABLE_IO|MR_JOB|MR_TASK',
    engine VARCHAR(16) NOT NULL COMMENT 'SPARK|MR|HIVE',

    -- Normalized common dimensions
    status VARCHAR(16) COMMENT 'true/false (normalized success/fail)',
    app_id VARCHAR(255),
    app_name VARCHAR(255),
    user_name VARCHAR(255),
    queue VARCHAR(255),

    -- Normalized common metrics (cross-engine)
    duration_ms DOUBLE,
    io_bytes_read DOUBLE,
    io_bytes_written DOUBLE,
    shuffle_bytes_read DOUBLE,
    shuffle_bytes_written DOUBLE,
    cpu_time_ms DOUBLE,
    gc_time_ms DOUBLE,
    memory_bytes_spilled DOUBLE,

    -- Spark-specific dimensions
    executor_id VARCHAR(64),
    stage_id INT,
    task_id VARCHAR(255),
    task_host VARCHAR(255),
    task_locality VARCHAR(64),
    task_speculative VARCHAR(16),

    -- Spark-specific metrics
    executor_run_time_ms DOUBLE,
    executor_cpu_time_ns DOUBLE,
    deserialize_time_ms DOUBLE,
    deserialize_cpu_time_ns DOUBLE,
    result_serialization_time_ms DOUBLE,
    scheduler_delay_ms DOUBLE,
    result_size_bytes DOUBLE,
    peak_execution_memory_bytes DOUBLE,
    shuffle_local_blocks_fetched DOUBLE,
    shuffle_records_read DOUBLE,
    shuffle_remote_bytes_read_to_disk DOUBLE,
    shuffle_remote_reqs_duration_ms DOUBLE,
    disk_bytes_spilled DOUBLE,
    shuffle_fetch_wait_time_ms DOUBLE,
    num_tasks DOUBLE,
    num_stages DOUBLE,

    -- SQL-specific
    execution_id VARCHAR(255),
    join_count DOUBLE,

    -- SQL Table IO / Hive Table IO
    table_name VARCHAR(512),
    table_operation VARCHAR(64),
    bytes DOUBLE,
    `rows` DOUBLE,
    files_read DOUBLE,
    time_ms_col DOUBLE,

    -- JVM-specific
    heap_used DOUBLE,
    non_heap_used DOUBLE,
    gc_name VARCHAR(128),
    gc_count DOUBLE,

    -- MR-specific dimensions
    job_id VARCHAR(255),
    job_name VARCHAR(512),
    task_type VARCHAR(32),

    -- MR-specific metrics
    map_output_bytes DOUBLE,
    physical_memory_bytes DOUBLE,
    virtual_memory_bytes DOUBLE,
    committed_heap_bytes DOUBLE,
    maps_duration_ms DOUBLE,
    reduces_duration_ms DOUBLE,
    launched_maps DOUBLE,
    launched_reduces DOUBLE,
    start_time_ms BIGINT,
    finish_time_ms BIGINT,

    -- MR IO detail (preserved alongside normalized columns)
    hdfs_bytes_read DOUBLE,
    hdfs_bytes_written DOUBLE,
    file_bytes_read DOUBLE,
    file_bytes_written DOUBLE,
    map_input_records DOUBLE,
    map_output_records DOUBLE,
    reduce_input_records DOUBLE,
    reduce_output_records DOUBLE,
    reduce_shuffle_bytes DOUBLE,
    spilled_records DOUBLE,

    -- MR file operations
    hdfs_read_ops DOUBLE,
    hdfs_write_ops DOUBLE,
    hdfs_large_read_ops DOUBLE,
    file_read_ops DOUBLE,
    file_write_ops DOUBLE,
    file_large_read_ops DOUBLE,

    -- Hive-specific
    operation VARCHAR(64),
    table_type VARCHAR(16),
    execution_engine VARCHAR(32),
    success_count DOUBLE,
    failure_count DOUBLE,
    input_rows DOUBLE,
    output_rows DOUBLE,

    -- Spark IO records
    io_records_read DOUBLE,
    io_records_written DOUBLE,

    INDEX idx_engine_type_time (engine, event_type, timestamp_ms),
    INDEX idx_app_time (app_id, timestamp_ms),
    INDEX idx_user_time (user_name, timestamp_ms),
    INDEX idx_queue_time (queue, timestamp_ms),
    INDEX idx_status_time (status, timestamp_ms)
);

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:
--
-- CREATE TABLE IF NOT EXISTS metric_events (
--     timestamp_ms DateTime64(3),
--     event_type LowCardinality(String),
--     engine LowCardinality(String),
--     status Nullable(LowCardinality(String)),
--     app_id Nullable(String),
--     app_name Nullable(String),
--     user_name Nullable(String),
--     queue Nullable(String),
--     duration_ms Nullable(Float64),
--     io_bytes_read Nullable(Float64),
--     io_bytes_written Nullable(Float64),
--     shuffle_bytes_read Nullable(Float64),
--     shuffle_bytes_written Nullable(Float64),
--     cpu_time_ms Nullable(Float64),
--     gc_time_ms Nullable(Float64),
--     memory_bytes_spilled Nullable(Float64),
--     executor_id Nullable(LowCardinality(String)),
--     stage_id Nullable(Int32),
--     task_id Nullable(String),
--     task_host Nullable(String),
--     task_locality Nullable(LowCardinality(String)),
--     task_speculative Nullable(LowCardinality(String)),
--     executor_run_time_ms Nullable(Float64),
--     executor_cpu_time_ns Nullable(Float64),
--     deserialize_time_ms Nullable(Float64),
--     deserialize_cpu_time_ns Nullable(Float64),
--     result_serialization_time_ms Nullable(Float64),
--     scheduler_delay_ms Nullable(Float64),
--     result_size_bytes Nullable(Float64),
--     peak_execution_memory_bytes Nullable(Float64),
--     shuffle_local_blocks_fetched Nullable(Float64),
--     shuffle_records_read Nullable(Float64),
--     shuffle_remote_bytes_read_to_disk Nullable(Float64),
--     shuffle_remote_reqs_duration_ms Nullable(Float64),
--     disk_bytes_spilled Nullable(Float64),
--     shuffle_fetch_wait_time_ms Nullable(Float64),
--     num_tasks Nullable(Float64),
--     num_stages Nullable(Float64),
--     execution_id Nullable(String),
--     join_count Nullable(Float64),
--     table_name Nullable(String),
--     table_operation Nullable(LowCardinality(String)),
--     bytes Nullable(Float64),
--     rows Nullable(Float64),
--     files_read Nullable(Float64),
--     time_ms_col Nullable(Float64),
--     heap_used Nullable(Float64),
--     non_heap_used Nullable(Float64),
--     gc_name Nullable(LowCardinality(String)),
--     gc_count Nullable(Float64),
--     job_id Nullable(String),
--     job_name Nullable(String),
--     task_type Nullable(LowCardinality(String)),
--     map_output_bytes Nullable(Float64),
--     physical_memory_bytes Nullable(Float64),
--     virtual_memory_bytes Nullable(Float64),
--     committed_heap_bytes Nullable(Float64),
--     maps_duration_ms Nullable(Float64),
--     reduces_duration_ms Nullable(Float64),
--     launched_maps Nullable(Float64),
--     launched_reduces Nullable(Float64),
--     start_time_ms Nullable(Int64),
--     finish_time_ms Nullable(Int64),
--     hdfs_bytes_read Nullable(Float64),
--     hdfs_bytes_written Nullable(Float64),
--     file_bytes_read Nullable(Float64),
--     file_bytes_written Nullable(Float64),
--     map_input_records Nullable(Float64),
--     map_output_records Nullable(Float64),
--     reduce_input_records Nullable(Float64),
--     reduce_output_records Nullable(Float64),
--     reduce_shuffle_bytes Nullable(Float64),
--     spilled_records Nullable(Float64),
--     hdfs_read_ops Nullable(Float64),
--     hdfs_write_ops Nullable(Float64),
--     hdfs_large_read_ops Nullable(Float64),
--     file_read_ops Nullable(Float64),
--     file_write_ops Nullable(Float64),
--     file_large_read_ops Nullable(Float64),
--     operation Nullable(LowCardinality(String)),
--     table_type Nullable(LowCardinality(String)),
--     execution_engine Nullable(LowCardinality(String)),
--     success_count Nullable(Float64),
--     failure_count Nullable(Float64),
--     input_rows Nullable(Float64),
--     output_rows Nullable(Float64),
--     io_records_read Nullable(Float64),
--     io_records_written Nullable(Float64)
-- ) ENGINE = MergeTree()
--   PARTITION BY toYYYYMM(timestamp_ms)
--   ORDER BY (engine, event_type, timestamp_ms, app_id);
