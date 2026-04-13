-- Migration: Add execution_engine column and MR metrics tables
-- Date: 2026-04-13
-- Prerequisite: Flink consumer v2 with MR category support and Hive execution_engine

-- ============================================================
-- MySQL Migration
-- ============================================================

-- 1. Add execution_engine to hive_query_metrics
ALTER TABLE hive_query_metrics
    ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32),
    ADD INDEX IF NOT EXISTS idx_engine_time (execution_engine, timestamp_ms);

-- 2. Add execution_engine to hive_table_io_metrics
ALTER TABLE hive_table_io_metrics
    ADD COLUMN IF NOT EXISTS execution_engine VARCHAR(32),
    ADD INDEX IF NOT EXISTS idx_engine_time (execution_engine, timestamp_ms);

-- 3. Backfill execution_engine from HiveConf default (adjust if known)
-- UPDATE hive_query_metrics SET execution_engine = 'mr' WHERE execution_engine IS NULL;

-- 4. Create mr_job_metrics table (auto-created by Flink consumer, but provided here for reference)
CREATE TABLE IF NOT EXISTS mr_job_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    job_name VARCHAR(512),
    user_name VARCHAR(255),
    state VARCHAR(32),
    queue VARCHAR(255),
    hdfs_bytes_read DOUBLE,
    hdfs_bytes_written DOUBLE,
    file_bytes_read DOUBLE,
    file_bytes_written DOUBLE,
    map_input_records DOUBLE,
    map_output_records DOUBLE,
    map_output_bytes DOUBLE,
    reduce_input_records DOUBLE,
    reduce_output_records DOUBLE,
    reduce_shuffle_bytes DOUBLE,
    spilled_records DOUBLE,
    cpu_time_ms DOUBLE,
    gc_time_ms DOUBLE,
    physical_memory_bytes DOUBLE,
    virtual_memory_bytes DOUBLE,
    committed_heap_bytes DOUBLE,
    maps_duration_ms DOUBLE,
    reduces_duration_ms DOUBLE,
    elapsed_time_ms DOUBLE,
    launched_maps DOUBLE,
    launched_reduces DOUBLE,
    INDEX idx_job_time (job_id, timestamp_ms),
    INDEX idx_state_time (state, timestamp_ms),
    INDEX idx_user_time (user_name, timestamp_ms)
);

-- 5. Create mr_task_metrics table
CREATE TABLE IF NOT EXISTS mr_task_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    task_type VARCHAR(32),
    job_id VARCHAR(255) NOT NULL,
    job_name VARCHAR(512),
    user_name VARCHAR(255),
    state VARCHAR(32),
    hdfs_bytes_read DOUBLE,
    hdfs_bytes_written DOUBLE,
    file_bytes_read DOUBLE,
    file_bytes_written DOUBLE,
    map_input_records DOUBLE,
    map_output_records DOUBLE,
    map_output_bytes DOUBLE,
    reduce_input_records DOUBLE,
    reduce_output_records DOUBLE,
    reduce_shuffle_bytes DOUBLE,
    spilled_records DOUBLE,
    cpu_time_ms DOUBLE,
    gc_time_ms DOUBLE,
    INDEX idx_task_time (task_id, timestamp_ms),
    INDEX idx_job_time (job_id, timestamp_ms),
    INDEX idx_type_time (task_type, timestamp_ms)
);

-- ============================================================
-- ClickHouse Migration (run separately on ClickHouse client)
-- ============================================================

/*
-- Add execution_engine to hive_query_metrics
ALTER TABLE hive_query_metrics
    ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String));

-- Add execution_engine to hive_table_io_metrics
ALTER TABLE hive_table_io_metrics
    ADD COLUMN IF NOT EXISTS execution_engine LowCardinality(Nullable(String));

-- mr_job_metrics and mr_task_metrics are auto-created by Flink consumer
*/
