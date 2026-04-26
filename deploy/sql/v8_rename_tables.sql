-- V8 Migration: Rename all tables to engine-scoped names + add missing dimension columns + redesign unified_metrics
-- Date: 2026-04-24
-- Prerequisite: Flink consumer v8+ with updated table name references
--
-- This migration:
-- 1. Renames all Spark tables to spark_* prefix for consistency with hive_* and mr_* naming
-- 2. Adds missing app_name dimension columns to Hive and MR tables
-- 3. Renames metric_events → unified_metrics with logical column name fixes

-- ========== PART 1: TABLE RENAMES ==========

-- ==================== MySQL ====================

-- Spark category tables
RENAME TABLE task_metrics TO spark_task_metrics;
RENAME TABLE stage_metrics TO spark_stage_metrics;
RENAME TABLE job_metrics TO spark_job_metrics;
RENAME TABLE jvm_memory_metrics TO spark_jvm_memory;
RENAME TABLE jvm_gc_metrics TO spark_jvm_gc;
RENAME TABLE sql_query_metrics TO spark_sql_metrics;
RENAME TABLE sql_query_table_metrics TO spark_sql_table;
RENAME TABLE task_histogram_buckets TO spark_task_histogram;
RENAME TABLE stage_histogram_buckets TO spark_stage_histogram;
RENAME TABLE job_histogram_buckets TO spark_job_histogram;
RENAME TABLE stage_governance TO spark_stage_skew;

-- Hive tables (hive_table_io_metrics renamed; hive_query_metrics kept)
RENAME TABLE hive_table_io_metrics TO hive_query_table;

-- MR tables: keep names (mr_job_metrics, mr_task_metrics unchanged)

-- Unified wide table: renamed + column fixes applied below
RENAME TABLE metric_events TO unified_metrics;

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:

-- RENAME TABLE task_metrics TO spark_task_metrics;
-- RENAME TABLE stage_metrics TO spark_stage_metrics;
-- RENAME TABLE job_metrics TO spark_job_metrics;
-- RENAME TABLE jvm_memory_metrics TO spark_jvm_memory;
-- RENAME TABLE jvm_gc_metrics TO spark_jvm_gc;
-- RENAME TABLE sql_query_metrics TO spark_sql_metrics;
-- RENAME TABLE sql_query_table_metrics TO spark_sql_table;
-- RENAME TABLE task_histogram_buckets TO spark_task_histogram;
-- RENAME TABLE stage_histogram_buckets TO spark_stage_histogram;
-- RENAME TABLE job_histogram_buckets TO spark_job_histogram;
-- RENAME TABLE stage_governance TO spark_stage_skew;
-- RENAME TABLE hive_table_io_metrics TO hive_query_table;
-- RENAME TABLE metric_events TO unified_metrics;


-- ========== PART 2: ADD MISSING DIMENSION COLUMNS ==========

-- 1. hive_query_metrics: ADD app_name and queue
--    Note: queue was NOT present in v7 migration (only hive_table_io_metrics got queue in v7)

-- ==================== MySQL ====================

ALTER TABLE hive_query_metrics
    ADD COLUMN IF NOT EXISTS app_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- 2. hive_query_table (formerly hive_table_io_metrics): ADD app_name
--    Note: queue already exists from v7

ALTER TABLE hive_query_table
    ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);

-- 3. mr_job_metrics: ADD app_name
--    Note: mr_job_metrics was created in v2 without app_name column

ALTER TABLE mr_job_metrics
    ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:

-- ALTER TABLE hive_query_metrics
--     ADD COLUMN IF NOT EXISTS app_name Nullable(String),
--     ADD COLUMN IF NOT EXISTS queue Nullable(String);

-- ALTER TABLE hive_query_table
--     ADD COLUMN IF NOT EXISTS app_name Nullable(String);

-- ALTER TABLE mr_job_metrics
--     ADD COLUMN IF NOT EXISTS app_name Nullable(String);


-- ========== PART 3: REDESIGN unified_metrics COLUMN RENAMES ==========

-- Fix naming inconsistencies in the unified wide table:
--   io_bytes_read    → bytes_read
--   io_bytes_written → bytes_written
--   io_records_read  → records_read
--   io_records_written → records_written
--   memory_bytes_spilled → bytes_spilled
--   time_ms_col → time_ms

-- ==================== MySQL ====================

ALTER TABLE unified_metrics
    CHANGE COLUMN io_bytes_read bytes_read DOUBLE,
    CHANGE COLUMN io_bytes_written bytes_written DOUBLE,
    CHANGE COLUMN io_records_read records_read DOUBLE,
    CHANGE COLUMN io_records_written records_written DOUBLE,
    CHANGE COLUMN memory_bytes_spilled bytes_spilled DOUBLE,
    CHANGE COLUMN time_ms_col time_ms DOUBLE;

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:
--
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS io_bytes_read TO bytes_read;
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS io_bytes_written TO bytes_written;
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS io_records_read TO records_read;
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS io_records_written TO records_written;
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS memory_bytes_spilled TO bytes_spilled;
-- ALTER TABLE unified_metrics RENAME COLUMN IF EXISTS time_ms_col TO time_ms;
