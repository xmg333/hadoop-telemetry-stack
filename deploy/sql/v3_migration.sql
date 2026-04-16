-- Migration: Add task duration, success/failure counts, and file operation metrics to mr_task_metrics
-- Date: 2026-04-15
-- Prerequisite: MR Agent with ByteBuddy instrumentation, Flink consumer v3

-- ============================================================
-- MySQL Migration
-- ============================================================

ALTER TABLE mr_task_metrics
    ADD COLUMN IF NOT EXISTS duration_ms DOUBLE,
    ADD COLUMN IF NOT EXISTS success_count DOUBLE,
    ADD COLUMN IF NOT EXISTS failure_count DOUBLE,
    ADD COLUMN IF NOT EXISTS hdfs_read_ops DOUBLE,
    ADD COLUMN IF NOT EXISTS hdfs_write_ops DOUBLE,
    ADD COLUMN IF NOT EXISTS hdfs_large_read_ops DOUBLE,
    ADD COLUMN IF NOT EXISTS file_read_ops DOUBLE,
    ADD COLUMN IF NOT EXISTS file_write_ops DOUBLE,
    ADD COLUMN IF NOT EXISTS file_large_read_ops DOUBLE;

-- ============================================================
-- ClickHouse Migration (run separately on ClickHouse client)
-- ============================================================

/*
ALTER TABLE mr_task_metrics
    ADD COLUMN IF NOT EXISTS duration_ms Nullable(Float64),
    ADD COLUMN IF NOT EXISTS success_count Nullable(Float64),
    ADD COLUMN IF NOT EXISTS failure_count Nullable(Float64),
    ADD COLUMN IF NOT EXISTS hdfs_read_ops Nullable(Float64),
    ADD COLUMN IF NOT EXISTS hdfs_write_ops Nullable(Float64),
    ADD COLUMN IF NOT EXISTS hdfs_large_read_ops Nullable(Float64),
    ADD COLUMN IF NOT EXISTS file_read_ops Nullable(Float64),
    ADD COLUMN IF NOT EXISTS file_write_ops Nullable(Float64),
    ADD COLUMN IF NOT EXISTS file_large_read_ops Nullable(Float64);
*/
