-- V7 Migration: Add per-table I/O metrics columns to hive_table_io_metrics

-- ==================== MySQL ====================

ALTER TABLE hive_table_io_metrics
    ADD COLUMN IF NOT EXISTS bytes DOUBLE,
    ADD COLUMN IF NOT EXISTS `rows` DOUBLE,
    ADD COLUMN IF NOT EXISTS files_read DOUBLE,
    ADD COLUMN IF NOT EXISTS time_ms DOUBLE,
    ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:

ALTER TABLE hive_table_io_metrics
    ADD COLUMN IF NOT EXISTS bytes Nullable(Float64),
    ADD COLUMN IF NOT EXISTS rows Nullable(Float64),
    ADD COLUMN IF NOT EXISTS files_read Nullable(Float64),
    ADD COLUMN IF NOT EXISTS time_ms Nullable(Float64),
    ADD COLUMN IF NOT EXISTS queue Nullable(String);
