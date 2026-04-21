-- V6 Migration: Add query_text column to SQL and Hive metric tables

-- ==================== MySQL ====================

ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS query_text TEXT;
ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS query_text TEXT;
ALTER TABLE metric_events ADD COLUMN IF NOT EXISTS query_text TEXT;

-- ==================== ClickHouse ====================
-- Run separately on ClickHouse instance:
--
-- ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS query_text Nullable(String);
-- ALTER TABLE hive_query_metrics ADD COLUMN IF NOT EXISTS query_text Nullable(String);
-- ALTER TABLE metric_events ADD COLUMN IF NOT EXISTS query_text Nullable(String);
