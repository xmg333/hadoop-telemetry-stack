-- Migration v4: Add app_name, user_name, queue to Spark metric tables;
--                Add queue to mr_task_metrics
-- Date: 2026-04-16
-- Prerequisite: Flink consumer v4+ with user/queue/appName support

-- task_metrics
ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- stage_metrics
ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- job_metrics
ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- jvm_memory_metrics
ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- jvm_gc_metrics
ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- sql_query_metrics
ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- sql_query_table_metrics
ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS app_name VARCHAR(255);
ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- mr_task_metrics (only queue; user_name already exists)
ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue VARCHAR(255);

-- ClickHouse equivalents (run separately):
-- ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE job_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE jvm_memory_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE jvm_gc_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE sql_query_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS app_name Nullable(String);
-- ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS user_name Nullable(String);
-- ALTER TABLE sql_query_table_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
-- ALTER TABLE mr_task_metrics ADD COLUMN IF NOT EXISTS queue Nullable(String);
