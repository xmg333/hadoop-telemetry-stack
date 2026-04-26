package x.mg.metrics.diagnostic.checks;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MySQL 检查器 — 验证表存在性、列结构完整性和数据写入状态
 */
public class MySQLChecker {

    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final int timeoutMs;

    /**
     * 每个表预期包含的列（与 CategoryJdbcSink DDL 对齐）。
     * 仅列出非 id 列；用于对比实际 schema。
     */
    private static final Map<String, String[]> EXPECTED_COLUMNS = new LinkedHashMap<>();
    static {
        EXPECTED_COLUMNS.put("spark_task_metrics", split(
            "timestamp_ms,app_id,executor_id,stage_id,task_id,task_success,task_host,task_locality,task_speculative," +
            "app_name,user_name,queue,duration_ms,io_bytes_read,io_bytes_written,io_records_read,io_records_written," +
            "shuffle_bytes_read,shuffle_bytes_written,shuffle_fetch_wait_time_ms,disk_bytes_spilled,memory_bytes_spilled," +
            "executor_run_time_ms,executor_cpu_time_ns,deserialize_time_ms,deserialize_cpu_time_ns," +
            "result_serialization_time_ms,jvm_gc_time_ms,scheduler_delay_ms,result_size_bytes," +
            "peak_execution_memory_bytes,shuffle_local_blocks_fetched,shuffle_records_read," +
            "shuffle_remote_bytes_read_to_disk,shuffle_remote_reqs_duration_ms"));

        EXPECTED_COLUMNS.put("spark_stage_metrics", split(
            "timestamp_ms,app_id,executor_id,stage_id,app_name,user_name,queue," +
            "duration_ms,num_tasks,executor_run_time_ms,executor_cpu_time_ns,jvm_gc_time_ms," +
            "peak_execution_memory_bytes,io_bytes_read,io_bytes_written"));

        EXPECTED_COLUMNS.put("spark_job_metrics", split(
            "timestamp_ms,app_id,job_id,job_success,app_name,user_name,queue," +
            "duration_ms,num_stages"));

        EXPECTED_COLUMNS.put("spark_jvm_memory", split(
            "timestamp_ms,app_id,executor_id,app_name,user_name,queue,heap_used,non_heap_used"));

        EXPECTED_COLUMNS.put("spark_jvm_gc", split(
            "timestamp_ms,app_id,executor_id,gc_name,app_name,user_name,queue,gc_count,gc_time_ms"));

        EXPECTED_COLUMNS.put("spark_task_histogram", split(
            "timestamp_ms,app_id,executor_id,stage_id,task_id,task_success,metric_name,bucket_le,bucket_count"));

        EXPECTED_COLUMNS.put("spark_stage_histogram", split(
            "timestamp_ms,app_id,executor_id,stage_id,metric_name,bucket_le,bucket_count"));

        EXPECTED_COLUMNS.put("spark_job_histogram", split(
            "timestamp_ms,app_id,job_id,job_success,metric_name,bucket_le,bucket_count"));

        EXPECTED_COLUMNS.put("spark_stage_skew", split(
            "timestamp_ms,app_id,stage_id,task_count,stage_duration_ms,avg_task_duration_ms," +
            "max_task_duration_ms,min_task_duration_ms,duration_skew_ratio," +
            "total_bytes_read,total_bytes_written,total_shuffle_bytes_read,total_shuffle_bytes_written," +
            "total_records_read,total_records_written,io_read_skew_ratio,io_write_skew_ratio," +
            "shuffle_read_skew_ratio,avg_output_bytes_per_task,avg_output_records_per_task," +
            "small_output_task_count,cpu_efficiency,gc_overhead_ratio,shuffle_wait_ratio," +
            "spill_ratio,deserialize_overhead,scheduler_delay_ratio,max_peak_memory_bytes,total_memory_spilled"));

        EXPECTED_COLUMNS.put("spark_sql_metrics", split(
            "timestamp_ms,app_id,execution_id,app_name,user_name,queue," +
            "duration_ms,shuffle_bytes_read,shuffle_bytes_written,join_count,query_text"));

        EXPECTED_COLUMNS.put("spark_sql_table", split(
            "timestamp_ms,app_id,execution_id,table_name,operation,app_name,user_name,queue," +
            "bytes,rows,files_read,time_ms"));

        EXPECTED_COLUMNS.put("hive_query_metrics", split(
            "timestamp_ms,query_id,operation,user_name,success,duration_ms," +
            "success_count,failure_count,input_bytes,output_bytes,input_rows,output_rows," +
            "execution_engine,query_text"));

        EXPECTED_COLUMNS.put("hive_query_table", split(
            "timestamp_ms,query_id,table_name,table_type,operation,user_name," +
            "input_table_count,output_table_count,execution_engine"));

        EXPECTED_COLUMNS.put("mr_job_metrics", split(
            "timestamp_ms,job_id,job_name,user_name,state,queue," +
            "hdfs_bytes_read,hdfs_bytes_written,file_bytes_read,file_bytes_written," +
            "map_input_records,map_output_records,map_output_bytes," +
            "reduce_input_records,reduce_output_records,reduce_shuffle_bytes,spilled_records," +
            "cpu_time_ms,gc_time_ms,physical_memory_bytes,virtual_memory_bytes,committed_heap_bytes," +
            "maps_duration_ms,reduces_duration_ms,elapsed_time_ms,launched_maps,launched_reduces," +
            "start_time_ms,finish_time_ms"));

        EXPECTED_COLUMNS.put("mr_task_metrics", split(
            "timestamp_ms,task_id,task_type,job_id,job_name,user_name,state,queue," +
            "hdfs_bytes_read,hdfs_bytes_written,file_bytes_read,file_bytes_written," +
            "map_input_records,map_output_records,map_output_bytes," +
            "reduce_input_records,reduce_output_records,reduce_shuffle_bytes,spilled_records," +
            "cpu_time_ms,gc_time_ms,duration_ms,success_count,failure_count," +
            "hdfs_read_ops,hdfs_write_ops,hdfs_large_read_ops"));

        EXPECTED_COLUMNS.put("unified_metrics", split(
            "timestamp_ms,event_type,engine,status,app_id,app_name,user_name,queue," +
            "duration_ms,bytes_read,bytes_written,shuffle_bytes_read,shuffle_bytes_written," +
            "cpu_time_ms,gc_time_ms,bytes_spilled," +
            "executor_id,stage_id,task_id,task_host,task_locality,task_speculative," +
            "executor_run_time_ms,executor_cpu_time_ns,deserialize_time_ms,deserialize_cpu_time_ns," +
            "result_serialization_time_ms,scheduler_delay_ms,result_size_bytes,peak_execution_memory_bytes," +
            "shuffle_local_blocks_fetched,shuffle_records_read," +
            "shuffle_remote_bytes_read_to_disk,shuffle_remote_reqs_duration_ms," +
            "disk_bytes_spilled,shuffle_fetch_wait_time_ms,num_tasks,num_stages," +
            "execution_id,join_count," +
            "table_name,table_operation,bytes,rows,files_read,time_ms," +
            "heap_used,non_heap_used,gc_name,gc_count," +
            "job_id,job_name,task_type,map_output_bytes," +
            "physical_memory_bytes,virtual_memory_bytes,committed_heap_bytes," +
            "maps_duration_ms,reduces_duration_ms,launched_maps,launched_reduces," +
            "start_time_ms,finish_time_ms," +
            "hdfs_bytes_read,hdfs_bytes_written,file_bytes_read,file_bytes_written," +
            "map_input_records,map_output_records,reduce_input_records,reduce_output_records," +
            "reduce_shuffle_bytes,spilled_records," +
            "hdfs_read_ops,hdfs_write_ops,hdfs_large_read_ops," +
            "operation,table_type,execution_engine,success_count,failure_count," +
            "input_rows,output_rows,records_read,records_written,query_text"));
    }

    private static String[] split(String s) { return s.split(","); }

    public MySQLChecker(String host, int port, String database, String username, String password, int timeoutMs) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.timeoutMs = timeoutMs;
    }

    public List<CheckItem> check() {
        List<CheckItem> items = new ArrayList<>();
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s?connectTimeout=%d&socketTimeout=%d",
            host, port, database, timeoutMs, timeoutMs);

        items.add(CheckItem.ok("JDBC URL: " + jdbcUrl));

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            items.add(CheckItem.ok("连接成功"));

            // 1. 查询所有已存在的表
            Set<String> existingTables = new HashSet<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + database + "'")) {
                while (rs.next()) existingTables.add(rs.getString("TABLE_NAME"));
            }

            // 2. 检查缺失的表
            List<String> missingTables = new ArrayList<>();
            for (String table : EXPECTED_COLUMNS.keySet()) {
                if (!existingTables.contains(table)) missingTables.add(table);
            }

            if (!missingTables.isEmpty()) {
                items.add(CheckItem.fail("缺失 " + missingTables.size() + " 个表: " + String.join(", ", missingTables),
                    "Flink Consumer 启动时会自动建表。请重启 Flink Consumer：\n" +
                    "  java -jar metrics-flink-consumer-dist.jar flink-consumer.conf\n" +
                    "或手动建表：参见 CategoryJdbcSink.createMySqlTables() 中的 DDL"));
            } else {
                items.add(CheckItem.ok("表结构: " + EXPECTED_COLUMNS.size() + " 个表全部存在"));
            }

            // 3. 逐表验证列完整性
            Map<String, Set<String>> actualColumns = loadActualColumns(conn);

            int tablesWithMissingCols = 0;
            List<String> allMissingCols = new ArrayList<>();
            for (Map.Entry<String, String[]> entry : EXPECTED_COLUMNS.entrySet()) {
                String table = entry.getKey();
                if (!existingTables.contains(table)) continue;

                Set<String> actual = actualColumns.getOrDefault(table, Collections.emptySet());
                List<String> missing = new ArrayList<>();
                for (String col : entry.getValue()) {
                    if (!actual.contains(col)) missing.add(col);
                }

                if (!missing.isEmpty()) {
                    tablesWithMissingCols++;
                    allMissingCols.add(table + ": " + String.join(", ", missing));
                }
            }

            if (tablesWithMissingCols > 0) {
                items.add(CheckItem.fail(
                    tablesWithMissingCols + " 个表缺少列（会导致 Flink Consumer 写入失败）:\n    " +
                    String.join("\n    ", allMissingCols),
                    "表结构与 Flink Consumer JAR 不匹配，原因：旧版 JAR 建表后升级了 JAR 但未更新表结构。\n" +
                    "修复方案（二选一）：\n" +
                    "  方案 1 (推荐): 删除所有表后重启 Flink Consumer 自动重建\n" +
                    "    mysql -u metrics -p telemetry -e \"DROP TABLE spark_task_metrics, spark_stage_metrics, spark_job_metrics, " +
                    "spark_jvm_memory, spark_jvm_gc, spark_task_histogram, spark_stage_histogram, " +
                    "spark_job_histogram, spark_stage_skew, spark_sql_metrics, spark_sql_table, " +
                    "hive_query_metrics, hive_query_table, mr_job_metrics, mr_task_metrics, unified_metrics\"\n" +
                    "    rm -f /tmp/flink-consumer-checkpoint.txt\n" +
                    "    重启 Flink Consumer\n" +
                    "  方案 2: 手动执行 ALTER TABLE 添加缺失列\n" +
                    "    参见 CategoryJdbcSink.migrateMySqlSchema() 中的迁移语句"));
            } else if (existingTables.containsAll(EXPECTED_COLUMNS.keySet())) {
                items.add(CheckItem.ok("列结构: " + EXPECTED_COLUMNS.size() + " 个表的列全部完整"));
            }

            // 4. 检查最近数据
            if (existingTables.contains("spark_task_metrics")) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(
                         "SELECT COUNT(*) as cnt FROM spark_task_metrics WHERE " +
                         "timestamp_ms >= UNIX_TIMESTAMP(NOW()) * 1000 - 1800000")) {
                    if (rs.next() && rs.getLong("cnt") > 0) {
                        items.add(CheckItem.ok("最近 30 分钟有 " + rs.getLong("cnt") + " 条 task_metrics 数据"));
                    } else {
                        items.add(CheckItem.warn("最近 30 分钟无 task_metrics 数据",
                            "请检查上游（Spark/Hive/MR Collector）是否正在发送数据"));
                    }
                }
            }

            // 5. unified_metrics 宽表数据检查
            if (existingTables.contains("unified_metrics")) {
                long metricEventsCount = 0;
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM unified_metrics")) {
                    if (rs.next()) metricEventsCount = rs.getLong("cnt");
                }

                long categoryCount = 0;
                String[] categoryTables = {"spark_task_metrics", "mr_task_metrics", "hive_query_metrics"};
                for (String t : categoryTables) {
                    if (existingTables.contains(t)) {
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM " + t)) {
                            if (rs.next()) categoryCount += rs.getLong("cnt");
                        }
                    }
                }

                if (metricEventsCount > 0) {
                    items.add(CheckItem.ok("unified_metrics 宽表有 " + metricEventsCount + " 行数据"));
                } else if (categoryCount > 0) {
                    items.add(CheckItem.fail("unified_metrics 宽表为空 (0 行)，但分类表共有 " + categoryCount + " 行",
                        "Flink Consumer 未写入 unified_metrics 宽表。可能原因：\n" +
                        "  1. Flink Consumer JAR 版本过旧，不包含 unified_metrics 写入逻辑\n" +
                        "  2. 需要重新构建并部署最新的 Flink Consumer JAR\n" +
                        "  3. 构建命令：mvn clean package -pl flink/metrics-flink-consumer,flink/metrics-flink-consumer-dist -am -DskipTests"));
                } else {
                    items.add(CheckItem.warn("unified_metrics 宽表为空",
                        "暂无任何数据，请先确保上游（Spark/Hive/MR）正在发送指标"));
                }
            }

        } catch (Exception e) {
            items.add(CheckItem.fail("连接失败: " + e.getMessage(),
                "请检查 MySQL 服务：kubectl get pods -l app=mysql\n" +
                "  或检查 JDBC URL / 用户名 / 密码"));
        }

        return items;
    }

    /**
     * 从 INFORMATION_SCHEMA.COLUMNS 加载所有表的列名
     */
    private Map<String, Set<String>> loadActualColumns(Connection conn) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + database + "'")) {
            while (rs.next()) {
                result.computeIfAbsent(rs.getString("TABLE_NAME"), k -> new HashSet<>())
                      .add(rs.getString("COLUMN_NAME"));
            }
        }
        return result;
    }
}
