package x.mg.metrics.diagnostic.state.handlers;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.checks.CheckItem;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.AnsiColors;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaFuture;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 数据流验证 — 提交 Spark / MR / Hive 测试作业，验证所有指标类别
 */
public class DataFlowCheckHandler extends CheckHandler {

    private static final String APP_NAME = "diagnostic-test";
    private static final int SPARKPI_SLICES = 200;

    // All metric category flags
    private static final String[] ALL_METRIC_FLAGS = {
        "--conf", "spark.telemetry.metrics.task.execution=true",
        "--conf", "spark.telemetry.metrics.task.shuffle-extended=true",
        "--conf", "spark.telemetry.metrics.task.info=true",
        "--conf", "spark.telemetry.metrics.stage.detailed=true",
        "--conf", "spark.telemetry.metrics.job.lifecycle=true",
        "--conf", "spark.telemetry.metrics.sql.query-execution=true"
    };

    // Common /opt paths to probe for Hadoop/Hive installations
    private static final String[] HADOOP_PROBE_PATHS = {"/opt/hadoop", "/opt/hadoop-3.2.0", "/opt/hadoop-3.4.3", "/opt/hadoop-2.7.0", "/opt/hadoop-3.4.0"};
    private static final String[] HIVE_PROBE_PATHS = {"/opt/hive", "/opt/apache-hive-2.3.9-bin", "/opt/apache-hive-3.1.3-bin"};
    private static final String[] HIVE_HOOK_JAR_PROBES = {
        "/opt/hive-telemetry-hook.jar",
        "/opt/apache-hive-2.3.9-bin/lib/hive-telemetry-hook.jar",
        "/opt/apache-hive-3.1.3-bin/lib/hive-telemetry-hook.jar"
    };
    private static final String[] HIBENCH_PROBE_PATHS = {"/root/hibench", "/opt/hibench", "/opt/HiBench", "/root/HiBench"};

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        Terminal terminal = context.getTerminal();
        List<CheckItem> items = new ArrayList<>();

        String sparkHome = System.getenv("SPARK_HOME");
        String javaHome = System.getenv("JAVA_HOME");
        String hadoopHome = detectHadoopHome();
        String hiveHome = detectHiveHome();
        String hibenchHome = detectHiBenchHome();
        java.io.File hiveHookJar = findHiveHookJar();

        if (sparkHome == null || sparkHome.isEmpty()) {
            items.add(CheckItem.skip("SPARK_HOME 未设置，跳过 Spark 测试"));
            CheckPrinter.print(terminal, items);
            items.clear();
        }
        if (hadoopHome == null) {
            items.add(CheckItem.skip("未检测到 Hadoop 安装，跳过 MR 测试"));
            CheckPrinter.print(terminal, items);
            items.clear();
        }
        if (hiveHome == null || hiveHookJar == null) {
            items.add(CheckItem.skip("未检测到 Hive 或 Hive Hook JAR，跳过 Hive 测试"));
            CheckPrinter.print(terminal, items);
            items.clear();
        }
        if (sparkHome == null && hadoopHome == null && hiveHome == null) {
            items.add(CheckItem.fail("无可用测试环境 (需要 SPARK_HOME 或 Hadoop 或 Hive)",
                "请设置 SPARK_HOME 环境变量，或在 /opt 下安装 Hadoop/Hive"));
            CheckPrinter.print(terminal, items);
            return DiagnosticState.GENERATE_REPORT;
        }

        // 1. 记录验证前各表行数
        terminal.writer().println(AnsiColors.DIM + "  读取验证前各表行数..." + AnsiColors.RESET);
        terminal.writer().flush();
        Map<String, Long> beforeCounts = getAllTableCounts(context);
        items.add(CheckItem.ok("验证前行数: " + formatCounts(beforeCounts)));

        // 2. 记录验证前 Kafka offset
        long beforeKafkaOffset = getKafkaEndOffset(context);

        // 3. 查找 Spark JAR
        java.io.File examplesJar = null;
        java.io.File pluginJar = null;
        String otelEndpoint = context.getConfig().getOtelCollectorEndpoint();
        boolean sparkReady = false;

        if (sparkHome != null && !sparkHome.isEmpty()) {
            examplesJar = findExamplesJar(sparkHome);
            String sparkVersion = detectSparkVersion(sparkHome);
            pluginJar = findPluginJar(sparkHome, sparkVersion);

            if (examplesJar == null) {
                items.add(CheckItem.warn("未找到 spark-examples JAR", "请检查 SPARK_HOME/examples/jars/"));
            } else if (pluginJar == null) {
                items.add(CheckItem.warn("未找到匹配 Spark " + sparkVersion + " 的 spark-telemetry JAR",
                    "请将 spark-telemetry-dist JAR 复制到 $SPARK_HOME/jars/"));
            } else {
                sparkReady = true;
            }
        }

        // 4. 提交 RDD 测试 (SparkPi) — 产生 task/stage/job/jvm metrics
        boolean rddSuccess = false;
        if (sparkReady) {
            items.add(CheckItem.ok("提交 Spark RDD 测试 (SparkPi, slices=" + SPARKPI_SLICES + ")..."));
            CheckPrinter.print(terminal, items);
            items.clear();

            SubmitResult rddResult = submitSparkPi(sparkHome, javaHome, otelEndpoint, examplesJar, pluginJar);
            if (rddResult.success) {
                items.add(CheckItem.ok("RDD 作业成功 (耗时 " + rddResult.durationMs + "ms)"));
                rddSuccess = true;
            } else {
                items.add(CheckItem.warn("RDD 作业失败: " + rddResult.error,
                    "Spark 指标无法生成。继续测试 MR/Hive"));
            }

            // 5. 提交 SQL 测试 — 产生 sql_query_metrics (only if RDD succeeded)
            if (rddSuccess) {
                items.add(CheckItem.ok("提交 Spark SQL 测试..."));
                CheckPrinter.print(terminal, items);
                items.clear();

                SubmitResult sqlResult = submitSparkSql(sparkHome, javaHome, otelEndpoint, pluginJar);
                if (sqlResult.success) {
                    items.add(CheckItem.ok("SQL 测试成功 (耗时 " + sqlResult.durationMs + "ms)"));
                } else {
                    items.add(CheckItem.warn("SQL 测试失败: " + sqlResult.error,
                        "SQL 指标无法生成。不影响其他指标类别"));
                }
            }
        }

        // 6. 启动 MR Collector + 提交 MR 测试作业
        Process mrCollectorProcess = null;
        if (hadoopHome != null) {
            // Start MR Collector to poll HistoryServer for completed jobs
            java.io.File mrCollectorJar = findMrCollectorJar();
            if (mrCollectorJar != null) {
                mrCollectorProcess = startMrCollector(mrCollectorJar, hadoopHome, javaHome, otelEndpoint, context);
                if (mrCollectorProcess != null) {
                    items.add(CheckItem.ok("MR Collector 已启动 (等待轮询...)"));
                    try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
                }
            }

            items.add(CheckItem.ok("提交 MapReduce 测试作业 (YARN)..."));
            CheckPrinter.print(terminal, items);
            items.clear();

            SubmitResult mrResult = submitMrJob(hadoopHome, javaHome, otelEndpoint);
            if (mrResult.success) {
                items.add(CheckItem.ok("MR 作业成功 (耗时 " + mrResult.durationMs + "ms)"));
                // Wait for MR Collector to poll and export
                if (mrCollectorProcess != null) {
                    terminal.writer().println(AnsiColors.DIM + "  等待 MR Collector 轮询并导出 (30s)..." + AnsiColors.RESET);
                    terminal.writer().flush();
                    try { Thread.sleep(30000); } catch (InterruptedException ignored) {}
                }
            } else {
                items.add(CheckItem.warn("MR 作业失败: " + mrResult.error,
                    "MR 指标无法生成。不影响 Spark 指标类别"));
            }
        } else {
            items.add(CheckItem.skip("未检测到 Hadoop 安装，跳过 MR 测试"));
        }

        // 7. 提交 Hive 测试查询
        if (hiveHome != null && hiveHookJar != null) {
            items.add(CheckItem.ok("提交 Hive 测试查询..."));
            CheckPrinter.print(terminal, items);
            items.clear();

            SubmitResult hiveResult = submitHiveQuery(hiveHome, hadoopHome, javaHome, hiveHookJar, otelEndpoint);
            if (hiveResult.success) {
                items.add(CheckItem.ok("Hive 查询成功 (耗时 " + hiveResult.durationMs + "ms)"));
            } else {
                items.add(CheckItem.warn("Hive 查询失败: " + hiveResult.error,
                    "Hive 指标无法生成。不影响其他指标类别"));
            }
        } else {
            String reason = hiveHome == null ? "未检测到 Hive 安装" : "未找到 hive-telemetry-hook JAR";
            items.add(CheckItem.skip(reason + "，跳过 Hive 测试"));
        }

        // 8. 运行HiBench测试（如果可用）
        if (hibenchHome != null && hadoopHome != null) {
            runHiBenchTests(hibenchHome, hadoopHome, javaHome, items, terminal);
        } else {
            items.add(CheckItem.skip("未检测到 HiBench 或 Hadoop，跳过 HiBench 测试"));
        }

        CheckPrinter.print(terminal, items);
        items.clear();

        // 8. 等待数据流经全链路
        terminal.writer().println(AnsiColors.DIM + "  等待 OTel SDK 导出 (15s)..." + AnsiColors.RESET);
        terminal.writer().flush();
        try { Thread.sleep(15000); } catch (InterruptedException ignored) {}

        CheckPrinter.print(terminal, items);
        items.clear();

        // 9. 检查 Kafka 新消息
        long afterKafkaOffset = -1;
        boolean kafkaNewData = false;
        for (int retry = 0; retry < 6; retry++) {
            try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            afterKafkaOffset = getKafkaEndOffset(context);
            if (beforeKafkaOffset >= 0 && afterKafkaOffset > beforeKafkaOffset) {
                kafkaNewData = true;
                break;
            }
        }

        if (kafkaNewData) {
            long newMessages = afterKafkaOffset - beforeKafkaOffset;
            items.add(CheckItem.ok("Kafka 新增 " + newMessages + " 条消息 (offset: " + beforeKafkaOffset + " → " + afterKafkaOffset + ")"));
            items.add(CheckItem.ok("Spark/Hive/MR → OTel Collector → Kafka 链路验证通过"));
        } else {
            items.add(CheckItem.warn("Kafka 无新增消息",
                "请检查 OTel Collector 的 Kafka exporter 配置"));
        }
        CheckPrinter.print(terminal, items);
        items.clear();

        // 10. 等待 Flink Consumer 消费
        if (kafkaNewData) {
            terminal.writer().println(AnsiColors.DIM + "  等待 Flink Consumer 消费 (20s)..." + AnsiColors.RESET);
            terminal.writer().flush();
            try { Thread.sleep(20000); } catch (InterruptedException ignored) {}
        }

        // 11. 逐表验证所有指标类别
        Map<String, Long> afterCounts = getAllTableCounts(context);
        terminal.writer().println(AnsiColors.DIM + "  验证各表数据..." + AnsiColors.RESET);
        terminal.writer().flush();

        String[] checkTables = {
            "spark_task_metrics", "spark_stage_metrics", "spark_job_metrics",
            "spark_jvm_memory", "spark_jvm_gc",
            "spark_sql_metrics", "spark_sql_table",
            "mr_job_metrics", "mr_task_metrics",
            "hive_query_metrics", "hive_query_table",
            "unified_metrics"
        };

        int passCount = 0;
        int failCount = 0;
        for (String table : checkTables) {
            long before = beforeCounts.getOrDefault(table, 0L);
            long after = afterCounts.getOrDefault(table, 0L);
            long delta = after - before;
            if (delta > 0) {
                items.add(CheckItem.ok(table + ": +" + delta + " 行 (共 " + after + " 行)"));
                passCount++;
            } else if (after > 0) {
                items.add(CheckItem.warn(table + ": 0 行新增 (共 " + after + " 行，可能复用已有数据)",
                    null));
                passCount++;
            } else {
                items.add(CheckItem.fail(table + ": 0 行 (无数据)",
                    getTableEmptyHint(table)));
                failCount++;
            }
        }

        if (failCount == 0) {
            items.add(CheckItem.ok("所有 " + passCount + " 个指标表验证通过"));
            items.add(CheckItem.ok("端到端数据流验证通过: Spark/Hive/MR → OTel Collector → Kafka → MySQL"));
        } else {
            items.add(CheckItem.warn(passCount + "/" + checkTables.length + " 个指标表有数据，" + failCount + " 个为空",
                "空表可能原因：\n" +
                "  1. 对应指标类别未开启\n" +
                "  2. 作业未使用该功能（如 SQL 表需要执行 SQL 查询）\n" +
                "  3. 数据尚未写入（等待时间不足）"));
        }

        // 12. 检查关键列系统性为 NULL
        List<CheckItem> nullChecks = checkNullColumns(context);
        items.addAll(nullChecks);

        CheckPrinter.print(terminal, items);

        // Cleanup: stop MR Collector process
        if (mrCollectorProcess != null && mrCollectorProcess.isAlive()) {
            mrCollectorProcess.destroyForcibly();
        }

        return DiagnosticState.GENERATE_REPORT;
    }

    private String getTableEmptyHint(String table) {
        switch (table) {
            case "spark_task_metrics": return "Task 指标需要 Spark 作业运行（默认开启）";
            case "spark_stage_metrics": return "Stage 指标需要 spark.telemetry.metrics.stage.detailed=true";
            case "spark_job_metrics": return "Job 指标需要 spark.telemetry.metrics.job.lifecycle=true";
            case "spark_jvm_memory": return "JVM 内存指标由 ExecutorPlugin 自动采集";
            case "spark_jvm_gc": return "JVM GC 指标由 ExecutorPlugin 自动采集";
            case "spark_sql_metrics": return "SQL 指标需要 spark.telemetry.metrics.sql.query-execution=true 且作业执行 SQL 查询";
            case "spark_sql_table": return "SQL Table IO 指标需要对实际数据源(parquet/orc/json等)执行 SQL 读写";
            case "mr_job_metrics": return "MR Job 指标需要 MR Collector 运行并轮询 HistoryServer (端口 19888)";
            case "mr_task_metrics": return "MR Task 指标需要 MR Collector 启用 task.counters=true 且 MR 作业完成";
            case "hive_query_metrics": return "Hive 指标需要 Hive Hook 配置 (hive.exec.post.hooks) 且 Hive 查询执行";
            case "hive_query_table": return "Hive Table IO 指标需要 Hive Hook 启用 query.tables=true 且查询涉及实际表";
            case "unified_metrics": return "unified_metrics 宽表与分类表同步写入，如分类表有数据但宽表为空请重新部署 Flink Consumer";
            default: return null;
        }
    }

    private Map<String, Long> getAllTableCounts(DiagnosticContext context) {
        Map<String, Long> counts = new HashMap<>();
        String[] tables = {
            "spark_task_metrics", "spark_stage_metrics", "spark_job_metrics",
            "spark_jvm_memory", "spark_jvm_gc",
            "spark_sql_metrics", "spark_sql_table",
            "hive_query_metrics", "hive_query_table",
            "mr_job_metrics", "mr_task_metrics",
            "spark_stage_skew", "unified_metrics",
            "spark_task_histogram", "spark_stage_histogram", "spark_job_histogram"
        };
        String url = String.format("jdbc:mysql://%s:%d/telemetry?connectTimeout=3000",
            context.getConfig().getMysqlHost(), context.getConfig().getMysqlPort());
        try (Connection conn = DriverManager.getConnection(url,
            context.getConfig().getMysqlUsername(), context.getConfig().getMysqlPassword())) {
            for (String table : tables) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (rs.next()) counts.put(table, rs.getLong(1));
                } catch (SQLException ignored) {
                    counts.put(table, -1L);
                }
            }
        } catch (Exception ignored) {}
        return counts;
    }

    private String formatCounts(Map<String, Long> counts) {
        StringBuilder sb = new StringBuilder();
        String[] core = {
            "spark_task_metrics", "spark_stage_metrics", "spark_job_metrics",
            "spark_jvm_memory", "spark_jvm_gc",
            "spark_sql_metrics", "spark_sql_table",
            "mr_job_metrics", "mr_task_metrics",
            "hive_query_metrics", "hive_query_table",
            "unified_metrics"
        };
        for (int i = 0; i < core.length; i++) {
            if (i > 0) sb.append(", ");
            long c = counts.getOrDefault(core[i], -1L);
            String shortName = core[i].replace("_metrics", "").replace("unified_metrics", "unified");
            sb.append(shortName).append("=").append(c >= 0 ? c : "?");
        }
        return sb.toString();
    }

    /**
     * 检查有数据的表中，关键列是否系统性为 NULL。
     * 例如 hive_query_metrics 有行但 input_bytes/output_bytes 全为 NULL。
     */
    private List<CheckItem> checkNullColumns(DiagnosticContext context) {
        List<CheckItem> items = new ArrayList<>();
        // Map: table -> critical columns that should not be all-NULL
        Map<String, String[]> nullCheckTables = new LinkedHashMap<>();
        nullCheckTables.put("hive_query_metrics", new String[]{
            "input_bytes", "output_bytes", "input_rows", "output_rows"});
        nullCheckTables.put("mr_job_metrics", new String[]{
            "hdfs_bytes_read", "hdfs_bytes_written", "cpu_time_ms", "elapsed_time_ms"});
        nullCheckTables.put("spark_sql_metrics", new String[]{
            "duration_ms", "shuffle_bytes_read", "shuffle_bytes_written", "join_count"});
        nullCheckTables.put("spark_sql_table", new String[]{
            "bytes", "rows", "files_read"});
        nullCheckTables.put("spark_task_metrics", new String[]{
            "duration_ms", "executor_run_time_ms", "executor_cpu_time_ns"});

        String url = String.format("jdbc:mysql://%s:%d/telemetry?connectTimeout=3000",
            context.getConfig().getMysqlHost(), context.getConfig().getMysqlPort());
        try (Connection conn = DriverManager.getConnection(url,
            context.getConfig().getMysqlUsername(), context.getConfig().getMysqlPassword())) {
            for (Map.Entry<String, String[]> entry : nullCheckTables.entrySet()) {
                String table = entry.getKey();
                String[] cols = entry.getValue();
                // Only check tables that have data
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                    if (!rs.next() || rs.getLong(1) == 0) continue;
                } catch (SQLException ignored) { continue; }

                // Check each critical column for all-NULL
                List<String> allNullCols = new ArrayList<>();
                for (String col : cols) {
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(
                             "SELECT COUNT(*) FROM " + table + " WHERE " + col + " IS NOT NULL")) {
                        if (rs.next() && rs.getLong(1) == 0) {
                            allNullCols.add(col);
                        }
                    } catch (SQLException ignored) {}
                }

                if (!allNullCols.isEmpty()) {
                    items.add(CheckItem.warn(
                        table + ": 有数据但以下列全为 NULL: " + String.join(", ", allNullCols),
                        getNullColumnHint(table, allNullCols)));
                }
            }
        } catch (Exception ignored) {}
        return items;
    }

    private String getNullColumnHint(String table, List<String> nullCols) {
        switch (table) {
            case "hive_query_metrics":
                return "Hive IO 指标来自 metastore 表统计 (totalSize/numRows)，非精确 MR counters。\n" +
                    "  - 依赖 Hive StatsTask 在 DML 后更新统计信息\n" +
                    "  - input_bytes 为输入表总大小（近似扫描量）\n" +
                    "  - 如统计全为 NULL，检查 Hive 是否启用自动统计收集";
            case "mr_job_metrics":
                return "MR Collector 从 HistoryServer REST API 获取 counters。\n" +
                    "  - 检查 HistoryServer 是否运行 (端口 19888)\n" +
                    "  - 检查 collection.job.counters=true 配置";
            case "spark_sql_metrics":
                return "SQL 指标列取决于查询类型。简单查询可能不产生 shuffle 数据";
            case "spark_sql_table":
                return "SQL Table IO 指标需要对实际数据源(parquet/orc)执行读写。\n" +
                    "  - 临时视图 (createOrReplaceTempView) 不产生 table IO 指标\n" +
                    "  - 需要对文件格式数据源执行读写操作";
            default:
                return null;
        }
    }

    private SubmitResult submitSparkPi(String sparkHome, String javaHome, String otelEndpoint,
                                        java.io.File examplesJar, java.io.File pluginJar) {
        List<String> cmd = new ArrayList<>();
        cmd.add(sparkHome + "/bin/spark-submit");
        cmd.add("--master"); cmd.add("yarn");
        cmd.add("--deploy-mode"); cmd.add("client");
        cmd.add("--class"); cmd.add("org.apache.spark.examples.SparkPi");
        cmd.add("--jars"); cmd.add(pluginJar.getAbsolutePath());
        cmd.add("--conf"); cmd.add("spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin");
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.exporter.endpoint=" + otelEndpoint);
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.service.name=" + APP_NAME + "-rdd");
        cmd.add("--conf"); cmd.add("spark.telemetry.otel.export.interval.ms=5000");
        for (String flag : ALL_METRIC_FLAGS) cmd.add(flag);
        cmd.add(examplesJar.getAbsolutePath());
        cmd.add(String.valueOf(SPARKPI_SLICES));
        return runCommand(cmd, sparkHome, null, javaHome);
    }

    private SubmitResult submitSparkSql(String sparkHome, String javaHome, String otelEndpoint,
                                          java.io.File pluginJar) {
        // Write PySpark SQL test script that reads/writes actual files for table IO metrics
        // Include JOIN queries to generate join_count metrics
        String pysparkScript =
            "from pyspark.sql import SparkSession\n" +
            "import pyspark.sql.functions as F\n" +
            "import tempfile, os, shutil\n" +
            "spark = SparkSession.builder.appName('diag-sql-test').getOrCreate()\n" +
            "tmpdir = tempfile.mkdtemp()\n" +
            "try:\n" +
            "  # Create two tables for JOIN\n" +
            "  df1 = spark.range(100).withColumn('name', F.concat(F.lit('name_'), F.col('id')))\n" +
            "  df2 = spark.range(50).withColumn('value', F.col('id') * 10)\n" +
            "  # Write parquet (generates table IO write metrics)\n" +
            "  df1.write.mode('overwrite').parquet(tmpdir + '/t1')\n" +
            "  df2.write.mode('overwrite').parquet(tmpdir + '/t2')\n" +
            "  # Read parquet (generates table IO scan metrics)\n" +
            "  t1 = spark.read.parquet(tmpdir + '/t1')\n" +
            "  t2 = spark.read.parquet(tmpdir + '/t2')\n" +
            "  t1.createOrReplaceTempView('t1')\n" +
            "  t2.createOrReplaceTempView('t2')\n" +
            "  # SQL queries against parquet-backed tables\n" +
            "  spark.sql('SELECT COUNT(*) FROM t1').collect()\n" +
            "  spark.sql('SELECT id, name FROM t1 WHERE id > 50 ORDER BY id').collect()\n" +
            "  spark.sql('SELECT name, SUM(id) as total FROM t1 GROUP BY name').collect()\n" +
            "  # JOIN query (generates join_count metrics)\n" +
            "  spark.sql('SELECT t1.id, t1.name, t2.value FROM t1 JOIN t2 ON t1.id = t2.id').collect()\n" +
            "  spark.sql('SELECT t1.id, t1.name, t2.value FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL').collect()\n" +
            "  # Write result (generates another write metric)\n" +
            "  spark.sql('SELECT * FROM t1 WHERE id < 10').write.mode('overwrite').parquet(tmpdir + '/t3')\n" +
            "finally:\n" +
            "  shutil.rmtree(tmpdir, ignore_errors=True)\n" +
            "spark.stop()\n";

        try {
            java.io.File scriptFile = new java.io.File(System.getProperty("java.io.tmpdir"), "diag_sql_test.py");
            java.io.PrintWriter pw = new java.io.PrintWriter(scriptFile);
            pw.print(pysparkScript);
            pw.close();

            List<String> cmd = new ArrayList<>();
            cmd.add(sparkHome + "/bin/spark-submit");
            cmd.add("--master"); cmd.add("yarn");
            cmd.add("--deploy-mode"); cmd.add("client");
            cmd.add("--jars"); cmd.add(pluginJar.getAbsolutePath());
            cmd.add("--conf"); cmd.add("spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin");
            cmd.add("--conf"); cmd.add("spark.telemetry.otel.exporter.endpoint=" + otelEndpoint);
            cmd.add("--conf"); cmd.add("spark.telemetry.otel.service.name=" + APP_NAME + "-sql");
            cmd.add("--conf"); cmd.add("spark.telemetry.otel.export.interval.ms=5000");
            for (String flag : ALL_METRIC_FLAGS) cmd.add(flag);
            cmd.add(scriptFile.getAbsolutePath());

            SubmitResult result = runCommand(cmd, sparkHome, javaHome);
            scriptFile.delete();
            return result;
        } catch (Exception e) {
            SubmitResult r = new SubmitResult();
            r.error = e.getMessage();
            return r;
        }
    }

    private SubmitResult runCommand(List<String> cmd, String sparkHome, String javaHome) {
        return runCommand(cmd, sparkHome, null, javaHome);
    }

    private SubmitResult runCommand(List<String> cmd, String sparkHome, String hadoopHome, String javaHome) {
        return runCommandWithEnv(cmd, sparkHome, hadoopHome, javaHome, null, null);
    }

    private SubmitResult runCommandWithEnv(List<String> cmd, String sparkHome, String hadoopHome, String javaHome,
                                            String extraEnvKey, String extraEnvValue) {
        SubmitResult result = new SubmitResult();
        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            if (javaHome != null) pb.environment().put("JAVA_HOME", javaHome);
            if (sparkHome != null) pb.environment().put("SPARK_HOME", sparkHome);
            if (hadoopHome != null) pb.environment().put("HADOOP_HOME", hadoopHome);
            if (extraEnvKey != null && extraEnvValue != null) {
                pb.environment().put(extraEnvKey, extraEnvValue);
            }
            pb.redirectErrorStream(true);

            long start = System.currentTimeMillis();
            Process proc = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            // Wait with timeout
            boolean finished = proc.waitFor(120, java.util.concurrent.TimeUnit.SECONDS);
            result.durationMs = System.currentTimeMillis() - start;

            if (!finished) {
                proc.destroyForcibly();
                result.error = "timeout (120s)";
                return result;
            }

            int exitCode = proc.exitValue();
            result.output = output.toString();

            if (exitCode == 0) {
                result.success = true;
            } else {
                result.error = "exit code " + exitCode;
                String[] lines = output.toString().split("\n");
                StringBuilder errTail = new StringBuilder();
                for (String l : lines) {
                    if (l.contains("Exception") || l.contains("Caused by:") || l.contains("NoSuchMethod") || l.contains("ClassNotFound") || l.contains("NoClassDefFound")) {
                        errTail.append(l.trim()).append("\n");
                    }
                }
                if (errTail.length() == 0) {
                    int startLine = Math.max(0, lines.length - 3);
                    for (int i = startLine; i < lines.length; i++) {
                        if (!lines[i].trim().isEmpty()) errTail.append(lines[i].trim()).append("\n");
                    }
                }
                if (errTail.length() > 0) {
                    result.error += "\n" + errTail.toString().trim();
                }
            }
        } catch (Exception e) {
            result.error = e.getMessage();
        }
        return result;
    }

    private java.io.File findExamplesJar(String sparkHome) {
        java.io.File dir = new java.io.File(sparkHome, "examples/jars");
        if (!dir.isDirectory()) return null;
        java.io.File[] files = dir.listFiles((d, name) -> name.startsWith("spark-examples") && name.endsWith(".jar"));
        return (files != null && files.length > 0) ? files[0] : null;
    }

    private String detectSparkVersion(String sparkHome) {
        java.io.File releaseFile = new java.io.File(sparkHome, "RELEASE");
        if (releaseFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new java.io.FileInputStream(releaseFile)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("version=")) return line.substring("version=".length()).trim();
                    if (line.contains("Spark ")) {
                        String[] parts = line.split("Spark ");
                        if (parts.length > 1) return parts[parts.length - 1].trim();
                    }
                }
            } catch (Exception ignored) {}
        }
        java.io.File ex = findExamplesJar(sparkHome);
        if (ex != null) {
            String name = ex.getName();
            int dash = name.lastIndexOf('-');
            if (dash > 0) return name.substring(dash + 1, name.length() - 4);
        }
        return "unknown";
    }

    private java.io.File findPluginJar(String sparkHome, String sparkVersion) {
        java.io.File dir = new java.io.File(sparkHome, "jars");
        if (!dir.isDirectory()) return null;
        java.io.File[] allTelemetry = dir.listFiles((d, name) ->
            name.contains("spark-telemetry") && name.endsWith(".jar") && !name.contains("omni"));
        if (allTelemetry != null && allTelemetry.length > 0) return allTelemetry[0];
        java.io.File[] omni = dir.listFiles((d, name) ->
            name.contains("spark-telemetry") && name.endsWith(".jar"));
        return (omni != null && omni.length > 0) ? omni[0] : null;
    }

    private String detectHadoopHome() {
        // Check env var first
        String env = System.getenv("HADOOP_HOME");
        if (env != null && !env.isEmpty() && new java.io.File(env, "bin/hadoop").exists()) return env;
        // Probe common paths
        for (String path : HADOOP_PROBE_PATHS) {
            if (new java.io.File(path, "bin/hadoop").exists()) return path;
        }
        // Scan /opt for hadoop* directories
        java.io.File opt = new java.io.File("/opt");
        if (opt.isDirectory()) {
            java.io.File[] dirs = opt.listFiles((d, name) -> name.startsWith("hadoop") && new java.io.File(d, name + "/bin/hadoop").exists());
            if (dirs != null && dirs.length > 0) return dirs[0].getAbsolutePath();
        }
        return null;
    }

    private String detectHiveHome() {
        String env = System.getenv("HIVE_HOME");
        if (env != null && !env.isEmpty() && new java.io.File(env, "bin/hive").exists()) return env;
        for (String path : HIVE_PROBE_PATHS) {
            if (new java.io.File(path, "bin/hive").exists()) return path;
        }
        java.io.File opt = new java.io.File("/opt");
        if (opt.isDirectory()) {
            java.io.File[] dirs = opt.listFiles((d, name) -> name.startsWith("apache-hive") && new java.io.File(d, name + "/bin/hive").exists());
            if (dirs != null && dirs.length > 0) return dirs[0].getAbsolutePath();
        }
        return null;
    }

    private String detectHiBenchHome() {
        String env = System.getenv("HIBENCH_HOME");
        if (env != null && !env.isEmpty() && new java.io.File(env, "bin/workloads").exists()) return env;
        for (String path : HIBENCH_PROBE_PATHS) {
            if (new java.io.File(path, "bin/workloads").exists()) return path;
        }
        return null;
    }

    private void runHiBenchTests(String hibenchHome, String hadoopHome, String javaHome,
                                  List<CheckItem> items, Terminal terminal) {
        // HiBench测试用例：WordCount, TeraSort, DFSIOE
        String[] workloads = {"micro/wordcount", "micro/terasort", "micro/dfsioe"};
        String[] workloadNames = {"WordCount", "TeraSort", "DFSIOE"};

        items.add(CheckItem.ok("检测到 HiBench，准备运行测试..."));
        CheckPrinter.print(terminal, items);
        items.clear();

        for (int i = 0; i < workloads.length; i++) {
            String workload = workloads[i];
            String name = workloadNames[i];

            items.add(CheckItem.ok("提交 HiBench " + name + " 测试..."));
            CheckPrinter.print(terminal, items);
            items.clear();

            SubmitResult result = submitHiBenchWorkload(hibenchHome, hadoopHome, javaHome, workload);
            if (result.success) {
                items.add(CheckItem.ok("HiBench " + name + " 成功 (耗时 " + result.durationMs + "ms)"));
            } else {
                items.add(CheckItem.warn("HiBench " + name + " 失败: " + result.error,
                    "继续其他测试..."));
            }
        }
    }

    private SubmitResult submitHiBenchWorkload(String hibenchHome, String hadoopHome, String javaHome, String workload) {
        // Run HiBench workload using its shell script
        List<String> cmd = new ArrayList<>();
        cmd.add("bash");
        cmd.add("-c");
        // Source Hadoop env and run HiBench
        String script = "cd " + hibenchHome + " && " +
            "export HADOOP_HOME=" + hadoopHome + " && " +
            "export JAVA_HOME=" + (javaHome != null ? javaHome : System.getenv("JAVA_HOME")) + " && " +
            "export HIBENCH_HOME=" + hibenchHome + " && " +
            "./bin/workloads/" + workload + "/prepare/prepare.sh 2>&1 && " +
            "./bin/workloads/" + workload + "/spark/run.sh 2>&1";
        cmd.add(script);

        return runCommand(cmd, null, hadoopHome, javaHome);
    }

    private java.io.File findHiveHookJar() {
        for (String path : HIVE_HOOK_JAR_PROBES) {
            java.io.File f = new java.io.File(path);
            if (f.exists()) return f;
        }
        // Search Hive auxlib
        String hiveHome = detectHiveHome();
        if (hiveHome != null) {
            java.io.File auxlib = new java.io.File(hiveHome, "auxlib");
            if (auxlib.isDirectory()) {
                java.io.File[] jars = auxlib.listFiles((d, name) ->
                    (name.contains("hive-telemetry") || name.contains("spark-telemetry")) && name.endsWith(".jar"));
                if (jars != null && jars.length > 0) return jars[0];
            }
        }
        return null;
    }

    private boolean isInHiveAuxlib(java.io.File jar) {
        String hiveHome = detectHiveHome();
        if (hiveHome == null) return false;
        java.io.File auxlib = new java.io.File(hiveHome, "auxlib");
        if (!auxlib.isDirectory()) return false;
        java.io.File[] jars = auxlib.listFiles((d, name) ->
            (name.contains("hive-telemetry") || name.contains("spark-telemetry")) && name.endsWith(".jar"));
        if (jars == null) return false;
        for (java.io.File auxJar : jars) {
            if (auxJar.getName().equals(jar.getName())) return true;
        }
        return false;
    }

    private java.io.File findMrCollectorJar() {
        // Search for MR collector dist JAR (not agent)
        String[] searchPaths = {".", "mapreduce-collector/mr-telemetry-dist/target",
            "mr-telemetry-dist/target", "/opt", "/tmp", "/root"};
        for (String basePath : searchPaths) {
            java.io.File dir = new java.io.File(basePath);
            if (!dir.isDirectory()) continue;
            java.io.File[] jars = dir.listFiles((d, name) ->
                name.contains("mr-telemetry") && name.endsWith(".jar")
                && !name.contains("original") && !name.contains("agent"));
            if (jars != null && jars.length > 0) return jars[0];
        }
        return null;
    }

    private Process startMrCollector(java.io.File mrCollectorJar, String hadoopHome, String javaHome,
                                      String otelEndpoint, DiagnosticContext context) {
        try {
            // Write temporary MR collector config
            String historyServerUrl = context.getConfig().getMrCollectorHistoryServerUrl();
            int pollInterval = context.getConfig().getMrCollectorPollIntervalSecs();

            String configContent =
                "mr-telemetry {\n" +
                "  history-server {\n" +
                "    url = \"" + historyServerUrl + "\"\n" +
                "    poll.interval.secs = " + pollInterval + "\n" +
                "  }\n" +
                "  otel {\n" +
                "    exporter.endpoint = \"" + otelEndpoint + "\"\n" +
                "    service.name = \"diagnostic-mr-collector\"\n" +
                "    export.interval.ms = 5000\n" +
                "  }\n" +
                "  collection {\n" +
                "    job.counters = true\n" +
                "    task.counters = true\n" +
                "    job.details = true\n" +
                "  }\n" +
                "}\n";

            java.io.File configFile = new java.io.File(System.getProperty("java.io.tmpdir"), "diag_mr_collector.conf");
            java.io.PrintWriter pw = new java.io.PrintWriter(configFile);
            pw.print(configContent);
            pw.close();

            List<String> cmd = new ArrayList<>();
            if (javaHome != null) cmd.add(javaHome + "/bin/java");
            else cmd.add("java");
            cmd.add("-jar");
            cmd.add(mrCollectorJar.getAbsolutePath());
            cmd.add(configFile.getAbsolutePath());

            ProcessBuilder pb = new ProcessBuilder(cmd);
            if (hadoopHome != null) pb.environment().put("HADOOP_HOME", hadoopHome);
            if (javaHome != null) pb.environment().put("JAVA_HOME", javaHome);
            pb.redirectErrorStream(true);
            pb.inheritIO(); // let it write to our stdout/stderr

            Process proc = pb.start();
            return proc;
        } catch (Exception e) {
            return null;
        }
    }

    private SubmitResult submitMrJob(String hadoopHome, String javaHome, String otelEndpoint) {
        // Find hadoop-mapreduce-examples JAR (has pi, teragen, wordcount etc.)
        java.io.File mrTestJar = findMrExamplesJar(hadoopHome);
        if (mrTestJar == null) {
            SubmitResult r = new SubmitResult();
            r.error = "未找到 hadoop-mapreduce-examples JAR";
            return r;
        }

        // Use pi — simple, no input data needed, works on any YARN cluster
        // Force YARN mode (not local) to ensure counters are collected via HADOOP_OPTS
        List<String> cmd = new ArrayList<>();
        cmd.add(hadoopHome + "/bin/hadoop");
        cmd.add("jar");
        cmd.add(mrTestJar.getAbsolutePath());
        cmd.add("pi");
        cmd.add("2");
        cmd.add("100");

        // Use environment variable to set YARN mode (Hadoop 3.x compatible)
        return runCommandWithEnv(cmd, null, hadoopHome, javaHome, "HADOOP_OPTS", "-Dmapreduce.framework.name=yarn");
    }

    private java.io.File findMrTestJar(String hadoopHome) {
        java.io.File share = new java.io.File(hadoopHome, "share/hadoop/mapreduce");
        if (!share.isDirectory()) return null;
        java.io.File[] jars = share.listFiles((d, name) ->
            name.contains("hadoop-mapreduce-client-jobclient") && name.endsWith(".jar") && name.contains("tests"));
        return (jars != null && jars.length > 0) ? jars[0] : null;
    }

    private java.io.File findMrExamplesJar(String hadoopHome) {
        java.io.File share = new java.io.File(hadoopHome, "share/hadoop/mapreduce");
        if (!share.isDirectory()) return null;
        java.io.File[] jars = share.listFiles((d, name) ->
            name.contains("hadoop-mapreduce-examples") && name.endsWith(".jar") && !name.contains("sources") && !name.contains("javadoc"));
        return (jars != null && jars.length > 0) ? jars[0] : null;
    }

    private SubmitResult submitHiveQuery(String hiveHome, String hadoopHome, String javaHome,
                                          java.io.File hiveHookJar, String otelEndpoint) {
        // SQL script: use STORED AS TEXTFILE (HDFS-backed) + INSERT to trigger MR counters
        // Add ANALYZE TABLE to populate table statistics for IO metrics
        String sqlScript =
            "CREATE DATABASE IF NOT EXISTS diagnostic_test;\n" +
            "USE diagnostic_test;\n" +
            "DROP TABLE IF EXISTS diag_src;\n" +
            "DROP TABLE IF EXISTS diag_dst;\n" +
            "CREATE TABLE diag_src (id INT, name STRING) STORED AS TEXTFILE;\n" +
            "CREATE TABLE diag_dst (id INT, name STRING) STORED AS TEXTFILE;\n" +
            "INSERT INTO diag_src VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie'), (4, 'dave'), (5, 'eve');\n" +
            "ANALYZE TABLE diag_src COMPUTE STATISTICS;\n" +  // Populate table stats for IO metrics
            "SELECT COUNT(*) FROM diag_src;\n" +
            "INSERT OVERWRITE TABLE diag_dst SELECT * FROM diag_src WHERE id > 2;\n" +
            "ANALYZE TABLE diag_dst COMPUTE STATISTICS;\n" +  // Populate table stats for IO metrics
            "SELECT * FROM diag_dst;\n" +
            "DROP TABLE diag_src;\n" +
            "DROP TABLE diag_dst;\n";

        try {
            java.io.File sqlFile = new java.io.File(System.getProperty("java.io.tmpdir"), "diag_hive_test.sql");
            java.io.PrintWriter pw = new java.io.PrintWriter(sqlFile);
            pw.print(sqlScript);
            pw.close();

            // Write HOCON config for Hive telemetry hook (avoids --hiveconf rejecting unknown keys)
            String hoconConfig =
                "hive-telemetry {\n" +
                "  otel {\n" +
                "    exporter.endpoint = \"" + otelEndpoint + "\"\n" +
                "    service.name = \"" + APP_NAME + "-hive\"\n" +
                "    export.interval.ms = 5000\n" +
                "  }\n" +
                "  metrics {\n" +
                "    enabled = true\n" +
                "    query.duration = true\n" +
                "    query.io = true\n" +
                "    query.tables = true\n" +
                "  }\n" +
                "}\n";
            java.io.File hoconFile = new java.io.File(System.getProperty("java.io.tmpdir"), "diag_hive_telemetry.conf");
            java.io.PrintWriter hpw = new java.io.PrintWriter(hoconFile);
            hpw.print(hoconConfig);
            hpw.close();

            List<String> cmd = new ArrayList<>();
            cmd.add(hiveHome + "/bin/hive");
            // Force MR execution engine + YARN (not local mode) to get counters
            cmd.add("--hiveconf");
            cmd.add("hive.execution.engine=mr");
            cmd.add("--hiveconf");
            cmd.add("mapreduce.framework.name=yarn");
            cmd.add("--hiveconf");
            cmd.add("hive.exec.mode.local.auto=false");
            cmd.add("--hiveconf");
            cmd.add("hive.exec.post.hooks=x.mg.metrics.hivetelemetry.HiveTelemetryHook");
            // Use config.path to pass OTel endpoint via HOCON (Hive 2.x rejects unknown --hiveconf keys)
            cmd.add("--hiveconf");
            cmd.add("hive.telemetry.config.path=" + hoconFile.getAbsolutePath());
            // Only set hive.aux.jars.path if the hook JAR is NOT in $HIVE_HOME/auxlib.
            // JARs in auxlib are auto-loaded by Hive. Setting hive.aux.jars.path to a
            // local path causes FileNotFoundException when YARN resolves it against HDFS.
            if (hiveHookJar != null && !isInHiveAuxlib(hiveHookJar)) {
                cmd.add("--hiveconf");
                cmd.add("hive.aux.jars.path=" + hiveHookJar.getAbsolutePath());
            }
            if (hadoopHome != null) {
                cmd.add("--hiveconf");
                cmd.add("yarn.resourcemanager.hostname=localhost");
            }
            cmd.add("-f");
            cmd.add(sqlFile.getAbsolutePath());

            SubmitResult result = runCommand(cmd, null, hadoopHome, javaHome);
            sqlFile.delete();
            return result;
        } catch (Exception e) {
            SubmitResult r = new SubmitResult();
            r.error = e.getMessage();
            return r;
        }
    }

    private long getKafkaEndOffset(DiagnosticContext context) {
        Properties props = new Properties();
        props.put("bootstrap.servers", context.getConfig().getKafkaBootstrapServers());
        props.put("request.timeout.ms", String.valueOf(context.getConfig().getKafkaTimeoutMs()));
        try (AdminClient admin = AdminClient.create(props)) {
            TopicPartition tp = new TopicPartition(context.getConfig().getKafkaMetricsTopic(), 0);
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            request.put(tp, OffsetSpec.latest());
            ListOffsetsResult result = admin.listOffsets(request);
            KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> future = result.partitionResult(tp);
            ListOffsetsResult.ListOffsetsResultInfo info = future.get();
            return info.offset();
        } catch (Exception ignored) {}
        return -1;
    }

    private static class SubmitResult {
        boolean success;
        long durationMs;
        String output;
        String error;
    }

    @Override
    public String getName() { return "数据流验证"; }
}
