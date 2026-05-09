package x.mg.metrics.diagnostic.state.handlers;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.checks.CheckItem;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.AnsiColors;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Grafana Dashboard SQL Checker - 自动检查所有 Grafana Panel 的 SQL 查询是否返回数据
 */
public class GrafanaSqlCheckHandler extends CheckHandler {

    private static final Pattern RAW_SQL_PATTERN = Pattern.compile("\"rawSql\":\\s*\"([^\"]+)\"");
    private static final Pattern SQL_SELECT_PATTERN = Pattern.compile("SELECT\\s+.*\\s+FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        Terminal terminal = context.getTerminal();
        List<CheckItem> items = new ArrayList<>();

        terminal.writer().println(AnsiColors.DIM + "  扫描 Grafana Dashboard 文件..." + AnsiColors.RESET);
        terminal.writer().flush();

        // 查找所有 Grafana JSON 文件
        List<File> dashboardFiles = findDashboardFiles();
        if (dashboardFiles.isEmpty()) {
            items.add(CheckItem.skip("未找到 Grafana dashboard 文件"));
            CheckPrinter.print(terminal, items);
            return DiagnosticState.GENERATE_REPORT;
        }

        items.add(CheckItem.ok("找到 " + dashboardFiles.size() + " 个 dashboard 文件"));

        // 提取所有 SQL 查询
        Map<String, List<SqlQuery>> allQueries = new LinkedHashMap<>();
        for (File file : dashboardFiles) {
            List<SqlQuery> queries = extractSqlQueries(file);
            if (!queries.isEmpty()) {
                allQueries.put(file.getName(), queries);
            }
        }

        int totalQueries = allQueries.values().stream().mapToInt(List::size).sum();
        items.add(CheckItem.ok("提取到 " + totalQueries + " 个 SQL 查询"));
        CheckPrinter.print(terminal, items);
        items.clear();

        // 执行 SQL 检查
        terminal.writer().println(AnsiColors.DIM + "  执行 SQL 查询检查..." + AnsiColors.RESET);
        terminal.writer().flush();

        Map<String, List<SqlCheckResult>> results = executeSqlChecks(context, allQueries);

        // 报告结果
        int emptyPanels = 0;
        for (Map.Entry<String, List<SqlCheckResult>> entry : results.entrySet()) {
            String dashboard = entry.getKey();
            for (SqlCheckResult result : entry.getValue()) {
                if (!result.success) {
                    items.add(CheckItem.fail(dashboard + ": " + result.table + " - 查询执行失败",
                        result.error));
                } else if (result.rowCount == 0) {
                    items.add(CheckItem.warn(dashboard + ": " + result.table + " - 返回 0 行",
                        "SQL: " + truncate(result.sql, 100)));
                    emptyPanels++;
                } else if (hasNullColumns(result)) {
                    items.add(CheckItem.warn(dashboard + ": " + result.table + " - 有 " +
                        result.nullColumns.size() + " 列为 NULL",
                        "NULL 列: " + String.join(", ", result.nullColumns)));
                }
            }
        }

        if (emptyPanels == 0) {
            items.add(CheckItem.ok("所有 Grafana Panel SQL 查询都返回了数据"));
        } else {
            items.add(CheckItem.warn("发现 " + emptyPanels + " 个空 Panel，请检查上面的列表", null));
        }

        CheckPrinter.print(terminal, items);
        return DiagnosticState.DATA_FLOW_CHECK;
    }

    private List<File> findDashboardFiles() {
        List<File> files = new ArrayList<>();
        // 搜索常见路径
        String[] paths = {
            "deploy/grafana",
            "../deploy/grafana",
            "../../deploy/grafana",
            "/opt/grafana/dashboards",
            "/opt/spark-telemetry/deploy/grafana",
            "/root/deploy/grafana",
            "/data/grafana/dashboards",
            "/var/lib/grafana/dashboards",
            "grafana_dashboards",
            "/tmp/grafana_dashboards"
        };
        for (String path : paths) {
            File dir = new File(path);
            if (dir.isDirectory()) {
                File[] jsonFiles = dir.listFiles((d, name) -> name.endsWith(".json"));
                if (jsonFiles != null) {
                    files.addAll(Arrays.asList(jsonFiles));
                }
            }
        }
        return files;
    }

    private List<SqlQuery> extractSqlQueries(File file) {
        List<SqlQuery> queries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String content = reader.lines().collect(Collectors.joining("\n"));

            Matcher matcher = RAW_SQL_PATTERN.matcher(content);
            while (matcher.find()) {
                String sql = matcher.group(1)
                    .replace("\\n", " ")
                    .replace("\\t", " ")
                    .replace("\\\\", "\\")
                    .replace("\\\"", "\"")
                    .trim();

                // 提取表名
                String table = extractTableName(sql);
                if (table != null && !table.isEmpty()) {
                    queries.add(new SqlQuery(table, sql, file.getName()));
                }
            }
        } catch (Exception e) {
            // 忽略读取错误
        }
        return queries;
    }

    private String extractTableName(String sql) {
        Matcher matcher = SQL_SELECT_PATTERN.matcher(sql);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private Map<String, List<SqlCheckResult>> executeSqlChecks(DiagnosticContext context,
                                                                Map<String, List<SqlQuery>> allQueries) {
        Map<String, List<SqlCheckResult>> results = new LinkedHashMap<>();

        String url = String.format("jdbc:mysql://%s:%d/metrics?connectTimeout=3000",
            context.getConfig().getMysqlHost(), context.getConfig().getMysqlPort());

        try (Connection conn = DriverManager.getConnection(url,
            context.getConfig().getMysqlUsername(), context.getConfig().getMysqlPassword())) {

            for (Map.Entry<String, List<SqlQuery>> entry : allQueries.entrySet()) {
                String dashboard = entry.getKey();
                List<SqlCheckResult> dashboardResults = new ArrayList<>();

                for (SqlQuery query : entry.getValue()) {
                    SqlCheckResult result = checkSqlQuery(conn, query);
                    dashboardResults.add(result);
                }

                results.put(dashboard, dashboardResults);
            }
        } catch (SQLException e) {
            // 连接失败，返回空结果
        }

        return results;
    }

    private SqlCheckResult checkSqlQuery(Connection conn, SqlQuery query) {
        SqlCheckResult result = new SqlCheckResult();
        result.sql = query.sql;
        result.table = query.table;
        result.dashboard = query.dashboard;

        try {
            // 简化 SQL：移除 Grafana 变量，添加 LIMIT
            String simplifiedSql = simplifySql(query.sql);

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(simplifiedSql)) {

                // 计算行数
                int rowCount = 0;
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                List<String> nullColumns = new ArrayList<>();

                while (rs.next() && rowCount < 10) {
                    rowCount++;
                    // 检查关键列是否为 NULL
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = metaData.getColumnName(i).toLowerCase();
                        if (isCriticalColumn(colName)) {
                            Object value = rs.getObject(i);
                            if (value == null) {
                                nullColumns.add(colName);
                            }
                        }
                    }
                }

                result.success = true;
                result.rowCount = rowCount;
                result.nullColumns = nullColumns.stream().distinct().collect(Collectors.toList());
            }
        } catch (SQLException e) {
            result.success = false;
            result.error = e.getMessage();
        }

        return result;
    }

    private String simplifySql(String sql) {
        // 移除 Grafana 变量
        String simplified = sql
            .replaceAll("\\$__unixEpochFrom\\(\\) \\* 1000", String.valueOf(System.currentTimeMillis() - 86400000))
            .replaceAll("\\$__unixEpochTo\\(\\) \\* 1000", String.valueOf(System.currentTimeMillis()))
            .replaceAll("\\$__interval_ms", "60000")
            .replaceAll("\\$\\w+", "'all'")
            .replaceAll("\\$\\{[^}]+\\}", "'all'");

        // 如果没有 LIMIT，添加一个
        if (!simplified.toUpperCase().contains("LIMIT")) {
            simplified += " LIMIT 10";
        }

        return simplified;
    }

    private boolean isCriticalColumn(String colName) {
        return colName.equals("value") || colName.equals("metric") ||
               colName.endsWith("_count") || colName.endsWith("_bytes") ||
               colName.endsWith("_ms") || colName.endsWith("_time");
    }

    private boolean hasNullColumns(SqlCheckResult result) {
        return result.nullColumns != null && !result.nullColumns.isEmpty();
    }

    private String truncate(String s, int maxLen) {
        if (s == null || s.length() <= maxLen) return s;
        return s.substring(0, maxLen) + "...";
    }

    @Override
    public String getName() { return "Grafana SQL 检查"; }

    private static class SqlQuery {
        final String table;
        final String sql;
        final String dashboard;

        SqlQuery(String table, String sql, String dashboard) {
            this.table = table;
            this.sql = sql;
            this.dashboard = dashboard;
        }
    }

    private static class SqlCheckResult {
        String table;
        String sql;
        String dashboard;
        boolean success;
        String error;
        int rowCount;
        List<String> nullColumns;
    }
}
