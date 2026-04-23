package x.mg.metrics.hivetelemetry;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import x.mg.metrics.hivetelemetry.model.HiveQueryMetrics;
import x.mg.metrics.hivetelemetry.model.HiveTableIOMetrics;

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hive telemetry hook that captures query metrics and exports via OpenTelemetry.
 * Loaded via hive.exec.post.hooks in hive-site.xml.
 * Compatible with Hive 2.x and 3.x.
 */
public class HiveTelemetryHook implements ExecuteWithHookContext {
    private static final Logger LOG = Logger.getLogger(HiveTelemetryHook.class.getName());

    @Override
    public void run(HookContext hookContext) throws Exception {
        try {
            // Only process POST_EXEC hooks
            if (hookContext.getHookType() != HookContext.HookType.POST_EXEC_HOOK) {
                return;
            }

            HiveHookContext ctx = HiveHookContext.getOrInit();
            if (!ctx.getConfig().isEnabled()) {
                return;
            }

            HiveQueryMetrics metrics = extractMetrics(hookContext);

            // Apply filters
            if (!ctx.getConfig().shouldAccept(metrics.getUserName(), metrics.getOperationName())) {
                return;
            }

            ctx.getMetricRecorder().record(metrics);

            // Force flush after each query to ensure metrics are exported
            // before JVM exit (critical for short-lived Hive CLI processes).
            ctx.flush();
        } catch (Exception e) {
            // ERROR ISOLATION: Never propagate exceptions to HiveServer2.
            LOG.log(Level.WARNING, "HiveTelemetryHook error: " + e.getMessage(), e);
        }
    }

    private HiveQueryMetrics extractMetrics(HookContext hookContext) {
        HiveQueryMetrics m = new HiveQueryMetrics();
        m.setTimestampMs(System.currentTimeMillis());

        // Core identifiers from QueryPlan (HookContext in Hive 2.x does not have getQueryId/getQueryStr)
        QueryPlan plan = hookContext.getQueryPlan();
        if (plan != null) {
            m.setQueryId(plan.getQueryId());
            m.setQueryText(plan.getQueryStr());

            // Duration
            Long startTime = plan.getQueryStartTime();
            if (startTime != null && startTime > 0) {
                m.setDurationMs(System.currentTimeMillis() - startTime);
            }
        }

        m.setOperationName(hookContext.getOperationName());
        // Default operation to "QUERY" if not set (HookContext may return null in some cases)
        if (m.getOperationName() == null || m.getOperationName().isEmpty()) {
            m.setOperationName("QUERY");
        }
        m.setUserName(hookContext.getUserName());

        // Execution engine
        try {
            org.apache.hadoop.hive.conf.HiveConf conf = hookContext.getConf();
            m.setExecutionEngine(conf != null ? conf.get("hive.execution.engine", "unknown") : "unknown");
            m.setQueue(conf != null ? conf.get("mapreduce.job.queuename", "") : "");
        } catch (Exception e) {
            LOG.log(Level.FINE, "Could not read execution engine: " + e.getMessage());
            m.setExecutionEngine("unknown");
        }

        // POST_EXEC implies success
        m.setSuccess(true);

        // Input tables (read) + IO estimates from table stats
        Set<ReadEntity> inputs = hookContext.getInputs();
        if (inputs != null) {
            for (ReadEntity entity : inputs) {
                Entity.Type type = entity.getType();
                if (type == Entity.Type.TABLE) {
                    Table t = entity.getTable();
                    m.addInputTable(t.getTableName());
                    addTableStats(t, m, true);
                } else if (type == Entity.Type.PARTITION) {
                    Partition p = entity.getPartition();
                    m.addInputTable(p.getTable().getTableName());
                    addTableStats(p.getTable(), m, true);
                } else if (type == Entity.Type.DATABASE) {
                    m.addInputDatabase(entity.getDatabase().getName());
                }
            }
        }

        // Output tables (written) + IO estimates from table stats
        Set<WriteEntity> outputs = hookContext.getOutputs();
        if (outputs != null) {
            for (WriteEntity entity : outputs) {
                Entity.Type type = entity.getType();
                if (type == Entity.Type.TABLE) {
                    Table t = entity.getTable();
                    m.addOutputTable(t.getTableName());
                    addTableStats(t, m, false);
                } else if (type == Entity.Type.PARTITION) {
                    Partition p = entity.getPartition();
                    m.addOutputTable(p.getTable().getTableName());
                    addTableStats(p.getTable(), m, false);
                }
            }
        }

        return m;
    }

    /**
     * Estimate IO from Hive metastore table statistics (totalSize, numRows).
     * These are maintained by Hive's StatsTask after each DML operation.
     * For input tables this is the full table size (approximate scan volume).
     * For output tables after INSERT OVERWRITE / CTAS this is the new data size.
     */
    private void addTableStats(Table table, HiveQueryMetrics m, boolean isInput) {
        try {
            Map<String, String> params = table.getParameters();
            if (params == null) return;

            long size = parseLong(params.get("totalSize"));
            long rows = parseLong(params.get("numRows"));
            long files = parseLong(params.get("numFiles"));

            // Aggregate stats for HIVE_QUERY category (backward compat)
            if (isInput) {
                if (size > 0) m.setInputBytes(m.getInputBytes() + size);
                if (rows > 0) m.setInputRows(m.getInputRows() + rows);
            } else {
                if (size > 0) m.setOutputBytes(m.getOutputBytes() + size);
                if (rows > 0) m.setOutputRows(m.getOutputRows() + rows);
            }

            // Per-table I/O metrics for HIVE_TABLE_IO category
            HiveTableIOMetrics tio = new HiveTableIOMetrics(table.getTableName(), isInput ? "input" : "output");
            tio.setBytes(size);
            tio.setRows(rows);
            tio.setFilesRead(files);
            m.addTableIOMetrics(tio);
        } catch (Exception e) {
            LOG.log(Level.FINE, "Could not read table stats for " + table.getTableName() + ": " + e.getMessage());
        }
    }

    private long parseLong(String value) {
        if (value == null || value.isEmpty()) return 0;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
