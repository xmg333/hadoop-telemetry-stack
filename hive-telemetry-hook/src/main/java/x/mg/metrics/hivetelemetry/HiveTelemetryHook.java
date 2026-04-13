package x.mg.metrics.hivetelemetry;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.Entity;
import x.mg.metrics.hivetelemetry.model.HiveQueryMetrics;

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
        } catch (Exception e) {
            // ERROR ISOLATION: Never propagate exceptions to HiveServer2.
            // A telemetry hook must never break query execution.
            LOG.log(Level.WARNING, "HiveTelemetryHook error (suppressed): " + e.getMessage(), e);
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
        m.setUserName(hookContext.getUserName());

        // Execution engine
        try {
            org.apache.hadoop.hive.conf.HiveConf conf = hookContext.getConf();
            m.setExecutionEngine(conf != null ? conf.get("hive.execution.engine", "unknown") : "unknown");
        } catch (Exception e) {
            LOG.log(Level.FINE, "Could not read execution engine: " + e.getMessage());
            m.setExecutionEngine("unknown");
        }

        // POST_EXEC implies success
        m.setSuccess(true);

        // Input tables (read)
        Set<ReadEntity> inputs = hookContext.getInputs();
        if (inputs != null) {
            for (ReadEntity entity : inputs) {
                Entity.Type type = entity.getType();
                if (type == Entity.Type.TABLE) {
                    m.addInputTable(entity.getTable().getTableName());
                } else if (type == Entity.Type.PARTITION) {
                    m.addInputTable(entity.getPartition().getTable().getTableName());
                } else if (type == Entity.Type.DATABASE) {
                    m.addInputDatabase(entity.getDatabase().getName());
                }
            }
        }

        // Output tables (written)
        Set<WriteEntity> outputs = hookContext.getOutputs();
        if (outputs != null) {
            for (WriteEntity entity : outputs) {
                Entity.Type type = entity.getType();
                if (type == Entity.Type.TABLE) {
                    m.addOutputTable(entity.getTable().getTableName());
                } else if (type == Entity.Type.PARTITION) {
                    m.addOutputTable(entity.getPartition().getTable().getTableName());
                }
            }
        }

        // IO counters from QueryPlan
        extractCounters(plan, m);

        return m;
    }

    private void extractCounters(QueryPlan plan, HiveQueryMetrics m) {
        if (plan == null) return;
        try {
            // getCounters() returns Map<GroupName, Map<CounterName, Long>>
            Map<String, Map<String, Long>> counterGroups = plan.getCounters();
            if (counterGroups == null) return;

            // Flatten all counter groups and look for common counter names
            for (Map<String, Long> group : counterGroups.values()) {
                if (group == null) continue;

                Long val;
                val = group.get("HDFS_BYTES_READ");
                if (val == null) val = group.get("BYTES_READ");
                if (val != null && val > m.getInputBytes()) m.setInputBytes(val);

                val = group.get("HDFS_BYTES_WRITTEN");
                if (val == null) val = group.get("BYTES_WRITTEN");
                if (val != null && val > m.getOutputBytes()) m.setOutputBytes(val);

                val = group.get("RECORDS_OUT_INTERMEDIATE");
                if (val == null) val = group.get("RECORDS_WRITTEN");
                if (val != null && val > m.getOutputRows()) m.setOutputRows(val);

                val = group.get("RECORDS_READ");
                if (val != null && val > m.getInputRows()) m.setInputRows(val);
            }
        } catch (Exception e) {
            // getCounters() may throw in some execution modes (Tez, LLAP)
            LOG.log(Level.FINE, "Could not extract counters: " + e.getMessage());
        }
    }
}
