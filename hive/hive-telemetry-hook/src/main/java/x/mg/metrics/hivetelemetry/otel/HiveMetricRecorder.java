package x.mg.metrics.hivetelemetry.otel;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import x.mg.metrics.hivetelemetry.config.HiveHookConfig;
import x.mg.metrics.hivetelemetry.model.HiveQueryMetrics;
import x.mg.metrics.hivetelemetry.model.HiveTableIOMetrics;

public class HiveMetricRecorder {
    private static final String METER_NAME = "hive-telemetry";

    private final Meter meter;
    private final HiveHookConfig config;

    private final LongHistogram queryDurationHistogram;
    private final LongCounter querySuccessCounter;
    private final LongCounter queryFailureCounter;
    private final LongCounter inputBytesCounter;
    private final LongCounter outputBytesCounter;
    private final LongCounter inputRowsCounter;
    private final LongCounter outputRowsCounter;
    private final LongCounter inputTablesCounter;
    private final LongCounter outputTablesCounter;
    private final LongCounter tableBytesCounter;
    private final LongCounter tableRowsCounter;
    private final LongCounter tableFilesReadCounter;
    private final LongCounter tableTimeMsCounter;

    public HiveMetricRecorder(OpenTelemetry openTelemetry, HiveHookConfig config) {
        this.meter = openTelemetry.getMeter(METER_NAME);
        this.config = config;

        this.queryDurationHistogram = meter.histogramBuilder("hive.query.duration_ms")
            .setDescription("Hive query duration").setUnit("ms").ofLongs().build();
        this.querySuccessCounter = meter.counterBuilder("hive.query.success")
            .setDescription("Hive query success count").build();
        this.queryFailureCounter = meter.counterBuilder("hive.query.failure")
            .setDescription("Hive query failure count").build();
        this.inputBytesCounter = meter.counterBuilder("hive.query.input_bytes")
            .setDescription("Hive query input bytes").setUnit("By").build();
        this.outputBytesCounter = meter.counterBuilder("hive.query.output_bytes")
            .setDescription("Hive query output bytes").setUnit("By").build();
        this.inputRowsCounter = meter.counterBuilder("hive.query.input_rows")
            .setDescription("Hive query input rows").setUnit("{rows}").build();
        this.outputRowsCounter = meter.counterBuilder("hive.query.output_rows")
            .setDescription("Hive query output rows").setUnit("{rows}").build();
        this.inputTablesCounter = meter.counterBuilder("hive.query.input_tables")
            .setDescription("Hive query input tables count").build();
        this.outputTablesCounter = meter.counterBuilder("hive.query.output_tables")
            .setDescription("Hive query output tables count").build();
        this.tableBytesCounter = meter.counterBuilder("hive.table.io.bytes")
            .setDescription("Per-table I/O bytes").setUnit("By").build();
        this.tableRowsCounter = meter.counterBuilder("hive.table.io.rows")
            .setDescription("Per-table I/O rows").setUnit("{rows}").build();
        this.tableFilesReadCounter = meter.counterBuilder("hive.table.io.files_read")
            .setDescription("Per-table files read").build();
        this.tableTimeMsCounter = meter.counterBuilder("hive.table.io.time_ms")
            .setDescription("Per-table time (query duration)").setUnit("ms").build();
    }

    public void record(HiveQueryMetrics m) {
        if (m == null) return;
        try {
            Attributes baseAttrs = buildBaseAttributes(m);

            // Duration
            if (config.isQueryDuration() && m.getDurationMs() > 0) {
                queryDurationHistogram.record(m.getDurationMs(), baseAttrs);
            }

            // Success/failure
            if (m.isSuccess()) {
                querySuccessCounter.add(1, baseAttrs);
            } else {
                queryFailureCounter.add(1, baseAttrs);
            }

            // IO volume
            if (config.isQueryIO()) {
                safeAdd(inputBytesCounter, m.getInputBytes(), baseAttrs);
                safeAdd(outputBytesCounter, m.getOutputBytes(), baseAttrs);
                safeAdd(inputRowsCounter, m.getInputRows(), baseAttrs);
                safeAdd(outputRowsCounter, m.getOutputRows(), baseAttrs);
            }

            // Per-table metrics
            if (config.isQueryTables()) {
                for (String table : m.getInputTables()) {
                    Attributes tableAttrs = Attributes.builder()
                        .putAll(baseAttrs)
                        .put("hive.query.input_table", table)
                        .build();
                    inputTablesCounter.add(1, tableAttrs);
                }
                for (String table : m.getOutputTables()) {
                    Attributes tableAttrs = Attributes.builder()
                        .putAll(baseAttrs)
                        .put("hive.query.output_table", table)
                        .build();
                    outputTablesCounter.add(1, tableAttrs);
                }

                // Per-table I/O metrics (bytes, rows, files_read, time_ms)
                for (HiveTableIOMetrics tio : m.getTableIOMetrics()) {
                    String tableAttrKey = "input".equals(tio.getTableType())
                        ? "hive.query.input_table" : "hive.query.output_table";
                    Attributes tioAttrs = Attributes.builder()
                        .putAll(baseAttrs)
                        .put(tableAttrKey, tio.getTableName())
                        .build();
                    if (tio.getBytes() > 0) tableBytesCounter.add(tio.getBytes(), tioAttrs);
                    if (tio.getRows() > 0) tableRowsCounter.add(tio.getRows(), tioAttrs);
                    if (tio.getFilesRead() > 0) tableFilesReadCounter.add(tio.getFilesRead(), tioAttrs);
                    if (m.getDurationMs() > 0) tableTimeMsCounter.add(m.getDurationMs(), tioAttrs);
                }
            }
        } catch (Exception e) {
            // Never let recording failures propagate to HiveServer2
        }
    }

    private void safeAdd(LongCounter counter, long value, Attributes attrs) {
        if (value > 0) {
            counter.add(value, attrs);
        }
    }

    private Attributes buildBaseAttributes(HiveQueryMetrics m) {
        AttributesBuilder b = Attributes.builder();
        if (m.getQueryId() != null) b.put("hive.query.id", m.getQueryId());
        if (m.getOperationName() != null) b.put("hive.query.operation", m.getOperationName());
        if (m.getUserName() != null) b.put("hive.query.user", m.getUserName());
        b.put("hive.query.success", String.valueOf(m.isSuccess()));
        if (m.getExecutionEngine() != null) b.put("hive.query.execution_engine", m.getExecutionEngine());
        if (m.getQueue() != null && !m.getQueue().isEmpty()) b.put("hive.query.queue", m.getQueue());
        if (m.getQueryText() != null && !m.getQueryText().isEmpty()) {
            String sql = m.getQueryText();
            int maxLen = config.getSqlMaxLength();
            b.put("hive.query.sql_text", sql.length() > maxLen ? sql.substring(0, maxLen) : sql);
        }
        return b.build();
    }
}
