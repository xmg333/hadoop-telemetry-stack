package x.mg.metrics.hivetelemetry.otel;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import x.mg.metrics.hivetelemetry.config.HiveHookConfig;
import x.mg.metrics.hivetelemetry.model.HiveQueryMetrics;
import x.mg.metrics.hivetelemetry.model.HiveTableIOMetrics;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HiveMetricRecorderTest {

    private HiveMetricRecorder recorder;

    @BeforeEach
    void setUp() {
        SdkMeterProvider meterProvider = SdkMeterProvider.builder().build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();
        // Build config purely from defaults (no HiveConf dependency)
        Config defaults = ConfigFactory.parseMap(buildTestDefaults()).resolve();
        HiveHookConfig config = new HiveHookConfig(defaults);
        recorder = new HiveMetricRecorder(openTelemetry, config);
    }

    private static Map<String, Object> buildTestDefaults() {
        Map<String, Object> defaults = new HashMap<>();
        defaults.put("hive-telemetry.metrics.query-duration", true);
        defaults.put("hive-telemetry.metrics.query-io", true);
        defaults.put("hive-telemetry.metrics.query-tables", true);
        defaults.put("hive-telemetry.sql.max-length", 4096);
        return defaults;
    }

    @Test
    void testRecordSuccess_AllFieldsPopulated() {
        HiveQueryMetrics metrics = new HiveQueryMetrics();
        metrics.setQueryId("query_123");
        metrics.setQueryText("SELECT * FROM src WHERE id > 100");
        metrics.setOperationName("QUERY");
        metrics.setUserName("hiveuser");
        metrics.setDurationMs(5000);
        metrics.setSuccess(true);
        metrics.setInputBytes(1024);
        metrics.setOutputBytes(2048);
        metrics.setInputRows(100);
        metrics.setOutputRows(50);
        metrics.setExecutionEngine("tez");
        metrics.setQueue("production");
        metrics.addInputTable("default.src");
        metrics.addOutputTable("default.results");

        HiveTableIOMetrics tableIO = new HiveTableIOMetrics("default.src", "input");
        tableIO.setBytes(1024);
        tableIO.setRows(100);
        tableIO.setFilesRead(5);
        metrics.addTableIOMetrics(tableIO);

        assertDoesNotThrow(() -> recorder.record(metrics));
    }

    @Test
    void testRecordFailure_FailureCounter() {
        HiveQueryMetrics metrics = new HiveQueryMetrics();
        metrics.setQueryId("query_failed");
        metrics.setQueryText("INVALID SQL");
        metrics.setOperationName("QUERY");
        metrics.setUserName("hiveuser");
        metrics.setDurationMs(100);
        metrics.setSuccess(false);
        metrics.setExecutionEngine("tez");

        assertDoesNotThrow(() -> recorder.record(metrics));
    }

    @Test
    void testRecord_MultipleTables() {
        HiveQueryMetrics metrics = new HiveQueryMetrics();
        metrics.setQueryId("query_multi");
        metrics.setQueryText("SELECT * FROM t1 JOIN t2 JOIN t3");
        metrics.setOperationName("QUERY");
        metrics.setUserName("hiveuser");
        metrics.setDurationMs(10000);
        metrics.setSuccess(true);
        metrics.setInputBytes(4096);
        metrics.setOutputBytes(1024);
        metrics.setInputRows(500);
        metrics.setOutputRows(200);
        metrics.setExecutionEngine("spark");

        metrics.addInputTable("db.table1");
        metrics.addInputTable("db.table2");
        metrics.addInputTable("db.table3");
        metrics.addOutputTable("db.result1");
        metrics.addOutputTable("db.result2");

        assertDoesNotThrow(() -> recorder.record(metrics));
    }

    @Test
    void testRecordNullMetrics() {
        assertDoesNotThrow(() -> recorder.record(null));
    }

    @Test
    void testRecordEmptyMetrics() {
        HiveQueryMetrics metrics = new HiveQueryMetrics();
        metrics.setQueryId("query_empty");
        metrics.setSuccess(true);
        assertDoesNotThrow(() -> recorder.record(metrics));
    }
}
