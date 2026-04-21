package x.mg.metrics.diagnostic.state;

/**
 * 诊断状态
 */
public enum DiagnosticState {
    INIT,
    LOAD_CONFIG,

    // 应用诊断
    CHECK_SPARK_PLUGIN,
    CHECK_HIVE_HOOK,
    CHECK_MR_COLLECTOR,

    // 后端诊断
    CHECK_OTEL_COLLECTOR,
    CHECK_KAFKA,
    CHECK_MYSQL,

    // Grafana 面板检查
    CHECK_GRAFANA,

    // 数据流
    DATA_FLOW_CHECK,

    // 结果
    GENERATE_REPORT,

    // 终止
    EXIT_SUCCESS,
    EXIT_FAILURE
}
