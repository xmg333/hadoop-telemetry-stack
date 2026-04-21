package x.mg.metrics.diagnostic.state;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.config.DiagnosticConfig;
import x.mg.metrics.diagnostic.state.handlers.*;
import x.mg.metrics.diagnostic.ui.AnsiColors;

/**
 * 诊断状态机 — 每个检查无论成败都继续下一个
 */
public class DiagnosticStateMachine {

    private final Terminal terminal;
    private final DiagnosticContext context;
    private DiagnosticState currentState;

    public DiagnosticStateMachine(Terminal terminal, DiagnosticConfig config) {
        this.terminal = terminal;
        this.context = new DiagnosticContext(config, terminal);
        this.currentState = DiagnosticState.INIT;
    }

    public DiagnosticState run() {
        while (!isTerminal(currentState)) {
            terminal.writer().println();
            terminal.writer().println(AnsiColors.GREEN + "▶ " + AnsiColors.BOLD + getLabel(currentState) + AnsiColors.RESET);
            terminal.writer().flush();

            try {
                currentState = getHandler(currentState).execute(context);
            } catch (Exception e) {
                terminal.writer().println(AnsiColors.RED + "  ✗ 异常: " + e.getMessage() + AnsiColors.RESET);
                terminal.writer().flush();
                // 继续下一个状态，不终止
                currentState = nextAfter(currentState);
            }
        }
        return currentState;
    }

    private boolean isTerminal(DiagnosticState s) {
        return s == DiagnosticState.EXIT_SUCCESS || s == DiagnosticState.EXIT_FAILURE;
    }

    private String getLabel(DiagnosticState s) {
        switch (s) {
            case INIT: return "初始化";
            case LOAD_CONFIG: return "加载配置";
            case CHECK_SPARK_PLUGIN: return "检查 Spark Plugin";
            case CHECK_HIVE_HOOK: return "检查 Hive Hook";
            case CHECK_MR_COLLECTOR: return "检查 MR Collector";
            case CHECK_OTEL_COLLECTOR: return "检查 OTel Collector";
            case CHECK_KAFKA: return "检查 Kafka";
            case CHECK_MYSQL: return "检查 MySQL";
            case CHECK_GRAFANA: return "检查 Grafana 面板";
            case DATA_FLOW_CHECK: return "数据流验证";
            case GENERATE_REPORT: return "生成报告";
            default: return s.name();
        }
    }

    private StateHandler getHandler(DiagnosticState s) {
        switch (s) {
            case INIT: return new InitHandler();
            case LOAD_CONFIG: return new LoadConfigHandler();
            case CHECK_SPARK_PLUGIN: return new CheckSparkPluginHandler();
            case CHECK_HIVE_HOOK: return new CheckHiveHookHandler();
            case CHECK_MR_COLLECTOR: return new CheckMrCollectorHandler();
            case CHECK_OTEL_COLLECTOR: return new CheckOtelCollectorHandler();
            case CHECK_KAFKA: return new CheckKafkaHandler();
            case CHECK_MYSQL: return new CheckMySqlHandler();
            case CHECK_GRAFANA: return new GrafanaSqlCheckHandler();
            case DATA_FLOW_CHECK: return new DataFlowCheckHandler();
            case GENERATE_REPORT: return new GenerateReportHandler();
            default: return (ctx) -> DiagnosticState.EXIT_FAILURE;
        }
    }

    /** 出错后跳过当前步骤，继续下一个 */
    private DiagnosticState nextAfter(DiagnosticState s) {
        switch (s) {
            case INIT: return DiagnosticState.LOAD_CONFIG;
            case LOAD_CONFIG: return DiagnosticState.CHECK_SPARK_PLUGIN;
            case CHECK_SPARK_PLUGIN: return DiagnosticState.CHECK_HIVE_HOOK;
            case CHECK_HIVE_HOOK: return DiagnosticState.CHECK_MR_COLLECTOR;
            case CHECK_MR_COLLECTOR: return DiagnosticState.CHECK_OTEL_COLLECTOR;
            case CHECK_OTEL_COLLECTOR: return DiagnosticState.CHECK_KAFKA;
            case CHECK_KAFKA: return DiagnosticState.CHECK_MYSQL;
            case CHECK_MYSQL: return DiagnosticState.CHECK_GRAFANA;
            case CHECK_GRAFANA: return DiagnosticState.DATA_FLOW_CHECK;
            case DATA_FLOW_CHECK: return DiagnosticState.GENERATE_REPORT;
            default: return DiagnosticState.EXIT_FAILURE;
        }
    }

    public DiagnosticContext getContext() { return context; }
}
