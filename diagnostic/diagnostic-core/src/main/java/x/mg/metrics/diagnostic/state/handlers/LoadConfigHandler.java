package x.mg.metrics.diagnostic.state.handlers;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.config.DiagnosticConfig;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.AnsiColors;

import java.io.PrintWriter;

/**
 * 加载配置 — 实时打印
 */
public class LoadConfigHandler extends CheckHandler {

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        Terminal terminal = context.getTerminal();
        PrintWriter pw = terminal.writer();

        try {
            context.getConfig().getRawConfig(); // 验证配置可读
            pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + "配置加载成功（使用默认值 + HOCON）");
            pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + "OTel Collector: " + context.getConfig().getOtelCollectorEndpoint());
            pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + "Kafka: " + context.getConfig().getKafkaBootstrapServers());
            pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + "MySQL: " + context.getConfig().getMysqlHost() + ":" + context.getConfig().getMysqlPort());
            pw.flush();
        } catch (Exception e) {
            pw.println(AnsiColors.YELLOW + "  ⚠ " + AnsiColors.RESET + "配置加载异常: " + e.getMessage() + "（使用默认值继续）");
            pw.flush();
        }
        return DiagnosticState.CHECK_SPARK_PLUGIN;
    }

    @Override
    public String getName() { return "加载配置"; }
}
