package x.mg.metrics.diagnostic.state.handlers;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.state.StateHandler;
import x.mg.metrics.diagnostic.ui.AnsiColors;

import java.io.PrintWriter;

/**
 * 初始化状态处理器 — 实时打印环境变量
 */
public class InitHandler implements StateHandler {

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        Terminal terminal = context.getTerminal();
        PrintWriter pw = terminal.writer();

        pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + "JLine 终端已初始化");
        pw.println();

        // 打印环境变量
        String[] envVars = {"HADOOP_HOME", "SPARK_HOME", "HIVE_HOME", "JAVA_HOME"};

        pw.println(AnsiColors.BOLD + "  环境变量:" + AnsiColors.RESET);
        for (String envVar : envVars) {
            String value = System.getenv(envVar);
            if (value != null && !value.isEmpty()) {
                pw.println(AnsiColors.GREEN + "  ✓ " + AnsiColors.RESET + envVar + "=" + value);
                context.addInfo(envVar + "=" + value);
            } else {
                pw.println(AnsiColors.YELLOW + "  ⚠ " + AnsiColors.RESET + envVar + " 未设置");
                context.addWarning(envVar + " 未设置");
            }
        }
        pw.println();
        pw.flush();

        return DiagnosticState.LOAD_CONFIG;
    }

    @Override
    public String getName() {
        return "初始化";
    }
}
