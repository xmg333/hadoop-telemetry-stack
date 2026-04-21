package x.mg.metrics.diagnostic;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import x.mg.metrics.diagnostic.config.DiagnosticConfig;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.state.DiagnosticStateMachine;
import x.mg.metrics.diagnostic.ui.AnsiColors;

import java.io.IOException;

/**
 * 诊断工具主入口
 */
public class DiagnosticApp {

    private Terminal terminal;
    private DiagnosticConfig config;

    public static void main(String[] args) {
        String configPath = null;

        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configPath = args[++i];
            } else if ("--help".equals(args[i])) {
                printUsage();
                return;
            }
        }

        new DiagnosticApp().run(configPath);
    }

    private static void printUsage() {
        System.out.println("用法：java -jar diagnostic-core.jar [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --config <path>   指定配置文件路径");
        System.out.println("  --help             显示帮助信息");
    }

    public void run(String configPath) {
        try {
            // 初始化终端
            terminal = TerminalBuilder.builder()
                .system(true)
                .color(true)
                .encoding(java.nio.charset.StandardCharsets.UTF_8)
                .build();

            // 加载配置
            config = configPath != null ? new DiagnosticConfig(configPath) : new DiagnosticConfig();

            // 打印标题
            printTitle();

            // 创建诊断上下文和状态机
            DiagnosticStateMachine stateMachine = new DiagnosticStateMachine(terminal, config);

            // 运行状态机
            DiagnosticState finalState = stateMachine.run();

            // 打印结束状态
            printEndState(finalState);

        } catch (IOException e) {
            terminal.writer().println(AnsiColors.RED + "错误：" + e.getMessage() + AnsiColors.RESET);
            terminal.writer().flush();
        }
    }

    private void printTitle() {
        terminal.writer().println();
        terminal.writer().println(AnsiColors.BOLD + AnsiColors.CYAN +
            "╔══════════════════════════════════════════════════════╗" + AnsiColors.RESET);
        terminal.writer().println(AnsiColors.BOLD + AnsiColors.CYAN +
            "║     遥测诊断工具                                           ║" + AnsiColors.RESET);
        terminal.writer().println(AnsiColors.BOLD + AnsiColors.CYAN +
            "║     Telemetry Diagnostic Tool                             ║" + AnsiColors.RESET);
        terminal.writer().println(AnsiColors.BOLD + AnsiColors.CYAN +
            "╚══════════════════════════════════════════════════════╝" + AnsiColors.RESET);
        terminal.writer().println();
        terminal.writer().flush();
    }

    private void printEndState(DiagnosticState state) {
        terminal.writer().println();
        if (state == DiagnosticState.EXIT_SUCCESS) {
            terminal.writer().println(AnsiColors.GREEN + "诊断完成" + AnsiColors.RESET);
        } else {
            terminal.writer().println(AnsiColors.RED + "诊断异常终止：" + state + AnsiColors.RESET);
        }
        terminal.writer().flush();
    }
}
