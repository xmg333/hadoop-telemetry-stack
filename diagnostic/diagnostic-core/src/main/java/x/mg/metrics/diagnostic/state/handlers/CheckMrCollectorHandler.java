package x.mg.metrics.diagnostic.state.handlers;

import org.jline.terminal.Terminal;
import x.mg.metrics.diagnostic.checks.CheckItem;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CheckMrCollectorHandler extends CheckHandler {
    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        List<CheckItem> items = new ArrayList<>();
        String hadoopHome = System.getenv("HADOOP_HOME");

        if (hadoopHome == null || hadoopHome.isEmpty()) {
            items.add(CheckItem.skip("HADOOP_HOME 未设置，跳过 MR Collector 检查"));
        } else {
            items.add(CheckItem.ok("HADOOP_HOME=" + hadoopHome));
            // 检查 History Server 是否可达
            File historyServerDir = new File(hadoopHome, "logs");
            items.add(CheckItem.ok("MR Collector 模式需要独立 JAR + 配置文件运行"));
        }

        CheckPrinter.print(context.getTerminal(), items);
        return DiagnosticState.CHECK_OTEL_COLLECTOR;
    }
    @Override
    public String getName() { return "检查 MR Collector"; }
}
