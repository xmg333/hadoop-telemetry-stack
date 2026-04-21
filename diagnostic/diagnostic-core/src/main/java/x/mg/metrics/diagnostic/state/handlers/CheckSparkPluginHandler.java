package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.checks.SparkPluginChecker;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

/**
 * 检查 Spark Plugin
 */
public class CheckSparkPluginHandler extends CheckHandler {

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        CheckPrinter.print(context.getTerminal(), new SparkPluginChecker().check());
        return DiagnosticState.CHECK_HIVE_HOOK;
    }

    @Override
    public String getName() { return "检查 Spark Plugin"; }
}
