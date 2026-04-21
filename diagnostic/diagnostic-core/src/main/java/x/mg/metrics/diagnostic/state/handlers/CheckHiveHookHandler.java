package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.checks.HiveHookChecker;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

public class CheckHiveHookHandler extends CheckHandler {
    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        CheckPrinter.print(context.getTerminal(), new HiveHookChecker().check());
        return DiagnosticState.CHECK_MR_COLLECTOR;
    }
    @Override
    public String getName() { return "检查 Hive Hook"; }
}
