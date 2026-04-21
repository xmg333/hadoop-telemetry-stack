package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.checks.MySQLChecker;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

public class CheckMySqlHandler extends CheckHandler {
    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        CheckPrinter.print(context.getTerminal(), new MySQLChecker(
            context.getConfig().getMysqlHost(),
            context.getConfig().getMysqlPort(),
            context.getConfig().getMysqlDatabase(),
            context.getConfig().getMysqlUsername(),
            context.getConfig().getMysqlPassword(),
            context.getConfig().getMysqlTimeoutMs()
        ).check());

        return DiagnosticState.DATA_FLOW_CHECK;
    }
    @Override
    public String getName() { return "检查 MySQL"; }
}
