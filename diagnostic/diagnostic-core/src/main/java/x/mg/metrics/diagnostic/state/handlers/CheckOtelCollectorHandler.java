package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.checks.OtelCollectorChecker;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

public class CheckOtelCollectorHandler extends CheckHandler {
    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        String endpoint = context.getConfig().getOtelCollectorEndpoint();
        int healthPort = context.getConfig().getOtelCollectorHealthCheckPort();
        int timeout = context.getConfig().getOtelCollectorTimeoutMs();

        CheckPrinter.print(context.getTerminal(),
            new OtelCollectorChecker(endpoint, healthPort, timeout).check());

        return DiagnosticState.CHECK_KAFKA;
    }
    @Override
    public String getName() { return "检查 OTel Collector"; }
}
