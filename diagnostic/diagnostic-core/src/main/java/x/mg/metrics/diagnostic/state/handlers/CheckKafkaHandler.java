package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.checks.KafkaChecker;
import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.ui.CheckPrinter;

public class CheckKafkaHandler extends CheckHandler {
    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        CheckPrinter.print(context.getTerminal(), new KafkaChecker(
            context.getConfig().getKafkaBootstrapServers(),
            context.getConfig().getKafkaMetricsTopic(),
            context.getConfig().getKafkaTracesTopic(),
            context.getConfig().getKafkaTimeoutMs()
        ).check());

        return DiagnosticState.CHECK_MYSQL;
    }
    @Override
    public String getName() { return "检查 Kafka"; }
}
