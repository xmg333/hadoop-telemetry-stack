package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.state.DiagnosticContext;
import x.mg.metrics.diagnostic.state.DiagnosticState;
import x.mg.metrics.diagnostic.state.StateHandler;

/**
 * 检查 OTel 应用状态处理器
 */
public class CheckOtelAppHandler implements StateHandler {

    @Override
    public DiagnosticState execute(DiagnosticContext context) {
        context.addInfo("OTel App: 检查 OTel SDK 配置");
        context.addInfo("OTel App: 检查环境变量");

        // TODO: 实现完整的 OTel 应用检查

        context.addInfo("OTel App: 检查完成");

        return DiagnosticState.CHECK_OTEL_COLLECTOR;
    }

    @Override
    public String getName() {
        return "检查 OTel App";
    }
}
