package x.mg.metrics.diagnostic.state;

/**
 * 状态处理器接口
 */
@FunctionalInterface
public interface StateHandler {

    /**
     * 执行状态处理
     * @param context 诊断上下文
     * @return 下一个状态
     */
    DiagnosticState execute(DiagnosticContext context);

    /**
     * 状态处理器名称
     * @return 处理器显示名称
     */
    default String getName() {
        return "StateHandler";
    }
}
