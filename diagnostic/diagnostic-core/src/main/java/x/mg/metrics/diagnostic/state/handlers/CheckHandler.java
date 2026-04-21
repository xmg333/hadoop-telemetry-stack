package x.mg.metrics.diagnostic.state.handlers;

import x.mg.metrics.diagnostic.state.StateHandler;

/**
 * Handler 基类 — 空 default 方法
 */
public abstract class CheckHandler implements StateHandler {
    @Override
    public String getName() { return "CheckHandler"; }
}
