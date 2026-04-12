package x.mg.metrics.mragent.advice;

import net.bytebuddy.asm.Advice;
import x.mg.metrics.mragent.AgentContext;

/**
 * ByteBuddy inline advice that wraps Mapper.run()/Reducer.run() calls.
 *
 * On enter: captures the Context reference, starts the periodic sampler.
 * On exit:  stops the sampler, reports final counters.
 *
 * IMPORTANT: This class must never throw from advice methods.
 * All exceptions are caught to prevent agent failures from affecting MR tasks.
 * The context parameter is typed as Object because Hadoop API classes
 * are not on the agent's compile classpath - accessed via reflection at runtime.
 */
public class RunAdvice {

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.Argument(0) Object context,
            @Advice.Origin String methodSignature) {
        try {
            AgentContext.getOrInit().onRunEnter(context, methodSignature);
        } catch (Throwable t) {
            // Must never throw from advice - swallow all errors
            System.err.println("[mr-telemetry-agent] MR Agent advice enter failed: " + t.getMessage());
        }
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onExit(
            @Advice.Argument(0) Object context,
            @Advice.Origin String methodSignature,
            @Advice.Thrown Throwable throwable) {
        try {
            AgentContext.getOrInit().onRunExit(context, methodSignature);
        } catch (Throwable t) {
            System.err.println("[mr-telemetry-agent] MR Agent advice exit failed: " + t.getMessage());
        }
    }
}
