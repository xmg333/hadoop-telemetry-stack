package x.mg.metrics.mragent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatchers;
import x.mg.metrics.mragent.advice.RunAdvice;
import x.mg.metrics.mragent.config.AgentConfig;

import java.lang.instrument.Instrumentation;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Java Agent entry point. Registered via Premain-Class in MANIFEST.MF.
 *
 * Uses ByteBuddy to instrument Mapper.run() and Reducer.run() methods,
 * injecting counter sampling during task execution for real-time IO metrics.
 *
 * Deployment (mapred-site.xml):
 *   mapreduce.map.java.opts = -javaagent:/path/to/mr-telemetry-agent.jar
 *     -Dmr.telemetry.agent.otel.exporter.endpoint=http://collector:4317
 */
public class MRAgent {

    private static final Logger LOG = Logger.getLogger(MRAgent.class.getName());

    public static void premain(String agentArgs, Instrumentation inst) {
        AgentConfig config = new AgentConfig();

        if (!config.isEnabled()) {
            LOG.info("MR Telemetry Agent disabled, skipping instrumentation");
            return;
        }

        LOG.info("MR Telemetry Agent starting...");
        LOG.info("  OTel endpoint: " + config.getOtelEndpoint());
        LOG.info("  Sampling interval: " + config.getSamplingIntervalSecs() + "s");

        try {
            installInstrumentation(inst);
            LOG.info("MR Telemetry Agent instrumentation installed successfully");
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Failed to install instrumentation", e);
        }
    }

    private static void installInstrumentation(Instrumentation inst) {
        AgentBuilder builder = new AgentBuilder.Default()
            .with(AgentBuilder.RedefinitionStrategy.DISABLED)
            .with(AgentBuilder.DescriptionStrategy.Default.HYBRID)
            .with(AgentBuilder.PoolStrategy.Default.FAST);

        // Instrument Mapper subclasses
        builder = builder
            .type(ElementMatchers.named("org.apache.hadoop.mapreduce.Mapper")
                .or(ElementMatchers.hasSuperType(
                    ElementMatchers.named("org.apache.hadoop.mapreduce.Mapper"))))
            .transform(new AgentBuilder.Transformer.ForAdvice()
                .include(MRAgent.class.getClassLoader())
                .advice(
                    ElementMatchers.isMethod().and(ElementMatchers.named("run")),
                    RunAdvice.class.getName()));

        // Instrument Reducer subclasses
        builder = builder
            .type(ElementMatchers.named("org.apache.hadoop.mapreduce.Reducer")
                .or(ElementMatchers.hasSuperType(
                    ElementMatchers.named("org.apache.hadoop.mapreduce.Reducer"))))
            .transform(new AgentBuilder.Transformer.ForAdvice()
                .include(MRAgent.class.getClassLoader())
                .advice(
                    ElementMatchers.isMethod().and(ElementMatchers.named("run")),
                    RunAdvice.class.getName()));

        builder.installOn(inst);
    }
}
