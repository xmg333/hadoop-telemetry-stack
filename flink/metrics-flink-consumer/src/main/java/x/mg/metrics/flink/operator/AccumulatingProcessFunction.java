package x.mg.metrics.flink.operator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import x.mg.metrics.flink.model.HistogramBucket;
import x.mg.metrics.flink.model.MetricSample;
import x.mg.metrics.flink.sink.WideRowAccumulator;

public class AccumulatingProcessFunction extends ProcessFunction<MetricItem, WideRowAccumulator.FlushResult> {
    private static final long serialVersionUID = 1L;

    private final int batchSize;
    private final long flushIntervalMs;

    private transient ValueState<WideRowAccumulator> accumulatorState;
    private transient boolean timerRegistered;

    public AccumulatingProcessFunction(int batchSize, long flushIntervalMs) {
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<WideRowAccumulator> desc = new ValueStateDescriptor<>(
            "wideRowAccumulator", TypeInformation.of(WideRowAccumulator.class));
        accumulatorState = getRuntimeContext().getState(desc);
        timerRegistered = false;
    }

    @Override
    public void processElement(MetricItem item, Context ctx, Collector<WideRowAccumulator.FlushResult> out) throws Exception {
        WideRowAccumulator acc = accumulatorState.value();
        if (acc == null) {
            acc = new WideRowAccumulator();
        }

        if (item.isSample()) {
            acc.accumulate(item.getSample());
        } else {
            acc.accumulateBucket(item.getBucket());
        }

        if (acc.pendingCount() >= batchSize) {
            WideRowAccumulator.FlushResult result = acc.drain();
            out.collect(result);
            accumulatorState.update(acc);
            timerRegistered = false;
        } else {
            accumulatorState.update(acc);
            if (!timerRegistered) {
                long nextFlush = ctx.timerService().currentProcessingTime() + flushIntervalMs;
                ctx.timerService().registerProcessingTimeTimer(nextFlush);
                timerRegistered = true;
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WideRowAccumulator.FlushResult> out) throws Exception {
        timerRegistered = false;
        WideRowAccumulator acc = accumulatorState.value();
        if (acc != null && acc.pendingCount() > 0) {
            WideRowAccumulator.FlushResult result = acc.drain();
            out.collect(result);
            accumulatorState.update(acc);
        }
    }
}
