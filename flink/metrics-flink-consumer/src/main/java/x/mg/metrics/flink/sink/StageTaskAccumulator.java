package x.mg.metrics.flink.sink;

import x.mg.metrics.flink.model.StageGovernanceRow;

import java.util.HashMap;
import java.util.Map;

public class StageTaskAccumulator {
    private static final double SMALL_FILE_THRESHOLD = 32 * 1024 * 1024; // 32MB

    private final String appId;
    private final int stageId;
    private long latestTimestamp;
    private final Map<String, RunningStats> stats = new HashMap<>();
    private int smallOutputTaskCount = 0;

    public StageTaskAccumulator(String appId, int stageId) {
        this.appId = appId;
        this.stageId = stageId;
    }

    public void accumulate(String columnName, double value) {
        stats.computeIfAbsent(columnName, k -> new RunningStats()).add(value);

        // Track small file tasks: count tasks where bytes_written < threshold
        if ("io_bytes_written".equals(columnName) && value < SMALL_FILE_THRESHOLD && value > 0) {
            smallOutputTaskCount++;
        }
    }

    public void updateTimestamp(long ts) {
        if (ts > latestTimestamp) latestTimestamp = ts;
    }

    public int getTaskCount() {
        RunningStats dur = stats.get("duration_ms");
        return dur != null ? dur.count : 0;
    }

    public String getAppId() { return appId; }
    public int getStageId() { return stageId; }

    public StageGovernanceRow toGovernanceRow(Double stageDurationMs) {
        StageGovernanceRow row = new StageGovernanceRow();
        row.setTimestampMs(latestTimestamp);
        row.setAppId(appId);
        row.setStageId(stageId);
        row.setStageDurationMs(stageDurationMs);

        // Task count (from duration_ms count, since every task has duration)
        RunningStats dur = stats.get("duration_ms");
        int taskCount = dur != null ? dur.count : 0;
        row.setTaskCount(taskCount);

        // Duration analysis
        if (dur != null && dur.count > 0) {
            row.setAvgTaskDurationMs(dur.avg());
            row.setMaxTaskDurationMs(dur.max);
            row.setMinTaskDurationMs(dur.min > 0 ? dur.min : 0);
            row.setDurationSkewRatio(dur.skewRatio());
        }

        // IO totals
        row.setTotalBytesRead(sum("io_bytes_read"));
        row.setTotalBytesWritten(sum("io_bytes_written"));
        row.setTotalShuffleBytesRead(sum("shuffle_bytes_read"));
        row.setTotalShuffleBytesWritten(sum("shuffle_bytes_written"));
        row.setTotalRecordsRead(sum("io_records_read"));
        row.setTotalRecordsWritten(sum("io_records_written"));

        // IO skew
        RunningStats ioRead = stats.get("io_bytes_read");
        if (ioRead != null && ioRead.count > 0) row.setIoReadSkewRatio(ioRead.skewRatio());
        RunningStats ioWrite = stats.get("io_bytes_written");
        if (ioWrite != null && ioWrite.count > 0) row.setIoWriteSkewRatio(ioWrite.skewRatio());
        RunningStats shRead = stats.get("shuffle_bytes_read");
        if (shRead != null && shRead.count > 0) row.setShuffleReadSkewRatio(shRead.skewRatio());

        // Small file indicators
        if (taskCount > 0) {
            row.setAvgOutputBytesPerTask(row.getTotalBytesWritten() / taskCount);
            row.setAvgOutputRecordsPerTask(row.getTotalRecordsWritten() / taskCount);
        }
        row.setSmallOutputTaskCount(smallOutputTaskCount);

        // Resource efficiency
        double totalRunTimeMs = sum("executor_run_time_ms");
        double totalCpuNs = sum("executor_cpu_time_ns");
        double totalGcMs = sum("jvm_gc_time_ms");
        double totalFetchWaitMs = sum("shuffle_fetch_wait_time_ms");
        double totalDiskSpilled = sum("disk_bytes_spilled");
        double totalBytesRead = row.getTotalBytesRead();
        double totalDeserializeMs = sum("deserialize_time_ms");
        double totalSchedulerDelay = sum("scheduler_delay_ms");
        double totalDurationMs = dur != null ? dur.sum : 0;

        if (totalRunTimeMs > 0) {
            row.setCpuEfficiency(totalCpuNs / (totalRunTimeMs * 1_000_000));
            row.setGcOverheadRatio(totalGcMs / totalRunTimeMs);
            row.setShuffleWaitRatio(totalFetchWaitMs / totalRunTimeMs);
            row.setDeserializeOverhead(totalDeserializeMs / totalRunTimeMs);
        }
        if (totalBytesRead > 0) {
            row.setSpillRatio(totalDiskSpilled / totalBytesRead);
        }
        if (totalDurationMs > 0) {
            row.setSchedulerDelayRatio(totalSchedulerDelay / totalDurationMs);
        }

        // Memory
        RunningStats peakMem = stats.get("peak_execution_memory_bytes");
        if (peakMem != null) row.setMaxPeakMemoryBytes(peakMem.max);
        row.setTotalMemorySpilled(sum("memory_bytes_spilled"));

        return row;
    }

    private double sum(String column) {
        RunningStats s = stats.get(column);
        return s != null ? s.sum : 0;
    }

    public static class RunningStats {
        double sum;
        double max = -1;
        double min = Long.MAX_VALUE;
        int count;

        void add(double v) {
            sum += v;
            if (v > max) max = v;
            if (v < min) min = v;
            count++;
        }

        double avg() { return count > 0 ? sum / count : 0; }
        double skewRatio() { return count > 0 && sum > 0 ? max * count / sum : 0; }
    }
}
