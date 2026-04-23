package x.mg.metrics.flink.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StageGovernanceRowTest {

    @Test
    void testAllGettersAndSetters() {
        StageGovernanceRow row = new StageGovernanceRow();

        row.setTimestampMs(1234567890000L);
        row.setAppId("app-123");
        row.setStageId(5);
        row.setTaskCount(100);

        row.setStageDurationMs(10000.0);
        row.setAvgTaskDurationMs(5000.0);
        row.setMaxTaskDurationMs(9000.0);
        row.setMinTaskDurationMs(1000.0);
        row.setDurationSkewRatio(9.0);

        row.setTotalBytesRead(1024000.0);
        row.setTotalBytesWritten(512000.0);
        row.setTotalShuffleBytesRead(2048000.0);
        row.setTotalShuffleBytesWritten(1024000.0);
        row.setTotalRecordsRead(10000.0);
        row.setTotalRecordsWritten(5000.0);

        row.setIoReadSkewRatio(2.5);
        row.setIoWriteSkewRatio(1.8);
        row.setShuffleReadSkewRatio(3.0);

        row.setAvgOutputBytesPerTask(5120.0);
        row.setAvgOutputRecordsPerTask(50.0);
        row.setSmallOutputTaskCount(10);

        row.setCpuEfficiency(0.85);
        row.setGcOverheadRatio(0.05);
        row.setShuffleWaitRatio(0.15);
        row.setSpillRatio(0.1);
        row.setDeserializeOverhead(0.02);
        row.setSchedulerDelayRatio(0.03);

        row.setMaxPeakMemoryBytes(1024000000.0);
        row.setTotalMemorySpilled(2048000.0);

        assertEquals(1234567890000L, row.getTimestampMs());
        assertEquals("app-123", row.getAppId());
        assertEquals(5, row.getStageId());
        assertEquals(100, row.getTaskCount());

        assertEquals(10000.0, row.getStageDurationMs());
        assertEquals(5000.0, row.getAvgTaskDurationMs());
        assertEquals(9000.0, row.getMaxTaskDurationMs());
        assertEquals(1000.0, row.getMinTaskDurationMs());
        assertEquals(9.0, row.getDurationSkewRatio());

        assertEquals(1024000.0, row.getTotalBytesRead());
        assertEquals(512000.0, row.getTotalBytesWritten());
        assertEquals(2048000.0, row.getTotalShuffleBytesRead());
        assertEquals(1024000.0, row.getTotalShuffleBytesWritten());
        assertEquals(10000.0, row.getTotalRecordsRead());
        assertEquals(5000.0, row.getTotalRecordsWritten());

        assertEquals(2.5, row.getIoReadSkewRatio());
        assertEquals(1.8, row.getIoWriteSkewRatio());
        assertEquals(3.0, row.getShuffleReadSkewRatio());

        assertEquals(5120.0, row.getAvgOutputBytesPerTask());
        assertEquals(50.0, row.getAvgOutputRecordsPerTask());
        assertEquals(10, row.getSmallOutputTaskCount());

        assertEquals(0.85, row.getCpuEfficiency());
        assertEquals(0.05, row.getGcOverheadRatio());
        assertEquals(0.15, row.getShuffleWaitRatio());
        assertEquals(0.1, row.getSpillRatio());
        assertEquals(0.02, row.getDeserializeOverhead());
        assertEquals(0.03, row.getSchedulerDelayRatio());

        assertEquals(1024000000.0, row.getMaxPeakMemoryBytes());
        assertEquals(2048000.0, row.getTotalMemorySpilled());
    }

    @Test
    void testNullByDefault() {
        StageGovernanceRow row = new StageGovernanceRow();

        assertEquals(0L, row.getTimestampMs());
        assertNull(row.getAppId());
        assertEquals(0, row.getStageId());
        assertEquals(0, row.getTaskCount());

        assertNull(row.getStageDurationMs());
        assertNull(row.getAvgTaskDurationMs());
        assertNull(row.getMaxTaskDurationMs());
        assertNull(row.getMinTaskDurationMs());
        assertNull(row.getDurationSkewRatio());

        assertNull(row.getTotalBytesRead());
        assertNull(row.getTotalBytesWritten());
        assertNull(row.getTotalShuffleBytesRead());
        assertNull(row.getTotalShuffleBytesWritten());
        assertNull(row.getTotalRecordsRead());
        assertNull(row.getTotalRecordsWritten());

        assertNull(row.getIoReadSkewRatio());
        assertNull(row.getIoWriteSkewRatio());
        assertNull(row.getShuffleReadSkewRatio());

        assertNull(row.getAvgOutputBytesPerTask());
        assertNull(row.getAvgOutputRecordsPerTask());
        assertEquals(0, row.getSmallOutputTaskCount());

        assertNull(row.getCpuEfficiency());
        assertNull(row.getGcOverheadRatio());
        assertNull(row.getShuffleWaitRatio());
        assertNull(row.getSpillRatio());
        assertNull(row.getDeserializeOverhead());
        assertNull(row.getSchedulerDelayRatio());

        assertNull(row.getMaxPeakMemoryBytes());
        assertNull(row.getTotalMemorySpilled());
    }

    @Test
    void testSerializable() {
        StageGovernanceRow row = new StageGovernanceRow();
        row.setTimestampMs(1234567890000L);
        row.setAppId("app-123");
        row.setStageId(5);
        row.setStageDurationMs(10000.0);

        assertDoesNotThrow(() -> {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
            oos.writeObject(row);
            oos.close();
        });
    }
}
