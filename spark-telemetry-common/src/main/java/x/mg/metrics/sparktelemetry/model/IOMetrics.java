package x.mg.metrics.sparktelemetry.model;

/**
 * IO metrics captured from Spark TaskMetrics.
 * Includes input/output bytes and records, shuffle read/write metrics, and spill metrics.
 */
public class IOMetrics {
    private long bytesRead;
    private long recordsRead;
    private long bytesWritten;
    private long recordsWritten;
    private long shuffleRemoteBytesRead;
    private long shuffleLocalBytesRead;
    private long shuffleRemoteBlocksFetched;
    private long shuffleFetchWaitTime;
    private long shuffleBytesWritten;
    private long shuffleWriteTime;
    private long shuffleRecordsWritten;
    private long diskBytesSpilled;
    private long memoryBytesSpilled;
    private long shuffleLocalBlocksFetched;
    private long shuffleRecordsRead;
    private long shuffleRemoteBytesReadToDisk;
    private long shuffleRemoteReqsDuration;

    public long getBytesRead() { return bytesRead; }
    public void setBytesRead(long bytesRead) { this.bytesRead = bytesRead; }

    public long getRecordsRead() { return recordsRead; }
    public void setRecordsRead(long recordsRead) { this.recordsRead = recordsRead; }

    public long getBytesWritten() { return bytesWritten; }
    public void setBytesWritten(long bytesWritten) { this.bytesWritten = bytesWritten; }

    public long getRecordsWritten() { return recordsWritten; }
    public void setRecordsWritten(long recordsWritten) { this.recordsWritten = recordsWritten; }

    public long getShuffleRemoteBytesRead() { return shuffleRemoteBytesRead; }
    public void setShuffleRemoteBytesRead(long v) { this.shuffleRemoteBytesRead = v; }

    public long getShuffleLocalBytesRead() { return shuffleLocalBytesRead; }
    public void setShuffleLocalBytesRead(long v) { this.shuffleLocalBytesRead = v; }

    public long getShuffleRemoteBlocksFetched() { return shuffleRemoteBlocksFetched; }
    public void setShuffleRemoteBlocksFetched(long v) { this.shuffleRemoteBlocksFetched = v; }

    public long getShuffleFetchWaitTime() { return shuffleFetchWaitTime; }
    public void setShuffleFetchWaitTime(long v) { this.shuffleFetchWaitTime = v; }

    public long getShuffleBytesWritten() { return shuffleBytesWritten; }
    public void setShuffleBytesWritten(long v) { this.shuffleBytesWritten = v; }

    public long getShuffleWriteTime() { return shuffleWriteTime; }
    public void setShuffleWriteTime(long v) { this.shuffleWriteTime = v; }

    public long getShuffleRecordsWritten() { return shuffleRecordsWritten; }
    public void setShuffleRecordsWritten(long v) { this.shuffleRecordsWritten = v; }

    public long getDiskBytesSpilled() { return diskBytesSpilled; }
    public void setDiskBytesSpilled(long v) { this.diskBytesSpilled = v; }

    public long getMemoryBytesSpilled() { return memoryBytesSpilled; }
    public void setMemoryBytesSpilled(long v) { this.memoryBytesSpilled = v; }

    public long getShuffleTotalBytesRead() {
        return shuffleRemoteBytesRead + shuffleLocalBytesRead;
    }

    public long getShuffleLocalBlocksFetched() { return shuffleLocalBlocksFetched; }
    public void setShuffleLocalBlocksFetched(long v) { this.shuffleLocalBlocksFetched = v; }

    public long getShuffleRecordsRead() { return shuffleRecordsRead; }
    public void setShuffleRecordsRead(long v) { this.shuffleRecordsRead = v; }

    public long getShuffleRemoteBytesReadToDisk() { return shuffleRemoteBytesReadToDisk; }
    public void setShuffleRemoteBytesReadToDisk(long v) { this.shuffleRemoteBytesReadToDisk = v; }

    public long getShuffleRemoteReqsDuration() { return shuffleRemoteReqsDuration; }
    public void setShuffleRemoteReqsDuration(long v) { this.shuffleRemoteReqsDuration = v; }
}
