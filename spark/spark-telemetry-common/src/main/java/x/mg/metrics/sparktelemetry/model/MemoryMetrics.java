package x.mg.metrics.sparktelemetry.model;

/**
 * JVM memory metrics captured from Dropwizard MetricRegistry gauges.
 */
public class MemoryMetrics {
    private long heapUsed;
    private long heapCommitted;
    private long heapMax;
    private long nonHeapUsed;
    private long nonHeapCommitted;
    private long nonHeapMax;
    private long directBufferCount;
    private long directBufferUsed;
    private long directBufferCapacity;
    private long mappedBufferCount;
    private long mappedBufferUsed;
    private long mappedBufferCapacity;

    public long getHeapUsed() { return heapUsed; }
    public void setHeapUsed(long v) { this.heapUsed = v; }

    public long getHeapCommitted() { return heapCommitted; }
    public void setHeapCommitted(long v) { this.heapCommitted = v; }

    public long getHeapMax() { return heapMax; }
    public void setHeapMax(long v) { this.heapMax = v; }

    public long getNonHeapUsed() { return nonHeapUsed; }
    public void setNonHeapUsed(long v) { this.nonHeapUsed = v; }

    public long getNonHeapCommitted() { return nonHeapCommitted; }
    public void setNonHeapCommitted(long v) { this.nonHeapCommitted = v; }

    public long getNonHeapMax() { return nonHeapMax; }
    public void setNonHeapMax(long v) { this.nonHeapMax = v; }

    public long getDirectBufferCount() { return directBufferCount; }
    public void setDirectBufferCount(long v) { this.directBufferCount = v; }

    public long getDirectBufferUsed() { return directBufferUsed; }
    public void setDirectBufferUsed(long v) { this.directBufferUsed = v; }

    public long getDirectBufferCapacity() { return directBufferCapacity; }
    public void setDirectBufferCapacity(long v) { this.directBufferCapacity = v; }

    public long getMappedBufferCount() { return mappedBufferCount; }
    public void setMappedBufferCount(long v) { this.mappedBufferCount = v; }

    public long getMappedBufferUsed() { return mappedBufferUsed; }
    public void setMappedBufferUsed(long v) { this.mappedBufferUsed = v; }

    public long getMappedBufferCapacity() { return mappedBufferCapacity; }
    public void setMappedBufferCapacity(long v) { this.mappedBufferCapacity = v; }
}
