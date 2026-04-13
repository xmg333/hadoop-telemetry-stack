package x.mg.metrics.sparktelemetry.model;

/**
 * Per-table IO metrics for a SQL query execution.
 * One instance per scan/write target within a single query.
 */
public class SqlTableIOMetrics {

    private long executionId;
    private String tableName;
    private String operation;  // "scan" or "write"

    private long bytes;
    private long rows;
    private long filesRead;
    private long timeMs;

    public long getExecutionId() { return executionId; }
    public void setExecutionId(long executionId) { this.executionId = executionId; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public long getBytes() { return bytes; }
    public void setBytes(long bytes) { this.bytes = bytes; }

    public long getRows() { return rows; }
    public void setRows(long rows) { this.rows = rows; }

    public long getFilesRead() { return filesRead; }
    public void setFilesRead(long filesRead) { this.filesRead = filesRead; }

    public long getTimeMs() { return timeMs; }
    public void setTimeMs(long timeMs) { this.timeMs = timeMs; }
}
