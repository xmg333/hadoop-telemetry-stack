package x.mg.metrics.hivetelemetry.model;

public class HiveTableIOMetrics {
    private final String tableName;
    private final String tableType; // "input" or "output"
    private long bytes;
    private long rows;
    private long filesRead;

    public HiveTableIOMetrics(String tableName, String tableType) {
        this.tableName = tableName;
        this.tableType = tableType;
    }

    public String getTableName() { return tableName; }
    public String getTableType() { return tableType; }
    public long getBytes() { return bytes; }
    public void setBytes(long bytes) { this.bytes = bytes; }
    public long getRows() { return rows; }
    public void setRows(long rows) { this.rows = rows; }
    public long getFilesRead() { return filesRead; }
    public void setFilesRead(long filesRead) { this.filesRead = filesRead; }
}
