package x.mg.metrics.flink.model;

import x.mg.metrics.flink.classify.MetricCategory;

import java.io.Serializable;
import java.util.Map;

/**
 * Unified wide-row model for the unified_metrics table.
 * Replaces 11 separate row models with a single class that uses
 * engine + event_type as discriminators and normalized common columns.
 */
public class MetricEventRow implements Serializable {
    private static final long serialVersionUID = 1L;
    // Identity (always populated)
    private long timestampMs;
    private String eventType;  // TASK, STAGE, JOB, JVM_MEMORY, JVM_GC, SQL_QUERY, SQL_TABLE_IO, HIVE_QUERY, HIVE_TABLE_IO, MR_JOB, MR_TASK
    private String engine;     // SPARK, MR, HIVE

    // Normalized common dimensions
    private String status;     // "true"/"false"/null (normalized success/fail)
    private String appId;      // Spark app_id, MR job_id, Hive query_id
    private String appName;
    private String userName;
    private String queue;

    // Normalized common metrics (cross-engine)
    private Double durationMs;
    private Double bytesRead;
    private Double bytesWritten;
    private Double shuffleBytesRead;
    private Double shuffleBytesWritten;
    private Double cpuTimeMs;
    private Double gcTimeMs;
    private Double bytesSpilled;

    // Spark-specific dimensions
    private String executorId;
    private Integer stageId;
    private String taskId;     // String to accommodate Spark Long and MR String
    private String taskHost;
    private String taskLocality;
    private String taskSpeculative;

    // Spark-specific metrics
    private Double executorRunTimeMs;
    private Double executorCpuTimeNs;
    private Double deserializeTimeMs;
    private Double deserializeCpuTimeNs;
    private Double resultSerializationTimeMs;
    private Double schedulerDelayMs;
    private Double resultSizeBytes;
    private Double peakExecutionMemoryBytes;
    private Double shuffleLocalBlocksFetched;
    private Double shuffleRecordsRead;
    private Double shuffleRemoteBytesReadToDisk;
    private Double shuffleRemoteReqsDurationMs;
    private Double diskBytesSpilled;
    private Double shuffleFetchWaitTimeMs;
    private Double numTasks;
    private Double numStages;

    // SQL-specific
    private String executionId;
    private Double joinCount;

    // SQL Table IO / Hive Table IO
    private String tableName;
    private String tableOperation;
    private Double bytes;
    private Double rows;
    private Double filesRead;
    private Double timeMs;

    // JVM-specific
    private Double heapUsed;
    private Double nonHeapUsed;
    private String gcName;
    private Double gcCount;

    // MR-specific dimensions
    private String jobId;
    private String jobName;
    private String taskType;

    // MR-specific metrics
    private Double mapOutputBytes;
    private Double physicalMemoryBytes;
    private Double virtualMemoryBytes;
    private Double committedHeapBytes;
    private Double mapsDurationMs;
    private Double reducesDurationMs;
    private Double launchedMaps;
    private Double launchedReduces;
    private Long startTimeMs;
    private Long finishTimeMs;
    private Double elapsedTimeMs;

    // MR IO detail (preserved alongside normalized ioBytesRead)
    private Double hdfsBytesRead;
    private Double hdfsBytesWritten;
    private Double fileBytesRead;
    private Double fileBytesWritten;
    private Double mapInputRecords;
    private Double mapOutputRecords;
    private Double reduceInputRecords;
    private Double reduceOutputRecords;
    private Double reduceShuffleBytes;
    private Double spilledRecords;

    // MR file operations
    private Double hdfsReadOps;
    private Double hdfsWriteOps;
    private Double hdfsLargeReadOps;

    // Hive-specific
    private String operation;
    private String tableType;
    private String executionEngine;
    private Double successCount;
    private Double failureCount;
    private Double inputRows;
    private Double outputRows;

    // IO records (Spark)
    private Double recordsRead;
    private Double recordsWritten;

    // SQL/Hive query text
    private String queryText;

    public MetricEventRow() {}

    public static MetricEventRow fromLabels(long timestampMs, MetricCategory cat, Map<String, String> labels) {
        MetricEventRow row = new MetricEventRow();
        row.timestampMs = timestampMs;
        row.eventType = cat.name();
        row.engine = getEngine(cat);

        switch (cat) {
            case TASK:
                row.timestampMs = timestampMs;
                row.status = labels.get("spark.task.success");
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
                row.stageId = parseIntNullable(labels.get("spark.stage.id"));
                row.taskId = labels.get("spark.task.id");
                row.taskHost = labels.get("spark.task.host");
                row.taskLocality = labels.get("spark.task.locality");
                row.taskSpeculative = labels.get("spark.task.speculative");
                break;
            case STAGE:
                row.status = null; // stages don't have success/fail
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
                row.stageId = parseIntNullable(labels.get("spark.stage.id"));
                break;
            case JOB:
                row.status = labels.get("spark.job.success");
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                break;
            case JVM_MEMORY:
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
                break;
            case JVM_GC:
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executorId = labels.getOrDefault("spark.executor.id", "unknown");
                row.gcName = labels.get("gc_name");
                break;
            case SQL_EXECUTION:
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executionId = labels.getOrDefault("spark.sql.execution_id", "unknown");
                row.queryText = labels.getOrDefault("spark.sql.query_text", null);
                break;
            case SQL_TABLE_IO:
                row.appId = labels.getOrDefault("spark.app.id", "unknown");
                row.appName = labels.getOrDefault("spark.app.name", "");
                row.userName = labels.getOrDefault("spark.user", "");
                row.queue = labels.getOrDefault("spark.yarn.queue", "");
                row.executionId = labels.getOrDefault("spark.sql.execution_id", "unknown");
                row.tableName = labels.getOrDefault("spark.sql.table_name", "unknown");
                row.tableOperation = labels.getOrDefault("spark.sql.operation", "unknown");
                break;
            case HIVE_QUERY:
                row.status = labels.getOrDefault("hive.query.success", "unknown");
                row.appId = labels.getOrDefault("hive.query.id", "unknown");
                row.appName = labels.getOrDefault("hive.query.id", "");  // query_id as app_name
                row.userName = labels.getOrDefault("hive.query.user", "unknown");
                row.queue = labels.getOrDefault("hive.query.queue", "");
                row.operation = labels.getOrDefault("hive.query.operation", "unknown");
                row.executionEngine = labels.getOrDefault("hive.query.execution_engine", "unknown");
                row.queryText = labels.getOrDefault("hive.query.sql_text", null);
                break;
            case HIVE_TABLE_IO:
                row.appId = labels.getOrDefault("hive.query.id", "unknown");
                row.appName = labels.getOrDefault("hive.query.id", "");  // query_id as app_name
                row.userName = labels.getOrDefault("hive.query.user", "unknown");
                row.queue = labels.getOrDefault("hive.query.queue", "");
                row.operation = labels.getOrDefault("hive.query.operation", "unknown");
                row.executionEngine = labels.getOrDefault("hive.query.execution_engine", "unknown");
                row.tableName = labels.getOrDefault("hive.query.input_table",
                        labels.getOrDefault("hive.query.output_table", "unknown"));
                row.tableType = labels.getOrDefault("hive.query.input_table", null) != null ? "input" : "output";
                break;
            case MR_JOB:
                // Use actual job finish time if available
                String finishTimeStr = labels.get("mr.job.finish_time_ms");
                if (finishTimeStr != null && !finishTimeStr.isEmpty()) {
                    try { row.timestampMs = Long.parseLong(finishTimeStr); }
                    catch (NumberFormatException e) { row.timestampMs = timestampMs; }
                }
                String startTimeStr = labels.get("mr.job.start_time_ms");
                if (startTimeStr != null && !startTimeStr.isEmpty()) {
                    try { row.startTimeMs = Long.parseLong(startTimeStr); }
                    catch (NumberFormatException e) { row.startTimeMs = null; }
                }
                row.finishTimeMs = row.timestampMs;

                row.jobId = labels.getOrDefault("mr.job.id", "unknown");
                row.appId = row.jobId; // MR job_id as the primary identity
                row.appName = labels.getOrDefault("mr.job.name", "");
                row.jobName = labels.getOrDefault("mr.job.name", "");
                row.userName = labels.getOrDefault("mr.job.user", "unknown");
                row.queue = labels.getOrDefault("mr.job.queue", "");
                // Normalize MR state to status
                String mrState = labels.getOrDefault("mr.job.state", "unknown");
                row.status = "SUCCEEDED".equals(mrState) ? "true" : "false";
                break;
            case MR_TASK:
                String taskFinishTime = labels.get("mr.task.finish_time_ms");
                if (taskFinishTime != null && !taskFinishTime.isEmpty()) {
                    try { row.timestampMs = Long.parseLong(taskFinishTime); }
                    catch (NumberFormatException e) { row.timestampMs = timestampMs; }
                }
                row.taskId = labels.getOrDefault("mr.task.id", "unknown");
                row.jobId = labels.getOrDefault("mr.job.id", "unknown");
                row.appId = row.jobId;
                row.taskType = labels.getOrDefault("mr.task.type", "unknown");
                row.jobName = labels.getOrDefault("mr.job.name", "");
                row.appName = labels.getOrDefault("mr.job.name", "");
                row.userName = labels.getOrDefault("mr.job.user", "unknown");
                row.queue = labels.getOrDefault("mr.job.queue", "");
                String mrTaskState = labels.getOrDefault("mr.task.state", "unknown");
                row.status = "SUCCEEDED".equals(mrTaskState) ? "true" : "false";
                break;
            default:
                break;
        }
        return row;
    }

    public static String getEngine(MetricCategory cat) {
        switch (cat) {
            case TASK:
            case STAGE:
            case JOB:
            case JVM_MEMORY:
            case JVM_GC:
            case SQL_EXECUTION:
            case SQL_TABLE_IO:
                return "SPARK";
            case MR_JOB:
            case MR_TASK:
                return "MR";
            case HIVE_QUERY:
            case HIVE_TABLE_IO:
                return "HIVE";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Normalize MR IO metrics into the unified columns after accumulation is complete.
     * Called before insert. For MR_JOB/MR_TASK, sums hdfs + file bytes into ioBytesRead/Written.
     */
    public void normalizeAggregatedMetrics() {
        if ("MR".equals(engine)) {
            // Sum hdfs + file into unified IO columns
            double readTotal = 0, writeTotal = 0;
            if (hdfsBytesRead != null) readTotal += hdfsBytesRead;
            if (fileBytesRead != null) readTotal += fileBytesRead;
            if (hdfsBytesWritten != null) writeTotal += hdfsBytesWritten;
            if (fileBytesWritten != null) writeTotal += fileBytesWritten;
            if (readTotal > 0) bytesRead = readTotal;
            if (writeTotal > 0) bytesWritten = writeTotal;

            // MR reduce_shuffle_bytes → unified shuffleBytesRead
            if (reduceShuffleBytes != null && reduceShuffleBytes > 0) {
                shuffleBytesRead = reduceShuffleBytes;
            }

            // MR elapsed_time_ms → duration_ms (for jobs)
            if (elapsedTimeMs != null && elapsedTimeMs > 0 && durationMs == null) {
                durationMs = elapsedTimeMs;
            }
        }

        if ("SPARK".equals(engine)) {
            // Spark: convert cpu_time_ns to cpu_time_ms for normalized column
            if (executorCpuTimeNs != null) {
                cpuTimeMs = executorCpuTimeNs / 1_000_000.0;
            }
            // Spark: jvm_gc_time_ms → gc_time_ms
            if (getJvmGcTimeMs() != null) {
                gcTimeMs = getJvmGcTimeMs();
            }
            // Spark: disk_bytes_spilled + memory_bytes_spilled → memory_bytes_spilled
            double spillTotal = 0;
            if (diskBytesSpilled != null) spillTotal += diskBytesSpilled;
            if (getSparkMemoryBytesSpilled() != null) spillTotal += getSparkMemoryBytesSpilled();
            if (spillTotal > 0) bytesSpilled = spillTotal;
        }

        if ("MR".equals(engine)) {
            // MR already has cpuTimeMs and gcTimeMs in ms
            // MR spilled_records is a count, not bytes — keep as-is in MR-specific column
        }

        if ("HIVE".equals(engine)) {
            // Hive: input_bytes → ioBytesRead, output_bytes → ioBytesWritten
            if (getHiveInputBytes() != null && getHiveInputBytes() > 0) {
                bytesRead = getHiveInputBytes();
            }
            if (getHiveOutputBytes() != null && getHiveOutputBytes() > 0) {
                bytesWritten = getHiveOutputBytes();
            }
        }
    }

    public void setMetricColumn(String columnName, double value) {
        switch (columnName) {
            // Normalized common metrics
            case "duration_ms": durationMs = value; break;
            case "cpu_time_ms": cpuTimeMs = value; break;
            case "gc_time_ms": gcTimeMs = value; break;

            // Spark task metrics
            case "io_bytes_read":
            case "bytes_read": bytesRead = value; break;
            case "io_bytes_written":
            case "bytes_written": bytesWritten = value; break;
            case "io_records_read":
            case "records_read": recordsRead = value; break;
            case "io_records_written":
            case "records_written": recordsWritten = value; break;
            case "shuffle_bytes_read": shuffleBytesRead = value; break;
            case "shuffle_bytes_written": shuffleBytesWritten = value; break;
            case "shuffle_fetch_wait_time_ms": shuffleFetchWaitTimeMs = value; break;
            case "disk_bytes_spilled": diskBytesSpilled = value; break;
            case "memory_bytes_spilled":
            case "bytes_spilled":
                // For Spark, this is memory_bytes_spilled; for MR it's spilled_records count
                if ("SPARK".equals(engine)) {
                    sparkMemoryBytesSpilled = value;
                } else {
                    bytesSpilled = value;
                }
                break;
            case "executor_run_time_ms": executorRunTimeMs = value; break;
            case "executor_cpu_time_ns": executorCpuTimeNs = value; break;
            case "deserialize_time_ms": deserializeTimeMs = value; break;
            case "deserialize_cpu_time_ns": deserializeCpuTimeNs = value; break;
            case "result_serialization_time_ms": resultSerializationTimeMs = value; break;
            case "jvm_gc_time_ms": jvmGcTimeMs = value; break;
            case "scheduler_delay_ms": schedulerDelayMs = value; break;
            case "result_size_bytes": resultSizeBytes = value; break;
            case "peak_execution_memory_bytes": peakExecutionMemoryBytes = value; break;
            case "shuffle_local_blocks_fetched": shuffleLocalBlocksFetched = value; break;
            case "shuffle_records_read": shuffleRecordsRead = value; break;
            case "shuffle_remote_bytes_read_to_disk": shuffleRemoteBytesReadToDisk = value; break;
            case "shuffle_remote_reqs_duration_ms": shuffleRemoteReqsDurationMs = value; break;

            // Stage metrics
            case "num_tasks": numTasks = value; break;
            case "num_stages": numStages = value; break;

            // SQL metrics
            case "join_count": joinCount = value; break;

            // SQL Table IO
            case "bytes": bytes = value; break;
            case "rows": rows = value; break;
            case "files_read": filesRead = value; break;
            case "time_ms": timeMs = value; break;

            // JVM metrics
            case "heap_used": heapUsed = value; break;
            case "non_heap_used": nonHeapUsed = value; break;
            case "gc_count": gcCount = value; break;

            // Hive metrics
            case "success_count": successCount = value; break;
            case "failure_count": failureCount = value; break;
            case "input_bytes": hiveInputBytes = value; break;
            case "output_bytes": hiveOutputBytes = value; break;
            case "input_rows": inputRows = value; break;
            case "output_rows": outputRows = value; break;
            case "input_table_count": inputTableCount = value; break;
            case "output_table_count": outputTableCount = value; break;

            // MR job metrics
            case "hdfs_bytes_read": hdfsBytesRead = value; break;
            case "hdfs_bytes_written": hdfsBytesWritten = value; break;
            case "file_bytes_read": fileBytesRead = value; break;
            case "file_bytes_written": fileBytesWritten = value; break;
            case "map_input_records": mapInputRecords = value; break;
            case "map_output_records": mapOutputRecords = value; break;
            case "map_output_bytes": mapOutputBytes = value; break;
            case "reduce_input_records": reduceInputRecords = value; break;
            case "reduce_output_records": reduceOutputRecords = value; break;
            case "reduce_shuffle_bytes": reduceShuffleBytes = value; break;
            case "spilled_records": spilledRecords = value; break;
            case "physical_memory_bytes": physicalMemoryBytes = value; break;
            case "virtual_memory_bytes": virtualMemoryBytes = value; break;
            case "committed_heap_bytes": committedHeapBytes = value; break;
            case "maps_duration_ms": mapsDurationMs = value; break;
            case "reduces_duration_ms": reducesDurationMs = value; break;
            case "elapsed_time_ms": elapsedTimeMs = value; break;
            case "launched_maps": launchedMaps = value; break;
            case "launched_reduces": launchedReduces = value; break;

            // MR task metrics
            case "hdfs_read_ops": hdfsReadOps = value; break;
            case "hdfs_write_ops": hdfsWriteOps = value; break;
            case "hdfs_large_read_ops": hdfsLargeReadOps = value; break;
        }
    }

    // Private helper fields for normalization
    private Double jvmGcTimeMs;
    private Double sparkMemoryBytesSpilled;
    private Double hiveInputBytes;
    private Double hiveOutputBytes;
    private Double inputTableCount;
    private Double outputTableCount;

    private static Integer parseIntNullable(String s) {
        try { return s != null ? Integer.parseInt(s) : null; }
        catch (NumberFormatException e) { return null; }
    }

    // ==================== Getters ====================

    public long getTimestampMs() { return timestampMs; }
    public String getEventType() { return eventType; }
    public String getEngine() { return engine; }
    public String getStatus() { return status; }
    public String getAppId() { return appId; }
    public String getAppName() { return appName; }
    public String getUserName() { return userName; }
    public String getQueue() { return queue; }
    public Double getDurationMs() { return durationMs; }
    public Double getBytesRead() { return bytesRead; }
    public Double getBytesWritten() { return bytesWritten; }
    public Double getShuffleBytesRead() { return shuffleBytesRead; }
    public Double getShuffleBytesWritten() { return shuffleBytesWritten; }
    public Double getCpuTimeMs() { return cpuTimeMs; }
    public Double getGcTimeMs() { return gcTimeMs; }
    public Double getBytesSpilled() { return bytesSpilled; }
    public String getExecutorId() { return executorId; }
    public Integer getStageId() { return stageId; }
    public String getTaskId() { return taskId; }
    public String getTaskHost() { return taskHost; }
    public String getTaskLocality() { return taskLocality; }
    public String getTaskSpeculative() { return taskSpeculative; }
    public Double getExecutorRunTimeMs() { return executorRunTimeMs; }
    public Double getExecutorCpuTimeNs() { return executorCpuTimeNs; }
    public Double getDeserializeTimeMs() { return deserializeTimeMs; }
    public Double getDeserializeCpuTimeNs() { return deserializeCpuTimeNs; }
    public Double getResultSerializationTimeMs() { return resultSerializationTimeMs; }
    public Double getSchedulerDelayMs() { return schedulerDelayMs; }
    public Double getResultSizeBytes() { return resultSizeBytes; }
    public Double getPeakExecutionMemoryBytes() { return peakExecutionMemoryBytes; }
    public Double getShuffleLocalBlocksFetched() { return shuffleLocalBlocksFetched; }
    public Double getShuffleRecordsRead() { return shuffleRecordsRead; }
    public Double getShuffleRemoteBytesReadToDisk() { return shuffleRemoteBytesReadToDisk; }
    public Double getShuffleRemoteReqsDurationMs() { return shuffleRemoteReqsDurationMs; }
    public Double getDiskBytesSpilled() { return diskBytesSpilled; }
    public Double getShuffleFetchWaitTimeMs() { return shuffleFetchWaitTimeMs; }
    public Double getNumTasks() { return numTasks; }
    public Double getNumStages() { return numStages; }
    public String getExecutionId() { return executionId; }
    public Double getJoinCount() { return joinCount; }
    public String getTableName() { return tableName; }
    public String getTableOperation() { return tableOperation; }
    public Double getBytes() { return bytes; }
    public Double getRows() { return rows; }
    public Double getFilesRead() { return filesRead; }
    public Double getTimeMs() { return timeMs; }
    public Double getHeapUsed() { return heapUsed; }
    public Double getNonHeapUsed() { return nonHeapUsed; }
    public String getGcName() { return gcName; }
    public Double getGcCount() { return gcCount; }
    public String getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public String getTaskType() { return taskType; }
    public Double getMapOutputBytes() { return mapOutputBytes; }
    public Double getPhysicalMemoryBytes() { return physicalMemoryBytes; }
    public Double getVirtualMemoryBytes() { return virtualMemoryBytes; }
    public Double getCommittedHeapBytes() { return committedHeapBytes; }
    public Double getMapsDurationMs() { return mapsDurationMs; }
    public Double getReducesDurationMs() { return reducesDurationMs; }
    public Double getLaunchedMaps() { return launchedMaps; }
    public Double getLaunchedReduces() { return launchedReduces; }
    public Long getStartTimeMs() { return startTimeMs; }
    public Long getFinishTimeMs() { return finishTimeMs; }
    public Double getHdfsBytesRead() { return hdfsBytesRead; }
    public Double getHdfsBytesWritten() { return hdfsBytesWritten; }
    public Double getFileBytesRead() { return fileBytesRead; }
    public Double getFileBytesWritten() { return fileBytesWritten; }
    public Double getMapInputRecords() { return mapInputRecords; }
    public Double getMapOutputRecords() { return mapOutputRecords; }
    public Double getReduceInputRecords() { return reduceInputRecords; }
    public Double getReduceOutputRecords() { return reduceOutputRecords; }
    public Double getReduceShuffleBytes() { return reduceShuffleBytes; }
    public Double getSpilledRecords() { return spilledRecords; }
    public Double getHdfsReadOps() { return hdfsReadOps; }
    public Double getHdfsWriteOps() { return hdfsWriteOps; }
    public Double getHdfsLargeReadOps() { return hdfsLargeReadOps; }
    public String getOperation() { return operation; }
    public String getTableType() { return tableType; }
    public String getExecutionEngine() { return executionEngine; }
    public Double getSuccessCount() { return successCount; }
    public Double getFailureCount() { return failureCount; }
    public Double getInputRows() { return inputRows; }
    public Double getOutputRows() { return outputRows; }
    public Double getRecordsRead() { return recordsRead; }
    public Double getRecordsWritten() { return recordsWritten; }
    public String getQueryText() { return queryText; }

    // Internal getters for normalization
    private Double getJvmGcTimeMs() { return jvmGcTimeMs; }
    private Double getSparkMemoryBytesSpilled() { return sparkMemoryBytesSpilled; }
    private Double getHiveInputBytes() { return hiveInputBytes; }
    private Double getHiveOutputBytes() { return hiveOutputBytes; }
    public Double getElapsedTimeMs() { return elapsedTimeMs; }
}
