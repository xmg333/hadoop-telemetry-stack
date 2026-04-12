package x.mg.metrics.mrtelemetry.poller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import x.mg.metrics.mrtelemetry.model.MRJobMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Extracts metrics from History Server JSON responses.
 */
public class CounterExtractor {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parse jobs list response.
     */
    public List<JobInfo> parseJobs(String json) throws IOException {
        List<JobInfo> jobs = new ArrayList<>();
        JsonNode root = MAPPER.readTree(json);
        JsonNode jobsNode = root.path("jobs").path("job");
        if (jobsNode.isArray()) {
            for (JsonNode jobNode : jobsNode) {
                jobs.add(parseJobInfo(jobNode));
            }
        } else if (jobsNode.isObject()) {
            jobs.add(parseJobInfo(jobsNode));
        }
        return jobs;
    }

    private JobInfo parseJobInfo(JsonNode node) {
        JobInfo info = new JobInfo();
        info.id = node.path("id").asText("");
        info.name = node.path("name").asText("");
        info.user = node.path("user").asText("");
        info.queue = node.path("queue").asText("default");
        info.state = node.path("state").asText("");
        info.submitTime = node.path("submitTime").asLong(0);
        info.startTime = node.path("startTime").asLong(0);
        info.finishTime = node.path("finishTime").asLong(0);
        // History Server job list API has no elapsedTime field; compute from start/finish
        long et = node.path("elapsedTime").asLong(0);
        if (et == 0 && info.finishTime > 0 && info.startTime > 0) {
            et = info.finishTime - info.startTime;
        }
        info.elapsedTime = et;
        info.totalMaps = node.path("mapsTotal").asInt(0);
        info.totalReduces = node.path("reducesTotal").asInt(0);
        info.failedMaps = node.path("mapsCompleted").asInt(0); // note: API uses mapsCompleted, not mapsFailed
        info.failedReduces = node.path("reducesCompleted").asInt(0);
        info.trackingUrl = node.path("trackingUrl").asText("");
        return info;
    }

    /**
     * Parse job counters response and populate MRJobMetrics.
     */
    public void populateCounters(MRJobMetrics metrics, String json) throws IOException {
        JsonNode root = MAPPER.readTree(json);
        JsonNode counterGroups = root.path("jobCounters").path("counterGroup");
        if (!counterGroups.isArray()) return;

        for (JsonNode group : counterGroups) {
            String groupName = group.path("counterGroupName").asText("");
            JsonNode counters = group.path("counter");
            if (!counters.isArray()) continue;

            for (JsonNode counter : counters) {
                String name = counter.path("name").asText("");
                long totalValue = counter.path("totalCounterValue").asLong(0);
                long mapValue = counter.path("mapCounterValue").asLong(0);
                long reduceValue = counter.path("reduceCounterValue").asLong(0);

                metrics.addCounter(groupName, name, totalValue);
                mapCounterToField(metrics, groupName, name, totalValue);
            }
        }
    }

    private void mapCounterToField(MRJobMetrics m, String group, String name, long value) {
        // FileSystemCounter
        if (group.contains("FileSystemCounter")) {
            switch (name) {
                case "HDFS_BYTES_READ": m.setHdfsBytesRead(value); break;
                case "HDFS_BYTES_WRITTEN": m.setHdfsBytesWritten(value); break;
                case "FILE_BYTES_READ": m.setFileBytesRead(value); break;
                case "FILE_BYTES_WRITTEN": m.setFileBytesWritten(value); break;
            }
        }
        // TaskCounter
        else if (group.contains("TaskCounter")) {
            switch (name) {
                case "CPU_MILLISECONDS": m.setCpuMilliseconds(value); break;
                case "GC_TIME_MILLIS": m.setGcTimeMillis(value); break;
                case "PHYSICAL_MEMORY_BYTES": m.setPhysicalMemoryBytes(value); break;
                case "VIRTUAL_MEMORY_BYTES": m.setVirtualMemoryBytes(value); break;
                case "COMMITTED_HEAP_BYTES": m.setCommittedHeapBytes(value); break;
                case "SPILLED_RECORDS": m.setSpilledRecords(value); break;
                case "MAP_INPUT_RECORDS": m.setMapInputRecords(value); break;
                case "MAP_OUTPUT_RECORDS": m.setMapOutputRecords(value); break;
                case "MAP_OUTPUT_BYTES": m.setMapOutputBytes(value); break;
                case "REDUCE_INPUT_RECORDS": m.setReduceInputRecords(value); break;
                case "REDUCE_OUTPUT_RECORDS": m.setReduceOutputRecords(value); break;
                case "REDUCE_SHUFFLE_BYTES": m.setReduceShuffleBytes(value); break;
            }
        }
        // JobCounter
        else if (group.contains("JobCounter")) {
            switch (name) {
                case "TOTAL_LAUNCHED_MAPS": m.setTotalMaps((int) value); break;
                case "TOTAL_LAUNCHED_REDUCES": m.setTotalReduces((int) value); break;
                case "NUM_FAILED_MAPS": m.setFailedMaps((int) value); break;
                case "NUM_FAILED_REDUCES": m.setFailedReduces((int) value); break;
                case "MILLIS_MAPS": m.setMillisMaps(value); break;
                case "MILLIS_REDUCES": m.setMillisReduces(value); break;
            }
        }
    }

    /**
     * Lightweight job info parsed from the jobs list.
     */
    public static class JobInfo {
        public String id;
        public String name;
        public String user;
        public String queue;
        public String state;
        public long submitTime;
        public long startTime;
        public long finishTime;
        public long elapsedTime;
        public int totalMaps;
        public int totalReduces;
        public int failedMaps;
        public int failedReduces;
        public String trackingUrl;
    }

    /**
     * Lightweight task info parsed from the tasks list.
     */
    public static class TaskInfo {
        public String id;
        public String type;     // "MAP" or "REDUCE"
        public String state;
        public long elapsedTime;
    }

    /**
     * Parse tasks list response for a job.
     */
    public List<TaskInfo> parseTasks(String json) throws IOException {
        List<TaskInfo> tasks = new ArrayList<>();
        JsonNode root = MAPPER.readTree(json);
        JsonNode tasksNode = root.path("tasks").path("task");
        if (tasksNode.isArray()) {
            for (JsonNode taskNode : tasksNode) {
                tasks.add(parseTaskInfo(taskNode));
            }
        } else if (tasksNode.isObject()) {
            tasks.add(parseTaskInfo(tasksNode));
        }
        return tasks;
    }

    private TaskInfo parseTaskInfo(JsonNode node) {
        TaskInfo info = new TaskInfo();
        info.id = node.path("id").asText("");
        info.type = node.path("type").asText("");
        info.state = node.path("state").asText("");
        info.elapsedTime = node.path("elapsedTime").asLong(0);
        return info;
    }

    /**
     * Parse task counters response and map to OTel metric names.
     * Uses the same OTel names as the Agent's CounterMapping.ALL.
     */
    public Map<String, Long> extractTaskCounters(String json) throws IOException {
        Map<String, Long> result = new HashMap<>();
        JsonNode root = MAPPER.readTree(json);
        JsonNode counterGroups = root.path("taskCounters").path("counterGroup");
        if (!counterGroups.isArray()) return result;

        for (JsonNode group : counterGroups) {
            String groupName = group.path("counterGroupName").asText("");
            JsonNode counters = group.path("counter");
            if (!counters.isArray()) continue;

            for (JsonNode counter : counters) {
                String name = counter.path("name").asText("");
                long value = counter.path("value").asLong(0);
                String otelName = mapCounterToOtelName(groupName, name);
                if (otelName != null) {
                    result.put(otelName, value);
                }
            }
        }
        return result;
    }

    private String mapCounterToOtelName(String group, String name) {
        if (group.contains("TaskCounter")) {
            switch (name) {
                case "MAP_INPUT_RECORDS": return "mr.task.io.map_input_records";
                case "MAP_OUTPUT_RECORDS": return "mr.task.io.map_output_records";
                case "MAP_OUTPUT_BYTES": return "mr.task.io.map_output_bytes";
                case "REDUCE_INPUT_RECORDS": return "mr.task.io.reduce_input_records";
                case "REDUCE_OUTPUT_RECORDS": return "mr.task.io.reduce_output_records";
                case "REDUCE_SHUFFLE_BYTES": return "mr.task.io.reduce_shuffle_bytes";
                case "SPILLED_RECORDS": return "mr.task.io.spilled_records";
                case "CPU_MILLISECONDS": return "mr.task.cpu_time_ms";
                case "GC_TIME_MILLIS": return "mr.task.gc_time_ms";
            }
        } else if (group.contains("FileSystemCounter")) {
            switch (name) {
                case "HDFS_BYTES_READ": return "mr.task.io.hdfs_bytes_read";
                case "HDFS_BYTES_WRITTEN": return "mr.task.io.hdfs_bytes_written";
                case "FILE_BYTES_READ": return "mr.task.io.file_bytes_read";
                case "FILE_BYTES_WRITTEN": return "mr.task.io.file_bytes_written";
            }
        }
        return null;
    }
}
