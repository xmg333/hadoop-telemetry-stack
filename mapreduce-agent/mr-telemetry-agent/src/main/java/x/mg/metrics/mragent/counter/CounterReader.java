package x.mg.metrics.mragent.counter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads Hadoop MR counters via reflection using the new MapReduce API:
 * context.getCounter(groupName, counterName).getValue()
 *
 * Works with both Hadoop 2.x and 3.x. In Hadoop 3.x, WrappedMapper/WrappedReducer
 * context objects delegate getCounter() to the underlying TaskInputOutputContext.
 */
public class CounterReader {

    private static final Logger LOG = Logger.getLogger(CounterReader.class.getName());

    /**
     * Read all configured counters from the Context object.
     * Returns a map of OTel metric name -> current long value.
     * Returns empty map on any error.
     */
    public Map<String, Long> readCounters(Object context) {
        if (context == null) return Collections.emptyMap();

        try {
            Object unwrapped = unwrapContext(context);
            Map<String, Long> result = new HashMap<>();

            // Read all counter values via getCounter(groupName, counterName)
            for (CounterMapping mapping : CounterMapping.ALL) {
                try {
                    long value = readCounterNewApi(unwrapped, mapping.groupName, mapping.counterName);
                    if (value > 0) {
                        result.put(mapping.otelName, value);
                    }
                } catch (Exception e) {
                    // Individual counter read failure should not block others
                }
            }

            // Supplement FileSystemCounter values from FileSystem.Statistics
            // for real-time values during task execution (Hadoop's Counters
            // object doesn't flush FS stats until task completion).
            readFileSystemStats(result);

            return result;
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to read counters: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Read file system statistics directly from FileSystem.getAllStatistics().
     * FileSystemCounter values in Hadoop's Counters object are only flushed at
     * task completion, so they appear as 0 during task execution.
     * Reading from FileSystem.Statistics gives real-time values.
     */
    private void readFileSystemStats(Map<String, Long> result) {
        try {
            // FileSystem.getAllStatistics() returns List<FileSystem.Statistics>
            Class<?> fsClass = Class.forName("org.apache.hadoop.fs.FileSystem");
            Method getAllStats = fsClass.getMethod("getAllStatistics");
            @SuppressWarnings("unchecked")
            Iterable<Object> stats = (Iterable<Object>) getAllStats.invoke(null);

            for (Object stat : stats) {
                Method getScheme = stat.getClass().getMethod("getScheme");
                String scheme = ((String) getScheme.invoke(stat)).toUpperCase();

                // Skip FILE scheme — production uses HDFS, not local filesystem
                if ("FILE".equals(scheme)) continue;

                Method getReadOps = stat.getClass().getMethod("getReadOps");
                long readOps = ((Number) getReadOps.invoke(stat)).longValue();

                Method getLargeReadOps = stat.getClass().getMethod("getLargeReadOps");
                long largeReadOps = ((Number) getLargeReadOps.invoke(stat)).longValue();

                Method getWriteOps = stat.getClass().getMethod("getWriteOps");
                long writeOps = ((Number) getWriteOps.invoke(stat)).longValue();

                String prefix = scheme.toLowerCase();

                System.err.println("[mr-telemetry-agent] FS stats: scheme=" + scheme
                    + " readOps=" + readOps + " writeOps=" + writeOps
                    + " largeReadOps=" + largeReadOps);

                // Only merge ops — bytes come from getCounter() which is per-task accurate.
                // FileSystem.Statistics are JVM-global and leak across tasks in container reuse.
                if (readOps > 0) result.merge("mr.task.io." + prefix + "_read_ops", readOps, Math::max);
                if (writeOps > 0) result.merge("mr.task.io." + prefix + "_write_ops", writeOps, Math::max);
                if (largeReadOps > 0) result.merge("mr.task.io." + prefix + "_large_read_ops", largeReadOps, Math::max);
            }
        } catch (Exception e) {
            System.err.println("[mr-telemetry-agent] FS stats read failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Read a counter value using the new MapReduce API:
     * context.getCounter(groupName, counterName).getValue()
     */
    private long readCounterNewApi(Object context, String groupName, String counterName) throws Exception {
        Method getCounterMethod = context.getClass().getMethod("getCounter", String.class, String.class);
        Object counter = getCounterMethod.invoke(context, groupName, counterName);
        if (counter == null) return 0L;
        Method getValueMethod = counter.getClass().getMethod("getValue");
        return (Long) getValueMethod.invoke(counter);
    }

    /**
     * Extract task identity from Context: task attempt ID, job ID, job name.
     * Tries the OUTER context first for getTaskAttemptID() to avoid wrong IDs
     * when YARN reuses a container (Mapper→Reducer in same JVM).
     */
    public TaskIdentity extractTaskIdentity(Object context) {
        if (context == null) return TaskIdentity.UNKNOWN;

        try {
            String taskId = "unknown";
            String jobId = "unknown";
            String jobName = "unknown";
            String user = "unknown";
            String queue = "unknown";

            // Try outer context first for task attempt ID — the outermost context
            // passed to Mapper.run()/Reducer.run() has the correct TaskAttemptID
            // even in YARN container reuse scenarios.
            taskId = tryGetTaskAttemptID(context);
            jobId = tryGetJobID(context);
            String[] nameUserQueue = tryGetNameUserQueue(context);

            // If outer context didn't yield results, try unwrapped
            if ("unknown".equals(taskId) || "unknown".equals(jobId)) {
                Object unwrapped = unwrapContext(context);
                if (!unwrapped.equals(context)) {
                    if ("unknown".equals(taskId)) taskId = tryGetTaskAttemptID(unwrapped);
                    if ("unknown".equals(jobId)) jobId = tryGetJobID(unwrapped);
                    if ("unknown".equals(nameUserQueue[0])) nameUserQueue = tryGetNameUserQueue(unwrapped);
                }
            }

            jobName = nameUserQueue[0];
            user = nameUserQueue[1];
            queue = nameUserQueue[2];

            return new TaskIdentity(taskId, jobId, jobName, user, queue);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to extract task identity: " + e.getMessage());
            return TaskIdentity.UNKNOWN;
        }
    }

    private String tryGetTaskAttemptID(Object obj) {
        try {
            Method m = obj.getClass().getMethod("getTaskAttemptID");
            return safeToString(m.invoke(obj));
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String tryGetJobID(Object obj) {
        try {
            Method m = obj.getClass().getMethod("getJobID");
            return safeToString(m.invoke(obj));
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String[] tryGetNameUserQueue(Object obj) {
        String[] result = {"unknown", "unknown", "unknown"};
        try {
            Method m = obj.getClass().getMethod("getConfiguration");
            Object config = m.invoke(obj);
            if (config != null) {
                Method getMethod = config.getClass().getMethod("get", String.class, String.class);
                result[0] = (String) getMethod.invoke(config, "mapreduce.job.name", "unknown");
                result[1] = (String) getMethod.invoke(config, "mapreduce.job.user.name", "unknown");
                result[2] = (String) getMethod.invoke(config, "mapreduce.job.queuename", "unknown");
            }
        } catch (Exception e) {
            // stay with defaults
        }
        return result;
    }

    /**
     * Unwrap Hadoop 3.x WrappedMapper/WrappedReducer context objects.
     * Falls back to looking for inner mapContext/reduceContext fields.
     */
    private Object unwrapContext(Object context) throws Exception {
        // Check if context has getCounter(String, String) directly
        try {
            context.getClass().getMethod("getCounter", String.class, String.class);
            return context;
        } catch (NoSuchMethodException e) {
            // continue unwrapping
        }

        // Use context class name to determine which field to check first.
        // Checking mapContext before reduceContext on a WrappedReducer could
        // accidentally pick up a stale mapContext from a parent class in the
        // hierarchy when YARN reuses a container (Mapper→Reducer in same JVM).
        String className = context.getClass().getName().toLowerCase();
        boolean likelyReducer = className.contains("reducer");
        boolean likelyMapper = className.contains("mapper");

        Object innerContext;
        if (likelyReducer) {
            innerContext = getFieldRecursive(context, "reduceContext");
            if (innerContext == null) innerContext = getFieldRecursive(context, "mapContext");
        } else if (likelyMapper) {
            innerContext = getFieldRecursive(context, "mapContext");
            if (innerContext == null) innerContext = getFieldRecursive(context, "reduceContext");
        } else {
            // Unknown wrapper — try reduceContext first (map is more common, but
            // getting a stale mapContext in a reducer is worse than vice versa)
            innerContext = getFieldRecursive(context, "reduceContext");
            if (innerContext == null) innerContext = getFieldRecursive(context, "mapContext");
        }

        if (innerContext != null) {
            return innerContext;
        }

        // No unwrapping possible, return as-is
        return context;
    }

    private Object getFieldRecursive(Object obj, String fieldName) {
        Class<?> current = obj.getClass();
        while (current != null && current != Object.class) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(obj);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private boolean hasMethod(Class<?> clazz, String methodName) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Method m : current.getMethods()) {
                if (m.getName().equals(methodName) && m.getParameterCount() == 0) {
                    return true;
                }
            }
            current = current.getSuperclass();
        }
        return false;
    }

    private String safeToString(Object obj) {
        return obj != null ? obj.toString() : "unknown";
    }
}
