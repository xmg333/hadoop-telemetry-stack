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

            // Read TaskCounter values via getCounter(groupName, counterName)
            for (CounterMapping mapping : CounterMapping.ALL) {
                if (!CounterMapping.TASK_COUNTER.equals(mapping.groupName)) continue;
                try {
                    long value = readCounterNewApi(unwrapped, mapping.groupName, mapping.counterName);
                    if (value > 0) {
                        result.put(mapping.otelName, value);
                    }
                } catch (Exception e) {
                    // Individual counter read failure should not block others
                }
            }

            // Read FileSystemCounter values from FileSystem.Statistics
            // (Counters object doesn't flush FS stats until task completion)
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

                Method getBytesRead = stat.getClass().getMethod("getBytesRead");
                long bytesRead = ((Number) getBytesRead.invoke(stat)).longValue();

                Method getBytesWritten = stat.getClass().getMethod("getBytesWritten");
                long bytesWritten = ((Number) getBytesWritten.invoke(stat)).longValue();

                Method getReadOps = stat.getClass().getMethod("getReadOps");
                long readOps = ((Number) getReadOps.invoke(stat)).longValue();

                Method getLargeReadOps = stat.getClass().getMethod("getLargeReadOps");
                long largeReadOps = ((Number) getLargeReadOps.invoke(stat)).longValue();

                Method getWriteOps = stat.getClass().getMethod("getWriteOps");
                long writeOps = ((Number) getWriteOps.invoke(stat)).longValue();

                String prefix = scheme.equals("FILE") ? "file" : scheme.toLowerCase();

                if (bytesRead > 0) result.put("mr.task.io." + prefix + "_bytes_read", bytesRead);
                if (bytesWritten > 0) result.put("mr.task.io." + prefix + "_bytes_written", bytesWritten);
                if (readOps > 0) result.put("mr.task.io." + prefix + "_read_ops", readOps);
                if (writeOps > 0) result.put("mr.task.io." + prefix + "_write_ops", writeOps);
                if (largeReadOps > 0) result.put("mr.task.io." + prefix + "_large_read_ops", largeReadOps);
            }
        } catch (Exception e) {
            System.err.println("[mr-telemetry-agent] FS stats failed: " + e.getMessage());
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
     */
    public TaskIdentity extractTaskIdentity(Object context) {
        if (context == null) return TaskIdentity.UNKNOWN;

        try {
            Object unwrapped = unwrapContext(context);

            String taskId = "unknown";
            String jobId = "unknown";
            String jobName = "unknown";
            String user = "unknown";
            String queue = "unknown";

            try {
                Method m = unwrapped.getClass().getMethod("getTaskAttemptID");
                taskId = safeToString(m.invoke(unwrapped));
            } catch (NoSuchMethodException e) {
                LOG.log(Level.FINE, "getTaskAttemptID not found on " + unwrapped.getClass().getName());
            }

            try {
                Method m = unwrapped.getClass().getMethod("getJobID");
                jobId = safeToString(m.invoke(unwrapped));
            } catch (NoSuchMethodException e) {
                LOG.log(Level.FINE, "getJobID not found on " + unwrapped.getClass().getName());
            }

            try {
                Method m = unwrapped.getClass().getMethod("getConfiguration");
                Object config = m.invoke(unwrapped);
                if (config != null) {
                    Method getMethod = config.getClass().getMethod("get", String.class, String.class);
                    jobName = (String) getMethod.invoke(config, "mapreduce.job.name", "unknown");
                    user = (String) getMethod.invoke(config, "mapreduce.job.user.name", "unknown");
                    queue = (String) getMethod.invoke(config, "mapreduce.job.queuename", "unknown");
                }
            } catch (NoSuchMethodException e) {
                LOG.log(Level.FINE, "getConfiguration not found on " + unwrapped.getClass().getName());
            }

            return new TaskIdentity(taskId, jobId, jobName, user, queue);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to extract task identity: " + e.getMessage());
            return TaskIdentity.UNKNOWN;
        }
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

        // Look for mapContext/reduceContext field (WrappedMapper/WrappedReducer)
        Object innerContext = getFieldRecursive(context, "mapContext");
        if (innerContext == null) {
            innerContext = getFieldRecursive(context, "reduceContext");
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
