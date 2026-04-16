package x.mg.metrics.sparktelemetry.adapter;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.lang.reflect.Method;

/**
 * Omnipackage facade for Spark 2.x listener registration.
 * <p>
 * Registered via {@code spark.extraListeners} in Spark 2.x (where SparkPlugin API
 * is not available). Detects Spark version at first event and delegates all listener
 * events to the version-specific {@code SparkTelemetryListener} via reflection.
 * <p>
 * In Spark 3/4, this class is not used directly — the version-specific listener
 * is created by {@code TelemetryDriverPlugin} instead.
 */
public class SparkTelemetryListener extends SparkListener {

    private static final ClassLoader SPARK_CLASSLOADER = org.apache.spark.scheduler.SparkListener.class.getClassLoader();

    private volatile Object delegate;
    private volatile Method onTaskEndMethod;
    private volatile Method onStageCompletedMethod;
    private volatile Method onJobStartMethod;
    private volatile Method onJobEndMethod;

    private void ensureDelegate() {
        if (delegate != null) return;
        synchronized (this) {
            if (delegate != null) return;
            try {
                String className = OmniContext.getAdapterPackage() + ".SparkTelemetryListener";
                Class<?> clazz = Class.forName(className, true, SPARK_CLASSLOADER);
                // Spark 2.x adapter uses no-arg constructor with lazy init
                delegate = clazz.getConstructor().newInstance();

                // Cache method references for fast event forwarding
                onTaskEndMethod = clazz.getMethod("onTaskEnd", SparkListenerTaskEnd.class);
                onStageCompletedMethod = clazz.getMethod("onStageCompleted", SparkListenerStageCompleted.class);
                onJobStartMethod = clazz.getMethod("onJobStart", SparkListenerJobStart.class);
                onJobEndMethod = clazz.getMethod("onJobEnd", SparkListenerJobEnd.class);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to create SparkTelemetryListener delegate for Spark " + OmniContext.getVersion(), e);
            }
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        ensureDelegate();
        try {
            onTaskEndMethod.invoke(delegate, taskEnd);
        } catch (Exception e) {
            // Never let telemetry failures affect Spark jobs
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        ensureDelegate();
        try {
            onStageCompletedMethod.invoke(delegate, stageCompleted);
        } catch (Exception e) {
            // Never let telemetry failures affect Spark jobs
        }
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        ensureDelegate();
        try {
            onJobStartMethod.invoke(delegate, jobStart);
        } catch (Exception e) {
            // Never let telemetry failures affect Spark jobs
        }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        ensureDelegate();
        try {
            onJobEndMethod.invoke(delegate, jobEnd);
        } catch (Exception e) {
            // Never let telemetry failures affect Spark jobs
        }
    }
}
