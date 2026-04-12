package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle

import java.util
import scala.collection.JavaConverters._

/**
 * Spark 3.x/4.x Plugin entry point.
 * Implements SparkPlugin to automatically initialize telemetry on driver and executors.
 *
 * Configuration:
 *   spark.plugins=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryPlugin
 */
class SparkTelemetryPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = {
    new TelemetryDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new TelemetryExecutorPlugin()
  }
}

/**
 * Driver-side plugin: initializes TelemetryLifecycle and registers the SparkListener.
 */
class TelemetryDriverPlugin extends DriverPlugin {

  private var listener: SparkTelemetryListener = _

  override def init(sc: org.apache.spark.SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val sparkConf = sc.getConf
    val confMap: Map[String, String] = sparkConf.getAll.toMap

    // Initialize telemetry lifecycle
    TelemetryLifecycle.init(confMap.asJava)

    // Register the listener
    listener = new SparkTelemetryListener(confMap)
    sc.addSparkListener(listener)

    util.Collections.emptyMap[String, String]()
  }

  override def shutdown(): Unit = {
    if (listener != null) {
      // Listener will be cleaned up with SparkContext
    }
    // Force flush before reset to ensure pending metrics are exported
    if (TelemetryLifecycle.isInitialized) {
      TelemetryLifecycle.getInstance.flush()
    }
    TelemetryLifecycle.reset()
  }
}

/**
 * Executor-side plugin: initializes TelemetryLifecycle on each executor.
 * Has access to PluginContext.metricRegistry() for JVM metrics.
 */
class TelemetryExecutorPlugin extends ExecutorPlugin {

  private var metricsSink: SparkTelemetryMetricsSink = _

  override def init(pluginContext: PluginContext, extraConf: util.Map[String, String]): Unit = {
    // Initialize telemetry lifecycle with executor config
    TelemetryLifecycle.init(extraConf)

    // Start collecting JVM metrics via Dropwizard MetricRegistry
    metricsSink = new SparkTelemetryMetricsSink(
      pluginContext.metricRegistry(),
      extraConf.asScala.toMap
    )
    metricsSink.start()
  }

  override def shutdown(): Unit = {
    if (metricsSink != null) {
      metricsSink.stop()
    }
    if (TelemetryLifecycle.isInitialized) {
      TelemetryLifecycle.getInstance.flush()
    }
    TelemetryLifecycle.reset()
  }
}
