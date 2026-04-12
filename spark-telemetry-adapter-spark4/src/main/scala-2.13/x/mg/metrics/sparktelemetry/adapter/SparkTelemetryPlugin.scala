package x.mg.metrics.sparktelemetry.adapter

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle

import java.util
import scala.jdk.CollectionConverters._

/**
 * Spark 4.x Plugin entry point.
 * Same interface as Spark 3.x but compiled against Spark 4.x + Scala 2.13.
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

class TelemetryDriverPlugin extends DriverPlugin {

  private var listener: SparkTelemetryListener = _

  override def init(sc: org.apache.spark.SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val sparkConf = sc.getConf
    val confMap: Map[String, String] = sparkConf.getAll.toMap

    TelemetryLifecycle.init(confMap.asJava)

    listener = new SparkTelemetryListener(confMap)
    sc.addSparkListener(listener)

    util.Collections.emptyMap[String, String]()
  }

  override def shutdown(): Unit = {
    if (TelemetryLifecycle.isInitialized) {
      TelemetryLifecycle.getInstance.flush()
    }
    TelemetryLifecycle.reset()
  }
}

class TelemetryExecutorPlugin extends ExecutorPlugin {

  private var metricsSink: SparkTelemetryMetricsSink = _

  override def init(pluginContext: PluginContext, extraConf: util.Map[String, String]): Unit = {
    TelemetryLifecycle.init(extraConf)

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
