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

class TelemetryDriverPlugin extends DriverPlugin with org.apache.spark.internal.Logging {

  private var listener: SparkTelemetryListener = _

  override def init(sc: org.apache.spark.SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val sparkConf = sc.getConf
    val confMap: Map[String, String] = sparkConf.getAll.toMap

    TelemetryLifecycle.init(confMap.asJava)

    listener = new SparkTelemetryListener(confMap)
    sc.addSparkListener(listener)

    // Register QueryExecutionListener for SQL metrics (driver-only)
    val sqlConfig = TelemetryLifecycle.getInstance.getConfig.isCaptureSqlQueryExecution
    if (sqlConfig) {
      try {
        org.apache.spark.sql.SparkSession.getActiveSession match {
          case Some(session) =>
            session.listenerManager.register(new SparkTelemetryQueryExecutionListener(confMap))
            logInfo("QueryExecutionListener registered for SQL metrics capture")
          case None =>
            // SparkSession not yet created (e.g. PySpark creates it after SparkContext).
            // Defer registration to the first job start via a one-shot SparkListener.
            logInfo("SparkSession not available during plugin init, deferring QEL registration")
            val qel = new SparkTelemetryQueryExecutionListener(confMap)
            val deferred = new org.apache.spark.scheduler.SparkListener {
              override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart): Unit = {
                try {
                  // At this point SparkContext is fully initialized, so getOrCreate() is safe
                  val session = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(
                    org.apache.spark.sql.SparkSession.builder().getOrCreate())
                  session.listenerManager.register(qel)
                  logInfo("QueryExecutionListener registered for SQL metrics capture (deferred)")
                } catch {
                  case e: Exception =>
                    logWarning("Failed to register QueryExecutionListener (deferred): " + e.getMessage, e)
                }
                sc.removeSparkListener(this)
              }
            }
            sc.addSparkListener(deferred)
        }
      } catch {
        case e: Exception =>
          logWarning("Failed to register QueryExecutionListener: " + e.getMessage, e)
      }
    }

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
