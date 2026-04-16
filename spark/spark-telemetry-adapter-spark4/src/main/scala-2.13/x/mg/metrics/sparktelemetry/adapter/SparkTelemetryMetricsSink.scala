package x.mg.metrics.sparktelemetry.adapter

import java.lang.management.{BufferPoolMXBean, GarbageCollectorMXBean, ManagementFactory}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.MetricRegistry
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{GCMetrics, MemoryMetrics, SparkMetricEvent}

import scala.jdk.CollectionConverters._

/**
 * Spark 4.x JVM Metrics collector.
 * Same logic as Spark 3.x but compiled with Scala 2.13 + Spark 4.x.
 */
class SparkTelemetryMetricsSink(
  metricRegistry: MetricRegistry,
  confMap: Map[String, String]
) {

  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(r => {
      val t = new Thread(r, "spark-telemetry-metrics")
      t.setDaemon(true)
      t
    })

  private val running = new AtomicBoolean(false)
  private val lifecycle = TelemetryLifecycle.getInstance
  private val config = lifecycle.getConfig
  private val intervalSeconds = Math.max(config.getExportIntervalMs / 1000, 5)

  private var executorId: String = "unknown"

  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null) executorId = env.executorId
      } catch {
        case _: Exception =>
      }

      scheduler.scheduleAtFixedRate(
        () => collectAndReport(),
        intervalSeconds,
        intervalSeconds,
        TimeUnit.SECONDS
      )
    }
  }

  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      scheduler.shutdown()
      scheduler.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  private def collectAndReport(): Unit = {
    try {
      if (!config.isSystemMetricsEnabled) return

      val event = new SparkMetricEvent
      event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM)
      event.setTimestamp(System.currentTimeMillis())
      event.setExecutorId(executorId)

      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null && env.conf != null) {
          event.setApplicationId(env.conf.getAppId)
          event.setApplicationName(env.conf.get("spark.app.name", ""))
        }
      } catch {
        case _: Exception =>
      }

      if (config.isCaptureJvmMemory) {
        event.setMemoryMetrics(collectMemoryMetrics())
      }

      if (config.isCaptureJvmGc) {
        event.setGcMetrics(collectGcMetrics())
      }

      if (config.shouldAcceptApp(event.getApplicationName)) {
        lifecycle.accept(event)
      }
    } catch {
      case _: Exception =>
    }
  }

  private def collectMemoryMetrics(): MemoryMetrics = {
    val mem = new MemoryMetrics
    val memoryBean = ManagementFactory.getMemoryMXBean

    val heap = memoryBean.getHeapMemoryUsage
    mem.setHeapUsed(heap.getUsed)
    mem.setHeapCommitted(heap.getCommitted)
    mem.setHeapMax(heap.getMax)

    val nonHeap = memoryBean.getNonHeapMemoryUsage
    mem.setNonHeapUsed(nonHeap.getUsed)
    mem.setNonHeapCommitted(nonHeap.getCommitted)
    mem.setNonHeapMax(nonHeap.getMax)

    if (config.isCaptureBufferPools) {
      val bufferBeans = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean])
      bufferBeans.asScala.foreach { bean =>
        bean.getName match {
          case "direct" =>
            mem.setDirectBufferCount(bean.getCount)
            mem.setDirectBufferUsed(bean.getMemoryUsed)
            mem.setDirectBufferCapacity(bean.getTotalCapacity)
          case "mapped" =>
            mem.setMappedBufferCount(bean.getCount)
            mem.setMappedBufferUsed(bean.getMemoryUsed)
            mem.setMappedBufferCapacity(bean.getTotalCapacity)
          case _ =>
        }
      }
    }

    mem
  }

  private def collectGcMetrics(): GCMetrics = {
    val gc = new GCMetrics
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans
    gcBeans.asScala.foreach { bean =>
      val count = bean.getCollectionCount
      val time = bean.getCollectionTime
      if (count >= 0 && time >= 0) {
        gc.addCollector(bean.getName, count, time)
      }
    }
    gc
  }
}
