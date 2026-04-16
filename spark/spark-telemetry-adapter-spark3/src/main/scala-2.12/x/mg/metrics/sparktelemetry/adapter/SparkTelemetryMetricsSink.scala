package x.mg.metrics.sparktelemetry.adapter

import java.lang.management.{BufferPoolMXBean, GarbageCollectorMXBean, ManagementFactory, MemoryMXBean, MemoryUsage}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.MetricRegistry
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{GCMetrics, MemoryMetrics, SparkMetricEvent}

import scala.collection.JavaConverters._

/**
 * Collects JVM-level metrics (memory, GC, buffer pools) from the Dropwizard MetricRegistry
 * and reports them through TelemetryLifecycle.
 *
 * This runs as a periodic scheduler on executors, sampling JVM gauges.
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
      // Try to get executor ID
      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null) executorId = env.executorId
      } catch {
        case _: Exception =>
      }

      scheduler.scheduleAtFixedRate(
        new Runnable { def run(): Unit = collectAndReport() },
        intervalSeconds,
        intervalSeconds,
        TimeUnit.SECONDS
      )
    }
  }

  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      scheduler.shutdown()
      try {
        scheduler.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }

  private def collectAndReport(): Unit = {
    try {
      if (!config.isSystemMetricsEnabled) return

      val event = new SparkMetricEvent
      event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM)
      event.setTimestamp(System.currentTimeMillis())
      event.setExecutorId(executorId)

      // Try to get app info
      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null && env.conf != null) {
          event.setApplicationId(env.conf.getAppId)
          event.setApplicationName(env.conf.get("spark.app.name", ""))
        }
      } catch {
        case _: Exception =>
      }

      // Memory metrics
      if (config.isCaptureJvmMemory) {
        event.setMemoryMetrics(collectMemoryMetrics())
      }

      // GC metrics
      if (config.isCaptureJvmGc) {
        event.setGcMetrics(collectGcMetrics())
      }

      if (config.shouldAcceptApp(event.getApplicationName)) {
        lifecycle.accept(event)
      }
    } catch {
      case e: Exception =>
        // Don't let metric collection failures affect Spark
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

    // Buffer pools
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
      val name = bean.getName
      val count = bean.getCollectionCount
      val time = bean.getCollectionTime
      if (count >= 0 && time >= 0) {
        gc.addCollector(name, count, time)
      }
    }
    gc
  }
}
