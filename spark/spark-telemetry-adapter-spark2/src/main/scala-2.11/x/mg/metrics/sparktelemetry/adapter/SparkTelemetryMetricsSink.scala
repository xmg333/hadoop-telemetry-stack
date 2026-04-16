package x.mg.metrics.sparktelemetry.adapter

import java.lang.management.{BufferPoolMXBean, GarbageCollectorMXBean, ManagementFactory}
import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.codahale.metrics.MetricRegistry
import x.mg.metrics.sparktelemetry.lifecycle.TelemetryLifecycle
import x.mg.metrics.sparktelemetry.model.{GCMetrics, MemoryMetrics, SparkMetricEvent}

import scala.collection.JavaConverters._

/**
 * Spark 2.x Metrics Sink.
 * Loaded by Spark's MetricsSystem via metrics.properties configuration.
 *
 * Configuration in metrics.properties:
 *   *.sink.telemetry.class=x.mg.metrics.sparktelemetry.adapter.SparkTelemetryMetricsSink
 *   *.sink.telemetry.period=10
 *
 * Constructor signature must match Spark's Sink loading convention:
 *   (Properties, MetricRegistry)
 */
class SparkTelemetryMetricsSink(
  properties: Properties,
  registry: MetricRegistry
) {

  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory {
      def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "spark-telemetry-metrics-sink")
        t.setDaemon(true)
        t
      }
    })

  private val running = new AtomicBoolean(false)
  private val period = Option(properties.getProperty("period")).map(_.toInt).getOrElse(10)

  // Initialize from Spark conf
  private var lifecycle: TelemetryLifecycle = _
  private var config: x.mg.metrics.sparktelemetry.config.TelemetryConfig = _

  // Initialize on construction (called by Spark's MetricsSystem)
  initAndStart()

  private def initAndStart(): Unit = {
    try {
      val confMap = new java.util.HashMap[String, String]()
      // Convert properties to map
      properties.asScala.foreach { case (k, v) => confMap.put(k, v) }

      // Try to get Spark conf
      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null && env.conf != null) {
          env.conf.getAll.foreach { case (k, v) => confMap.put(k, v) }
        }
      } catch {
        case _: Exception =>
      }

      TelemetryLifecycle.init(confMap)
      lifecycle = TelemetryLifecycle.getInstance
      config = lifecycle.getConfig

      start()
    } catch {
      case e: Exception =>
      // Don't crash Spark if sink fails to init
    }
  }

  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      scheduler.scheduleAtFixedRate(
        new Runnable { def run(): Unit = collectAndReport() },
        period,
        period,
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
      if (lifecycle == null || config == null || !config.isSystemMetricsEnabled) return

      val event = new SparkMetricEvent
      event.setEventType(SparkMetricEvent.EventType.PERIODIC_SYSTEM)
      event.setTimestamp(System.currentTimeMillis())

      try {
        val env = org.apache.spark.SparkEnv.get
        if (env != null) {
          event.setExecutorId(env.executorId)
          if (env.conf != null) {
            event.setApplicationId(env.conf.getAppId)
            event.setApplicationName(env.conf.get("spark.app.name", ""))
          }
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
