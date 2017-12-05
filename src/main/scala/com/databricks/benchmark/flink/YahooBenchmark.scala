package com.databricks.benchmark.flink

import java.sql.Timestamp
import java.util.{Properties, UUID}

import scala.io.Source

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger._
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.util.Collector

import com.databricks.benchmark.utils.Json
import com.databricks.benchmark.yahoo._

object YahooBenchmark {

  // Transcribed from https://github.com/dataArtisans/yahoo-streaming-benchmark/blob/d8381f473ab0b72e33469d2b98ed1b77317fe96d/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyFlinkWindows.java#L179
  class EventAndProcessingTimeTrigger(triggerIntervalMs: Int) extends Trigger[Any, TimeWindow] {

    var nextTimer: Long = 0L

    override def onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      // register system timer only for the first time
      val firstTimerSet = ctx.getKeyValueState("firstTimerSet", classOf[java.lang.Boolean], new java.lang.Boolean(false))
      if (!firstTimerSet.value()) {
        nextTimer = System.currentTimeMillis() + triggerIntervalMs
        ctx.registerProcessingTimeTimer(nextTimer)
        firstTimerSet.update(true)
      }
      return TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // schedule next timer
      nextTimer = System.currentTimeMillis() + triggerIntervalMs
      ctx.registerProcessingTimeTimer(nextTimer)
      TriggerResult.FIRE;
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
      ctx.deleteProcessingTimeTimer(nextTimer)
      ctx.deleteEventTimeTimer(window.maxTimestamp())
    }
  }

  /**
   * A logger that prints out the number of records processed and the timestamp, which we can later use for throughput calculation.
   */
  class ThroughputLogger(logFreq: Long) extends FlatMapFunction[Event, Integer] {
    private var totalReceived: Long = 0

    override def flatMap(element: Event, collector: Collector[Integer]): Unit = {
      if (totalReceived == 0) {
        println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
      }
      totalReceived += 1
      if (totalReceived % logFreq == 0) {
        println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
      }
    }
  }

  class StaticJoinMapper(campaigns: Map[String, String]) extends FlatMapFunction[Event, (String, String, Timestamp)] {
    override def flatMap(element: Event, collector: Collector[(String, String, Timestamp)]): Unit = {
      collector.collect((campaigns(element.ad_id), element.ad_id, element.event_time))
    }
  }

  class AdTimestampExtractor extends TimestampExtractor[(String, String, Timestamp)] {

    var maxTimestampSeen: Long = 0L

    override def extractTimestamp(element: (String, String, Timestamp), currentTimestamp: Long): Long = {
      val timestamp = element._3.getTime
      maxTimestampSeen = Math.max(timestamp, maxTimestampSeen)
      timestamp
    }

    override def extractWatermark(element: (String, String, Timestamp), currentTimestamp: Long) = Long.MinValue

    override def getCurrentWatermark(): Long = maxTimestampSeen - 1L
  }

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val windowMillis: Time = Time.milliseconds(params.getLong("windowMillis", 10000))
    val parallelism: Int = params.getInt("parallelism", 5)
    require(parallelism > 0, "Parallelism needs to be a positive integer.")
    // Used for assigning event times from out of order data

    // Used when generating input
    val numCampaigns: Int = params.getInt("numCampaigns", 100)
    val tuplesPerSecond: Int = params.getInt("tuplesPerSecond", 50000)
    val rampUpTimeSeconds: Int = params.getInt("rampUpTimeSeconds", 0)
    val triggerIntervalMs: Int = params.getInt("triggerIntervalMs", 0)
    require(triggerIntervalMs >= 0, "Trigger interval can't be negative.")
    // Logging frequency in #records for throughput calculations
    val logFreq: Int = params.getInt("logFreq", 10000000)
    val outputTopic: String = params.get("outputTopic", "YahooBenchmarkOutput")

    val props = params.getProperties

    val env: StreamExecutionEnvironment = FlinkBenchmarkUtils.getExecutionEnvironment(parallelism)

    if (params.getBoolean("enableObjectReuse", true)) {
      env.getConfig.enableObjectReuse()
    }

    val campaignAdSeq: Seq[CampaignAd] = generateCampaignMapping(numCampaigns)

    // According to https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamingRuntimeContext.java
    // it looks like broadcast variables aren't actually supported in streaming...
    // curious how others are achieving a join against a static set. For now we simply include the lookup map in the closure.
    val campaignLookup: Map[String, String] = campaignAdSeq.map(ca => (ca.ad_id, ca.campaign_id)).toMap

    val events = if (params.getBoolean("useFixedGenerator", true)) {
      env.addSource(new EventGenerator(campaignAdSeq))
    } else {
      env.addSource(new OriginalEventGenerator(campaignAdSeq))
    }

    events.flatMap(new ThroughputLogger(logFreq))

    val windowedEvents: WindowedStream[(String, String, Timestamp), Tuple, TimeWindow] = events
      .filter(_.event_type == "view")
      .flatMap(new StaticJoinMapper(campaignLookup))
      .assignTimestamps(new AdTimestampExtractor())
      .keyBy(0) // campaign_id
      .window(TumblingEventTimeWindows.of(windowMillis))

    // set a trigger to reduce latency. Leave it out to increase throughput
    if (triggerIntervalMs > 0) {
      windowedEvents.trigger(new EventAndProcessingTimeTrigger(triggerIntervalMs))
    }

    val windowedCounts = windowedEvents.fold(new WindowedCount(null, "", 0, new java.sql.Timestamp(0L)),
      (acc: WindowedCount, r: (String, String, Timestamp)) => {
        val lastUpdate = if (acc.lastUpdate.getTime < r._3.getTime) r._3 else acc.lastUpdate
        acc.count += 1
        acc.lastUpdate = lastUpdate
        acc
      },
      (key: Tuple, window: TimeWindow, input: Iterable[WindowedCount], out: Collector[WindowedCount]) => {
        val windowedCount = input.iterator.next()
        out.collect(new WindowedCount(
          new java.sql.Timestamp(window.getStart), key.getField(0), windowedCount.count, windowedCount.lastUpdate))
      }
    )

    // only write to kafka when specified
    if (params.has("bootstrap.servers")) {
      FlinkBenchmarkUtils.writeJsonStream(windowedCounts, outputTopic, props)
    }

    env.execute("Flink Yahoo Benchmark")
  }

  /** Generate in-memory ad_id to campaign_id map. We generate 10 ads per campaign. */
  private def generateCampaignMapping(numCampaigns: Int): Seq[CampaignAd] = {
    Seq.tabulate(numCampaigns) { _ =>
      val campaign = UUID.randomUUID().toString
      Seq.tabulate(10)(_ => CampaignAd(UUID.randomUUID().toString, campaign))
    }.flatten
  }
}
