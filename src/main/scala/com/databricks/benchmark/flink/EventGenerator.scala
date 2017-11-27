package com.databricks.benchmark.flink

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import com.databricks.benchmark.yahoo._

/**
 * Our event generator for Flink.
 * @param tuplesPerSecond Target rate of event generation
 * @param rampUpTimeSeconds Time before we start generating events at full speed. During the first `rampUpTimeSeconds`, data is generated
 *                          at a linearly increasing rate. This allows for the JVM to warm up and allow systems to perform at higher speeds.
 * @param campaigns The ad campaigns to generate events for
 */
class EventGenerator(
    tuplesPerSecond: Int,
    rampUpTimeSeconds: Int,
    campaigns: Seq[CampaignAd]) extends RichParallelSourceFunction[Event] {

  var running = true
  private val uuid = UUID.randomUUID().toString // used as a dummy value for all events, based on ref code

  private val adTypeLength = Variables.AD_TYPES.length
  private val eventTypeLength = Variables.EVENT_TYPES.length
  private val campaignLength = campaigns.length
  private lazy val parallelism = getRuntimeContext().getNumberOfParallelSubtasks()

  private def generateElement(i: Long, currentTime: Long): Event = {
    val ad_id = campaigns(i % campaignLength toInt).ad_id // ad id for the current event index
    val ad_type = Variables.AD_TYPES(i % adTypeLength toInt) // current adtype for event index
    val event_type = Variables.EVENT_TYPES(i % eventTypeLength toInt) // current event type for event index
    Event(
      uuid, // random user, irrelevant
      uuid, // random page, irrelevant
      ad_id,
      ad_type,
      event_type,
      new java.sql.Timestamp(currentTime),
      "255.255.255.255") // generic ipaddress, irrelevant
  }

  override def run(sourceContext: SourceContext[Event]): Unit = {
    var start = 0L
    val startTime = System.currentTimeMillis()

    while (running) {
      val emitStartTime = System.currentTimeMillis()
      val elements = loadPerNode(startTime, emitStartTime)
      var i = 0
      while (i < elements) {
        // We generate batches of elements with the same event timestamp, otherwise getting the timestamp becomes very expensive
        sourceContext.collect(generateElement((start + i) % 10000, emitStartTime))
        i += 1
      }
      start += elements
      // Sleep for the rest of timeslice if needed
      val emitTime = System.currentTimeMillis() - emitStartTime
      if (emitTime < 1000) {
        Thread.sleep(1000 - emitTime);
      }
    }
    sourceContext.close()
  }

  override def cancel(): Unit = {
    running = false
  }


  /**
   * The amount of records to produce for a second given the parallelism, ramp up time and current time.
   */
  private def loadPerNode(startTime: Long, currentTime: Long): Int = {
    val timeSinceStart = (currentTime - startTime) / 1000
    if (timeSinceStart < rampUpTimeSeconds) {
      Math.rint(timeSinceStart * 1.0 / rampUpTimeSeconds * tuplesPerSecond / parallelism).toInt
    } else {
      tuplesPerSecond / parallelism
    }
  }
}
