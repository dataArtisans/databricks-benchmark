package com.databricks.benchmark.flink

import java.util.UUID

import com.databricks.benchmark.yahoo._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * Original (broken) event generator that is not rate-limited.
 *
 * @param campaigns The ad campaigns to generate events for
 */
class OriginalEventGenerator(
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
    var i = 0L
    while (running) {
      sourceContext.collect(generateElement(i % 10000, System.currentTimeMillis()))
      i += 1
    }
    sourceContext.close()
  }

  override def cancel(): Unit = {
    running = false
  }
}
