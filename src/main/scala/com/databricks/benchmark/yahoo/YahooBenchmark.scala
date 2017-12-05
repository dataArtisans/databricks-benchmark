package com.databricks.benchmark.yahoo

import java.util.UUID

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.databricks.spark.LocalKafka

import com.databricks.benchmark.yahoo._

/**
 * Benchmark for measuring throughput and latency. Details available at:
 * [[https://yahooeng.tumblr.com/post/135321837876/benchmarking-streaming-computation-engines-at]].
 */
class YahooBenchmark(
    kafkaCluster: LocalKafka,
    override val tuplesPerSecond: Long,
    override val recordGenParallelism: Int,
    override val rampUpTimeSeconds: Int,
    kafkaEventsTopicPartitions: Int = 1,
    kafkaOutputTopicPartitions: Int = 1,
    numCampaigns: Int = 100,
    override val readerWaitTimeMs: Long = 300000) extends Benchmark[YahooBenchmarkRunner] {

  override val benchmarkParams: Map[String, Any] = Map(
    "numCampaigns" -> numCampaigns,
    "kafkaEventsTopicPartitions" -> kafkaEventsTopicPartitions,
    "kafkaOutputTopicPartitions" -> kafkaOutputTopicPartitions)

  override protected def init(): Unit = {
    super.init()
    kafkaCluster.deleteTopicIfExists(Variables.OUTPUT_TOPIC)
    kafkaCluster.createTopic(Variables.OUTPUT_TOPIC, partitions = kafkaOutputTopicPartitions, replFactor = 1)
    kafkaCluster.deleteTopicIfExists(Variables.CAMPAIGNS_TOPIC)
    kafkaCluster.createTopic(Variables.CAMPAIGNS_TOPIC, partitions = kafkaEventsTopicPartitions, replFactor = 1)
    kafkaCluster.deleteTopicIfExists(Variables.EVENTS_TOPIC)
    kafkaCluster.createTopic(Variables.EVENTS_TOPIC, partitions = kafkaEventsTopicPartitions, replFactor = 1)
  }

  lazy val campaigns = spark.range(1, numCampaigns).flatMap { e =>
    val campaign = UUID.randomUUID().toString
    Seq.tabulate(10)(_ => CampaignAd(UUID.randomUUID().toString, campaign))
  }.collect()

  override protected def produceRecords(): Unit = {
    runner.generateData(campaigns, tuplesPerSecond, recordGenParallelism, rampUpTimeSeconds)
  }

  override protected def startReader(): Unit = {
    runner.start()
  }

  override protected def stopReader(): Unit = {
    runner.stop()
  }

  override protected def saveResults(outputPath: String, trial: Int): Unit = {
    val throughput = runner.getThroughput()
    val latency = runner.getLatency()
    throughput.crossJoin(latency).coalesce(1).write.mode("overwrite").json(outputPath.stripSuffix("/") + s"/trial=$trial")
  }
}

object YahooBenchmark {
  val outputSchema = new StructType()
    .add("time_window", LongType)
    .add("campaign_id", StringType)
    .add("count", LongType)

  def getBenchmarkResults(outputPath: String): DataFrame = {
    val df = spark.read.json(outputPath)
      .select(
        'trial,
        'start,
        'end,
        'totalDurationMillis,
        'totalInput as 'recordsProcessed,
        'throughput,
        'latency_min,
        'latency_95,
        'latency_99,
        'latency_max,
        'latency_avg)
    display(df.orderBy('trial))
    df
  }
}
