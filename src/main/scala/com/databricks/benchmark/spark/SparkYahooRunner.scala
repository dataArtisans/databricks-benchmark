import com.databricks.benchmark.yahoo.{YahooBenchmark, YahooBenchmarkRunner}
import com.databricks.spark.LocalKafka

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 * Runner for the Yahoo Benchmark using Apache Spark.
 */
class SparkYahooRunner(
    override val spark: SparkSession,
    kafkaCluster: LocalKafka,
    parallelism: Int) extends YahooBenchmarkRunner {

  import spark.implicits._

  require(parallelism >= 1, "Parallelism can't be less than 1")

  import com.databricks.benchmark.yahoo._

  import org.apache.spark.sql._
  import org.apache.spark.sql.streaming._
  import StreamingQueryListener._

  private var stream: StreamingQuery = _
  @volatile private var numRecs: Long = 0L
  @volatile private var startTime: Long = 0L
  @volatile private var endTime: Long = 0L

  class Listener extends StreamingQueryListener {
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
      startTime = System.currentTimeMillis
    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      endTime = System.currentTimeMillis
    }
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      numRecs += event.progress.numInputRows
    }
  }

  lazy val listener = new Listener

  override def start(): Unit = {
    // Original processing performed in `generateData` since we generate data in-memory
    spark.streams.addListener(listener)
    startTime = 0L
    endTime = 0L
    numRecs = 0L
    Thread.sleep(1000000000L)
  }

  override def generateData(
      campaigns: Array[CampaignAd],
      tuplesPerSecond: Long,
      recordGenParallelism: Int,
      rampUpTimeSeconds: Int): Unit = {

    val sc = spark.sparkContext

    sc.setLocalProperty("spark.scheduler.pool", "yahoo-benchmark")

    val millisTime = udf((t: java.sql.Timestamp) => t.getTime)
    spark.sql(s"set spark.sql.shuffle.partitions = 1")

    val query = YahooBenchmarkRunner.generateStream(spark, campaigns, tuplesPerSecond, parallelism, rampUpTimeSeconds)
      .where($"event_type" === "view")
      .select($"ad_id", $"event_time")
      .join(campaigns.toSeq.toDS().cache(), Seq("ad_id"))
      .groupBy(millisTime(window($"event_time", "10 seconds").getField("start")) as 'time_window, $"campaign_id")
      .agg(count("*") as 'count, max('event_time) as 'lastUpdate)
      .select(to_json(struct("*")) as 'value)

    if (kafkaCluster != null) {
      stream = query
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaCluster.kafkaNodesString)
        .option("topic", Variables.OUTPUT_TOPIC)
        .option("checkpointLocation", s"/tmp/${java.util.UUID.randomUUID()}")
        .outputMode("update")
        .start()
    } else {
      stream = query
        .writeStream
        .format("console")
        .outputMode("update")
        .start()
    }

    sc.setLocalProperty("spark.scheduler.pool", null)
  }

  override val params = Map("parallelism" -> parallelism)

  /**
   * The throughput calculation for Spark is straightforward thanks to the StreamingQueryListener. We take the number of records
   * processed from each `onQueryProgress` update, we take the start and end times from `onQueryStarted` which is a synchronous call,
   * and `onQueryTerminated`, which is an asynchronous call respectively. Therefore the throughput we calculate is actually a lower
   * bound, as the `onQueryTerminated` call can be made arbitrarily late. If the benchmark cluster is only running the benchmark however,
   * then this `arbitrarily lateness` should be very short (almost instantaneous).
   */
  override def getThroughput(): DataFrame = {
    val ping = System.currentTimeMillis()
    while (endTime == 0 && (ping - System.currentTimeMillis) < 10000) {
      Thread.sleep(1000)
    }
    spark.streams.removeListener(listener)
    assert(endTime != 0L, "Did not receive endTime from the listener in 10 seconds...")
    val start = new java.sql.Timestamp(startTime)
    val end = new java.sql.Timestamp(endTime)
    val duration = endTime - startTime
    Seq((start, end, numRecs, duration, numRecs * 1000.0 / duration)).toDS()
      .toDF("start", "end", "totalInput", "totalDurationMillis", "throughput")
  }

  /**
   * We calculate the latency as the difference between the Kafka ingestion timestamp for a given `time_window` and `campaign_id`
   * pair and the event timestamp of the latest record generated that belongs to that bucket.
   */
  override def getLatency(): DataFrame = {
    import org.apache.spark.sql.types._
    val schema = YahooBenchmark.outputSchema.add("lastUpdate", TimestampType)
    val realTimeMs = udf((t: java.sql.Timestamp) => t.getTime)

    if (kafkaCluster != null) {
      spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaCluster.kafkaNodesString)
        .option("subscribe", Variables.OUTPUT_TOPIC)
        .load()
        .withColumn("result", from_json($"value".cast("string"), schema))
        .select(
          $"timestamp" as 'resultOutput,
          $"result.*")
        .groupBy($"time_window", $"campaign_id")
        .agg(max($"resultOutput") as 'resultOutput, max('lastUpdate) as 'lastUpdate)
        .withColumn("diff", realTimeMs($"resultOutput") - realTimeMs($"lastUpdate"))
        .selectExpr(
          "min(diff) as latency_min",
          "mean(diff) as latency_avg",
          "percentile_approx(diff, 0.95) as latency_95",
          "percentile_approx(diff, 0.99) as latency_99",
          "max(diff) as latency_max")
    } else {
      spark.range(0, 1)
        .selectExpr(
          "1 as latency_min",
          "1 as latency_avg",
          "1 as latency_95",
          "1 as latency_99",
          "1 as latency_max")
    }
  }
}

object SparkYahooRunner {
  def main(args: Array[String]): Unit = {

    // Benchmark Configurations - Ideal for Community Edititon
    val stopSparkOnKafkaNodes = false
    val stopSparkOnFlinkNodes = false
    val numKafkaNodes = 1
    val numFlinkNodes = 1
    val flinkTaskSlots = 1 // Number of CPUs available for a taskmanager.
    val runDurationMillis = 100000 // How long to keep each stream running
    val numTrials = 3
    val benchmarkResultsBase = "/tmp/streaming/benchmarks" // Where to store the results of the benchmark

    //////////////////////////////////
    // Event Generation
    //////////////////////////////////
    val recordsPerSecond = 5000000
    val rampUpTimeSeconds = 10 // Ramps up event generation to the specified rate for the given duration to allow the JVM to warm up
    val recordGenerationParallelism = 1 // Parallelism within Spark to generate data for the Kafka Streams benchmark

    val numCampaigns = 1000 // The number of campaigns to generate events for. Configures the cardinality of the state that needs to be updated

    val kafkaEventsTopicPartitions = 1 // Number of partitions within Kafka for the `events` stream
    val kafkaOutputTopicPartitions = 1 // Number of partitions within Kafka for the `outout` stream. We write data out to Kafka instead of Redis

    //////////////////////////////////
    // Kafka Streams
    //////////////////////////////////
    // Total number of Kafka Streams applications that will be running.
    val kafkaStreamsNumExecutors = 1
    // Number of threads to use within each Kafka Streams application.
    val kafkaStreamsNumThreadsPerExecutor = 1

    //////////////////////////////////
    // Flink
    //////////////////////////////////
    // Parallelism to use in Flink. Needs to be <= numFlinkNodes * flinkTaskSlots
    val flinkParallelism = 1
    // We can have Flink emit updates for windows more frequently to reduce latency by sacrificing throughput. Setting this to 0 means that
    // we emit updates for windows according to the watermarks, i.e. we will emit the result of a window, once the watermark passes the end
    // of the window.
    val flinkTriggerIntervalMillis = 0
    // How often to log the throughput of Flink in #records. Setting this lower will give us finer grained results, but will sacrifice throughput
    val flinkThroughputLoggingFreq = 100000

    //////////////////////////////////
    // Spark
    //////////////////////////////////
    val sparkParallelism = 1

    val spark = SparkSession
      .builder
      .appName("SparkYahooBenchmark")
      .master("local[1]")
      .getOrCreate()

//    val kafkaCluster = LocalKafka.setup(spark, stopSparkOnKafkaNodes = stopSparkOnKafkaNodes, numKafkaNodes = numKafkaNodes)

    val benchmark = new YahooBenchmark(
      spark,
      null, //kafkaCluster
      tuplesPerSecond = recordsPerSecond,
      recordGenParallelism = recordGenerationParallelism,
      rampUpTimeSeconds = rampUpTimeSeconds,
      kafkaEventsTopicPartitions = kafkaEventsTopicPartitions,
      kafkaOutputTopicPartitions = kafkaOutputTopicPartitions,
      numCampaigns = numCampaigns,
      readerWaitTimeMs = runDurationMillis)

    val sparkRunner = new SparkYahooRunner(
      spark,
      kafkaCluster = null, //kafkaCluster
      parallelism = sparkParallelism)

    benchmark.run(sparkRunner, s"$benchmarkResultsBase/spark", numRuns = numTrials)
  }
}
