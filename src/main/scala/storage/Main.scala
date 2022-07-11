import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode

object Storage {
  // The schema (data structure) of a Report sent as JSON on Kafka
  val reportSchema = new StructType()
      .add("droneId", StringType)
      .add("position", new StructType()
        .add("latitude", DoubleType)
        .add("longitude", DoubleType)
      )
      .add("citizens", new ArrayType(
        new StructType()
          .add("firstName", StringType)
          .add("lastName", StringType)
          .add("peaceScore", IntegerType),
        false
      ))
      .add("words", new ArrayType(StringType, false))
      .add("date", LongType)

  // The schema (data structure) of an Alert sent as JSON on Kafka
  val alertSchema = new StructType()
    .add("droneId", StringType)
    .add("position", new StructType()
      .add("latitude", DoubleType)
      .add("longitude", DoubleType)
    )
    .add("date", LongType)
    .add("citizen", new StructType()
      .add("firstName", StringType)
      .add("lastName", StringType)
      .add("peaceScore", IntegerType)
    )

  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Storage")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Connect to Kafka as continuous stream on both topics "reports" and "alerts"
    // We use a group.id so that if we start several instances (scale up), messages are not treated twice
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("group.id", "storage")
      .option("subscribe", "reports,alerts")
      .load()

    // We parse the JSON string in the "value" column for the topic "reports"
    val reports = df.filter($"topic" === "reports")
      .select(from_json($"value".cast(StringType), reportSchema).as("report"))
      .select($"report.*")

    // We parse the JSON string in the "value" column for the topic "alerts"
    val alerts = df.filter($"topic" === "alerts")
      .select(from_json($"value".cast(StringType), alertSchema).as("alert"))
      .select($"alert.*")

    // We write the Reports as JSON in local files
    // For better scalability, this should be replaced with a distributed data lake (ex. HDFS/S3)
    // We use a processing time trigger of 10s to reduce the number of small files
    val reportsQuery = reports.writeStream
      .format("json")
      .option("path", "../storage/reports")
      .option("checkpointLocation", "./checkpoints/reports")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10000))
      .start()

    // We write the Alerts as JSON in local files at another path
    val alertsQuery = alerts.writeStream
      .format("json")
      .option("path", "../storage/alerts")
      .option("checkpointLocation", "./checkpoints/alerts")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10000))
      .start()

    reportsQuery.awaitTermination()
    alertsQuery.awaitTermination()
    spark.close()
  }
}
