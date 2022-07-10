import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.ForeachWriter
import java.sql.Struct

object PeaceStateAlertHandler {
  // The schema (data structure) of a Report sent as JSON on Kafka
  val schema = new StructType()
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

  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("AlertHandler")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Connect to Kafka as continuous stream on topic "reports"
    // We use a group.id so that if we start several instances (scale up), messages are not treated twice
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("group.id", "alert_handler")
      .option("subscribe", "reports")
      .load()

    // We parse the JSON string in the "value" column
    val report = df.select(from_json($"value".cast(StringType), schema).as("report"))
      .select($"report.*")

    // We create one row per citizen and then only keep ones with a low peace score
    // With the distribution used, 1% of citizens have a score below 33
    val alert = report.select($"droneId", $"position", $"date", explode($"citizens").as("citizen"))
      .filter($"citizen.peaceScore" <= 33)

    // We convert the data back to a JSON string in the value column to send back to Kafka
    val record = alert.select(to_json(struct($"*")).as("value"))

    // We publish back the alerts on a new topic "alerts"
    val query = record.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "alerts")
      .option("checkpointLocation", "./checkpoint")
      .start()

    // We display the alerts in the console
    val logQuery = alert.writeStream
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true

        override def process(row: Row): Unit = {
          val citizen = row.getAs[Row]("citizen")

          printf(s"Drone %s detected citizen %s %s with peace score of %s\n",
            row.getAs[String]("droneId"),
            citizen.getAs[String]("firstName"),
            citizen.getAs[String]("lastName"),
            citizen.getAs[Int]("peaceScore"),
          )
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start()

    query.awaitTermination()
    logQuery.awaitTermination()
    spark.close()
  }
}
