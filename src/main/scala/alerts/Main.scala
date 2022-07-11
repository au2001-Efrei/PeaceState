import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

import java.sql.Struct
import scala.jdk.CollectionConverters._

object Alerts {
  final val RATIO: Double = 0.5

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("PeaceState")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "reports")
      .load()

    val reportSchema = new StructType()
      .add("id", StringType)
      .add("droneId", StringType)
      .add("droneLat", DoubleType)
      .add("droneLng", DoubleType)
      .add("citizens", StringType)
      .add("words", StringType)
      .add("date", LongType)

    val citizenSchema = new StructType()
      .add("id", StringType)
      .add("name", StringType)
      .add("score", DoubleType)

    val reportStream = df.select(
        from_csv(
          $"value".cast(StringType),
          reportSchema,
          Map("delimiter" -> ";")
        ).as("report")
      )
      .select(
        $"report.id",
        $"report.droneId",
        $"report.droneLat",
        $"report.droneLng",
        split($"report.citizens", "\\|").as("citizens"),
        split($"report.words", "\\|").as("words"),
        $"report.date"
      )
      .select(
        $"id",
        $"droneId",
        $"droneLat",
        $"droneLng",
        transform($"citizens", citizen =>
          from_csv(
            citizen,
            citizenSchema,
            Map("delimiter" -> ":")
          )
        ).as("citizens"),
        $"words",
        $"date"
      )

    val alertStream = reportStream.select(
        $"droneId",
        $"droneLat",
        $"droneLng",
        explode($"citizens").as("citizen"),
        $"date"
      )
      .filter($"citizen.score" < Alerts.RATIO)

    val recordStream = alertStream.select(
        to_csv(
          struct(
            $"droneId",
            $"droneLat",
            $"droneLng",
            to_csv(
              struct(
                $"citizen.id",
                $"citizen.name",
                $"citizen.score"
              ),
              Map("delimiter" -> ":").asJava
            ).as("citizen"),
            $"date"
          ),
          Map("delimiter" -> ";").asJava
        ).as("value")
      )

    val stream1 = alertStream.writeStream
      .foreachBatch((batch: Dataset[Row], n: Long) => {
        batch.foreach(row => {
          val citizen = row.getAs[Row]("citizen")

          printf(s"RIOT: Citizen %s has a score of %.1f%%\n",
            citizen.getAs[String]("name"),
            citizen.getAs[Double]("score") * 100,
          )
        })
      })
      .start()

    val stream2 = recordStream.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "alerts")
      .option("checkpointLocation", "checkpoints/alerts")
      .start()

    stream1.awaitTermination()
    stream2.awaitTermination()
    spark.close()
  }
}
