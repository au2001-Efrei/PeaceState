import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType

object Storing {
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

    val stream = reportStream.writeStream
      .format("parquet")
      .option("path", "storage")
      .option("checkpointLocation", "checkpoints/storing")
      .start()

    stream.awaitTermination()
    spark.close()
  }
}
