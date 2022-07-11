import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

object Analytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("PeaceState")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val reportSchema = new StructType()
      .add("id", StringType)
      .add("droneId", StringType)
      .add("droneLat", DoubleType)
      .add("droneLng", DoubleType)
      .add("citizens", new ArrayType(
        new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("score", DoubleType),
        false
      ))
      .add("words", new ArrayType(StringType, false))
      .add("date", LongType)

    val reportStream = spark
      .read
      .schema(reportSchema)
      .parquet("storage")

    // 1. The proportion of alerts on weekends
    reportStream
      .select(
        explode($"citizens").as("citizen"),
        dayofweek(from_unixtime($"date")).as("dow")
      ).filter($"citizen.score" < Alerts.RATIO)
      .select(($"dow" === 1 || $"dow" === 7).as("weekend"))
      .agg(round(avg($"weekend".cast(IntegerType)) * 100, 3).as("weekendProportion"))
      .show()

    // 2. The min/max/average score of each citizen
    reportStream
      .select(explode($"citizens").as("citizen"))
      .groupBy($"citizen.id", $"citizen.name")
      .agg(
        round(avg($"citizen.score") * 100, 2).as("avgScore"),
        round(min($"citizen.score") * 100, 2).as("minScore"),
        round(max($"citizen.score") * 100, 2).as("maxScore")
      )
      .select($"name", $"avgScore", $"minScore", $"maxScore")
      .show()

    // 3. The number of alerts per day
    reportStream
      .select(
        explode($"citizens").as("citizen"),
        to_date(from_unixtime($"date")).as("day")
      ).filter($"citizen.score" < Alerts.RATIO)
      .groupBy($"day")
      .agg(count($"*").as("alertCount"))
      .show()

    // 2. The ratio of alerts per word heard
    reportStream
      .select(
        $"words",
        explode($"citizens").as("citizen")
      )
      .select(
        $"citizen",
        explode($"words").as("word")
      )
      .groupBy($"word")
      .agg(round(avg(
        ($"citizen.score" < Alerts.RATIO).cast(IntegerType)
      ) * 100, 3).as("alertRatio"))
      .show()

    spark.close()
  }
}
