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

object Statistics {
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
      .appName("Statistics")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Load the Reports from the local data storage as JSON
    val reports = spark
      .read
      .schema(reportSchema)
      .json("../storage/reports")

    // Load the Alerts from the local data storage as JSON
    val alerts = spark
      .read
      .schema(alertSchema)
      .json("../storage/alerts")

    // Demonstration questions:

    // 1. The distribution of alerts depending on the day of the week
    // In the Alerts, we convert the date column (Unix Timestamp) to the day of week
    // Then, we group by the day of week and display the number of rows in each group (for each weekday)
    val daysDistribution = alerts.groupBy(
        dayofweek(from_unixtime($"date" / 1000)).as("dow")
      )
      .count()
      .orderBy($"dow")
    daysDistribution.show(false)

    // 2. The average peace score per word heard
    // In the Reports, we create one row per citizen and per word heard
    // Then, we group by word and compute the average peace score for each group (word)
    val wordsCorrelation = reports.select(
        $"words",
        explode($"citizens").as("citizen")
      )
      .select(
        $"citizen",
        explode($"words").as("word")
      )
      .groupBy($"word")
      .agg(avg($"citizen.peaceScore").as("avgPeaceScore"))
      .orderBy($"avgPeaceScore")
    wordsCorrelation.show(1000, false)

    // 3. The number of different active drones
    // We can simply use count_distinct on the Reports with column droneId
    val droneCount = reports.agg(
      count_distinct($"droneId").as("droneCount")
    )
    droneCount.show(false)

    // 4. The list of citizens with most alerts
    // In the Alerts, we create a group for each citizen name
    // Then, we count the number of rows in each group (number of alerts per Citizen)
    // Finally, we sort it by most alerts on top and limit it to 100 people
    val citizensRating = alerts.groupBy($"citizen.firstName", $"citizen.lastName")
      .count()
      .orderBy($"count".desc)
    citizensRating.show(100, false)

    spark.close()
  }
}
