import scala.io.Source

// Source of random data used for generating Citizens and words
object DataSource {
  // Read from a CSV file embedded in the JAR file with getResource
  // There is exactly one entry on each line, so we use getLines
  val firstNames: List[String] =
    Source.fromURL(getClass.getResource("/first_names.csv")).getLines().toList

  val lastNames: List[String] =
    Source.fromURL(getClass.getResource("/last_names.csv")).getLines().toList

  val words: List[String] =
    Source.fromURL(getClass.getResource("/words.csv")).getLines().toList
}
