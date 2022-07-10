import java.util.Date
import java.util.UUID
import scala.util.Random

// The data structure to represent the physical location of a drone
case class Coordinates(latitude: Double, longitude: Double)

// The data structure to represent a drone Report (emitted every second)
case class Report(droneId: UUID, position: Coordinates, citizens: List[Citizen], words: List[String], date: Date)

object RandomReport {
  // A helper function to generate a Report with random citizens, words and position
  def create(droneId: UUID): Report = {
    // Generate a random position uniformly anywhere on Earth
    val latitude = Random.between(-90.0, 90.0)
    val longitude = Random.between(-180.0, 180.0)
    val coordinates = Coordinates(latitude, longitude)

    // Choose a number of Citizens to include in the report between 2 and 4
    val nbCitizens = 2 + Random.nextInt(3)
    // Generate the list of citizens with the other helper functions
    val citizens = List.fill(nbCitizens)(RandomCitizen.create())

    // Choose a number of words to include in the report between 2 and 9
    val nbWords = 2 + Random.nextInt(8)
    // Generate the list of words by selecting them at random
    val words = List.fill(nbWords)(
      DataSource.words(Random.nextInt(DataSource.words.length))
    )

    // Return a Report with all the generated data and the current date
    Report(droneId, coordinates, citizens, words, new Date())
  }
}
