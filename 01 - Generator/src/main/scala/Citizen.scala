import scala.util.Random

// The data structure to represent a Citizen
case class Citizen(firstName: String, lastName: String, peaceScore: Int)

object RandomCitizen {
  // A helper function to generate a Citizen with a random name and peace score
  def create(): Citizen = {
    // Generate a first and last name by choosing them at random
    val firstName = DataSource.firstNames(Random.nextInt(DataSource.firstNames.length))
    val lastName = DataSource.lastNames(Random.nextInt(DataSource.lastNames.length))

    // Generate a score with a gaussian (normal) distribution with average 80% and standard deviation 20%
    val rawScore = 0.8 + Random.nextGaussian() * 0.2

    // Convert that score to an integer and clamp it between 0 and 100
    val peaceScore = Math.min(100, Math.max(0, Math.round(rawScore * 100))).toInt

    // Return a Citizen with the generated data
    Citizen(firstName, lastName, peaceScore)
  }
}
