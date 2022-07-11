import scala.util.Random
import java.util.UUID

case class Citizen(id: UUID, name: String)

object Citizen {
  val population = List.fill(100)(
      Citizen.random()
    )

  def random(): Citizen = {
    val id = UUID.randomUUID()

    val firstName = List.fill(Random.between(3, 11))(
      "abcdefghijklmnopqrstuvwxyz".charAt(Random.nextInt(26))
    ).mkString.capitalize

    val lastName = List.fill(Random.between(2, 11))(
      "abcdefghijklmnopqrstuvwxyz".charAt(Random.nextInt(26))
    ).mkString.capitalize

    Citizen(id, s"$firstName $lastName")
  }

  def getRandom(): Citizen = {
    population(Random.nextInt(population.length))
  }
}
