import java.util.Date
import java.util.UUID
import scala.util.Random

case class CitizenReport(citizen: Citizen, score: Double)

case class Report(id: UUID, drone: Drone, citizens: List[CitizenReport], words: List[String], date: Date)

object Report {
  val dictionary = List.fill(1000)(
    List.fill(Random.between(3, 11))(
      "abcdefghijklmnopqrstuvwxyz".charAt(Random.nextInt(26))
    ).mkString
  )

  def random(drone: Drone): Report = {
    val id = UUID.randomUUID()

    val citizens = List.fill(Random.between(5, 11))(
      CitizenReport(Citizen.getRandom(), Random.between(0.0, 1.0))
    )

    val words = List.fill(Random.between(10, 21))(
      dictionary(Random.nextInt(dictionary.length))
    )

    Report(id, drone, citizens, words, new Date())
  }
}
