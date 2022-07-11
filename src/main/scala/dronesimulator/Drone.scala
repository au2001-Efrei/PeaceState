import java.util.Date
import java.util.UUID
import scala.util.Random

case class Coordinates(lat: Double, lng: Double)

case class Drone(id: UUID, position: Coordinates)

object Drone {
  def random(): Drone = {
    val id = UUID.randomUUID()

    val lat = Random.nextDouble() * 180 - 90
    val lng = Random.nextDouble() * 360 - 180
    val coordinates = Coordinates(lat, lng)

    Drone(id, coordinates)
  }
}
