import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.UUIDSerializer

import java.util.Properties
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import scala.util.Random

object DroneSimulator {
  val config = new Properties()
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[UUIDSerializer])
  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ReportSerializer])

  val producer = new KafkaProducer[UUID, Report](config)

  def generate(drone: Drone, producer: KafkaProducer[UUID, Report]): Unit = {
    try {
      val report = Report.random(drone)

      val record = new ProducerRecord[UUID, Report]("reports", report.id, report)
      producer.send(record).get()

      printf(s"Sent report %s\n", report.toString())
    } catch {
      case e: Exception => e.printStackTrace()
    }

    Thread.sleep(60 * 1000)

    val lat = (drone.position.lat + Random.nextGaussian() + 90 + 180) % 180 - 90
    val lng = (drone.position.lng + Random.nextGaussian() + 180 + 360) % 360 - 180
    val newDrone = Drone(drone.id, Coordinates(lat, lng))
    generate(newDrone, producer)
  }

  def main(args: Array[String]): Unit = {
    val drones = List.fill(100)(Drone.random())

    val start = System.currentTimeMillis()

    val threads = drones.map(drone => Future {
      blocking {
        Thread.sleep(Random.between(0, 60 * 1000))
        generate(drone, producer)
      }
    })

    Await.ready(Future.sequence(threads), Duration.Inf)
  }
}
