import spray.json._
import java.util.UUID
import java.util.Date

// Object used by Spray JSON to convert Citizens and Reports to a JSON object
// A JSON object is needed to send it as String to Kafka
object MyJsonProtocol extends DefaultJsonProtocol {
  // Converts a UUID to a String with the built-in toString method
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(uuid: UUID) = JsString(uuid.toString)
    def read(value: JsValue) = {
      value match {
        case JsString(uuid) => UUID.fromString(uuid)
        case _              => throw new DeserializationException("Expected hexadecimal UUID string")
      }
    }
  }

  // Converts a Date to a Number by using the Unix Timestamp
  implicit object DateFormat extends JsonFormat[Date] {
    def write(date: Date) = JsNumber(date.getTime())
    def read(value: JsValue) = {
      value match {
        case JsNumber(time) => new Date(time.toLong)
        case _              => throw new DeserializationException("Expected numeric Date timestamp")
      }
    }
  }

  // Convert the 3 case classes to JSON maps
  implicit val citizenFormat = jsonFormat3(Citizen)
  implicit val coordinatesFormat = jsonFormat2(Coordinates)
  implicit val reportFormat = jsonFormat5(Report)
}
