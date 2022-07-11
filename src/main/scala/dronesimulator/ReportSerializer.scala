import org.apache.kafka.common.serialization.Serializer

class ReportSerializer extends Serializer[Report] {
    override def serialize(topic: String, report: Report): Array[Byte] =
        (
            report.id.toString() + ";" +
            report.drone.id.toString() + ";" +
            report.drone.position.lat.toString() + ";" +
            report.drone.position.lng.toString() + ";" +
            report.citizens.map(citizen =>
                citizen.citizen.id.toString() + ":" +
                citizen.citizen.name + ":" +
                citizen.score
            ).mkString("|") + ";" +
            report.words.mkString("|") + ";" +
            (report.date.getTime() / 1000).toString()
        ).getBytes()
}
