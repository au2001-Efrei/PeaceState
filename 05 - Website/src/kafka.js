import EventEmitter from "events"
import { Kafka, logLevel } from "kafkajs"

const kafka = new Kafka({
    logLevel: logLevel.WARN,
    brokers: ["localhost:9092"]
})

const consumer = kafka.consumer({
    groupId: "website"
})

await consumer.connect()
await consumer.subscribe({
    topic: "alerts",
    fromBeginning: true
})

const events = new EventEmitter()

const store = []

consumer.run({
    eachMessage({ message }) {
        const data = JSON.parse(message.value.toString())

        store.push(data)
        events.emit("alert", data)
    }
})

export default callback => {
    const listener = alert => {
        callback(alert)
    }

    store.forEach(listener)

    events.on("alert", listener)

    return () => events.removeListener("alert", listener)
}
