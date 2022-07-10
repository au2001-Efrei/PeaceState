import { Server } from "socket.io"

import subscribe from "./kafka.js"

export default (server) => {
    const io = new Server(server)

    io.on("connection", socket => {
        const unsubscribe = subscribe(alert => {
            socket.emit("alert", alert)
        })

        socket.on("close", () => {
            unsubscribe()
        })
    })
}
