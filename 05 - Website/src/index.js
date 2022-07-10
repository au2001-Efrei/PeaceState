import express from "express"

import websocket from "./websocket.js"

const app = express()
const port = process.env.PORT || 3000

app.use("/", express.static("./static"))

const server = app.listen(port, () => {
    console.log(`http://localhost:${port}`)
})

websocket(server)
