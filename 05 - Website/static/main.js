var socket = io();

var entries = document.getElementById("entries");

function toDegreesMinutesAndSeconds(coordinate) {
    var absolute = Math.abs(coordinate);
    var degrees = Math.floor(absolute);
    var minutesNotTruncated = (absolute - degrees) * 60;
    var minutes = Math.floor(minutesNotTruncated);
    var seconds = Math.floor((minutesNotTruncated - minutes) * 60);

    return degrees + "° " + minutes + "’ " + seconds + "”";
}

function convertDMS(lat, lng) {
    var latitude = toDegreesMinutesAndSeconds(lat);
    var latitudeCardinal = lat >= 0 ? "N" : "S";

    var longitude = toDegreesMinutesAndSeconds(lng);
    var longitudeCardinal = lng >= 0 ? "E" : "W";

    return latitude + " " + latitudeCardinal + ", " + longitude + " " + longitudeCardinal;
}

socket.on("alert", function (data) {
    var entry = document.createElement("tr");

    var droneIdElem = document.createElement("td");
    droneIdElem.innerText = data.droneId;
    entry.appendChild(droneIdElem);

    var citizenElem = document.createElement("td");
    citizenElem.innerText = data.citizen.firstName + " " + data.citizen.lastName;
    entry.appendChild(citizenElem);

    var peaceScoreElem = document.createElement("td");
    peaceScoreElem.innerText = data.citizen.peaceScore + "%";
    entry.appendChild(peaceScoreElem);

    var locationElem = document.createElement("td");
    locationElem.innerText = convertDMS(data.position.latitude, data.position.longitude);
    entry.appendChild(locationElem);

    var dateElem = document.createElement("td");
    dateElem.innerText = new Intl.DateTimeFormat("en-US", {
        weekday: "short",
        day: "2-digit",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
    }).format(new Date(data.date));
    entry.appendChild(dateElem);

    entries.appendChild(entry);
});
