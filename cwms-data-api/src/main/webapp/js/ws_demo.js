// small helper function for selecting element by id
let id = id => document.getElementById(id);

let clobId = "/TIME SERIES TEXT/6261044";

let addr = "ws://localhost:7000/cwms-data/ws/clob"
    // +  "?id=" + encodeURI(clobId) + "&officeId=SPK"
let ws = new WebSocket(addr);
ws.onmessage = msg => updateChat(msg);
// ws.onclose = () => alert("WebSocket connection closed");

// Add event listeners to button and input field
id("send").addEventListener("click", () => sendAndClear(id("message").value));
id("message").addEventListener("keypress", function (e) {
    if (e.keyCode === 13) { // Send message if enter is pressed in input field
        sendAndClear(e.target.value);
    }
});

function sendAndClear(message) {
    if (message !== "") {
        ws.send(message);
        id("message").value = "";
    }
}

function updateChat(msg) { // Update chat-panel and list of connected users
    let data = JSON.parse(msg.data);
    id("chat").insertAdjacentHTML("afterbegin", data.userMessage);
    id("userlist").innerHTML = data.userlist.map(user => "<li>" + user + "</li>").join("");
}