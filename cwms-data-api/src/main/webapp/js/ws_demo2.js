
var webSocket;
var messageDiv;
var textInput;

var websocketReadyStateArray;
var connectBtn;
var sendTextBtn;
var sendJSONObjectBtn;
var disconnectBtn;
function init(){
    messageDiv = document.getElementById("message");
    textInput = document.getElementById("text");

    websocketReadyStateArray = new Array('Connecting', 'Connected', 'Closing', 'Closed');
    connectBtn = document.getElementById('connect');
    sendTextBtn = document.getElementById('sendText');
    sendJSONObjectBtn = document.getElementById('sendJSONObject');
    disconnectBtn = document.getElementById('disconnect');
    connectBtn.disabled = false;
    sendTextBtn.disabled = true;
    sendJSONObjectBtn.disabled = true;
    disconnectBtn.disabled = true;
}
function connect(){
    try{

        // var clobId = "/TIME SERIES TEXT/6261044";
        var clobId = "/TIME SERIES TEXT/1952044"
        webSocket = new WebSocket("ws://localhost:7000/cwms-data/ws/clob2"   + "?id=" + encodeURI(clobId) + "&office=SPK");
        messageDiv.innerHTML = "<p>Socket status:" + websocketReadyStateArray[webSocket.readyState] + "</p>";
        webSocket.onopen = function(){
            messageDiv.innerHTML += "<p>Socket status:" + websocketReadyStateArray[webSocket.readyState] + "</p>";
            connectBtn.disabled = true;
            sendTextBtn.disabled = false;
            sendJSONObjectBtn.disabled = false;
            disconnectBtn.disabled = false;
        }
        webSocket.onmessage = function(msg){
            messageDiv.innerHTML += "<p>Server response : " + msg.data + "</p>";
        }
        webSocket.onclose = function(){
            messageDiv.innerHTML += "<p>Socket status:" + websocketReadyStateArray[webSocket.readyState] + "</p>";
            connectBtn.disabled = false;
            sendTextBtn.disabled = true;
            sendJSONObjectBtn.disabled = true;
            disconnectBtn.disabled = true;
        }
    }catch(exception){
        messageDiv.innerHTML += 'Exception happen, ' + exception;
    }
}
function sendText(){
    var sendText = textInput.value.trim();
    if(sendText==''){

        messageDiv.innerHTML = '<p>Please input some text to send.</p>';
        return;
    }else{
        try{

            webSocket.send(sendText);
            messageDiv.innerHTML = '<p>Send text : ' + sendText + '</p>'
        }catch(exception){
            messageDiv.innerHTML = '<p>Send error : ' + exception + '</p>'
        }
    }
}
function sendJSONOjbect(){
    var sendText = textInput.value.trim();
    if(sendText==''){

        messageDiv.innerHTML = '<p>Please input some text to send.</p>';
        return;
    }else{
        try{
            currDate = new Date();
            currHour = currDate.getHours();
            currMinutes = currDate.getMinutes();
            currSeconds = currDate.getSeconds();

            currentTime = currHour + ':' + currMinutes + ':' + currSeconds;
            jsonObj = {time:currentTime, text:sendText}
            tmpSendText = JSON.stringify(jsonObj)
            webSocket.send(tmpSendText);
            messageDiv.innerHTML = '<p>Send JSON object : ' + tmpSendText + '</p>'
        }catch(exception){
            messageDiv.innerHTML = '<p>Send error : ' + exception + '</p>'
        }
    }
}
// When you focus on the input text box, it will call this function to select all the text in the input text box.
function selectAll(){
    textInput.select();
}
function disconnect(){
    webSocket.close();
}