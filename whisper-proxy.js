const http = require('http');
const url = require('url');
const config = require('config');
const WebSocket = require('ws');

const { tryParseJSON, getLanguageName } = require('./lib/utils');

// Create a new WebSocket connection to the external URL for each client
const externalUrl = config.get('whisper.proxy.address');
const apiKey = config.get('whisper.apiKey');

const server = http.createServer();
const wss = new WebSocket.Server({ server });

const fixInitialMessage = (message, ws) => {
  const obj = tryParseJSON(message);

  ws.partialUtterances = obj.partialUtterances == "true" ? true : false;
  ws.minUtteranceLength = parseInt(obj.minUtteranceLength);

  // Convert language to two-letter code
  if (obj.language && obj.language != 'auto') {
    obj.language = getLanguageName(obj.language);
  }

  // Construct OpenAI session update
  const sessionUpdate = {
    type: 'session.update',
    session: {
      turn_detection: ws.partialUtterances ? { type: 'server_vad' } : null,
      language: obj.language === 'auto' ? undefined : obj.language
    }
  };

  // Clean up internal fields
  delete obj.partialUtterances;
  delete obj.minUtteranceLength;
  delete obj.language;

  return JSON.stringify(sessionUpdate);
};

const fixResultMessage = (message, partialUtterances, minUtteranceLength, openTime) => {
  const obj = tryParseJSON(message);
  const newMsg = {};

  // Handle different event types from OpenAI Realtime API
  if (obj.type === 'response.text.delta') {
    if (partialUtterances) {
      newMsg.partial = obj.delta;
      newMsg.locale = obj.language;
    }
  } else if (obj.type === 'response.done') {
    newMsg.text = obj.response?.output[0]?.content[0]?.text || '';
    newMsg.locale = obj.language;
  } else if (obj.type === 'error') {
    return JSON.stringify({
      errorCode: 4000,
      errorMessage: `OpenAI API error: ${obj.error?.message || 'Unknown error'}`
    });
  } else {
    return null;
  }

  // Add timing information
  if (newMsg.text || newMsg.partial) {
    const currentTime = new Date().getTime() / 1000;
    newMsg.time_begin = Math.floor(openTime);
    newMsg.time_end = Math.floor(currentTime);
  }

  return JSON.stringify(newMsg);
};

wss.on('connection', function connection(ws, req) {
  const location = url.parse(req.url, true);
  const queue = [];

  ws.firstMessage = true;
  ws.openTime = new Date().getTime() / 1000;
  ws.on('open', function open(s) {
    ws.lastMessage = null;
    console.log('New mod_audio_fork connection for Whisper');
  });

  ws.on('close', function close(code) {
    ws.firstMessage = false;
    console.log('mod_audio_fork disconnected from Whisper');
    console.log('last message', code, JSON.stringify(ws.lastMessage));

    // Close proxy socket to OpenAI
    if (code == 1000) {
      ws?.externalWs.close(1000);
    }
  });

  ws.on('message', function incoming(message) {
    ws.lastMessage = tryParseJSON(message);
    if (ws.firstMessage) {
      ws.firstMessage = false;
      message = fixInitialMessage(message, ws);
      console.log('received first message: %s', message);
    } else {
      // Convert audio frames to OpenAI format
      message = JSON.stringify({ 
        type: 'input_audio_buffer.append',
        audio: message.toString('base64') 
      });
    }

    // Proxy the message to the external WebSocket server
    if (ws.externalWs?.readyState === WebSocket.OPEN) {
      ws.externalWs.send(message);
    } else {
      queue.push(message);
    }
  });

  // Proxy socket that is actually connected to OpenAI
  ws.externalWs = connectExternal(queue, ws);
});

const connectExternal = (queue, proxyWs) => {
  const ws = new WebSocket(externalUrl, {
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'OpenAI-Beta': 'realtime=v1'
    }
  });

  ws.on('open', function() {
    console.log('Connected to OpenAI Realtime API');
    for (const m of queue) {
      ws.send(m);
    }
  });

  // Handle messages from the external WebSocket server
  ws.on('message', function incoming(message) {
    if (proxyWs.readyState === WebSocket.OPEN) {
      let newMsg = fixResultMessage(message, proxyWs.partialUtterances, proxyWs.minUtteranceLength, proxyWs.openTime);
      if (newMsg) {
        proxyWs.send(newMsg);
      }
    }
  });

  ws.on('close', function(code, reason) {
    console.log("OpenAI connection closed", code, reason.toString());
    if (code >= 4000) {
      console.error("OpenAI API error");
      return proxyWs.send(JSON.stringify({errorCode: code, errorMessage: 'OpenAI API error'}));
    }

    if (code !== 1000) {
      proxyWs.externalWs = connectExternal(queue, proxyWs);
    }
  });

  ws.on('error', function(e) {
    console.error("OpenAI connection error", e);
    proxyWs.send(JSON.stringify({errorCode: 500, errorMessage: 'Connection error'}));
  });

  return ws;
};

server.listen(8778, function listening() {
  console.log('Whisper proxy listening on %d', server.address().port);
});