const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX = 4;

const rooms = {
  '1': new Map(),
  '10': new Map()
};

function roomSize(rate) {
  const m = rooms[String(rate)] || rooms['1'];
  let n = 0;
  m.forEach((ws) => {
    if (ws.readyState === 1) n++;
  });
  return n;
}

function broadcast(rate, obj, exceptWs) {
  const m = rooms[String(rate)] || rooms['1'];
  const raw = JSON.stringify(obj);
  m.forEach((ws) => {
    if (ws !== exceptWs && ws.readyState === 1) {
      try { ws.send(raw); } catch (e) {}
    }
  });
}

function sendCount(rate) {
  broadcast(rate, { t: 'count', n: roomSize(rate) });
}

function leave(rate, nick, ws) {
  const m = rooms[String(rate)];
  if (!m) return;
  if (m.get(nick) === ws) m.delete(nick);
  broadcast(rate, { t: 'leave', nick: nick });
  sendCount(rate);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('WSI server OK\n1x: ' + roomSize(1) + '/' + MAX + '\n10x: ' + roomSize(10) + '/' + MAX + '\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let rate = null;
  let nick = null;
  let skin = 'survivor';
  let joined = false;

  ws.on('message', (buf) => {
    let data;
    try { data = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!data || !data.t) return;

    if (data.t === 'join') {
      if (joined) return;
      rate = data.rate === 10 ? 10 : 1;
      nick = String(data.nick || 'Guest').slice(0, 16);
      skin = data.skin || 'survivor';
      const m = rooms[String(rate)];
      let base = nick, i = 1;
      while (m.has(nick)) { nick = (base + i).slice(0, 16); i++; }
      if (roomSize(rate) >= MAX) {
        try { ws.send(JSON.stringify({ t: 'full' })); } catch (e) {}
        try { ws.close(); } catch (e) {}
        return;
      }
      m.set(nick, ws);
      joined = true;
      const n = roomSize(rate);
      try { ws.send(JSON.stringify({ t: 'ok', n: n, nick: nick, rate: rate })); } catch (e) {}
      broadcast(rate, { t: 'join', nick: nick, skin: skin }, ws);
      sendCount(rate);
      return;
    }

    if (!joined || rate == null) return;
    data.nick = nick;
    if (data.t === 'pos') data.skin = data.skin || skin;
    if (data.t === 'chat') data.msg = String(data.msg || '').slice(0, 80);
    broadcast(rate, data, ws);
  });

  ws.on('close', () => {
    if (joined && rate != null && nick) leave(rate, nick, ws);
  });
  ws.on('error', () => {
    if (joined && rate != null && nick) leave(rate, nick, ws);
  });
});

server.listen(PORT, () => {
  console.log('WSI server on port', PORT);
});
