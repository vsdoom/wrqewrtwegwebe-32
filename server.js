// Relay server for Egypt Simulator co-op.
// Players connect here, never to each other, so nobody's IP address is shared.
// Each connection gets a random ID, and messages are forwarded between players in a room.

const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;   // Render sets PORT automatically
const MAX_PLAYERS = 4;
const CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const rooms = new Map(); // code -> { host: id, members: Map(id -> socket) }
const wss = new WebSocketServer({ port: PORT, maxPayload: 64 * 1024 });

function newCode() {
  let code;
  do {
    code = "";
    for (let i = 0; i < 4; i++) code += CHARS[crypto.randomInt(CHARS.length)];
  } while (rooms.has(code));
  return code;
}

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

wss.on("connection", (ws) => {
  ws.id = crypto.randomBytes(3).toString("hex");
  ws.room = null;
  ws.alive = true;
  ws.on("pong", () => { ws.alive = true; });

  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.op === "create" && !ws.room) {
      const code = newCode();
      rooms.set(code, { host: ws.id, members: new Map([[ws.id, ws]]) });
      ws.room = code;
      return send(ws, { op: "created", code, id: ws.id });
    }

    if (m.op === "join" && !ws.room) {
      const code = String(m.code || "").toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { op: "error", msg: "No room with that code" });
      if (room.members.size >= MAX_PLAYERS) {
        return send(ws, { op: "error", msg: "That room is full (4 players max)" });
      }
      const peers = [...room.members.keys()];
      room.members.set(ws.id, ws);
      ws.room = code;
      send(ws, { op: "joined", code, id: ws.id, peers });
      for (const [id, other] of room.members) {
        if (id !== ws.id) send(other, { op: "peer_joined", id: ws.id });
      }
      return;
    }

    if (m.op === "send" && ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      const out = { op: "msg", from: ws.id, data: m.data };
      if (m.to) {
        const target = room.members.get(m.to);
        if (target) send(target, out);
      } else {
        for (const [id, other] of room.members) {
          if (id !== ws.id) send(other, out);
        }
      }
    }
  });

  ws.on("close", () => leave(ws));
});

function leave(ws) {
  const code = ws.room;
  const room = rooms.get(code);
  if (!room) return;
  room.members.delete(ws.id);
  ws.room = null;

  if (room.host === ws.id || room.members.size === 0) {
    // the host left: close the room for everyone
    for (const [, other] of room.members) {
      other.room = null;
      send(other, { op: "host_left" });
    }
    rooms.delete(code);
  } else {
    for (const [, other] of room.members) {
      send(other, { op: "peer_left", id: ws.id });
    }
  }
}

// Keep idle connections alive and drop dead ones
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 30000);

console.log(`Relay listening on port ${PORT}`);
