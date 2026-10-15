import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = process.env.PORT || 10000;
const MAX = 5;
const rooms = new Map();

const uid = () => crypto.randomBytes(16).toString("hex");
const rank = c => c % 13;
const value = c => Math.min(rank(c) + 1, 10);
const score = hand => hand.reduce((n, c) => n + value(c), 0);
const clean = s => String(s ?? "").trim().slice(0, 80);

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function code() {
  let c;
  do {
    c = crypto.randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(c));
  return c;
}

function send(ws, obj) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function error(ws, message) {
  send(ws, { type: "error", message });
}

function human(name, token = uid()) {
  return {
    name: clean(name).slice(0, 24) || "Player",
    token,
    bot: false,
    ws: null,
    credits: 100,
    hand: [],
    discards: [],
    online: false
  };
}

function bot() {
  return {
    name: "Bot",
    token: null,
    bot: true,
    ws: null,
    credits: 100,
    hand: [],
    discards: [],
    online: true
  };
}

function makeRoom() {
  const room = {
    code: code(),
    phase: "lobby",
    round: 0,
    players: [],
    queue: [],
    deck: [],
    turn: 0,
    first: 0,
    drawn: false,
    last: null,
    result: null,
    log: [],
    ledger: [],
    rotation: 0,
    timer: null
  };
  rooms.set(room.code, room);
  return room;
}

function log(room, message) {
  room.log.push(message);
  room.log = room.log.slice(-50);
}

function publicPlayers(room) {
  return room.players.map(p => ({
    name: p.name,
    bot: p.bot,
    online: p.online,
    credits: p.credits,
    cardCount: p.hand.length,
    discards: p.discards
  }));
}

function broadcast(room) {
  const players = publicPlayers(room);
  const showdown = room.phase === "end"
    ? room.players.map(p => [...p.hand])
    : null;

  for (const p of [...room.players, ...room.queue]) {
    if (p.bot || !p.ws) continue;

    const seat = room.players.indexOf(p);

    send(p.ws, {
      type: "state",
      state: {
        code: room.code,
        seat,
        players,
        queue: room.queue.map(x => x.name),
        queuePosition: room.queue.indexOf(p) + 1,
        round: room.round,
        phase: room.phase,
        turn: room.turn,
        deckCount: room.deck.length,
        hand: seat >= 0 ? p.hand : [],
        drawn: room.drawn,
        last: room.last,
        result: room.result,
        showdown,
        log: room.log,
        ledger: room.ledger.slice(-30)
      }
    });
  }
}

function attach(ws, room, p) {
  if (p.ws && p.ws !== ws) {
    try { p.ws.close(); } catch {}
  }
  p.ws = ws;
  p.online = true;
  ws.room = room;
  ws.player = p;
  send(ws, {
    type: "joined",
    code: room.code,
    token: p.token
  });
}

function join(ws, data) {
  const room = rooms.get(
    clean(data.code).toUpperCase()
  );
  if (!room) return error(ws, "ไม่พบห้อง");

  if (ws.room) {
    return error(ws, "คุณอยู่ในห้องแล้ว");
  }

  const token = clean(data.token);
  let p = [...room.players, ...room.queue]
    .find(x => !x.bot && x.token === token);

  if (p) {
    attach(ws, room, p);
    log(room, p.name + " กลับเข้าห้อง");
    broadcast(room);
    scheduleBot(room);
    return;
  }

  p = human(data.name);

  if (room.phase === "play") {
    const index = room.players.findIndex(x => x.bot);
    if (index >= 0) {
      const old = room.players[index];
      p.hand = [...old.hand];
      p.discards = [...old.discards];
      p.credits = old.credits;
      room.players[index] = p;
      log(room, p.name + " รับช่วงที่นั่ง Bot");
    } else {
      room.queue.push(p);
      log(room, p.name + " เข้าคิวรอรอบถัดไป");
    }
  } else if (
    room.phase === "lobby" &&
    room.players.length < MAX
  ) {
    room.players.push(p);
    log(room, p.name + " เข้าห้อง");
  } else {
    room.queue.push(p);
    log(room, p.name + " เข้าคิวรอเล่น");
  }

  attach(ws, room, p);
  broadcast(room);
  scheduleBot(room);
}

function fillBots(room) {
  while (room.players.length < MAX) {
    room.players.push(bot());
  }
}

function promoteQueue(room) {
  // หากมี Bot หรือที่นั่งว่าง ให้คนในคิวเข้าก่อน
  for (let i = 0; i < MAX && room.queue.length; i++) {
    if (!room.players[i] || room.players[i].bot) {
      room.players[i] = room.queue.shift();
    }
  }

  // หากเต็มด้วยคนจริง 5 คน ให้หมุนเวียน
  // คนที่รอคิวได้ลงเล่น และผู้เล่นเดิมไปต่อท้ายคิว
  if (room.queue.length) {
    const i = room.rotation % MAX;
    room.rotation++;
    const incoming = room.queue.shift();
    const outgoing = room.players[i];
    room.players[i] = incoming;
    room.queue.push(outgoing);
    log(room, incoming.name + " ได้ลงเล่นรอบใหม่");
  }
}

function deal(room) {
  clearTimeout(room.timer);
  promoteQueue(room);
  fillBots(room);

  room.round++;
  room.phase = "play";
  room.result = null;
  room.last = null;
  room.drawn = false;
  room.deck = shuffle(
    Array.from({ length: 52 }, (_, i) => i)
  );

  for (const p of room.players) {
    p.hand = [];
    p.discards = [];
    for (let i = 0; i < 5; i++) {
      p.hand.push(room.deck.pop());
    }
    p.hand.sort((a, b) => a - b);
  }

  room.turn = room.first % MAX;
  log(room, "เริ่มรอบ " + room.round);
  broadcast(room);
  scheduleBot(room);
}

function finish(room, caller = null) {
  clearTimeout(room.timer);
  if (room.phase !== "play") return;

  const scores = room.players.map(p => score(p.hand));
  const lowest = Math.min(...scores);
  const winners = scores
    .map((v, i) => v === lowest ? i : -1)
    .filter(i => i >= 0);

  let winner = winners[0];

  // แคงสำเร็จเมื่อผู้เรียกมีคะแนนต่ำสุด
  if (caller !== null && winners.includes(caller)) {
    winner = caller;
  }

  const winning = room.players[winner];

  for (let i = 0; i < MAX; i++) {
    if (i === winner) continue;
    const loser = room.players[i];
    const amount = 10;
    loser.credits -= amount;
    winning.credits += amount;
    room.ledger.push({
      round: room.round,
      reason: "ผลรอบ",
      amount,
      from: loser.name,
      to: winning.name
    });
  }

  room.phase = "end";
  room.result = {
    winner,
    message: "🏆 " + winning.name +
      " ชนะด้วย " + scores[winner] + " แต้ม"
  };
  room.first = winner;
  log(room, room.result.message);
  broadcast(room);
}

function nextTurn(room) {
  room.drawn = false;
  room.turn = (room.turn + 1) % MAX;

  if (room.deck.length === 0) {
    finish(room);
    return;
  }

  broadcast(room);
  scheduleBot(room);
}

function draw(room, seat) {
  if (room.drawn) throw Error("จั่วแล้ว");
  if (!room.deck.length) {
    finish(room);
    return;
  }
  room.players[seat].hand.push(room.deck.pop());
  room.drawn = true;
  broadcast(room);
  scheduleBot(room);
}

function discard(room, seat, cards, follow = false) {
  const p = room.players[seat];
  if (!Array.isArray(cards) ||
      cards.length < 1 || cards.length > 3) {
    throw Error("ต้องเลือกไพ่ 1–3 ใบ");
  }

  if (new Set(cards).size !== cards.length ||
      !cards.every(c => Number.isInteger(c) &&
        p.hand.includes(c)) ||
      !cards.every(c => rank(c) === rank(cards[0]))) {
    throw Error("ไพ่ไม่ถูกต้องหรือไม่ใช่หน้าเดียวกัน");
  }

  if (follow) {
    if (room.drawn || !room.last ||
        room.last.player !== (seat + 4) % MAX ||
        rank(cards[0]) !== rank(room.last.cards[0])) {
      throw Error("ไม่สามารถตามไพ่ชุดนี้ได้");
    }
  } else if (!room.drawn) {
    throw Error("ต้องจั่วก่อนทิ้งไพ่");
  }

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }

  room.last = { player: seat, cards: [...cards] };
  log(room, p.name + (follow ? " ตามไพ่" : " ทิ้งไพ่"));

  if (p.hand.length === 0) {
    finish(room, seat);
  } else {
    nextTurn(room);
  }
}

function scheduleBot(room) {
  clearTimeout(room.timer);
  if (room.phase !== "play") return;

  const seat = room.turn;
  const p = room.players[seat];
  if (!p.bot && p.online) return;

  room.timer = setTimeout(() => {
    if (room.phase !== "play" || room.turn !== seat) return;

    const current = room.players[seat];
    if (!current.bot && current.online) return;

    try {
      // Bot หรือผู้เล่นที่หลุดเชื่อมต่อ
      if (!room.drawn) {
        if (room.round > 0 &&
            score(current.hand) <= 12) {
          finish(room, seat);
          return;
        }
        draw(room, seat);
      } else {
        const cards = [...current.hand];
        cards.sort((a, b) => value(b) - value(a));
        discard(room, seat, [cards[0]]);
      }
    } catch (e) {
      console.error("Bot:", e);
      nextTurn(room);
    }
  }, 1100);
}

function action(ws, data) {
  if (!data || typeof data !== "object") return;

  if (data.action === "create") {
    if (ws.room) return error(ws, "คุณอยู่ในห้องแล้ว");
    const room = makeRoom();
    const p = human(data.name);
    room.players.push(p);
    attach(ws, room, p);
    log(room, p.name + " สร้างห้อง");
    broadcast(room);
    return;
  }

  if (data.action === "join") {
    join(ws, data);
    return;
  }

  const room = ws.room;
  const p = ws.player;

  if (!room || !p || p.ws !== ws) {
    return error(ws, "กรุณาเข้าห้องก่อน");
  }

  try {
    if (data.action === "chat") {
      const msg = clean(data.message);
      if (msg) log(room, p.name + ": " + msg);
      broadcast(room);
      return;
    }

    if (data.action === "start") {
      if (room.phase !== "lobby") {
        throw Error("เกมเริ่มไปแล้ว");
      }
      deal(room);
      return;
    }

    if (data.action === "next") {
      if (room.phase !== "end") {
        throw Error("ยังไม่จบรอบ");
      }
      deal(room);
      return;
    }

    const seat = room.players.indexOf(p);

    if (seat < 0) {
      throw Error("คุณอยู่ในคิว รอรอบถัดไป");
    }
    if (room.phase !== "play") {
      throw Error("ยังไม่ได้เริ่มเล่น");
    }
    if (room.turn !== seat) {
      throw Error("ยังไม่ถึงตาของคุณ");
    }

    if (data.action === "kang") {
      if (room.drawn) throw Error("ต้องแคงก่อนจั่ว");
      finish(room, seat);
    } else if (data.action === "draw") {
      draw(room, seat);
    } else if (data.action === "discard") {
      discard(room, seat, data.cards);
    } else if (data.action === "follow") {
      discard(room, seat, data.cards, true);
    }
  } catch (e) {
    error(ws, e.message);
  }
}

const server = http.createServer((req, res) => {
  const pathname = new URL(
    req.url, "http://localhost"
  ).pathname;

  if (pathname !== "/" && pathname !== "/index.html") {
    res.writeHead(404);
    res.end("Not found");
    return;
  }

  fs.readFile(
    path.join(process.cwd(), "index.html"),
    (err, content) => {
      if (err) {
        res.writeHead(500);
        res.end("Missing index.html");
        return;
      }
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store"
      });
      res.end(content);
    }
  );
});

const wss = new WebSocketServer({ server });

wss.on("connection", ws => {
  ws.on("message", raw => {
    try {
      if (raw.length > 8192) return;
      action(ws, JSON.parse(raw.toString()));
    } catch {
      error(ws, "ข้อมูลไม่ถูกต้อง");
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    const p = ws.player;
    if (!room || !p || p.ws !== ws) return;

    p.online = false;
    p.ws = null;
    log(room, p.name + " หลุดการเชื่อมต่อ");
    broadcast(room);
    scheduleBot(room);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("KANG ONLINE V11 running on port " + PORT);
});
