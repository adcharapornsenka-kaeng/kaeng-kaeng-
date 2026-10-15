import http from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const uid = () => randomBytes(16).toString("hex");
const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const points = c => Math.min(rank(c), 10);
const score = hand => hand.reduce((n, c) => n + points(c), 0);
const aces = hand => hand.filter(c => rank(c) === 1).length;
const shuffle = a => {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
};

const names = ["Bot Alice", "Bot Ben", "Bot Cat", "Bot Dan", "Bot Eva"];

function category(hand) {
  const n = hand.length;
  const counts = new Map();
  for (const c of hand) counts.set(rank(c), (counts.get(rank(c)) || 0) + 1);
  const values = [...counts.values()].sort((a, b) => b - a);
  const flush = n === 5 && hand.every(c => suit(c) === suit(hand[0]));
  const rs = hand.map(rank).sort((a, b) => a - b);
  const straight = n === 5 && (
    rs.every((v, i) => i === 0 || v === rs[i - 1] + 1) ||
    JSON.stringify(rs) === JSON.stringify([1, 10, 11, 12, 13])
  );

  if (straight && flush) return { name: "สเตรทฟลัช", base: 60 };
  if (values[0] === 4) return { name: "โฟร์การ์ด", base: 50 };
  if (n === 5 && values[0] === 3 && values[1] === 2)
    return { name: "ฟูลเฮาส์", base: 40 };
  if (straight) return { name: "สเตรท", base: 20 };
  if (flush) return { name: "ฟลัช", base: 20 };
  if (score(hand) === 50) return { name: "50 แต้ม", base: 20 };
  if (values[0] === 3) return { name: "ตอง", base: 20 };
  return { name: "ธรรมดา", base: 10 };
}

function createPlayer(name, bot = false) {
  return {
    id: uid(),
    token: uid(),
    name: String(name || "Player").slice(0, 32),
    bot,
    online: bot,
    ws: null,
    credits: 1000,
    hand: [],
    discards: []
  };
}

function createRoom() {
  let code;
  do {
    code = randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));

  const room = {
    code,
    players: names.map(n => createPlayer(n, true)),
    queue: [],
    phase: "lobby",
    round: 0,
    turn: 0,
    dealer: 0,
    starter: 0,
    deck: [],
    last: null,
    actionTaken: false,
    lastWinnerId: null,
    champagneMoney: 0,
    ledger: [],
    log: [],
    result: null,
    showdown: [],
    vote: null,
    pendingShuffle: false,
    botTimer: null
  };

  rooms.set(code, room);
  return room;
}

function log(room, text) {
  room.log.push(text);
  if (room.log.length > 100) room.log.shift();
}

function transfer(room, from, to, amount, reason) {
  amount = Math.max(0, Math.floor(amount));
  if (!amount) return;
  from.credits -= amount;
  if (to) to.credits += amount;
  room.ledger.push({
    round: room.round,
    from: from.name,
    to: to ? to.name : "Champagne Money",
    amount,
    reason
  });
  if (room.ledger.length > 150) room.ledger.shift();
}

function publicState(room, player) {
  return {
    type: "state",
    code: room.code,
    seat: room.players.indexOf(player),
    phase: room.phase,
    round: room.round,
    turn: room.turn,
    dealer: room.dealer,
    starter: room.starter,
    deckCount: room.deck.length,
    hand: [...player.hand],
    players: room.players.map(p => ({
      name: p.name,
      bot: p.bot,
      online: p.online,
      credits: p.credits,
      cardCount: p.hand.length,
      discards: [...p.discards]
    })),
    last: room.last
      ? { player: room.last.player, cards: [...room.last.cards] }
      : null,
    actionTaken: room.actionTaken,
    champagneMoney: room.champagneMoney,
    ledger: room.ledger,
    log: room.log,
    result: room.result,
    showdown: room.showdown,
    vote: room.vote
  };
}

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(room) {
  for (const p of room.players) {
    if (!p.bot && p.online) {
      send(p.ws, publicState(room, p));
    }
  }
  for (const q of room.queue) {
    send(q.ws, {
      type: "queue",
      position: room.queue.indexOf(q) + 1
    });
  }
}

function error(player, message) {
  send(player.ws, { type: "error", message });
}

function attach(room, player, ws) {
  if (player.ws && player.ws !== ws) player.ws.close();
  player.ws = ws;
  player.online = true;
  player.bot = false;
  ws.room = room;
  ws.player = player;
  send(ws, {
    type: "identity",
    code: room.code,
    token: player.token
  });
  broadcast(room);
}

function replaceBot(room, name, ws) {
  const index = room.players.findIndex(p => p.bot);
  if (index < 0) return false;
  const old = room.players[index];
  const p = createPlayer(name);
  p.id = old.id;
  p.hand = old.hand;
  p.discards = old.discards;
  p.credits = old.credits;
  room.players[index] = p;
  attach(room, p, ws);
  return true;
}

function joinRoom(ws, code, name, token) {
  const room = rooms.get(String(code || "").toUpperCase());
  if (!room) {
    send(ws, { type: "error", message: "ไม่พบรหัสห้องนี้" });
    return;
  }

  const returning = room.players.find(
    p => !p.bot && p.token === token && token
  );

  if (returning) {
    attach(room, returning, ws);
    scheduleBot(room);
    return;
  }

  if (replaceBot(room, name, ws)) {
    scheduleBot(room);
    return;
  }

  const q = { name, token: uid(), ws };
  room.queue.push(q);
  ws.room = room;
  ws.queueEntry = q;
  send(ws, {
    type: "identity",
    code: room.code,
    token: q.token
  });
  broadcast(room);
}

function startRound(room) {
  if (room.phase === "play") return;

  if (room.pendingShuffle) {
    shuffle(room.players);
    room.pendingShuffle = false;
  }

  room.round++;
  room.phase = "play";
  room.deck = shuffle(Array.from({ length: 52 }, (_, i) => i));
  room.last = null;
  room.actionTaken = false;
  room.result = null;
  room.showdown = [];
  room.vote = null;

  if (room.lastWinnerId) {
    const winnerSeat = room.players.findIndex(
      p => p.id === room.lastWinnerId
    );
    if (winnerSeat >= 0) room.dealer = winnerSeat;
  }

  room.starter = room.dealer;
  room.turn = room.starter;

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
  }

  log(room, `🃏 เริ่มรอบ ${room.round}`);
  broadcast(room);
  scheduleBot(room);
}

function chooseLowest(room, start) {
  let best = start;
  for (let k = 1; k < 5; k++) {
    const i = (start + k) % 5;
    if (score(room.players[i].hand) < score(room.players[best].hand)) {
      best = i;
    }
  }
  return best;
}

function finishRound(room, winnerIndex, reason, callerIndex = null) {
  if (room.phase !== "play") return;

  const winner = room.players[winnerIndex];
  const cat = category(winner.hand);
  const rate = cat.base + aces(winner.hand) * 10;
  const failed = callerIndex !== null && callerIndex !== winnerIndex;

  if (failed) {
    const caller = room.players[callerIndex];
    transfer(room, caller, winner, rate * 4, "แคงผิด");
  } else {
    for (let i = 0; i < 5; i++) {
      if (i !== winnerIndex) {
        transfer(room, room.players[i], winner, rate, reason);
      }
    }
  }

  winner.credits -= 2;
  room.champagneMoney += 2;
  room.ledger.push({
    round: room.round,
    from: winner.name,
    to: "Champagne Money",
    amount: 2,
    reason: "ค่าผู้ชนะ"
  });

  room.showdown = room.players.map(p => ({
    name: p.name,
    hand: [...p.hand],
    points: score(p.hand),
    count: p.hand.length
  }));

  room.phase = "end";
  room.lastWinnerId = winner.id;
  room.dealer = winnerIndex;
  room.starter = winnerIndex;

  room.result = {
    message: `🏆 ${winner.name} ชนะ! (${reason})`,
    category: cat.name,
    amount: failed ? rate * 4 : rate,
    failed
  };

  log(room, room.result.message);
  log(room, `💰 ${failed ? "แคงผิด จ่ายรวม" : "ผู้แพ้จ่ายคนละ"} ${room.result.amount} เครดิต`);
  broadcast(room);
}

function kang(room, player) {
  if (!canAct(room, player, false)) return;

  const caller = room.turn;
  const callerPoints = score(player.hand);
  const opponents = room.players.some(
    (p, i) => i !== caller && score(p.hand) < callerPoints
  );

  if (!opponents) {
    finishRound(room, caller, "แคงสำเร็จ");
  } else {
    const winner = chooseLowest(room, (caller + 1) % 5);
    finishRound(room, winner, "แคงผิด", caller);
  }
}

function canAct(room, player, afterAction) {
  if (room.phase !== "play") {
    error(player, "เกมยังไม่อยู่ระหว่างเล่น");
    return false;
  }
  if (room.players[room.turn] !== player) {
    error(player, "ยังไม่ถึงตาคุณ");
    return false;
  }
  if (room.actionTaken !== afterAction) {
    error(player, afterAction
      ? "ต้องจั่วหรือไหลก่อนทิ้ง"
      : "คุณจั่วหรือไหลแล้ว");
    return false;
  }
  return true;
}

function drawCard(room, player) {
  if (!canAct(room, player, false)) return;
  if (!room.deck.length) {
    error(player, "กองจั่วหมดแล้ว");
    return;
  }

  player.hand.push(room.deck.pop());
  room.actionTaken = true;
  broadcast(room);
}

function followCards(room, player) {
  if (!canAct(room, player, false)) return;

  const previous = (room.turn + 4) % 5;
  const last = room.last;

  if (!last || last.player !== previous || !last.cards.length) {
    error(player, "ไม่มีไพ่จากคนก่อนหน้าให้ไหล");
    return;
  }

  // ไพ่หน้าเดียวกันทั้งหมด ไหลอัตโนมัติ ไม่จำกัดจำนวน
  const firstRank = rank(last.cards[0]);
  const cards = last.cards.filter(c => rank(c) === firstRank);

  if (!cards.length || cards.length !== last.cards.length) {
    error(player, "ไพ่ที่ไหลต้องเป็นหน้าเดียวกัน");
    return;
  }

  const discards = room.players[previous].discards;
  if (!cards.every(c => discards.includes(c))) {
    error(player, "ไพ่ในกองทิ้งไม่ตรงกัน");
    return;
  }

  for (const c of cards) {
    const i = discards.indexOf(c);
    discards.splice(i, 1);
    player.hand.push(c);
  }

  room.last = null;
  room.actionTaken = true;
  log(room, `🃏 ${player.name} ไหล ${cards.length} ใบ`);
  broadcast(room);
}

function discardCards(room, player, cards) {
  if (!canAct(room, player, true)) return;

  if (!Array.isArray(cards) || cards.length < 1 || cards.length > 3) {
    error(player, "ทิ้งไพ่ได้ครั้งละ 1–3 ใบ");
    return;
  }

  if (
    new Set(cards).size !== cards.length ||
    !cards.every(c => Number.isInteger(c) && player.hand.includes(c))
  ) {
    error(player, "ไพ่ที่เลือกไม่ถูกต้อง");
    return;
  }

  if (cards.length > 1 && !cards.every(c => rank(c) === rank(cards[0]))) {
    error(player, "ทิ้งหลายใบต้องเป็นไพ่หน้าเดียวกัน");
    return;
  }

  for (const c of cards) {
    player.hand.splice(player.hand.indexOf(c), 1);
    player.discards.push(c);
  }

  // ไม่บันทึกเหตุการณ์ทิ้งไพ่ลงช่องแชต
  room.last = {
    player: room.turn,
    cards: [...cards]
  };

  if (cards.length === 3) {
    for (const p of room.players) {
      if (p !== player) transfer(room, p, player, 10, "ทิ้งตอง");
    }
    log(room, `💰 ${player.name} ได้โบนัสตอง 40 เครดิต`);
  }

  if (!player.hand.length) {
    finishRound(room, room.turn, "ไพ่หมดมือ");
    return;
  }

  if (!room.deck.length) {
    const winner = chooseLowest(room, (room.turn + 1) % 5);
    finishRound(room, winner, "กองจั่วหมด");
    return;
  }

  room.turn = (room.turn + 1) % 5;
  room.actionTaken = false;
  broadcast(room);
  scheduleBot(room);
}

function botPlay(room) {
  if (room.phase !== "play") return;

  const bot = room.players[room.turn];
  if (!bot?.bot) return;

  const previous = (room.turn + 4) % 5;
  const canFollow =
    room.last &&
    room.last.player === previous &&
    room.last.cards.length > 0 &&
    room.last.cards.every(c => rank(c) === rank(room.last.cards[0]));

  if (!room.actionTaken) {
    if (score(bot.hand) <= 9 && Math.random() < 0.5) {
      kang(room, bot);
      return;
    }

    if (canFollow && Math.random() < 0.65) {
      followCards(room, bot);
    } else if (room.deck.length) {
      drawCard(room, bot);
    } else if (canFollow) {
      followCards(room, bot);
    } else {
      const winner = chooseLowest(room, (room.turn + 1) % 5);
      finishRound(room, winner, "กองจั่วหมด");
      return;
    }
  }

  if (room.phase !== "play" || room.players[room.turn] !== bot) return;

  const groups = new Map();
  for (const c of bot.hand) {
    if (!groups.has(rank(c))) groups.set(rank(c), []);
    groups.get(rank(c)).push(c);
  }

  let selected = [];
  for (const group of groups.values()) {
    if (group.length >= 3) {
      selected = group.slice(0, 3);
      break;
    }
  }

  if (!selected.length) {
    const sorted = [...bot.hand].sort((a, b) => points(b) - points(a));
    selected = sorted.slice(0, 1);
  }

  discardCards(room, bot, selected);
}

function scheduleBot(room) {
  clearTimeout(room.botTimer);
  if (room.phase !== "play") return;
  if (!room.players[room.turn]?.bot) return;

  room.botTimer = setTimeout(() => {
    botPlay(room);
  }, 900);
}

function promoteQueue(room, seat) {
  while (room.queue.length) {
    const q = room.queue.shift();
    if (q.ws.readyState !== WebSocket.OPEN) continue;

    const old = room.players[seat];
    const p = createPlayer(q.name);
    p.id = old.id;
    p.hand = old.hand;
    p.discards = old.discards;
    p.credits = old.credits;
    p.token = q.token;
    room.players[seat] = p;
    q.ws.queueEntry = null;
    attach(room, p, q.ws);
    return;
  }
}

function leaveRoom(ws) {
  const room = ws.room;
  if (!room) return;

  const queued = room.queue.findIndex(q => q.ws === ws);
  if (queued >= 0) {
    room.queue.splice(queued, 1);
    send(ws, { type: "left" });
    ws.room = null;
    ws.queueEntry = null;
    broadcast(room);
    return;
  }

  const seat = room.players.findIndex(p => p.ws === ws);
  if (seat < 0) return;

  const old = room.players[seat];
  const bot = createPlayer(names[seat], true);
  bot.id = old.id;
  bot.hand = old.hand;
  bot.discards = old.discards;
  bot.credits = old.credits;
  room.players[seat] = bot;

  send(ws, { type: "left" });
  ws.room = null;
  ws.player = null;

  promoteQueue(room, seat);
  broadcast(room);
  scheduleBot(room);
}

function requestShuffle(room, player) {
  if (room.vote) {
    error(player, "มีการลงคะแนนอยู่แล้ว");
    return;
  }

  const humans = room.players
    .filter(p => !p.bot && p.online)
    .map(p => p.id);

  room.vote = {
    requester: player.name,
    required: humans,
    approvals: [player.id]
  };

  if (humans.length === 1) {
    room.pendingShuffle = true;
    room.vote = null;
    log(room, "🎲 อนุมัติจับที่นั่งใหม่");
  }

  broadcast(room);
}

function approveShuffle(room, player) {
  if (!room.vote) return;
  if (!room.vote.required.includes(player.id)) return;
  if (!room.vote.approvals.includes(player.id)) {
    room.vote.approvals.push(player.id);
  }

  if (room.vote.required.every(id => room.vote.approvals.includes(id))) {
    room.pendingShuffle = true;
    room.vote = null;
    log(room, "🎲 ทุกคนอนุมัติจับที่นั่งใหม่");
  }
  broadcast(room);
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/" || req.url === "/index.html") {
      const html = await readFile(
        new URL("./index.html", import.meta.url)
      );
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store"
      });
      res.end(html);
      return;
    }

    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("KANG ONLINE V12.6 OK");
      return;
    }

    res.writeHead(404);
    res.end("Not found");
  } catch (e) {
    console.error(e);
    res.writeHead(500);
    res.end("Server error");
  }
});

const wss = new WebSocketServer({ server });

wss.on("connection", ws => {
  ws.on("message", raw => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, { type: "error", message: "ข้อมูลไม่ถูกต้อง" });
      return;
    }

    const action = data.action;

    if (action === "create") {
      if (ws.room) leaveRoom(ws);
      const room = createRoom();
      const p = createPlayer(data.name);
      room.players[0] = p;
      attach(room, p, ws);
      return;
    }

    if (action === "join") {
      if (ws.room) leaveRoom(ws);
      joinRoom(ws, data.code, data.name, data.token);
      return;
    }

    if (action === "leave") {
      leaveRoom(ws);
      return;
    }

    const room = ws.room;
    const player = ws.player;

    if (!room || !player || !room.players.includes(player)) {
      send(ws, {
        type: "error",
        message: "กรุณาสร้างห้องหรือเข้าห้องก่อน"
      });
      return;
    }

    switch (action) {
      case "start":
        if (room.phase === "lobby") startRound(room);
        break;

      case "next":
        if (room.phase === "end") startRound(room);
        break;

      case "kang":
        kang(room, player);
        break;

      case "draw":
        drawCard(room, player);
        break;

      case "follow":
        followCards(room, player);
        break;

      case "discard":
        discardCards(room, player, data.cards);
        break;

      case "chat": {
        const text = String(data.text || "").trim().slice(0, 300);
        if (text) {
          log(room, `💬 ${player.name}: ${text}`);
          broadcast(room);
        }
        break;
      }

      case "credit": {
        const amount = Number(data.amount);
        if ([100, 200, 300, 400, 500].includes(amount)) {
          player.credits += amount;
          log(room, `💰 ${player.name} เพิ่มเครดิต ${amount}`);
          broadcast(room);
        }
        break;
      }

      case "requestSeats":
        requestShuffle(room, player);
        break;

      case "approveSeats":
        approveShuffle(room, player);
        break;

      case "rejectSeats":
        if (room.vote) {
          room.vote = null;
          log(room, `🎲 ${player.name} ไม่อนุมัติจับที่นั่งใหม่`);
          broadcast(room);
        }
        break;

      default:
        error(player, "ไม่รู้จักคำสั่งนี้");
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;

    const queued = room.queue.findIndex(q => q.ws === ws);
    if (queued >= 0) {
      room.queue.splice(queued, 1);
      broadcast(room);
      return;
    }

    const player = ws.player;
    if (player && player.ws === ws) {
      player.online = false;
      player.ws = null;
      broadcast(room);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`KANG ONLINE V12.6 listening on ${PORT}`);
});
