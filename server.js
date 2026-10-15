import http from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const BOT_THINK_MS = 4000;
const BOT_DISCARD_MS = 4000;
const HUMAN_TIMEOUT_MS = 90000;
const DISCONNECT_GRACE_MS = 15000;

const uid = () => randomBytes(16).toString("hex");
const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const points = c => Math.min(rank(c), 10);
const score = hand => hand.reduce((n, c) => n + points(c), 0);
const aces = hand => hand.filter(c => rank(c) === 1).length;

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function category(hand) {
  const counts = new Map();
  for (const c of hand) {
    counts.set(rank(c), (counts.get(rank(c)) || 0) + 1);
  }

  const values = [...counts.values()].sort((a, b) => b - a);
  const rs = hand.map(rank).sort((a, b) => a - b);
  const flush = hand.length === 5 &&
    hand.every(c => suit(c) === suit(hand[0]));

  const straight = hand.length === 5 && (
    rs.every((v, i) => !i || v === rs[i - 1] + 1) ||
    JSON.stringify(rs) === "[1,10,11,12,13]"
  );

  if (straight && flush)
    return { name: "สเตรทฟลัช", base: 60 };
  if (values[0] === 4)
    return { name: "โฟร์การ์ด", base: 50 };
  if (hand.length === 5 && values[0] === 3 && values[1] === 2)
    return { name: "ฟูลเฮาส์", base: 40 };
  if (straight) return { name: "สเตรท", base: 20 };
  if (flush) return { name: "ฟลัช", base: 20 };
  if (score(hand) === 50) return { name: "50 แต้ม", base: 20 };
  if (values[0] === 3) return { name: "ตอง", base: 20 };
  return { name: "ธรรมดา", base: 10 };
}

function newPlayer(name, bot = false) {
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
    players: Array.from(
      { length: 5 },
      (_, i) => newPlayer(`Bot ${i + 1}`, true)
    ),
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
    turnTimer: null,
    turnVersion: 0,
    deadline: null,
    botStage: "",
    helperSeat: -1,
    helperPlayerId: null,
    disconnectedAt: null
  };

  rooms.set(code, room);
  return room;
}

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function fail(p, message) {
  send(p?.ws, { type: "error", message });
}

function log(room, message) {
  room.log.push(message);
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

function publicState(room, p) {
  return {
    type: "state",
    code: room.code,
    seat: room.players.indexOf(p),
    phase: room.phase,
    round: room.round,
    turn: room.turn,
    dealer: room.dealer,
    starter: room.starter,
    deckCount: room.deck.length,
    hand: [...p.hand],
    players: room.players.map(x => ({
      name: x.name,
      bot: x.bot,
      online: x.online,
      credits: x.credits,
      cardCount: x.hand.length,
      discards: [...x.discards]
    })),
    last: room.last ? {
      player: room.last.player,
      cards: [...room.last.cards]
    } : null,
    actionTaken: room.actionTaken,
    champagneMoney: room.champagneMoney,
    ledger: room.ledger,
    log: room.log,
    result: room.result,
    showdown: room.showdown,
    vote: room.vote,
    deadline: room.deadline,
    botStage: room.botStage,
    helperSeat: room.helperSeat
  };
}

function broadcast(room) {
  for (const p of room.players) {
    if (!p.bot && p.online) {
      send(p.ws, publicState(room, p));
    }
  }

  room.queue.forEach((q, i) => {
    send(q.ws, {
      type: "queue",
      position: i + 1
    });
  });
}

function clearTurnTimer(room) {
  clearTimeout(room.turnTimer);
  room.turnTimer = null;
  room.turnVersion++;
  room.deadline = null;
  room.botStage = "";
  room.helperSeat = -1;
  room.helperPlayerId = null;
  room.disconnectedAt = null;
}

function setTurnTimer(room, ms, callback) {
  clearTimeout(room.turnTimer);

  const version = ++room.turnVersion;
  const turn = room.turn;
  const playerId = room.players[turn]?.id;

  room.deadline = Date.now() + ms;

  room.turnTimer = setTimeout(() => {
    if (
      room.phase !== "play" ||
      room.turnVersion !== version ||
      room.turn !== turn ||
      room.players[turn]?.id !== playerId
    ) return;

    room.turnTimer = null;
    callback();
  }, ms);
}

function canAct(room, p, taken, automatic = false) {
  if (room.phase !== "play") {
    if (!automatic) fail(p, "เกมยังไม่เริ่ม");
    return false;
  }

  if (room.players[room.turn] !== p) {
    if (!automatic) fail(p, "ยังไม่ถึงตาคุณ");
    return false;
  }

  if (!automatic && room.helperSeat === room.turn) {
    fail(p, "Bot กำลังช่วยเล่นตานี้");
    return false;
  }

  if (room.actionTaken !== taken) {
    if (!automatic) {
      fail(p, taken
        ? "ต้องจั่วหรือไหลก่อนทิ้ง"
        : "คุณจั่วหรือไหลแล้ว");
    }
    return false;
  }

  return true;
}

function followable(room) {
  const previous = (room.turn + 4) % 5;
  const last = room.last;

  if (!last || last.player !== previous) return [];

  const cards = last.cards;
  if (!cards.length) return [];

  if (!cards.every(c => rank(c) === rank(cards[0]))) {
    return [];
  }

  const pile = room.players[previous].discards;
  if (!cards.every(c => pile.includes(c))) return [];

  return cards;
}

function draw(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic)) return false;

  if (!room.deck.length) {
    if (!automatic) fail(p, "กองจั่วหมดแล้ว");
    return false;
  }

  p.hand.push(room.deck.pop());
  room.actionTaken = true;
  broadcast(room);
  return true;
}

function follow(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic)) return false;

  const cards = followable(room);

  if (!cards.length) {
    if (!automatic) fail(p, "ไม่มีไพ่ให้ไหล");
    return false;
  }

  const previous = (room.turn + 4) % 5;
  const pile = room.players[previous].discards;

  for (const c of cards) {
    pile.splice(pile.indexOf(c), 1);
    p.hand.push(c);
  }

  room.last = null;
  room.actionTaken = true;

  log(room, `🃏 ${p.name} ไหล ${cards.length} ใบ`);
  broadcast(room);
  return true;
}

function chooseLowest(room, start) {
  let best = start;

  for (let k = 1; k < 5; k++) {
    const i = (start + k) % 5;

    if (
      score(room.players[i].hand) <
      score(room.players[best].hand)
    ) {
      best = i;
    }
  }

  return best;
}

function finish(room, winnerIndex, reason, callerIndex = null) {
  if (room.phase !== "play") return;

  clearTurnTimer(room);

  const winner = room.players[winnerIndex];
  const cat = category(winner.hand);
  const rate = cat.base + aces(winner.hand) * 10;
  const failed = callerIndex !== null &&
    callerIndex !== winnerIndex;

  if (failed) {
    transfer(
      room,
      room.players[callerIndex],
      winner,
      rate * 4,
      "แคงผิด"
    );
  } else {
    for (let i = 0; i < 5; i++) {
      if (i !== winnerIndex) {
        transfer(
          room,
          room.players[i],
          winner,
          rate,
          reason
        );
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
  log(room, `💰 ${failed
    ? "แคงผิด จ่ายรวม"
    : "ผู้แพ้จ่ายคนละ"} ${room.result.amount} เครดิต`);

  broadcast(room);
}

function kang(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic)) return false;

  const caller = room.turn;
  const callerPoints = score(p.hand);

  const lowerExists = room.players.some(
    (x, i) => i !== caller && score(x.hand) < callerPoints
  );

  if (!lowerExists) {
    finish(room, caller, "แคงสำเร็จ");
  } else {
    const winner = chooseLowest(room, (caller + 1) % 5);
    finish(room, winner, "แคงผิด", caller);
  }

  return true;
}

function discard(room, p, cards, automatic = false) {
  if (!canAct(room, p, true, automatic)) return false;

  if (
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length ||
    !cards.every(c =>
      Number.isInteger(c) && p.hand.includes(c)
    ) ||
    (
      cards.length > 1 &&
      !cards.every(c => rank(c) === rank(cards[0]))
    )
  ) {
    if (!automatic) {
      fail(p, "เลือกทิ้งไพ่ 1–3 ใบ หน้าเดียวกัน");
    }
    return false;
  }

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }

  // แสดงไพ่บนโต๊ะ แต่ไม่ส่งข้อความทิ้งไพ่เข้าแชต
  room.last = {
    player: room.turn,
    cards: [...cards]
  };

  if (cards.length === 3) {
    for (const other of room.players) {
      if (other !== p) {
        transfer(room, other, p, 10, "โบนัสตอง");
      }
    }
    log(room, `💰 ${p.name} ได้โบนัสตอง 40 เครดิต`);
  }

  if (!p.hand.length) {
    finish(room, room.turn, "ไพ่หมดมือ");
    return true;
  }

  if (!room.deck.length) {
    const winner = chooseLowest(
      room,
      (room.turn + 1) % 5
    );
    finish(room, winner, "กองจั่วหมด");
    return true;
  }

  room.turn = (room.turn + 1) % 5;
  room.actionTaken = false;

  scheduleTurn(room);
  broadcast(room);

  return true;
}

function botDiscardChoice(hand) {
  const groups = new Map();

  for (const c of hand) {
    const r = rank(c);

    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(c);
  }

  for (const group of groups.values()) {
    if (group.length >= 3) {
      return group.slice(0, 3);
    }
  }

  // เลือกทิ้งไพ่แต้มสูงเพื่อลดแต้มในมือ
  return [...hand]
    .sort((a, b) => points(b) - points(a))
    .slice(0, 1);
}

function beginBotTurn(room, helper = false) {
  if (room.phase !== "play") return;

  const seat = room.turn;
  const p = room.players[seat];

  if (!p) return;

  clearTurnTimer(room);

  if (helper && !p.bot) {
    room.helperSeat = seat;
    room.helperPlayerId = p.id;
  }

  room.botStage = "thinking";

  setTurnTimer(room, BOT_THINK_MS, () => {
    if (room.phase !== "play" || room.turn !== seat) return;
    if (room.players[seat] !== p) return;

    room.botStage = "discarding";

    if (!room.actionTaken) {
      if (score(p.hand) <= 9 && Math.random() < 0.45) {
        kang(room, p, true);
        return;
      }

      let acted = false;

      if (followable(room).length && Math.random() < 0.7) {
        acted = follow(room, p, true);
      }

      if (!acted && room.deck.length) {
        acted = draw(room, p, true);
      }

      if (!acted && followable(room).length) {
        acted = follow(room, p, true);
      }

      if (!acted) {
        finish(
          room,
          chooseLowest(room, (room.turn + 1) % 5),
          "กองจั่วหมด"
        );
        return;
      }
    }

    // จังหวะที่สอง: คิดเลือกไพ่ก่อนทิ้งอีก 4 วินาที
    room.botStage = "discarding";

    setTurnTimer(room, BOT_DISCARD_MS, () => {
      if (room.phase !== "play" || room.turn !== seat) return;
      if (room.players[seat] !== p) return;

      const cards = botDiscardChoice(p.hand);

      if (!cards.length) {
        finish(room, seat, "ไพ่หมดมือ");
        return;
      }

      discard(room, p, cards, true);
    });

    broadcast(room);
  });

  broadcast(room);
}

function scheduleTurn(room) {
  clearTurnTimer(room);

  if (room.phase !== "play") return;

  const p = room.players[room.turn];
  if (!p) return;

  if (p.bot) {
    beginBotTurn(room);
    return;
  }

  if (!p.online || p.ws?.readyState !== WebSocket.OPEN) {
    room.disconnectedAt = Date.now();

    setTurnTimer(room, DISCONNECT_GRACE_MS, () => {
      if (
        room.phase === "play" &&
        room.players[room.turn] === p &&
        !p.online
      ) {
        log(room, `🤖 Bot ช่วยเล่นแทน ${p.name} ที่หลุด`);
        beginBotTurn(room, true);
      }
    });

    broadcast(room);
    return;
  }

  setTurnTimer(room, HUMAN_TIMEOUT_MS, () => {
    if (
      room.phase === "play" &&
      room.players[room.turn] === p
    ) {
      log(room, `⏰ ${p.name} หมดเวลา Bot ช่วยเล่นตานี้`);
      beginBotTurn(room, true);
    }
  });

  broadcast(room);
}

function startRound(room) {
  if (room.phase === "play") return;

  clearTurnTimer(room);

  // จับที่นั่งใหม่เฉพาะระหว่างรอบ
  if (room.pendingShuffle) {
    const winnerId = room.lastWinnerId;
    shuffle(room.players);

    if (winnerId) {
      const index = room.players.findIndex(
        p => p.id === winnerId
      );
      if (index >= 0) room.dealer = index;
    } else {
      room.dealer = 0;
    }

    room.pendingShuffle = false;
  }

  room.round++;
  room.phase = "play";
  room.deck = shuffle(
    Array.from({ length: 52 }, (_, i) => i)
  );
  room.last = null;
  room.actionTaken = false;
  room.result = null;
  room.showdown = [];
  room.vote = null;

  if (room.lastWinnerId) {
    const index = room.players.findIndex(
      p => p.id === room.lastWinnerId
    );
    if (index >= 0) room.dealer = index;
  }

  room.starter = room.dealer;
  room.turn = room.starter;

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
  }

  log(room, `🃏 เริ่มรอบ ${room.round}`);

  scheduleTurn(room);
  broadcast(room);
}

function attach(room, p, ws) {
  if (p.ws && p.ws !== ws) {
    const old = p.ws;
    p.ws = null;
    old.close();
  }

  p.bot = false;
  p.online = true;
  p.ws = ws;

  ws.room = room;
  ws.player = p;
  ws.queueEntry = null;

  send(ws, {
    type: "identity",
    code: room.code,
    token: p.token
  });

  // ผู้เล่นกลับมาก่อน Bot เริ่มช่วย ให้คืนเวลาตามปกติ
  if (
    room.phase === "play" &&
    room.players[room.turn] === p &&
    room.helperSeat !== room.turn
  ) {
    scheduleTurn(room);
  }

  broadcast(room);
}

function replaceBot(room, name, ws) {
  const seat = room.players.findIndex(p => p.bot);
  if (seat < 0) return false;

  const old = room.players[seat];
  const p = newPlayer(name);

  p.id = old.id;
  p.hand = old.hand;
  p.discards = old.discards;
  p.credits = old.credits;

  room.players[seat] = p;

  attach(room, p, ws);

  if (room.phase === "play" && room.turn === seat) {
    scheduleTurn(room);
  }

  return true;
}

function joinRoom(ws, code, name, token) {
  const room = rooms.get(
    String(code || "").trim().toUpperCase()
  );

  if (!room) {
    send(ws, {
      type: "error",
      message: "ไม่พบห้องนี้ อาจเกิดจาก Render รีสตาร์ต"
    });
    return;
  }

  const returning = room.players.find(
    p => !p.bot && token && p.token === token
  );

  if (returning) {
    attach(room, returning, ws);
    return;
  }

  const queued = room.queue.find(
    q => token && q.token === token
  );

  if (queued) {
    if (queued.ws && queued.ws !== ws) queued.ws.close();
    queued.ws = ws;
    ws.room = room;
    ws.queueEntry = queued;
    send(ws, {
      type: "identity",
      code: room.code,
      token: queued.token
    });
    broadcast(room);
    return;
  }

  if (replaceBot(room, name, ws)) return;

  const q = {
    name: String(name || "Player").slice(0, 32),
    token: uid(),
    ws
  };

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

function promoteQueue(room, seat) {
  while (room.queue.length) {
    const q = room.queue.shift();

    if (q.ws.readyState !== WebSocket.OPEN) continue;

    const old = room.players[seat];
    const p = newPlayer(q.name);

    p.id = old.id;
    p.hand = old.hand;
    p.discards = old.discards;
    p.credits = old.credits;
    p.token = q.token;

    room.players[seat] = p;
    attach(room, p, q.ws);

    if (room.phase === "play" && room.turn === seat) {
      scheduleTurn(room);
    }
    return;
  }
}

function leaveRoom(ws) {
  const room = ws.room;
  if (!room) return;

  const queueIndex = room.queue.findIndex(q => q.ws === ws);

  if (queueIndex >= 0) {
    room.queue.splice(queueIndex, 1);
    send(ws, { type: "left" });
    ws.room = null;
    ws.queueEntry = null;
    broadcast(room);
    return;
  }

  const seat = room.players.findIndex(p => p.ws === ws);

  if (seat < 0) {
    send(ws, { type: "left" });
    ws.room = null;
    ws.player = null;
    return;
  }

  const old = room.players[seat];
  const bot = newPlayer(`Bot ${seat + 1}`, true);

  bot.id = old.id;
  bot.hand = old.hand;
  bot.discards = old.discards;
  bot.credits = old.credits;

  room.players[seat] = bot;

  send(ws, { type: "left" });

  ws.room = null;
  ws.player = null;

  promoteQueue(room, seat);

  if (room.phase === "play" && room.turn === seat) {
    scheduleTurn(room);
  }

  broadcast(room);
}

function requestShuffle(room, p) {
  if (room.vote) {
    fail(p, "กำลังลงคะแนนอยู่แล้ว");
    return;
  }

  const required = room.players
    .filter(x => !x.bot && x.online)
    .map(x => x.id);

  room.vote = {
    requester: p.name,
    required,
    approvals: [p.id]
  };

  if (required.length <= 1) {
    room.pendingShuffle = true;
    room.vote = null;
    log(room, "🎲 อนุมัติจับที่นั่งใหม่ในรอบถัดไป");
  }

  broadcast(room);
}

function approveShuffle(room, p) {
  const vote = room.vote;
  if (!vote || !vote.required.includes(p.id)) return;

  if (!vote.approvals.includes(p.id)) {
    vote.approvals.push(p.id);
  }

  if (
    vote.required.every(id => vote.approvals.includes(id))
  ) {
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
      res.writeHead(200, {
        "Content-Type": "text/plain; charset=utf-8"
      });
      res.end("KANG ONLINE V12.7 OK");
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
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", raw => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "ข้อมูลไม่ถูกต้อง"
      });
      return;
    }

    const action = data.action;

    if (action === "create") {
      if (ws.room) leaveRoom(ws);

      const room = createRoom();
      const p = newPlayer(data.name);

      room.players[0] = p;
      attach(room, p, ws);
      return;
    }

    if (action === "join") {
      if (ws.room) leaveRoom(ws);

      joinRoom(
        ws,
        data.code,
        data.name,
        data.token
      );
      return;
    }

    if (action === "leave") {
      leaveRoom(ws);
      return;
    }

    const room = ws.room;
    const p = ws.player;

    if (
      !room ||
      !p ||
      !room.players.includes(p) ||
      p.ws !== ws
    ) {
      send(ws, {
        type: "error",
        message: "กรุณาเข้าห้องก่อน"
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
        kang(room, p);
        break;

      case "draw":
        if (draw(room, p)) {
          // คงเวลาเดิมสำหรับการเลือกทิ้ง
          broadcast(room);
        }
        break;

      case "follow":
        if (follow(room, p)) broadcast(room);
        break;

      case "discard":
        discard(room, p, data.cards);
        break;

      case "chat": {
        const message = String(data.text || "")
          .trim()
          .slice(0, 300);

        if (message) {
          log(room, `💬 ${p.name}: ${message}`);
          broadcast(room);
        }
        break;
      }

      case "credit": {
        const amount = Number(data.amount);

        if ([100, 200, 300, 400, 500].includes(amount)) {
          p.credits += amount;
          log(room, `💰 ${p.name} เพิ่มเครดิต ${amount}`);
          broadcast(room);
        }
        break;
      }

      case "requestSeats":
        requestShuffle(room, p);
        break;

      case "approveSeats":
        approveShuffle(room, p);
        break;

      case "rejectSeats":
        if (room.vote) {
          room.vote = null;
          log(room, `🎲 ${p.name} ไม่อนุมัติจับที่นั่งใหม่`);
          broadcast(room);
        }
        break;

      default:
        fail(p, "ไม่รู้จักคำสั่ง");
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;

    const qi = room.queue.findIndex(q => q.ws === ws);

    if (qi >= 0) {
      // เก็บ token ของคนในคิวไว้ให้กลับมาได้
      room.queue[qi].ws = null;
      return;
    }

    const p = ws.player;

    if (!p || p.ws !== ws) return;

    p.online = false;
    p.ws = null;

    if (
      room.phase === "play" &&
      room.players[room.turn] === p &&
      room.helperSeat !== room.turn
    ) {
      scheduleTurn(room);
    }

    broadcast(room);
  });
});

// ตรวจสอบการเชื่อมต่อที่หลุดโดยไม่แจ้ง close
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`KANG ONLINE V12.7 running on ${PORT}`);
});
