import http from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const THINK = 4000;
const DISCARD = 4000;
const HUMAN_WAIT = 90000;
const OFFLINE_WAIT = 15000;

const uid = () => randomBytes(16).toString("hex");
const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const value = c => Math.min(rank(c), 10);
const score = h => h.reduce((n, c) => n + value(c), 0);
const aces = h => h.filter(c => rank(c) === 1).length;

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

  const groups = [...counts.values()].sort((a, b) => b - a);
  const rs = hand.map(rank).sort((a, b) => a - b);

  const flush = hand.length === 5 &&
    hand.every(c => suit(c) === suit(hand[0]));

  const straight = hand.length === 5 && (
    rs.every((r, i) => i === 0 || r === rs[i - 1] + 1) ||
    rs.join(",") === "1,10,11,12,13"
  );

  if (straight && flush) return { name: "สเตรทฟลัช", base: 60 };
  if (groups[0] === 4) return { name: "โฟร์การ์ด", base: 50 };
  if (hand.length === 5 && groups[0] === 3 && groups[1] === 2)
    return { name: "ฟูลเฮาส์", base: 40 };
  if (straight) return { name: "สเตรท", base: 20 };
  if (flush) return { name: "ฟลัช", base: 20 };
  if (score(hand) === 50) return { name: "50 แต้ม", base: 20 };
  if (groups[0] === 3) return { name: "ตอง", base: 20 };

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

function makeRoom() {
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
    timer: null,
    timerVersion: 0,
    deadline: null,
    botStage: "",
    helperSeat: -1
  };

  rooms.set(code, room);
  return room;
}

function send(ws, obj) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function error(p, msg) {
  send(p?.ws, { type: "error", message: msg });
}

function note(room, msg) {
  room.log.push(msg);
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
    to: to?.name || "Champagne Money",
    amount,
    reason
  });

  if (room.ledger.length > 150) room.ledger.shift();
}

// V12.8: ไหลได้เมื่อผู้เล่นมีไพ่หน้าเดียวกับไพ่ที่ทิ้ง
function followable(room) {
  if (room.phase !== "play" || room.actionTaken) return [];

  const previous = (room.turn + 4) % 5;
  const current = room.players[room.turn];
  const last = room.last;

  if (
    !current ||
    !last ||
    last.player !== previous ||
    !Array.isArray(last.cards) ||
    !last.cards.length
  ) return [];

  const cards = last.cards;
  const r = rank(cards[0]);

  if (!cards.every(c => rank(c) === r)) return [];
  if (!current.hand.some(c => rank(c) === r)) return [];

  const pile = room.players[previous].discards;
  if (!cards.every(c => pile.includes(c))) return [];

  return [...cards];
}

function publicState(room, p) {
  const seat = room.players.indexOf(p);

  const canFollow =
    room.phase === "play" &&
    seat === room.turn &&
    room.helperSeat !== room.turn &&
    !room.actionTaken;

  return {
    type: "state",
    code: room.code,
    seat,
    phase: room.phase,
    round: room.round,
    turn: room.turn,
    dealer: room.dealer,
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
    last: room.last
      ? { player: room.last.player, cards: [...room.last.cards] }
      : null,
    actionTaken: room.actionTaken,
    canFollow: canFollow && followable(room).length > 0,
    followCards: canFollow ? followable(room) : [],
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
    send(q.ws, { type: "queue", position: i + 1 });
  });
}

function cancelTimer(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.timerVersion++;
  room.deadline = null;
  room.botStage = "";
  room.helperSeat = -1;
}

function schedule(room, ms, callback) {
  clearTimeout(room.timer);

  const version = ++room.timerVersion;
  const seat = room.turn;
  const id = room.players[seat]?.id;

  room.deadline = Date.now() + ms;

  room.timer = setTimeout(() => {
    if (
      room.phase !== "play" ||
      room.timerVersion !== version ||
      room.turn !== seat ||
      room.players[seat]?.id !== id
    ) return;

    room.timer = null;

    try {
      callback();
    } catch (e) {
      console.error("Turn timer error", e);
      recoverTurn(room);
    }
  }, ms);
}

function recoverTurn(room) {
  if (room.phase !== "play") return;

  const p = room.players[room.turn];
  if (!p) return;

  if (room.actionTaken) {
    const cards = botDiscardChoice(p.hand);

    if (cards.length) {
      discard(room, p, cards, true);
    } else {
      finish(room, room.turn, "ไพ่หมดมือ");
    }
  } else {
    beginBotTurn(room, !p.bot);
  }
}

function canAct(room, p, taken, automatic = false) {
  if (room.phase !== "play") {
    if (!automatic) error(p, "เกมยังไม่เริ่ม");
    return false;
  }

  if (room.players[room.turn] !== p) {
    if (!automatic) error(p, "ยังไม่ถึงตาคุณ");
    return false;
  }

  if (!automatic && room.helperSeat === room.turn) {
    error(p, "Bot กำลังช่วยเล่นตานี้");
    return false;
  }

  if (room.actionTaken !== taken) {
    if (!automatic) {
      error(
        p,
        taken ? "ต้องจั่วหรือไหลก่อนทิ้ง" : "คุณจั่วหรือไหลแล้ว"
      );
    }
    return false;
  }

  return true;
}

function draw(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;

  if (!room.deck.length) {
    if (!auto) error(p, "กองจั่วหมด");
    return false;
  }

  p.hand.push(room.deck.pop());
  room.actionTaken = true;
  broadcast(room);
  return true;
}

function follow(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;

  const cards = followable(room);

  if (!cards.length) {
    if (!auto) error(p, "ไม่มีไพ่หน้าเดียวกันให้ไหล");
    return false;
  }

  const pile = room.players[(room.turn + 4) % 5].discards;

  for (const c of cards) {
    pile.splice(pile.indexOf(c), 1);
    p.hand.push(c);
  }

  room.last = null;
  room.actionTaken = true;

  note(room, `🃏 ${p.name} ไหล ${cards.length} ใบ`);
  broadcast(room);
  return true;
}

function lowest(room, start) {
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

  cancelTimer(room);

  const winner = room.players[winnerIndex];
  const cat = category(winner.hand);
  const rate = cat.base + aces(winner.hand) * 10;

  const failed =
    callerIndex !== null &&
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

  room.result = {
    message: `🏆 ${winner.name} ชนะ! (${reason})`,
    category: cat.name,
    amount: failed ? rate * 4 : rate,
    failed
  };

  note(room, room.result.message);
  broadcast(room);
}

function kang(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;

  const caller = room.turn;

  const hasLower = room.players.some(
    (x, i) => i !== caller && score(x.hand) < score(p.hand)
  );

  if (hasLower) {
    finish(
      room,
      lowest(room, (caller + 1) % 5),
      "แคงผิด",
      caller
    );
  } else {
    finish(room, caller, "แคงสำเร็จ");
  }

  return true;
}

function discard(room, p, cards, auto = false) {
  if (!canAct(room, p, true, auto)) return false;

  if (
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length ||
    !cards.every(c => Number.isInteger(c) && p.hand.includes(c)) ||
    (
      cards.length > 1 &&
      !cards.every(c => rank(c) === rank(cards[0]))
    )
  ) {
    if (!auto) error(p, "เลือกทิ้ง 1–3 ใบ หน้าเดียวกัน");
    return false;
  }

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }

  // ไพ่ทิ้งแสดงบนโต๊ะ แต่ไม่แสดงเหตุการณ์ทิ้งในแชต
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
    note(room, `💰 ${p.name} ได้โบนัสตอง 40 เครดิต`);
  }

  if (!p.hand.length) {
    finish(room, room.turn, "ไพ่หมดมือ");
    return true;
  }

  if (!room.deck.length) {
    finish(
      room,
      lowest(room, (room.turn + 1) % 5),
      "กองจั่วหมด"
    );
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
    if (group.length >= 3) return group.slice(0, 3);
  }

  return [...hand]
    .sort((a, b) => value(b) - value(a))
    .slice(0, 1);
}

function beginBotTurn(room, helper = false) {
  if (room.phase !== "play") return;

  const seat = room.turn;
  const p = room.players[seat];
  if (!p) return;

  cancelTimer(room);

  if (helper && !p.bot) {
    room.helperSeat = seat;
  }

  room.botStage = "thinking";

  schedule(room, THINK, () => {
    if (room.phase !== "play" || room.players[seat] !== p) return;

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
          lowest(room, (room.turn + 1) % 5),
          "กองจั่วหมด"
        );
        return;
      }
    }

    room.botStage = "discarding";

    schedule(room, DISCARD, () => {
      if (room.phase !== "play" || room.players[seat] !== p) {
        return;
      }

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
  cancelTimer(room);

  if (room.phase !== "play") return;

  const p = room.players[room.turn];
  if (!p) return;

  if (p.bot) {
    beginBotTurn(room);
    return;
  }

  if (!p.online || p.ws?.readyState !== WebSocket.OPEN) {
    schedule(room, OFFLINE_WAIT, () => {
      if (
        room.players[room.turn] === p &&
        !p.online
      ) {
        note(room, `🤖 Bot ช่วยเล่นแทน ${p.name} ที่หลุด`);
        beginBotTurn(room, true);
      }
    });

    broadcast(room);
    return;
  }

  schedule(room, HUMAN_WAIT, () => {
    if (room.players[room.turn] === p) {
      note(room, `⏰ ${p.name} หมดเวลา Bot ช่วยเล่นตานี้`);
      beginBotTurn(room, true);
    }
  });

  broadcast(room);
}

function startRound(room) {
  if (room.phase === "play") return;

  cancelTimer(room);

  if (room.pendingShuffle) {
    shuffle(room.players);
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
    const i = room.players.findIndex(
      p => p.id === room.lastWinnerId
    );
    if (i >= 0) room.dealer = i;
  }

  room.turn = room.dealer;

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
  }

  note(room, `🃏 เริ่มรอบ ${room.round}`);

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
    if (queued.ws && queued.ws !== ws) {
      queued.ws.close();
    }

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

    if (
      !q.ws ||
      q.ws.readyState !== WebSocket.OPEN
    ) continue;

    const old = room.players[seat];
    const p = newPlayer(q.name);

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

  const qi = room.queue.findIndex(q => q.ws === ws);

  if (qi >= 0) {
    room.queue.splice(qi, 1);
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
    error(p, "กำลังลงคะแนนอยู่แล้ว");
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
    note(room, "🎲 อนุมัติจับที่นั่งใหม่ในรอบถัดไป");
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
    note(room, "🎲 ทุกคนอนุมัติจับที่นั่งใหม่");
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
        "Content-Type": "text/plain"
      });
      res.end("KANG ONLINE V12.8 OK");
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

      const room = makeRoom();
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
        draw(room, p);
        break;

      case "follow":
        follow(room, p);
        break;

      case "discard":
        discard(room, p, data.cards);
        break;

      case "chat": {
        const msg = String(data.text || "")
          .trim()
          .slice(0, 300);

        if (msg) {
          note(room, `💬 ${p.name}: ${msg}`);
          broadcast(room);
        }
        break;
      }

      case "credit": {
        const n = Number(data.amount);

        if ([100, 200, 300, 400, 500].includes(n)) {
          p.credits += n;
          note(room, `💰 ${p.name} เพิ่มเครดิต ${n}`);
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
          note(room, `🎲 ${p.name} ไม่อนุมัติจับที่นั่งใหม่`);
          broadcast(room);
        }
        break;

      default:
        error(p, "ไม่รู้จักคำสั่ง");
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    if (!room) return;

    const qi = room.queue.findIndex(q => q.ws === ws);

    if (qi >= 0) {
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
  console.log(`KANG ONLINE V12.8 running on ${PORT}`);
});
