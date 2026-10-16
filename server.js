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

function makePlayer(name, bot = false) {
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
      (_, i) => makePlayer(`Bot ${i + 1}`, true)
    ),
    queue: [],
    phase: "lobby",
    round: 0,
    dealer: 0,
    turn: 0,
    deck: [],
    last: null,
    actionTaken: false,
    followChain: false,
    lastWinnerId: null,
    champagneMoney: 0,
    ledger: [],
    chat: [],
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

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function fail(p, message) {
  send(p?.ws, { type: "error", message });
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

  if (room.ledger.length > 200) room.ledger.shift();
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

function lowestSeat(room, start) {
  let best = start;
  for (let k = 1; k < 5; k++) {
    const i = (start + k) % 5;
    if (score(room.players[i].hand) < score(room.players[best].hand)) {
      best = i;
    }
  }
  return best;
}

function clearTimer(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.timerVersion++;
  room.deadline = null;
  room.botStage = "";
  room.helperSeat = -1;
}

function setTimer(room, delay, callback) {
  clearTimeout(room.timer);
  const version = ++room.timerVersion;
  const seat = room.turn;
  const id = room.players[seat]?.id;
  room.deadline = Date.now() + delay;

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
      console.error("Timer error", e);
      recoverTurn(room);
    }
  }, delay);
}

function getFollowCards(room) {
  if (
    room.phase !== "play" ||
    !room.last ||
    room.actionTaken
  ) return [];

  const previous = (room.turn + 4) % 5;
  if (room.last.player !== previous) return [];

  const cards = room.last.cards;
  if (!Array.isArray(cards) || !cards.length) return [];

  const target = rank(cards[0]);
  if (!cards.every(c => rank(c) === target)) return [];

  return room.players[room.turn].hand.filter(
    c => rank(c) === target
  );
}

function publicState(room, p) {
  const seat = room.players.indexOf(p);
  const myTurn = room.phase === "play" &&
    seat === room.turn &&
    room.helperSeat !== room.turn;

  const followCards = myTurn && !room.actionTaken
    ? getFollowCards(room)
    : [];

  return {
    type: "state",
    code: room.code,
    seat,
    phase: room.phase,
    round: room.round,
    dealer: room.dealer,
    turn: room.turn,
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
    followChain: room.followChain,
    canFollow: followCards.length > 0,
    followCards,
    champagneMoney: room.champagneMoney,
    ledger: room.ledger,
    chat: room.chat,
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
    send(q.ws, { type: "queue", position: i + 1, code: room.code });
  });
}

function finishRound(room, winnerSeat, reason, callerSeat = null) {
  if (room.phase !== "play") return;

  clearTimer(room);

  const winner = room.players[winnerSeat];
  const cat = category(winner.hand);
  const rate = cat.base + aces(winner.hand) * 10;
  const wasOverlaid = callerSeat !== null && callerSeat !== winnerSeat;

  if (wasOverlaid) {
    transfer(
      room,
      room.players[callerSeat],
      winner,
      rate * 4,
      "โดนทับ"
    );
  } else {
    for (let i = 0; i < 5; i++) {
      if (i !== winnerSeat) {
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
  room.followChain = false;
  room.lastWinnerId = winner.id;
  room.dealer = winnerSeat;

  room.result = {
    message: wasOverlaid
      ? `💀 ${room.players[callerSeat].name} โดนทับ! ${winner.name} ชนะ`
      : `🏆 ${winner.name} ชนะ! (${reason})`,
    category: cat.name,
    amount: wasOverlaid ? rate * 4 : rate,
    failed: wasOverlaid,
    winnerSeat,
    overlaidSeat: wasOverlaid ? callerSeat : null,
    winnerName: winner.name,
    overlaidName: wasOverlaid ? room.players[callerSeat].name : null,
    effectUntil: Date.now() + 5000
  };

  broadcast(room);
}

function canAct(room, p, taken, auto = false) {
  if (room.phase !== "play") {
    if (!auto) fail(p, "เกมยังไม่เริ่ม");
    return false;
  }
  if (room.players[room.turn] !== p) {
    if (!auto) fail(p, "ยังไม่ถึงตาคุณ");
    return false;
  }
  if (!auto && room.helperSeat === room.turn) {
    fail(p, "Bot กำลังช่วยเล่นตานี้");
    return false;
  }
  if (room.actionTaken !== taken) {
    if (!auto) fail(p, taken ? "ต้องจั่วก่อนทิ้ง" : "คุณจั่วแล้ว");
    return false;
  }
  return true;
}

function kang(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;
  if (room.followChain) {
    if (!auto) fail(p, "กำลังอยู่ในช่วงไหล");
    return false;
  }

  const caller = room.turn;
  const hasLower = room.players.some(
    (x, i) => i !== caller && score(x.hand) < score(p.hand)
  );

  if (hasLower) {
    finishRound(room, lowestSeat(room, (caller + 1) % 5), "โดนทับ", caller);
  } else {
    finishRound(room, caller, "แคงสำเร็จ");
  }
  return true;
}

function draw(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;
  if (room.followChain) {
    if (!auto) fail(p, "กำลังอยู่ในช่วงไหล");
    return false;
  }
  if (!room.deck.length) {
    if (!auto) fail(p, "กองจั่วหมด");
    return false;
  }

  p.hand.push(room.deck.pop());
  room.actionTaken = true;

  if (!auto) scheduleTurn(room);
  broadcast(room);
  return true;
}

function discardBonus(room, p, cards) {
  if (cards.length !== 3) return;
  for (const other of room.players) {
    if (other !== p) transfer(room, other, p, 10, "โบนัสตอง");
  }
}

function removeFromHand(p, cards) {
  for (const c of cards) {
    const i = p.hand.indexOf(c);
    if (i < 0) return false;
    p.hand.splice(i, 1);
    p.discards.push(c);
  }
  return true;
}

function follow(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return false;

  const cards = getFollowCards(room);
  if (!cards.length) {
    if (!auto) fail(p, "ไม่มีไพ่หน้าเดียวกันให้ไหล");
    return false;
  }

  const seat = room.turn;
  removeFromHand(p, cards);
  discardBonus(room, p, cards);

  room.last = { player: seat, cards: [...cards] };

  if (!p.hand.length) {
    finishRound(room, seat, "ไหลจนไพ่หมดมือ");
    return true;
  }

  room.turn = (seat + 1) % 5;
  room.actionTaken = false;
  room.followChain = true;

  if (!getFollowCards(room).length) {
    endFollowChain(room);
  } else {
    scheduleTurn(room);
  }
  broadcast(room);
  return true;
}

function endFollowChain(room) {
  if (room.phase !== "play") return;

  room.followChain = false;
  room.actionTaken = false;

  if (!room.deck.length) {
    finishRound(room, lowestSeat(room, room.turn), "กองจั่วหมด");
    return;
  }

  scheduleTurn(room);
  broadcast(room);
}

function discard(room, p, cards, auto = false) {
  if (!canAct(room, p, true, auto)) return false;
  if (room.followChain) {
    if (!auto) fail(p, "ต้องไหลให้จบก่อน");
    return false;
  }

  if (
    !Array.isArray(cards) ||
    cards.length < 1 ||
    new Set(cards).size !== cards.length ||
    !cards.every(c => Number.isInteger(c) && p.hand.includes(c)) ||
    !cards.every(c => rank(c) === rank(cards[0]))
  ) {
    if (!auto) fail(p, "ทิ้งได้ไม่จำกัดใบ แต่ต้องหน้าเดียวกัน");
    return false;
  }

  const seat = room.turn;
  removeFromHand(p, cards);
  discardBonus(room, p, cards);
  room.last = { player: seat, cards: [...cards] };

  if (!p.hand.length) {
    finishRound(room, seat, "ทิ้งจนไพ่หมดมือ");
    return true;
  }

  room.turn = (seat + 1) % 5;
  room.actionTaken = false;
  room.followChain = false;

  if (getFollowCards(room).length) {
    room.followChain = true;
    scheduleTurn(room);
  } else if (!room.deck.length) {
    finishRound(room, lowestSeat(room, room.turn), "กองจั่วหมด");
  } else {
    scheduleTurn(room);
  }

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

  return [...groups.values()].sort(
    (a, b) => b.length - a.length || points(b[0]) - points(a[0])
  )[0] || [];
}

function recoverTurn(room) {
  if (room.phase !== "play") return;
  const p = room.players[room.turn];

  if (room.followChain) {
    if (getFollowCards(room).length) follow(room, p, true);
    else endFollowChain(room);
  } else if (room.actionTaken) {
    const cards = botDiscardChoice(p.hand);
    if (cards.length) discard(room, p, cards, true);
    else finishRound(room, room.turn, "ไพ่หมดมือ");
  } else {
    beginBotTurn(room, !p.bot);
  }
}

function beginBotTurn(room, helper = false) {
  if (room.phase !== "play") return;

  const seat = room.turn;
  const p = room.players[seat];

  clearTimer(room);
  if (helper && !p.bot) room.helperSeat = seat;

  room.botStage = "thinking";

  setTimer(room, BOT_THINK_MS, () => {
    if (room.phase !== "play" || room.players[seat] !== p) return;

    if (room.followChain) {
      if (getFollowCards(room).length) follow(room, p, true);
      else endFollowChain(room);
      return;
    }

    if (!room.actionTaken) {
      if (score(p.hand) <= 9 && Math.random() < 0.45) {
        kang(room, p, true);
        return;
      }

      if (!draw(room, p, true)) {
        if (!room.deck.length) {
          finishRound(room, lowestSeat(room, seat), "กองจั่วหมด");
        }
        return;
      }
    }

    room.botStage = "discarding";

    setTimer(room, BOT_DISCARD_MS, () => {
      if (room.phase !== "play" || room.players[seat] !== p) return;

      const cards = botDiscardChoice(p.hand);
      if (cards.length) discard(room, p, cards, true);
      else finishRound(room, seat, "ไพ่หมดมือ");
    });

    broadcast(room);
  });

  broadcast(room);
}

function scheduleTurn(room) {
  clearTimer(room);
  if (room.phase !== "play") return;

  const p = room.players[room.turn];

  if (room.followChain && !getFollowCards(room).length) {
    endFollowChain(room);
    return;
  }

  if (p.bot) {
    beginBotTurn(room);
    return;
  }

  if (!p.online || p.ws?.readyState !== WebSocket.OPEN) {
    setTimer(room, DISCONNECT_GRACE_MS, () => {
      if (room.players[room.turn] === p && !p.online) {
        beginBotTurn(room, true);
      }
    });
    broadcast(room);
    return;
  }

  setTimer(room, HUMAN_TIMEOUT_MS, () => {
    if (room.players[room.turn] === p) {
      beginBotTurn(room, true);
    }
  });

  broadcast(room);
}

function startRound(room) {
  if (room.phase === "play") return;

  clearTimer(room);

  if (room.pendingShuffle) {
    shuffle(room.players);
    room.pendingShuffle = false;
  }

  if (room.lastWinnerId) {
    const seat = room.players.findIndex(
      p => p.id === room.lastWinnerId
    );
    if (seat >= 0) room.dealer = seat;
  }

  room.round++;
  room.phase = "play";
  room.deck = shuffle(Array.from({ length: 52 }, (_, i) => i));
  room.turn = room.dealer;
  room.last = null;
  room.actionTaken = false;
  room.followChain = false;
  room.result = null;
  room.showdown = [];
  room.vote = null;

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
  }

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

  send(ws, { type: "identity", code: room.code, token: p.token });

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
  const p = makePlayer(name);

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
  const room = rooms.get(String(code || "").trim().toUpperCase());

  if (!room) {
    send(ws, { type: "error", message: "ไม่พบห้อง กรุณาสร้างห้องใหม่" });
    return;
  }

  const returning = room.players.find(
    p => !p.bot && token && p.token === token
  );
  if (returning) {
    attach(room, returning, ws);
    return;
  }

  const queued = room.queue.find(q => token && q.token === token);
  if (queued) {
    if (queued.ws && queued.ws !== ws) queued.ws.close();
    queued.ws = ws;
    ws.room = room;
    ws.queueEntry = queued;
    send(ws, { type: "identity", code: room.code, token: queued.token });
    broadcast(room);
    return;
  }

  if (replaceBot(room, name, ws)) return;

  const entry = {
    name: String(name || "Player").slice(0, 32),
    token: uid(),
    ws
  };

  room.queue.push(entry);
  ws.room = room;
  ws.queueEntry = entry;

  send(ws, { type: "identity", code: room.code, token: entry.token });
  broadcast(room);
}

function promoteQueue(room, seat) {
  while (room.queue.length) {
    const q = room.queue.shift();
    if (!q.ws || q.ws.readyState !== WebSocket.OPEN) continue;

    const old = room.players[seat];
    const p = makePlayer(q.name);

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
  const bot = makePlayer(`Bot ${seat + 1}`, true);
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
  }

  broadcast(room);
}

function approveShuffle(room, p) {
  const vote = room.vote;
  if (!vote || !vote.required.includes(p.id)) return;

  if (!vote.approvals.includes(p.id)) vote.approvals.push(p.id);

  if (vote.required.every(id => vote.approvals.includes(id))) {
    room.pendingShuffle = true;
    room.vote = null;
  }
  broadcast(room);
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/" || req.url === "/index.html") {
      const html = await readFile(new URL("./index.html", import.meta.url));
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store"
      });
      res.end(html);
      return;
    }

    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("KANG ONLINE V13.0 OK");
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
  ws.on("pong", () => { ws.isAlive = true; });

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
      const room = makeRoom();
      const p = makePlayer(data.name);
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
    const p = ws.player;

    if (!room || !p || !room.players.includes(p) || p.ws !== ws) {
      send(ws, { type: "error", message: "กรุณาเข้าห้องก่อน" });
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
        const text = String(data.text || "").trim().slice(0, 300);
        if (text) {
          room.chat.push({ name: p.name, text });
          if (room.chat.length > 100) room.chat.shift();
          broadcast(room);
        }
        break;
      }

      case "credit": {
        const amount = Number(data.amount);
        if ([100, 200, 300, 400, 500].includes(amount)) {
          p.credits += amount;
          room.ledger.push({
            round: room.round,
            from: "ระบบ",
            to: p.name,
            amount,
            reason: "เติมเครดิตจำลอง"
          });
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
  console.log(`KANG ONLINE V13.0 running on ${PORT}`);
});
