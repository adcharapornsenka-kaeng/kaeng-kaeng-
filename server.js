import http from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const id = () => randomBytes(16).toString("hex");
const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const value = c => Math.min(rank(c), 10);
const score = h => h.reduce((n, c) => n + value(c), 0);
const aceCount = h => h.filter(c => rank(c) === 1).length;
const next = i => (i + 1) % 5;

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function roomCode() {
  let code;
  do {
    code = randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));
  return code;
}

function makeHuman(name) {
  return {
    id: id(),
    token: id(),
    name: String(name || "Player").trim().slice(0, 32),
    bot: false,
    online: true,
    credits: 0,
    hand: [],
    discards: [],
    ws: null
  };
}

function makeBot(n) {
  return {
    id: id(),
    token: null,
    name: `BOT ${n}`,
    bot: true,
    online: true,
    credits: 0,
    hand: [],
    discards: [],
    ws: null
  };
}

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function fail(ws, message) {
  send(ws, { type: "error", message });
}

function log(r, text) {
  r.log.push(text);
  if (r.log.length > 100) r.log.shift();
}

function entry(r, from, to, amount, reason) {
  r.ledger.push({
    round: r.round,
    from,
    to,
    amount,
    reason
  });
  if (r.ledger.length > 300) r.ledger.shift();
}

function pay(r, from, to, amount, reason) {
  if (!Number.isFinite(amount) || amount <= 0) return;
  from.credits -= amount;
  to.credits += amount;
  entry(r, from.name, to.name, amount, reason);
}

function category(hand) {
  const n = hand.length;
  const ranks = hand.map(rank);
  const suits = hand.map(suit);
  const counts = new Map();

  for (const x of ranks) {
    counts.set(x, (counts.get(x) || 0) + 1);
  }

  const groups = [...counts.values()].sort((a, b) => b - a);
  const unique = [...new Set(ranks)].sort((a, b) => a - b);

  const flush = n === 5 && new Set(suits).size === 1;

  const straight =
    n === 5 &&
    unique.length === 5 &&
    (
      unique[4] - unique[0] === 4 ||
      unique.join(",") === "1,10,11,12,13"
    );

  if (straight && flush) {
    return { name: "Straight Flush", rate: 60 };
  }

  if (n === 4 && groups[0] === 4) {
    return { name: "Four of a Kind", rate: 50 };
  }

  if (n === 5 && groups[0] === 3 && groups[1] === 2) {
    return { name: "Full House", rate: 40 };
  }

  if (flush) return { name: "สี", rate: 20 };
  if (straight) return { name: "เรียง", rate: 20 };

  if (n === 5 && score(hand) === 50) {
    return { name: "เซียน 50 แต้ม", rate: 20 };
  }

  if (n === 3 && groups[0] === 3) {
    return { name: "ตอง", rate: 20 };
  }

  return { name: "ธรรมดา", rate: 10 };
}

function winningRate(hand) {
  return category(hand).rate + aceCount(hand) * 10;
}

function firstInTurnOrder(indices, afterSeat) {
  for (let step = 1; step <= 5; step++) {
    const seat = (afterSeat + step) % 5;
    if (indices.includes(seat)) return seat;
  }
  return indices[0];
}

function publicState(r, p) {
  return {
    type: "state",
    code: r.code,
    seat: r.players.indexOf(p),
    phase: r.phase,
    round: r.round,
    turn: r.turn,
    starter: r.starter,
    deckCount: r.deck.length,
    hand: [...p.hand],
    players: r.players.map(x => ({
      id: x.id,
      name: x.name,
      bot: x.bot,
      online: x.online,
      credits: x.credits,
      cardCount: x.hand.length,
      discards: [...x.discards]
    })),
    last: r.last
      ? { player: r.last.player, cards: [...r.last.cards] }
      : null,
    actionTaken: r.actionTaken,
    champagneMoney: r.champagneMoney,
    ledger: r.ledger.slice(-60),
    log: r.log.slice(-45),
    result: r.result,
    showdown: r.showdown,
    vote: r.vote
      ? {
          requester: r.vote.requester,
          approvals: [...r.vote.approvals],
          required: r.vote.required,
          waiting: r.vote.waiting
        }
      : null
  };
}

function broadcast(r) {
  for (const p of r.players) {
    if (!p.bot) send(p.ws, publicState(r, p));
  }

  r.queue.forEach((p, i) => {
    send(p.ws, {
      type: "queue",
      code: r.code,
      position: i + 1
    });
  });
}

function stopBot(r) {
  if (r.botTimer) clearTimeout(r.botTimer);
  r.botTimer = null;
}

function transferSeat(old, replacement) {
  replacement.hand = [...old.hand];
  replacement.discards = [...old.discards];
  replacement.credits = old.credits;
}

function attach(r, p, ws) {
  if (p.ws && p.ws !== ws) {
    try { p.ws.close(); } catch {}
  }

  p.ws = ws;
  p.online = true;
  ws.room = r;
  ws.player = p;

  send(ws, {
    type: "identity",
    code: r.code,
    token: p.token
  });

  broadcast(r);
}

function fillFromQueue(r) {
  while (r.queue.length) {
    const seat = r.players.findIndex(p => p.bot);
    if (seat < 0) break;

    const p = r.queue.shift();
    const old = r.players[seat];

    transferSeat(old, p);
    r.players[seat] = p;

    attach(r, p, p.ws);
    log(r, `${p.name} เข้ามาแทน ${old.name}`);
  }

  broadcast(r);
}

function endRound(r, winnerSeat, reason, failed = false, caller = null) {
  if (r.phase !== "play") return;

  stopBot(r);

  const winner = r.players[winnerSeat];

  r.showdown = r.players.map(p => ({
    name: p.name,
    hand: [...p.hand],
    points: score(p.hand),
    count: p.hand.length
  }));

  let amount;

  if (failed) {
    amount = (10 + 10 * aceCount(winner.hand)) * 4;
    pay(r, caller, winner, amount, "แคงไม่สำเร็จ");
  } else {
    amount = winningRate(winner.hand);

    for (const p of r.players) {
      if (p !== winner) {
        pay(r, p, winner, amount, reason);
      }
    }
  }

  winner.credits -= 2;
  r.champagneMoney += 2;

  entry(
    r,
    winner.name,
    "Champagne Pool",
    2,
    "Champagne Money"
  );

  r.result = {
    winner: winnerSeat,
    name: winner.name,
    reason,
    failed,
    amount,
    category: category(winner.hand).name,
    message: `🏆 ${winner.name} ชนะ — ${reason}`
  };

  r.phase = "end";
  r.actionTaken = false;

  log(r, r.result.message);
  broadcast(r);
}

function endWhenDeckEmpty(r, anchorSeat) {
  if (r.phase !== "play") return;

  const minCards = Math.min(...r.players.map(p => p.hand.length));

  const candidates = r.players
    .map((p, i) => p.hand.length === minCards ? i : -1)
    .filter(i => i !== -1);

  const winner = candidates.length === 1
    ? candidates[0]
    : firstInTurnOrder(candidates, anchorSeat);

  endRound(r, winner, "กองจั่วหมด");
}

function beginRound(r) {
  if (!["lobby", "end"].includes(r.phase)) return false;

  stopBot(r);

  if (r.pendingShuffle) {
    shuffle(r.players);
    r.pendingShuffle = false;
    log(r, "จับที่นั่งใหม่สำเร็จ");
  }

  r.vote = null;
  r.round += 1;
  r.phase = "play";
  r.deck = shuffle(Array.from({ length: 52 }, (_, i) => i));

  for (const p of r.players) {
    p.hand = r.deck.splice(0, 5);
    p.discards = [];
  }

  r.starter = Math.floor(Math.random() * 5);
  r.turn = r.starter;
  r.last = null;
  r.actionTaken = false;
  r.result = null;
  r.showdown = null;

  log(r, `เริ่มรอบ ${r.round}: ${r.players[r.turn].name} เริ่ม`);

  broadcast(r);
  scheduleBot(r);
  return true;
}

function drawCard(r, p) {
  if (
    r.phase !== "play" ||
    r.players[r.turn] !== p ||
    r.actionTaken ||
    r.deck.length === 0
  ) return false;

  p.hand.push(r.deck.pop());
  r.actionTaken = true;

  log(r, `${p.name} หยิบไพ่ 1 ใบ`);
  broadcast(r);
  return true;
}

function followCards(r, p, cards) {
  if (
    r.phase !== "play" ||
    r.players[r.turn] !== p ||
    r.actionTaken ||
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length
  ) return false;

  const previous = (r.turn + 4) % 5;

  if (
    !r.last ||
    r.last.player !== previous ||
    !cards.every(c => r.last.cards.includes(c))
  ) return false;

  const owner = r.players[previous];

  if (!cards.every(c => owner.discards.includes(c))) return false;

  for (const c of cards) {
    owner.discards.splice(owner.discards.indexOf(c), 1);
    r.last.cards.splice(r.last.cards.indexOf(c), 1);
    p.hand.push(c);
  }

  r.actionTaken = true;

  log(r, `${p.name} ตามไพ่ ${cards.length} ใบ`);
  broadcast(r);
  return true;
}

function discardCards(r, p, cards) {
  if (
    r.phase !== "play" ||
    r.players[r.turn] !== p ||
    !r.actionTaken ||
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length ||
    !cards.every(c => p.hand.includes(c))
  ) return false;

  if (
    cards.length > 1 &&
    new Set(cards.map(rank)).size !== 1
  ) return false;

  const discardingSeat = r.turn;

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
  }

  p.discards.push(...cards);

  r.last = {
    player: discardingSeat,
    cards: [...cards]
  };

  if (cards.length === 3) {
    for (const other of r.players) {
      if (other !== p) {
        pay(r, other, p, 10, "โบนัสทิ้งตอง");
      }
    }

    log(r, `${p.name} ทิ้งตอง รับโบนัส 40 เครดิต`);
  } else {
    log(r, `${p.name} ทิ้งไพ่ ${cards.length} ใบ`);
  }

  if (p.hand.length === 0) {
    endRound(r, discardingSeat, "ไพ่หมดมือ");
    return true;
  }

  r.turn = next(r.turn);
  r.actionTaken = false;

  if (r.deck.length === 0) {
    endWhenDeckEmpty(r, discardingSeat);
    return true;
  }

  broadcast(r);
  scheduleBot(r);
  return true;
}

function callKang(r, p) {
  if (
    r.phase !== "play" ||
    r.players[r.turn] !== p ||
    r.actionTaken
  ) return false;

  const callerSeat = r.turn;
  const myScore = score(p.hand);

  const lower = r.players
    .map((x, i) => ({ seat: i, points: score(x.hand) }))
    .filter(x => x.seat !== callerSeat && x.points < myScore);

  if (!lower.length) {
    endRound(r, callerSeat, "แคงสำเร็จ");
    return true;
  }

  const lowest = Math.min(...lower.map(x => x.points));

  const tied = lower
    .filter(x => x.points === lowest)
    .map(x => x.seat);

  const winnerSeat = firstInTurnOrder(tied, callerSeat);

  endRound(
    r,
    winnerSeat,
    "แคงไม่สำเร็จ",
    true,
    p
  );

  return true;
}

function scheduleBot(r) {
  stopBot(r);

  if (r.phase !== "play") return;

  const p = r.players[r.turn];
  if (!p?.bot) return;

  r.botTimer = setTimeout(() => {
    if (r.phase !== "play" || r.players[r.turn] !== p) return;

    if (!r.actionTaken) {
      const previous = (r.turn + 4) % 5;

      const available =
        r.last?.player === previous
          ? r.last.cards
          : [];

      const useful = available.filter(c => value(c) <= 4);

      if (useful.length && Math.random() < 0.65) {
        followCards(r, p, useful.slice(0, 3));
      } else if (r.deck.length > 0) {
        drawCard(r, p);
      } else if (available.length) {
        followCards(r, p, [available[0]]);
      } else {
        endWhenDeckEmpty(r, (r.turn + 4) % 5);
        return;
      }
    }

    if (r.phase !== "play" || r.players[r.turn] !== p) return;

    const groups = new Map();

    for (const c of p.hand) {
      const k = rank(c);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(c);
    }

    const triple = [...groups.values()].find(g => g.length >= 3);
    const pair = [...groups.values()].find(g => g.length >= 2);

    const chosen = triple
      ? triple.slice(0, 3)
      : pair
        ? pair.slice(0, 2)
        : [...p.hand].sort((a, b) => value(b) - value(a)).slice(0, 1);

    discardCards(r, p, chosen);
  }, 650);
}

function requestShuffle(r, p) {
  if (r.vote) return false;

  const required = r.players
    .filter(x => !x.bot && x.online)
    .map(x => x.id);

  r.vote = {
    requester: p.name,
    approvals: new Set([p.id]),
    required,
    waiting: r.phase === "play"
  };

  if (r.vote.approvals.size === required.length) {
    r.pendingShuffle = true;
    r.vote = null;
    log(r, "อนุมัติจับที่นั่งใหม่แล้ว");
  }

  broadcast(r);
  return true;
}

function leaveRoom(ws) {
  const r = ws.room;
  const p = ws.player;

  if (!r || !p) {
    send(ws, { type: "left" });
    return;
  }

  stopBot(r);

  const queueIndex = r.queue.indexOf(p);

  if (queueIndex >= 0) {
    r.queue.splice(queueIndex, 1);
  }

  const seat = r.players.indexOf(p);

  if (seat >= 0) {
    const replacement = makeBot(seat + 1);

    transferSeat(p, replacement);
    r.players[seat] = replacement;

    log(r, `${p.name} ออกจากห้อง`);
  }

  if (r.vote) {
    r.vote = null;
    log(r, "ยกเลิกการลงคะแนนจับที่นั่งใหม่");
  }

  p.online = false;
  p.ws = null;
  ws.room = null;
  ws.player = null;

  fillFromQueue(r);
  broadcast(r);

  send(ws, { type: "left" });

  if (r.phase === "play") scheduleBot(r);
}

function handle(ws, data) {
  const action = data?.action;

  if (action === "leave") {
    leaveRoom(ws);
    return;
  }

  if (action === "create") {
    if (ws.room) return fail(ws, "กรุณาออกจากห้องเดิมก่อน");

    const p = makeHuman(data.name);
    const code = roomCode();

    const r = {
      code,
      players: [
        p,
        makeBot(1),
        makeBot(2),
        makeBot(3),
        makeBot(4)
      ],
      queue: [],
      phase: "lobby",
      round: 0,
      turn: 0,
      starter: 0,
      deck: [],
      last: null,
      actionTaken: false,
      champagneMoney: 0,
      ledger: [],
      log: [],
      result: null,
      showdown: null,
      vote: null,
      pendingShuffle: false,
      botTimer: null
    };

    rooms.set(code, r);
    attach(r, p, ws);
    return;
  }

  if (action === "join") {
    if (ws.room) return fail(ws, "กรุณาออกจากห้องเดิมก่อน");

    const code = String(data.code || "").trim().toUpperCase();
    const r = rooms.get(code);

    if (!r) return fail(ws, "ไม่พบรหัสห้อง");

    const token = String(data.token || "");

    let p = token
      ? r.players.find(x => !x.bot && x.token === token)
      : null;

    if (!p && token) {
      p = r.queue.find(x => x.token === token);
    }

    if (p) {
      attach(r, p, ws);
      return;
    }

    p = makeHuman(data.name);

    const seat = r.players.findIndex(x => x.bot);

    if (seat >= 0) {
      const old = r.players[seat];

      transferSeat(old, p);
      r.players[seat] = p;

      attach(r, p, ws);
      log(r, `${p.name} เข้ามาแทน ${old.name}`);
    } else {
      p.ws = ws;
      ws.room = r;
      ws.player = p;
      r.queue.push(p);

      send(ws, {
        type: "identity",
        code,
        token: p.token
      });

      broadcast(r);
    }

    if (r.phase === "play") scheduleBot(r);
    return;
  }

  const r = ws.room;
  const p = ws.player;

  if (!r || !p) return fail(ws, "กรุณาเข้าห้องก่อน");

  if (!r.players.includes(p)) {
    return fail(ws, "คุณกำลังอยู่ในคิวรอ");
  }

  let ok = false;

  switch (action) {
    case "start":
      if (r.phase === "lobby") ok = beginRound(r);
      break;

    case "next":
      if (r.phase === "end") ok = beginRound(r);
      break;

    case "draw":
      ok = drawCard(r, p);
      break;

    case "follow":
      ok = followCards(r, p, data.cards);
      break;

    case "discard":
      ok = discardCards(r, p, data.cards);
      break;

    case "kang":
      ok = callKang(r, p);
      break;

    case "credit": {
      const amount = Number(data.amount);

      if ([100, 200, 300, 400, 500].includes(amount)) {
        p.credits += amount;
        entry(r, "ระบบ", p.name, amount, "เติมเครดิตจำลอง");
        broadcast(r);
        ok = true;
      }
      break;
    }

    case "chat": {
      const text = String(data.text || "").trim().slice(0, 250);

      if (text) {
        log(r, `💬 ${p.name}: ${text}`);
        broadcast(r);
        ok = true;
      }
      break;
    }

    case "requestSeats":
      ok = requestShuffle(r, p);
      break;

    case "approveSeats":
      if (r.vote?.required.includes(p.id)) {
        r.vote.approvals.add(p.id);

        if (r.vote.approvals.size === r.vote.required.length) {
          r.pendingShuffle = true;
          r.vote = null;
          log(r, "ทุกคนอนุมัติจับที่นั่งใหม่");
        }

        broadcast(r);
        ok = true;
      }
      break;

    case "rejectSeats":
      if (r.vote?.required.includes(p.id)) {
        r.vote = null;
        log(r, `${p.name} ปฏิเสธการจับที่นั่งใหม่`);
        broadcast(r);
        ok = true;
      }
      break;
  }

  if (!ok) {
    fail(ws, "ทำรายการไม่ได้ กรุณาตรวจสอบกติกาหรือตาเล่น");
  }
}

const server = http.createServer(async (req, res) => {
  if (req.url === "/" || req.url === "/index.html") {
    try {
      const html = await readFile(
        new URL("./index.html", import.meta.url)
      );

      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store"
      });

      res.end(html);
    } catch {
      res.writeHead(500);
      res.end("index.html not found");
    }
    return;
  }

  if (req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json"
    });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server });

wss.on("connection", ws => {
  ws.on("message", raw => {
    try {
      if (raw.length > 20000) {
        return fail(ws, "ข้อมูลยาวเกินไป");
      }

      handle(ws, JSON.parse(raw.toString()));
    } catch {
      fail(ws, "รูปแบบข้อมูลไม่ถูกต้อง");
    }
  });

  ws.on("close", () => {
    const r = ws.room;
    const p = ws.player;

    if (!r || !p || p.ws !== ws) return;

    p.online = false;
    p.ws = null;

    log(r, `${p.name} ออฟไลน์`);
    broadcast(r);

    // ผู้เล่นหลุดยังมีสิทธิ์กลับเข้าที่เดิมด้วย token
    // ถ้ากำลังถึงตาผู้เล่นที่หลุด เกมรอให้กลับมา
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`KANG ONLINE V12.1 running on port ${PORT}`);
});
