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
const next = seat => (seat + 1) % 5;

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

function makeCode() {
  let code;
  do {
    code = randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));
  return code;
}

function human(name) {
  return {
    id: uid(),
    token: uid(),
    name: String(name || "Player").trim().slice(0, 32) || "Player",
    bot: false,
    online: true,
    credits: 0,
    hand: [],
    discards: [],
    ws: null
  };
}

function bot(number) {
  return {
    id: uid(),
    token: null,
    name: `BOT ${number}`,
    bot: true,
    online: true,
    credits: 0,
    hand: [],
    discards: [],
    ws: null
  };
}

function send(ws, message) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function error(ws, message) {
  send(ws, { type: "error", message });
}

function log(room, message) {
  room.log.push(message);
  if (room.log.length > 100) room.log.shift();
}

function addLedger(room, from, to, amount, reason) {
  room.ledger.push({
    round: room.round,
    from,
    to,
    amount,
    reason
  });

  if (room.ledger.length > 300) room.ledger.shift();
}

function pay(room, from, to, amount, reason) {
  if (!Number.isFinite(amount) || amount <= 0) return;

  from.credits -= amount;
  to.credits += amount;

  addLedger(
    room,
    from.name,
    to.name,
    amount,
    reason
  );
}

function category(hand) {
  const n = hand.length;
  const ranks = hand.map(rank);
  const suits = hand.map(suit);
  const counts = new Map();

  for (const r of ranks) {
    counts.set(r, (counts.get(r) || 0) + 1);
  }

  const groups = [...counts.values()].sort((a, b) => b - a);
  const unique = [...new Set(ranks)].sort((a, b) => a - b);

  const flush =
    n === 5 &&
    new Set(suits).size === 1;

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

  if (flush) {
    return { name: "สี", rate: 20 };
  }

  if (straight) {
    return { name: "เรียง", rate: 20 };
  }

  if (n === 5 && score(hand) === 50) {
    return { name: "เซียน 50 แต้ม", rate: 20 };
  }

  if (n === 3 && groups[0] === 3) {
    return { name: "ตอง", rate: 20 };
  }

  return { name: "ธรรมดา", rate: 10 };
}

function winningRate(hand) {
  return category(hand).rate + aces(hand) * 10;
}

function firstAfter(candidates, anchor) {
  for (let offset = 1; offset <= 5; offset++) {
    const seat = (anchor + offset) % 5;
    if (candidates.includes(seat)) return seat;
  }

  return candidates[0];
}

function publicState(room, player) {
  return {
    type: "state",
    code: room.code,
    seat: room.players.indexOf(player),
    phase: room.phase,
    round: room.round,
    turn: room.turn,
    starter: room.starter,
    dealer: room.dealer,
    deckCount: room.deck.length,
    hand: [...player.hand],
    players: room.players.map(p => ({
      id: p.id,
      name: p.name,
      bot: p.bot,
      online: p.online,
      credits: p.credits,
      cardCount: p.hand.length,
      discards: [...p.discards]
    })),
    last: room.last
      ? {
          player: room.last.player,
          cards: [...room.last.cards]
        }
      : null,
    actionTaken: room.actionTaken,
    champagneMoney: room.champagneMoney,
    ledger: room.ledger.slice(-60),
    log: room.log.slice(-45),
    result: room.result,
    showdown: room.showdown,
    vote: room.vote
      ? {
          requester: room.vote.requester,
          approvals: [...room.vote.approvals],
          required: room.vote.required
        }
      : null
  };
}

function broadcast(room) {
  for (const player of room.players) {
    if (!player.bot) {
      send(player.ws, publicState(room, player));
    }
  }

  room.queue.forEach((player, index) => {
    send(player.ws, {
      type: "queue",
      code: room.code,
      position: index + 1
    });
  });
}

function stopBot(room) {
  if (room.botTimer) clearTimeout(room.botTimer);
  room.botTimer = null;
}

function inheritSeat(oldPlayer, newPlayer) {
  newPlayer.id = oldPlayer.id;
  newPlayer.hand = [...oldPlayer.hand];
  newPlayer.discards = [...oldPlayer.discards];
  newPlayer.credits = oldPlayer.credits;
}

function attach(room, player, ws) {
  if (player.ws && player.ws !== ws) {
    try {
      player.ws.close();
    } catch {}
  }

  player.ws = ws;
  player.online = true;

  ws.room = room;
  ws.player = player;

  send(ws, {
    type: "identity",
    code: room.code,
    token: player.token
  });

  broadcast(room);
}

function promoteQueue(room) {
  while (room.queue.length) {
    const seat = room.players.findIndex(p => p.bot);
    if (seat < 0) break;

    const player = room.queue.shift();
    const previous = room.players[seat];

    inheritSeat(previous, player);
    room.players[seat] = player;

    attach(room, player, player.ws);
    log(room, `${player.name} เข้ามาแทน ${previous.name}`);
  }

  broadcast(room);
}

function finishRound(
  room,
  winnerSeat,
  reason,
  failedKang = false,
  caller = null
) {
  if (room.phase !== "play") return;

  stopBot(room);

  const winner = room.players[winnerSeat];

  room.showdown = room.players.map(p => ({
    name: p.name,
    hand: [...p.hand],
    points: score(p.hand),
    count: p.hand.length
  }));

  let amount;

  if (failedKang) {
    amount = (10 + aces(winner.hand) * 10) * 4;

    pay(
      room,
      caller,
      winner,
      amount,
      "แคงไม่สำเร็จ"
    );
  } else {
    amount = winningRate(winner.hand);

    for (const player of room.players) {
      if (player !== winner) {
        pay(room, player, winner, amount, reason);
      }
    }
  }

  winner.credits -= 2;
  room.champagneMoney += 2;

  addLedger(
    room,
    winner.name,
    "Champagne Pool",
    2,
    "Champagne Money"
  );

  // ผู้ชนะได้แจกไพ่และเล่นคนแรกในรอบถัดไป
  room.lastWinnerId = winner.id;
  room.dealer = winnerSeat;

  room.result = {
    winner: winnerSeat,
    winnerId: winner.id,
    name: winner.name,
    reason,
    failed: failedKang,
    amount,
    category: category(winner.hand).name,
    message: `🏆 ${winner.name} ชนะ — ${reason}`
  };

  room.phase = "end";
  room.actionTaken = false;

  log(room, room.result.message);
  log(
    room,
    `🃏 ${winner.name} จะเป็นผู้แจกไพ่และเล่นคนแรกในรอบถัดไป`
  );

  broadcast(room);
}

// กติกาล่าสุด:
// กองจั่วหมด = ผู้มีแต้มรวมต่ำที่สุดชนะ
// ไม่ใช้จำนวนไพ่ในมือเป็นตัวตัดสิน
function deckFinished(room, anchor) {
  if (room.phase !== "play") return;

  const lowestScore = Math.min(
    ...room.players.map(p => score(p.hand))
  );

  const candidates = room.players
    .map((p, i) => score(p.hand) === lowestScore ? i : -1)
    .filter(i => i >= 0);

  const winnerSeat =
    candidates.length === 1
      ? candidates[0]
      : firstAfter(candidates, anchor);

  finishRound(
    room,
    winnerSeat,
    "กองจั่วหมด — แต้มรวมต่ำที่สุด"
  );
}

function beginRound(room) {
  if (!["lobby", "end"].includes(room.phase)) {
    return false;
  }

  stopBot(room);

  if (room.pendingShuffle) {
    shuffle(room.players);
    room.pendingShuffle = false;
    log(room, "จับที่นั่งใหม่สำเร็จ");
  }

  room.vote = null;
  room.round++;
  room.phase = "play";

  room.deck = shuffle(
    Array.from({ length: 52 }, (_, i) => i)
  );

  for (const player of room.players) {
    player.hand = room.deck.splice(0, 5);
    player.discards = [];
  }

  let starter = -1;

  if (room.lastWinnerId) {
    starter = room.players.findIndex(
      p => p.id === room.lastWinnerId
    );
  }

  if (starter < 0) {
    starter = Math.floor(Math.random() * 5);
  }

  room.starter = starter;
  room.dealer = starter;
  room.turn = starter;
  room.last = null;
  room.actionTaken = false;
  room.result = null;
  room.showdown = null;

  log(
    room,
    `เริ่มรอบ ${room.round}: ${room.players[starter].name} แจกไพ่และเล่นก่อน`
  );

  broadcast(room);
  scheduleBot(room);

  return true;
}

function drawCard(room, player) {
  if (
    room.phase !== "play" ||
    room.players[room.turn] !== player ||
    room.actionTaken ||
    room.deck.length === 0
  ) {
    return false;
  }

  player.hand.push(room.deck.pop());
  room.actionTaken = true;

  log(room, `${player.name} จั่วไพ่ 1 ใบ`);
  broadcast(room);

  return true;
}

function followCards(room, player, cards) {
  if (
    room.phase !== "play" ||
    room.players[room.turn] !== player ||
    room.actionTaken ||
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length ||
    !cards.every(Number.isInteger)
  ) {
    return false;
  }

  const previousSeat = (room.turn + 4) % 5;

  if (
    !room.last ||
    room.last.player !== previousSeat ||
    !cards.every(c => room.last.cards.includes(c))
  ) {
    return false;
  }

  const previousPlayer = room.players[previousSeat];

  if (
    !cards.every(c => previousPlayer.discards.includes(c))
  ) {
    return false;
  }

  for (const card of cards) {
    previousPlayer.discards.splice(
      previousPlayer.discards.indexOf(card),
      1
    );

    room.last.cards.splice(
      room.last.cards.indexOf(card),
      1
    );

    player.hand.push(card);
  }

  room.actionTaken = true;

  log(room, `${player.name} ไหลไพ่ ${cards.length} ใบ`);
  broadcast(room);

  return true;
}

function discardCards(room, player, cards) {
  if (
    room.phase !== "play" ||
    room.players[room.turn] !== player ||
    !room.actionTaken ||
    !Array.isArray(cards) ||
    cards.length < 1 ||
    cards.length > 3 ||
    new Set(cards).size !== cards.length ||
    !cards.every(
      c => Number.isInteger(c) && player.hand.includes(c)
    )
  ) {
    return false;
  }

  if (
    cards.length > 1 &&
    new Set(cards.map(rank)).size !== 1
  ) {
    return false;
  }

  const discardingSeat = room.turn;

  for (const card of cards) {
    player.hand.splice(
      player.hand.indexOf(card),
      1
    );
  }

  player.discards.push(...cards);

  room.last = {
    player: discardingSeat,
    cards: [...cards]
  };

  if (cards.length === 3) {
    for (const other of room.players) {
      if (other !== player) {
        pay(
          room,
          other,
          player,
          10,
          "โบนัสทิ้งตอง"
        );
      }
    }

    log(
      room,
      `${player.name} ทิ้งตอง รับโบนัส 40 เครดิต`
    );
  } else {
    log(
      room,
      `${player.name} ทิ้งไพ่ ${cards.length} ใบ`
    );
  }

  // ไพ่หมดมือ = ชนะทันที
  if (player.hand.length === 0) {
    finishRound(
      room,
      discardingSeat,
      "ไพ่หมดมือ"
    );
    return true;
  }

  room.turn = next(room.turn);
  room.actionTaken = false;

  // เมื่อกองจั่วหมด ให้เทียบแต้มรวมในมือ
  if (room.deck.length === 0) {
    deckFinished(room, discardingSeat);
    return true;
  }

  broadcast(room);
  scheduleBot(room);

  return true;
}

function kang(room, player) {
  if (
    room.phase !== "play" ||
    room.players[room.turn] !== player ||
    room.actionTaken
  ) {
    return false;
  }

  const callerSeat = room.turn;
  const callerScore = score(player.hand);

  const lower = room.players
    .map((p, i) => ({
      seat: i,
      points: score(p.hand)
    }))
    .filter(
      x =>
        x.seat !== callerSeat &&
        x.points < callerScore
    );

  if (lower.length === 0) {
    finishRound(
      room,
      callerSeat,
      "แคงสำเร็จ"
    );
    return true;
  }

  const minimum = Math.min(
    ...lower.map(x => x.points)
  );

  const candidates = lower
    .filter(x => x.points === minimum)
    .map(x => x.seat);

  const winnerSeat = firstAfter(
    candidates,
    callerSeat
  );

  finishRound(
    room,
    winnerSeat,
    "แคงไม่สำเร็จ",
    true,
    player
  );

  return true;
}

function scheduleBot(room) {
  stopBot(room);

  if (room.phase !== "play") return;

  const player = room.players[room.turn];
  if (!player?.bot) return;

  room.botTimer = setTimeout(() => {
    if (
      room.phase !== "play" ||
      room.players[room.turn] !== player
    ) {
      return;
    }

    if (!room.actionTaken) {
      if (
        score(player.hand) <= 4 &&
        Math.random() < 0.6
      ) {
        kang(room, player);
        return;
      }

      const previousSeat = (room.turn + 4) % 5;

      const available =
        room.last?.player === previousSeat
          ? room.last.cards
          : [];

      const useful = available.filter(
        c => points(c) <= 4
      );

      if (
        useful.length &&
        Math.random() < 0.7
      ) {
        followCards(
          room,
          player,
          useful.slice(0, 3)
        );
      } else if (room.deck.length > 0) {
        drawCard(room, player);
      } else if (available.length) {
        followCards(
          room,
          player,
          [available[0]]
        );
      } else {
        deckFinished(room, previousSeat);
        return;
      }
    }

    if (
      room.phase !== "play" ||
      room.players[room.turn] !== player
    ) {
      return;
    }

    const groups = new Map();

    for (const card of player.hand) {
      const r = rank(card);

      if (!groups.has(r)) {
        groups.set(r, []);
      }

      groups.get(r).push(card);
    }

    const triple = [...groups.values()]
      .find(g => g.length >= 3);

    const pair = [...groups.values()]
      .find(g => g.length >= 2);

    const selected = triple
      ? triple.slice(0, 3)
      : pair
        ? pair.slice(0, 2)
        : [...player.hand]
            .sort((a, b) => points(b) - points(a))
            .slice(0, 1);

    discardCards(room, player, selected);
  }, 700);
}

function requestSeats(room, player) {
  if (room.vote) return false;

  const required = room.players
    .filter(p => !p.bot && p.online)
    .map(p => p.id);

  room.vote = {
    requester: player.name,
    approvals: new Set([player.id]),
    required
  };

  if (
    room.vote.approvals.size ===
    room.vote.required.length
  ) {
    room.pendingShuffle = true;
    room.vote = null;

    log(room, "อนุมัติจับที่นั่งใหม่แล้ว");
  }

  broadcast(room);
  return true;
}

function leaveRoom(ws) {
  const room = ws.room;
  const player = ws.player;

  if (!room || !player) {
    send(ws, { type: "left" });
    return;
  }

  stopBot(room);

  const queueIndex = room.queue.indexOf(player);

  if (queueIndex >= 0) {
    room.queue.splice(queueIndex, 1);
  }

  const seat = room.players.indexOf(player);

  if (seat >= 0) {
    const replacement = bot(seat + 1);

    inheritSeat(player, replacement);
    room.players[seat] = replacement;

    log(
      room,
      `${player.name} ออกจากห้อง — Bot รับช่วงต่อ`
    );
  }

  if (room.vote) {
    room.vote = null;
    log(room, "ยกเลิกการลงคะแนนจับที่นั่งใหม่");
  }

  player.online = false;
  player.ws = null;

  ws.room = null;
  ws.player = null;

  promoteQueue(room);
  broadcast(room);

  send(ws, { type: "left" });

  if (room.phase === "play") {
    scheduleBot(room);
  }
}

function handle(ws, data) {
  const action = data?.action;

  if (action === "leave") {
    leaveRoom(ws);
    return;
  }

  if (action === "create") {
    if (ws.room) {
      return error(
        ws,
        "กรุณาออกจากห้องเดิมก่อน"
      );
    }

    const player = human(data.name);

    const room = {
      code: makeCode(),
      players: [
        player,
        bot(1),
        bot(2),
        bot(3),
        bot(4)
      ],
      queue: [],
      phase: "lobby",
      round: 0,
      turn: 0,
      starter: 0,
      dealer: 0,
      lastWinnerId: null,
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

    rooms.set(room.code, room);
    attach(room, player, ws);
    return;
  }

  if (action === "join") {
    if (ws.room) {
      return error(
        ws,
        "กรุณาออกจากห้องเดิมก่อน"
      );
    }

    const roomCode = String(
      data.code || ""
    ).trim().toUpperCase();

    const room = rooms.get(roomCode);

    if (!room) {
      return error(ws, "ไม่พบรหัสห้อง");
    }

    const token = String(
      data.token || ""
    );

    let player = token
      ? room.players.find(
          p => !p.bot && p.token === token
        )
      : null;

    if (!player && token) {
      player = room.queue.find(
        p => p.token === token
      );
    }

    if (player) {
      attach(room, player, ws);
      return;
    }

    player = human(data.name);

    const seat = room.players.findIndex(
      p => p.bot
    );

    if (seat >= 0) {
      const old = room.players[seat];

      inheritSeat(old, player);
      room.players[seat] = player;

      attach(room, player, ws);

      log(
        room,
        `${player.name} เข้ามาแทน ${old.name}`
      );
    } else {
      player.ws = ws;
      ws.room = room;
      ws.player = player;

      room.queue.push(player);

      send(ws, {
        type: "identity",
        code: room.code,
        token: player.token
      });

      broadcast(room);
    }

    if (room.phase === "play") {
      scheduleBot(room);
    }

    return;
  }

  const room = ws.room;
  const player = ws.player;

  if (!room || !player) {
    return error(ws, "กรุณาเข้าห้องก่อน");
  }

  if (!room.players.includes(player)) {
    return error(ws, "คุณอยู่ในคิวรอ");
  }

  let success = false;

  switch (action) {
    case "start":
      if (room.phase === "lobby") {
        success = beginRound(room);
      }
      break;

    case "next":
      if (room.phase === "end") {
        success = beginRound(room);
      }
      break;

    case "draw":
      success = drawCard(room, player);
      break;

    case "follow":
      success = followCards(
        room,
        player,
        data.cards
      );
      break;

    case "discard":
      success = discardCards(
        room,
        player,
        data.cards
      );
      break;

    case "kang":
      success = kang(room, player);
      break;

    case "credit": {
      const amount = Number(data.amount);

      if (
        [100, 200, 300, 400, 500]
          .includes(amount)
      ) {
        player.credits += amount;

        addLedger(
          room,
          "ระบบ",
          player.name,
          amount,
          "เติมเครดิตจำลอง"
        );

        broadcast(room);
        success = true;
      }

      break;
    }

    case "chat": {
      const text = String(
        data.text || ""
      ).trim().slice(0, 250);

      if (text) {
        log(
          room,
          `💬 ${player.name}: ${text}`
        );

        broadcast(room);
        success = true;
      }

      break;
    }

    case "requestSeats":
      success = requestSeats(
        room,
        player
      );
      break;

    case "approveSeats":
      if (
        room.vote?.required.includes(
          player.id
        )
      ) {
        room.vote.approvals.add(
          player.id
        );

        if (
          room.vote.approvals.size ===
          room.vote.required.length
        ) {
          room.pendingShuffle = true;
          room.vote = null;

          log(
            room,
            "ทุกคนอนุมัติจับที่นั่งใหม่"
          );
        }

        broadcast(room);
        success = true;
      }
      break;

    case "rejectSeats":
      if (
        room.vote?.required.includes(
          player.id
        )
      ) {
        room.vote = null;

        log(
          room,
          `${player.name} ไม่อนุมัติจับที่นั่งใหม่`
        );

        broadcast(room);
        success = true;
      }
      break;
  }

  if (!success) {
    error(
      ws,
      "ทำรายการไม่ได้ กรุณาตรวจสอบกติกาหรือตาเล่น"
    );
  }
}

const server = http.createServer(
  async (req, res) => {
    if (
      req.url === "/" ||
      req.url === "/index.html"
    ) {
      try {
        const html = await readFile(
          new URL(
            "./index.html",
            import.meta.url
          )
        );

        res.writeHead(200, {
          "Content-Type":
            "text/html; charset=utf-8",
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

      res.end(
        JSON.stringify({ ok: true })
      );

      return;
    }

    res.writeHead(404);
    res.end("Not found");
  }
);

const wss = new WebSocketServer({
  server
});

wss.on("connection", ws => {
  ws.on("message", raw => {
    try {
      if (raw.length > 20000) {
        return error(
          ws,
          "ข้อมูลยาวเกินไป"
        );
      }

      const data = JSON.parse(
        raw.toString()
      );

      handle(ws, data);
    } catch {
      error(
        ws,
        "รูปแบบข้อมูลไม่ถูกต้อง"
      );
    }
  });

  ws.on("close", () => {
    const room = ws.room;
    const player = ws.player;

    if (
      !room ||
      !player ||
      player.ws !== ws
    ) {
      return;
    }

    player.online = false;
    player.ws = null;

    log(
      room,
      `${player.name} ออฟไลน์`
    );

    broadcast(room);
  });
});

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `KANG ONLINE V12.2 running on port ${PORT}`
    );
  }
);
