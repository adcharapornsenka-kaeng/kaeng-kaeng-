import http from 'node:http';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();
const MAX_PLAYERS = 5;
const INITIAL_POINTS = 100;
const BOT_DELAY = 700;

const server = http.createServer((req, res) => {
  const path = new URL(
    req.url || '/',
    'http://localhost'
  ).pathname;

  if (path === '/' || path === '/index.html') {
    try {
      const html = fs.readFileSync(
        new URL('./index.html', import.meta.url)
      );

      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store'
      });

      res.end(html);
    } catch {
      res.writeHead(500);
      res.end('index.html unavailable');
    }
  } else if (path === '/health') {
    res.writeHead(200, {
      'Content-Type': 'application/json'
    });
    res.end(JSON.stringify({ ok: true }));
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

const wss = new WebSocketServer({ server });

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

const rank = c => c % 13;
const suit = c => Math.floor(c / 13);
const value = c => Math.min(rank(c) + 1, 10);

const points = hand =>
  hand.reduce((sum, c) => sum + value(c), 0);

const aces = hand =>
  hand.filter(c => rank(c) === 0).length;

const size = room => room.players.length;
const name = (room, i) => room.players[i].name;

function shuffle() {
  const cards = Array.from(
    { length: 52 },
    (_, i) => i
  );

  for (let i = cards.length - 1; i > 0; i--) {
    const j = crypto.randomInt(i + 1);

    [cards[i], cards[j]] = [
      cards[j],
      cards[i]
    ];
  }

  return cards;
}

function special(hand) {
  if (hand.length !== 5) return null;

  const ranks = hand
    .map(rank)
    .sort((a, b) => a - b);

  const counts = [...new Set(ranks)]
    .map(r =>
      ranks.filter(x => x === r).length
    )
    .sort((a, b) => b - a);

  const flush = hand.every(
    c => suit(c) === suit(hand[0])
  );

  const straight =
    new Set(ranks).size === 5 &&
    (
      ranks[4] - ranks[0] === 4 ||
      ranks.join(',') === '0,9,10,11,12'
    );

  const options = [];

  if (flush && straight) {
    options.push(['เรียงสี', 60]);
  }

  if (counts[0] === 4) {
    options.push(['โฟร์', 50]);
  }

  if (counts[0] === 3 && counts[1] === 2) {
    options.push(['ฟูลเฮาส์', 40]);
  }

  if (counts[0] === 3) {
    options.push(['ตอง', 20]);
  }

  if (flush) {
    options.push(['สี', 20]);
  }

  if (points(hand) === 50) {
    options.push(['เซียน 50 แต้ม', 20]);
  }

  if (straight) {
    options.push(['เรียง', 20]);
  }

  return options.sort(
    (a, b) => b[1] - a[1]
  )[0] || null;
}

function newRoomCode() {
  let code;

  do {
    code = crypto.randomBytes(3)
      .toString('hex')
      .toUpperCase();
  } while (rooms.has(code));

  return code;
}

function note(room, message) {
  room.log.push(message);
  room.log = room.log.slice(-40);
}

function broadcast(room) {
  room.players.forEach((player, seat) => {
    if (player.bot) return;

    send(player.ws, {
      type: 'state',
      state: {
        code: room.code,
        seat,

        players: room.players.map((p, i) => ({
          name: p.name,
          bot: p.bot,
          online:
            p.bot ||
            p.ws?.readyState === WebSocket.OPEN,
          cardCount:
            room.hands[i]?.length || 0,
          discards:
            room.discards[i] || [],
          credits: room.scores[i]
        })),

        round: room.round,
        phase: room.phase,
        turn: room.turn,
        first: room.first,
        deckCount: room.deck.length,
        hand: room.hands[seat] || [],
        last: room.last,
        drawn: room.drawn,

        ledger: room.ledger.slice(-25),
        log: room.log.slice(-12),

        result: room.result,

        showdown:
          room.phase === 'end'
            ? room.hands
            : null,

        scoreOnly: true
      }
    });
  });
}

function transfer(room, from, to, amount, reason) {
  if (
    !Number.isInteger(amount) ||
    amount <= 0 ||
    from === to
  ) {
    return;
  }

  room.scores[from] -= amount;
  room.scores[to] += amount;

  room.ledger.push({
    round: room.round,
    from: name(room, from),
    to: name(room, to),
    amount,
    reason
  });

  room.ledger = room.ledger.slice(-150);
}

function pay(room, winner, amount, reason) {
  for (let i = 0; i < size(room); i++) {
    if (i !== winner) {
      transfer(
        room,
        i,
        winner,
        amount,
        reason
      );
    }
  }
}

function chooseWinner(room, candidates) {
  for (
    let offset = 0;
    offset < size(room);
    offset++
  ) {
    const i =
      (room.first + offset) % size(room);

    if (candidates.includes(i)) {
      return i;
    }
  }

  return candidates[0] ?? null;
}

function endRound(room, mode, caller = null) {
  if (room.phase !== 'play') return;

  room.phase = 'end';
  room.drawn = false;

  let winner = null;
  let message = '';

  if (mode === 'special') {
    const found = room.hands
      .map((hand, i) => ({
        i,
        sp: special(hand)
      }))
      .filter(x => x.sp);

    const highest = Math.max(
      ...found.map(x => x.sp[1])
    );

    const best = found.filter(
      x => x.sp[1] === highest
    );

    if (best.length === 1) {
      winner = best[0].i;

      pay(
        room,
        winner,
        highest,
        'ไพ่พิเศษ'
      );

      message =
        `${name(room, winner)} ` +
        `ชนะไพ่พิเศษ ${best[0].sp[0]} ` +
        `ได้ ${highest * (size(room) - 1)} คะแนน`;
    } else {
      message =
        'ไพ่พิเศษเสมอ ไม่มีการโอนคะแนน';
    }
  }

  else if (mode === 'kang') {
    const opponents = room.players
      .map((_, i) => i)
      .filter(i => i !== caller);

    const lowest = Math.min(
      ...opponents.map(
        i => points(room.hands[i])
      )
    );

    if (
      points(room.hands[caller]) <= lowest
    ) {
      winner = caller;

      const amount =
        10 + 10 * aces(room.hands[caller]);

      pay(
        room,
        winner,
        amount,
        'แคงสำเร็จ'
      );

      message =
        `${name(room, winner)} ` +
        `แคงสำเร็จ ได้ ` +
        `${amount * (size(room) - 1)} คะแนน`;
    } else {
      winner = chooseWinner(
        room,
        opponents.filter(
          i =>
            points(room.hands[i]) === lowest
        )
      );

      const amount =
        50 + 10 * aces(room.hands[winner]);

      transfer(
        room,
        caller,
        winner,
        amount,
        'แคงไม่สำเร็จ'
      );

      message =
        `${name(room, caller)} ` +
        `แคงไม่สำเร็จ เสีย ${amount} คะแนน`;
    }
  }

  else if (mode === 'deck') {
    const lowest = Math.min(
      ...room.hands.map(points)
    );

    winner = chooseWinner(
      room,
      room.hands
        .map((hand, i) =>
          points(hand) === lowest ? i : -1
        )
        .filter(i => i >= 0)
    );

    message =
      'ไพ่จั่วหมด ไม่มีคะแนนเพิ่ม';
  }

  room.result = {
    winner,
    mode,
    message
  };

  note(room, message);
  broadcast(room);
}

function scheduleBot(room) {
  if (
    room.phase !== 'play' ||
    !room.players[room.turn]?.bot ||
    room.botScheduled
  ) {
    return;
  }

  room.botScheduled = true;

  setTimeout(() => {
    room.botScheduled = false;

    if (
      room.phase !== 'play' ||
      !room.players[room.turn]?.bot
    ) {
      return;
    }

    try {
      botMove(room);
    } catch (error) {
      note(
        room,
        'Bot error: ' + error.message
      );

      broadcast(room);
    }
  }, BOT_DELAY);
}

function update(room) {
  broadcast(room);
  scheduleBot(room);
}

function deal(room) {
  room.deck = shuffle();

  room.hands = Array.from(
    { length: size(room) },
    () => []
  );

  room.discards = Array.from(
    { length: size(room) },
    () => []
  );

  room.last = null;
  room.drawn = false;
  room.result = null;

  for (let n = 0; n < 5; n++) {
    for (
      let i = 0;
      i < size(room);
      i++
    ) {
      room.hands[i].push(
        room.deck.pop()
      );
    }
  }

  room.round++;
  room.turn = room.first;
  room.phase = 'play';

  note(
    room,
    `เริ่มรอบ ${room.round}`
  );

  if (
    room.hands.some(hand => special(hand))
  ) {
    endRound(room, 'special');
  } else {
    update(room);
  }
}

function advance(room) {
  room.drawn = false;

  room.turn =
    (room.turn + 1) % size(room);

  if (room.deck.length === 0) {
    endRound(room, 'deck');
  } else {
    update(room);
  }
}

function discard(room, seat, cards, kind) {
  const hand = room.hands[seat];

  const valid =
    Array.isArray(cards) &&
    cards.length >= 1 &&
    cards.length <= 3 &&
    new Set(cards).size === cards.length &&
    cards.every(
      c =>
        Number.isInteger(c) &&
        hand.includes(c)
    ) &&
    cards.every(
      c => rank(c) === rank(cards[0])
    );

  if (!valid) {
    throw Error(
      'เลือกไพ่หน้าเดียวกัน 1–3 ใบ'
    );
  }

  if (kind === 'follow') {
    if (
      room.drawn ||
      !room.last ||
      room.last.player !==
        (seat + size(room) - 1) % size(room) ||
      rank(cards[0]) !==
        rank(room.last.cards[0])
    ) {
      throw Error(
        'ตามไพ่ผู้เล่นก่อนหน้าไม่ได้'
      );
    }
  } else if (!room.drawn) {
    throw Error('ต้องจั่วก่อนทิ้ง');
  }

  cards.forEach(c => {
    hand.splice(
      hand.indexOf(c),
      1
    );
  });

  room.discards[seat].push(...cards);

  room.last = {
    player: seat,
    cards
  };

  note(
    room,
    `${name(room, seat)} ทิ้งไพ่`
  );

  advance(room);
}

function draw(room, seat) {
  if (
    room.drawn ||
    !room.deck.length
  ) {
    throw Error('จั่วไม่ได้');
  }

  const card = room.deck.pop();

  room.hands[seat].push(card);
  room.drawn = true;

  const matching =
    room.hands[seat].filter(
      c => rank(c) === rank(card)
    );

  if (matching.length >= 3) {
    const triple = matching.slice(0, 3);

    triple.forEach(c => {
      room.hands[seat].splice(
        room.hands[seat].indexOf(c),
        1
      );
    });

    room.discards[seat].push(...triple);

    room.last = {
      player: seat,
      cards: triple
    };

    pay(
      room,
      seat,
      10,
      'ตอง'
    );

    note(
      room,
      `${name(room, seat)} ได้ตอง ` +
      `+${10 * (size(room) - 1)} คะแนน`
    );

    advance(room);
  } else {
    update(room);
  }
}

function botMove(room) {
  const seat = room.turn;
  const hand = room.hands[seat];

  if (!room.drawn) {
    const others = room.hands.filter(
      (_, i) => i !== seat
    );

    const minOther = Math.min(
      ...others.map(points)
    );

    if (
      points(hand) <= 8 &&
      points(hand) <= minOther &&
      crypto.randomInt(100) < 65
    ) {
      endRound(room, 'kang', seat);
      return;
    }

    if (
      room.last &&
      room.last.player ===
        (seat + size(room) - 1) % size(room)
    ) {
      const matches = hand.filter(
        c =>
          rank(c) ===
          rank(room.last.cards[0])
      );

      if (
        matches.length &&
        crypto.randomInt(100) < 60
      ) {
        discard(
          room,
          seat,
          matches.slice(0, 3),
          'follow'
        );

        return;
      }
    }

    draw(room, seat);

    if (
      room.phase !== 'play' ||
      room.turn !== seat ||
      !room.drawn
    ) {
      return;
    }
  }

  const sorted = [
    ...room.hands[seat]
  ].sort(
    (a, b) => value(b) - value(a)
  );

  discard(
    room,
    seat,
    [sorted[0]],
    'discard'
  );
}

function startGame(room) {
  if (
    room.phase !== 'lobby' ||
    room.round !== 0
  ) {
    throw Error('เกมเริ่มไปแล้ว');
  }

  if (
    room.players.length < 1 ||
    room.players.length > MAX_PLAYERS
  ) {
    throw Error(
      'จำนวนผู้เล่นไม่ถูกต้อง'
    );
  }

  const humans = room.players.length;

  for (
    let i = humans;
    i < MAX_PLAYERS;
    i++
  ) {
    room.players.push({
      name: `Bot ${i - humans + 1}`,
      bot: true,
      ws: null,
      token: null
    });

    room.scores.push(
      INITIAL_POINTS
    );
  }

  const cards = shuffle().slice(
    0,
    size(room)
  );

  const priority = [
    14, 2, 3, 4, 5, 6, 7,
    8, 9, 10, 12, 11, 13
  ];

  room.first = 0;

  for (
    let i = 1;
    i < size(room);
    i++
  ) {
    const current = cards[i];
    const best = cards[room.first];

    if (
      priority[rank(current)] >
        priority[rank(best)] ||
      (
        priority[rank(current)] ===
          priority[rank(best)] &&
        suit(current) < suit(best)
      )
    ) {
      room.first = i;
    }
  }

  deal(room);
}

function handle(ws, data) {
  if (
    !data ||
    typeof data !== 'object'
  ) {
    throw Error('คำสั่งไม่ถูกต้อง');
  }

  const { action } = data;

  if (action === 'create') {
    if (ws.room) {
      throw Error(
        'คุณอยู่ในห้องแล้ว'
      );
    }

    const roomCode = newRoomCode();

    const token = crypto
      .randomBytes(20)
      .toString('hex');

    const room = {
      code: roomCode,
      players: [],
      scores: [INITIAL_POINTS],
      round: 0,
      phase: 'lobby',
      first: 0,
      turn: 0,
      deck: [],
      hands: [],
      discards: [],
      ledger: [],
      last: null,
      drawn: false,
      result: null,
      log: [],
      botScheduled: false
    };

    room.players.push({
      name:
        String(data.name || 'ผู้เล่น 1')
          .trim()
          .slice(0, 24) ||
        'ผู้เล่น 1',
      token,
      ws,
      bot: false
    });

    rooms.set(roomCode, room);

    ws.room = roomCode;
    ws.token = token;

    send(ws, {
      type: 'joined',
      code: roomCode,
      token
    });

    broadcast(room);
    return;
  }

  if (action === 'join') {
    const roomCode = String(
      data.code || ''
    ).trim().toUpperCase();

    const room = rooms.get(roomCode);

    if (!room) {
      throw Error('ไม่พบห้อง');
    }

    const existing = room.players.find(
      p =>
        !p.bot &&
        data.token &&
        p.token === data.token
    );

    if (existing) {
      if (
        existing.ws &&
        existing.ws !== ws
      ) {
        existing.ws.close();
      }

      existing.ws = ws;

      ws.room = roomCode;
      ws.token = existing.token;

      send(ws, {
        type: 'joined',
        code: roomCode,
        token: existing.token
      });

      broadcast(room);
      return;
    }

    if (ws.room) {
      throw Error(
        'คุณอยู่ในห้องแล้ว'
      );
    }

    if (
      room.phase !== 'lobby' ||
      room.players.length >= MAX_PLAYERS
    ) {
      throw Error(
        'ห้องเต็มหรือเริ่มเกมแล้ว'
      );
    }

    const token = crypto
      .randomBytes(20)
      .toString('hex');

    room.players.push({
      name:
        String(
          data.name ||
          `ผู้เล่น ${room.players.length + 1}`
        ).trim().slice(0, 24) ||
        'ผู้เล่น',
      token,
      ws,
      bot: false
    });

    room.scores.push(
      INITIAL_POINTS
    );

    ws.room = roomCode;
    ws.token = token;

    send(ws, {
      type: 'joined',
      code: roomCode,
      token
    });

    broadcast(room);
    return;
  }

  const room = rooms.get(ws.room);

  const seat = room?.players.findIndex(
    p =>
      !p.bot &&
      p.token === ws.token &&
      p.ws === ws
  );

  if (!room || seat < 0) {
    throw Error(
      'กรุณาเข้าห้องก่อน'
    );
  }

  if (action === 'chat') {
    const message = String(
      data.message || ''
    ).trim().slice(0, 180);

    if (message) {
      note(
        room,
        `${name(room, seat)}: ${message}`
      );

      broadcast(room);
    }

    return;
  }

  if (action === 'start') {
    if (seat !== 0) {
      throw Error(
        'เจ้าของห้องเท่านั้นที่เริ่มเกมได้'
      );
    }

    startGame(room);
    return;
  }

  if (action === 'next') {
    if (room.phase !== 'end') {
      throw Error(
        'ยังเริ่มรอบใหม่ไม่ได้'
      );
    }

    if (seat !== 0) {
      throw Error(
        'เจ้าของห้องเท่านั้นที่เริ่มรอบใหม่ได้'
      );
    }

    room.first =
      room.result?.winner ??
      (room.first + 1) % size(room);

    deal(room);
    return;
  }

  if (
    room.phase !== 'play' ||
    room.turn !== seat
  ) {
    throw Error(
      'ยังไม่ถึงตาของคุณ'
    );
  }

  if (action === 'kang') {
    if (room.drawn) {
      throw Error(
        'แคงได้ก่อนจั่วเท่านั้น'
      );
    }

    endRound(
      room,
      'kang',
      seat
    );

    return;
  }

  if (action === 'draw') {
    draw(room, seat);
    return;
  }

  if (
    action === 'discard' ||
    action === 'follow'
  ) {
    discard(
      room,
      seat,
      data.cards,
      action
    );

    return;
  }

  throw Error(
    'คำสั่งไม่ถูกต้อง'
  );
}

wss.on('connection', ws => {
  ws.on('message', raw => {
    try {
      if (raw.length > 8192) {
        throw Error(
          'ข้อความยาวเกินไป'
        );
      }

      handle(
        ws,
        JSON.parse(raw.toString())
      );
    } catch (error) {
      send(ws, {
        type: 'error',
        message: error.message
      });
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);

    const player = room?.players.find(
      p =>
        !p.bot &&
        p.token === ws.token
    );

    if (player?.ws === ws) {
      player.ws = null;
      broadcast(room);
    }
  });
});

server.listen(PORT, () => {
  console.log(
    `KANG ONLINE running on port ${PORT}`
  );
});
