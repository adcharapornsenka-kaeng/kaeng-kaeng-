import http from "node:http";
import { readFile } from "node:fs/promises";
import { randomBytes, scryptSync, timingSafeEqual, createHash } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import pg from "pg";

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) throw new Error("DATABASE_URL is required");

const db = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DB_SSL === "false" ? false : {
    rejectUnauthorized: false
  }
});

const rooms = new Map();
const onlineMembers = new Map();
const CREDIT_OPTIONS = [100, 200, 300, 400, 500];
const uid = () => randomBytes(24).toString("hex");
const hash = x => createHash("sha256").update(x).digest("hex");

const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const points = c => Math.min(rank(c), 10);
const score = hand => hand.reduce((n, c) => n + points(c), 0);

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function initDatabase() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS members (
      id BIGSERIAL PRIMARY KEY,
      username VARCHAR(24) UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      credits BIGINT NOT NULL DEFAULT 0,
      wins INTEGER NOT NULL DEFAULT 0,
      losses INTEGER NOT NULL DEFAULT 0,
      games INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db.query(`
    ALTER TABLE members
    ALTER COLUMN credits SET DEFAULT 0
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      token_hash TEXT PRIMARY KEY,
      member_id BIGINT REFERENCES members(id) ON DELETE CASCADE,
      expires_at TIMESTAMPTZ NOT NULL
    )
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS match_history (
      id BIGSERIAL PRIMARY KEY,
      member_id BIGINT REFERENCES members(id) ON DELETE CASCADE,
      room_code VARCHAR(12) NOT NULL,
      round_number INTEGER NOT NULL,
      outcome VARCHAR(12) NOT NULL,
      credit_change BIGINT NOT NULL,
      reason TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS kang_unique_round
    ON match_history(member_id, room_code, round_number)
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS credit_history (
      id BIGSERIAL PRIMARY KEY,
      member_id BIGINT REFERENCES members(id) ON DELETE CASCADE,
      amount BIGINT NOT NULL,
      reason TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

function passwordHash(password) {
  const salt = randomBytes(16).toString("hex");
  return salt + ":" + scryptSync(password, salt, 64).toString("hex");
}

function checkPassword(password, stored) {
  const [salt, hex] = String(stored).split(":");
  if (!salt || !hex) return false;
  const a = scryptSync(password, salt, 64);
  const b = Buffer.from(hex, "hex");
  return a.length === b.length && timingSafeEqual(a, b);
}

function send(ws, message) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function fail(ws, message) {
  send(ws, { type: "error", message });
}

function makePlayer(name, bot = false, memberId = null) {
  return {
    id: uid(),
    token: uid(),
    name: String(name || "Player").slice(0, 24),
    bot,
    memberId,
    ws: null,
    online: bot,
    credits: 0,
    delta: 0,
    hand: [],
    discards: [],
    resetPending: false
  };
}

function makeRoom(mode, kind) {
  let code;

  do {
    code = randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));

  const room = {
    code,
    mode,
    kind,
    ownerId: null,
    players: Array.from({ length: 5 }, (_, i) =>
      kind === "bot" ? makePlayer("Bot " + (i + 1), true) : null
    ),
    phase: "lobby",
    round: 0,
    dealer: 0,
    turn: 0,
    deck: [],
    last: null,
    drawn: false,
    chain: false,
    followOrigin: null,
    followers: [],
    champagne: 0,
    ledger: [],
    chat: [],
    result: null,
    showdown: [],
    timer: null,
    timerVersion: 0,
    deadline: 0,
    helper: false,
    saveBusy: false,
    saveError: false,
    lastWinnerId: null,
    shuffleVotes: null,
    shuffleNext: false
  };

  rooms.set(code, room);
  return room;
}

function humans(room) {
  return room.players.filter(p => p && !p.bot);
}

function clearTimer(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.deadline = 0;
  room.timerVersion++;
  room.helper = false;
}

function setTimer(room, ms, callback) {
  clearTimeout(room.timer);
  const version = ++room.timerVersion;
  const seat = room.turn;
  room.deadline = Date.now() + ms;

  room.timer = setTimeout(() => {
    if (
      room.phase !== "play" ||
      room.turn !== seat ||
      room.timerVersion !== version
    ) return;

    Promise.resolve(callback()).catch(e => {
      console.error("Timer:", e);
      recoverTurn(room);
    });
  }, ms);
}

function clearChain(room) {
  room.chain = false;
  room.followOrigin = null;
  room.followers = [];
}

function eligibleFollow(room) {
  if (
    room.phase !== "play" ||
    !room.chain ||
    room.drawn ||
    !room.last?.cards?.length ||
    room.followers.length >= 4
  ) return [];

  if (room.last.seat !== (room.turn + 4) % 5) return [];

  return room.players[room.turn].hand.filter(
    c => rank(c) === rank(room.last.cards[0])
  );
}

function roomState(room, player) {
  const seat = room.players.indexOf(player);
  const canPlay =
    room.phase === "play" &&
    room.turn === seat &&
    !room.helper;

  return {
    type: "state",
    code: room.code,
    mode: room.mode,
    kind: room.kind,
    ownerId: room.ownerId,
    myId: player.id,
    phase: room.phase,
    round: room.round,
    seat,
    dealer: room.dealer,
    turn: room.turn,
    hand: [...player.hand],
    players: room.players.map(p => p ? {
      id: p.id,
      name: p.name,
      bot: p.bot,
      online: p.online,
      credits: p.credits,
      count: p.hand.length,
      discards: [...p.discards],
      resetPending: p.resetPending
    } : null),
    deck: room.deck.length,
    last: room.last,
    drawn: room.drawn,
    chain: room.chain,
    followOrigin: room.followOrigin,
    followers: [...room.followers],
    canFollow: canPlay && eligibleFollow(room).length > 0,
    followCards: canPlay ? eligibleFollow(room) : [],
    deadline: room.deadline,
    helper: room.helper,
    champagne: room.champagne,
    ledger: room.ledger.slice(-100),
    chat: room.chat.slice(-60),
    result: room.result,
    showdown: room.showdown,
    saveBusy: room.saveBusy,
    saveError: room.saveError,
    shuffleVotes: room.shuffleVotes
  };
}

function broadcast(room) {
  for (const p of humans(room)) {
    if (p.online) send(p.ws, roomState(room, p));
  }
}

function logCredit(room, from, to, amount, reason) {
  room.ledger.push({
    round: room.round,
    from,
    to,
    amount,
    reason
  });
  if (room.ledger.length > 250) room.ledger.shift();
}

function adjust(player, amount) {
  player.credits += amount;
  player.delta += amount;
}

function transfer(room, from, to, amount, reason) {
  if (!from || !to || from === to || amount <= 0) return;
  adjust(from, -amount);
  adjust(to, amount);
  logCredit(room, from.name, to.name, amount, reason);
}

function tripleBonus(room, player, cards) {
  if (cards.length !== 3) return;
  for (const p of room.players) {
    if (p !== player) {
      transfer(room, p, player, 10, "โบนัสตอง");
    }
  }
}

function classify(hand) {
  const counts = new Map();

  for (const c of hand) {
    counts.set(rank(c), (counts.get(rank(c)) || 0) + 1);
  }

  const groups = [...counts.values()].sort((a, b) => b - a);
  const nums = hand.map(rank).sort((a, b) => a - b);

  const flush = hand.length === 5 &&
    hand.every(c => suit(c) === suit(hand[0]));

  const straight = hand.length === 5 &&
    (
      nums.every((v, i) => !i || v === nums[i - 1] + 1) ||
      nums.join(",") === "1,10,11,12,13"
    );

  if (straight && flush) return ["สเตรทฟลัช", 60];
  if (groups[0] === 4) return ["โฟร์การ์ด", 50];
  if (groups[0] === 3 && groups[1] === 2) return ["ฟูลเฮาส์", 40];
  if (straight) return ["สเตรท", 20];
  if (flush) return ["ฟลัช", 20];
  if (score(hand) === 50) return ["50 แต้ม", 20];
  if (groups[0] === 3) return ["ตอง", 20];

  return ["ธรรมดา", 10];
}

function paymentRate(hand) {
  return classify(hand)[1] +
    hand.filter(c => rank(c) === 1).length * 10;
}

function lowestSeat(room, excluded = -1) {
  const seats = [0, 1, 2, 3, 4].filter(i => i !== excluded);
  return seats.reduce((a, b) =>
    score(room.players[b].hand) < score(room.players[a].hand)
      ? b : a
  );
}

async function profile(ws) {
  if (!ws.memberId) return;

  const [m, h, c] = await Promise.all([
    db.query(
      "SELECT id,username,credits,wins,losses,games FROM members WHERE id=$1",
      [ws.memberId]
    ),
    db.query(
      `SELECT room_code,round_number,outcome,credit_change,reason,created_at
       FROM match_history WHERE member_id=$1 ORDER BY id DESC LIMIT 30`,
      [ws.memberId]
    ),
    db.query(
      `SELECT amount,reason,created_at
       FROM credit_history WHERE member_id=$1 ORDER BY id DESC LIMIT 40`,
      [ws.memberId]
    )
  ]);

  send(ws, {
    type: "profile",
    member: m.rows[0],
    history: h.rows,
    creditHistory: c.rows
  });
}

async function updateMemberCredit(id, amount, reason, reset = false) {
  const client = await db.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      reset
        ? "UPDATE members SET credits=0 WHERE id=$1 RETURNING credits"
        : "UPDATE members SET credits=credits+$2 WHERE id=$1 RETURNING credits",
      reset ? [id] : [id, amount]
    );

    await client.query(
      `INSERT INTO credit_history(member_id,amount,reason)
       VALUES($1,$2,$3)`,
      [id, amount, reason]
    );

    await client.query("COMMIT");
    return Number(result.rows[0]?.credits ?? 0);
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function saveRound(room) {
  if (room.mode !== "member") return;

  const client = await db.connect();

  try {
    await client.query("BEGIN");

    for (let i = 0; i < 5; i++) {
      const p = room.players[i];
      if (!p?.memberId) continue;

      const won = i === room.result.winnerSeat;

      const inserted = await client.query(
        `INSERT INTO match_history
         (member_id,room_code,round_number,outcome,credit_change,reason)
         VALUES($1,$2,$3,$4,$5,$6)
         ON CONFLICT(member_id,room_code,round_number)
         DO NOTHING RETURNING id`,
        [
          p.memberId,
          room.code,
          room.round,
          won ? "win" : "loss",
          p.delta,
          room.result.reason
        ]
      );

      if (!inserted.rowCount) continue;

      await client.query(
        `UPDATE members
         SET credits=credits+$1,
             games=games+1,
             wins=wins+$2,
             losses=losses+$3
         WHERE id=$4`,
        [p.delta, won ? 1 : 0, won ? 0 : 1, p.memberId]
      );

      await client.query(
        `INSERT INTO credit_history(member_id,amount,reason)
         VALUES($1,$2,$3)`,
        [
          p.memberId,
          p.delta,
          `เกม ${room.code} รอบ ${room.round}`
        ]
      );
    }

    await client.query("COMMIT");

    for (const p of humans(room)) p.delta = 0;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function settle(room) {
  if (room.mode !== "member") return;

  room.saveBusy = true;
  room.saveError = false;
  broadcast(room);

  try {
    await saveRound(room);

    for (const p of humans(room)) {
      if (p.resetPending) {
        await updateMemberCredit(
          p.memberId,
          0,
          "รีเซ็ตเครดิตเป็น 0 หลังจบรอบ",
          true
        );
        p.credits = 0;
        p.delta = 0;
        p.resetPending = false;
      }
    }
  } catch (e) {
    console.error("Save failed:", e);
    room.saveError = true;
  } finally {
    room.saveBusy = false;
    broadcast(room);

    for (const p of humans(room)) {
      if (p.online) await profile(p.ws);
    }
  }
}

async function finishRound(
  room,
  winnerSeat,
  reason,
  failedSeat = null,
  followWin = false
) {
  if (room.phase !== "play") return;

  clearTimer(room);

  const winner = room.players[winnerSeat];
  const amount = paymentRate(winner.hand);
  const category = classify(winner.hand)[0];
  const failed = failedSeat !== null && failedSeat !== winnerSeat;
  const origin = room.followOrigin;

  if (failed) {
    transfer(
      room,
      room.players[failedSeat],
      winner,
      amount * 4,
      "โดนทับ"
    );
  } else {
    room.players.forEach((p, i) => {
      if (i !== winnerSeat) {
        transfer(room, p, winner, amount, reason);
      }
    });
  }

  let followBonus = 0;

  if (
    followWin &&
    Number.isInteger(origin) &&
    origin !== winnerSeat
  ) {
    transfer(
      room,
      room.players[origin],
      winner,
      10,
      "โบนัสไหลจนหมดมือ"
    );
    followBonus = 10;
  }

  adjust(winner, -2);
  room.champagne += 2;
  logCredit(room, winner.name, "Champagne", 2, "ค่าผู้ชนะ");

  room.showdown = room.players.map(p => ({
    name: p.name,
    hand: [...p.hand],
    points: score(p.hand)
  }));

  room.phase = "end";
  room.dealer = winnerSeat;
  room.lastWinnerId = winner.id;

  room.result = {
    winnerSeat,
    winnerName: winner.name,
    overlaidSeat: failed ? failedSeat : null,
    failed,
    reason,
    category,
    amount: failed ? amount * 4 : amount,
    followBonus,
    originName: followBonus ? room.players[origin].name : null,
    effectUntil: Date.now() + 5000
  };

  clearChain(room);
  broadcast(room);
  await settle(room);
}

function canAct(room, p, drawn, automatic = false) {
  return room.phase === "play" &&
    room.players[room.turn] === p &&
    room.drawn === drawn &&
    (automatic || !room.helper);
}

async function kang(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic) || room.chain) return;

  const seat = room.turn;
  const lower = room.players.some(
    (q, i) => i !== seat && score(q.hand) < score(p.hand)
  );

  await finishRound(
    room,
    lower ? lowestSeat(room, seat) : seat,
    lower ? "โดนทับ" : "แคงสำเร็จ",
    lower ? seat : null
  );
}

function draw(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic) || room.chain) return false;
  if (!room.deck.length) return false;

  p.hand.push(room.deck.pop());
  room.drawn = true;

  if (!automatic) scheduleTurn(room);
  broadcast(room);
  return true;
}

function finishChain(room) {
  clearChain(room);

  if (!room.deck.length) {
    void finishRound(room, lowestSeat(room), "กองจั่วหมด");
  } else {
    scheduleTurn(room);
  }

  broadcast(room);
}

async function follow(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic)) return;

  const cards = eligibleFollow(room);
  if (!cards.length) return;

  const seat = room.turn;

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }

  tripleBonus(room, p, cards);

  room.followers.push(seat);
  room.last = { seat, cards };

  if (!p.hand.length) {
    await finishRound(
      room, seat, "ไหลจนหมดมือ", null, true
    );
    return;
  }

  if (room.followers.length === 4) {
    const origin = room.players[room.followOrigin];

    for (const i of room.followers) {
      transfer(
        room,
        origin,
        room.players[i],
        10,
        "ไหลครบ 4 คน"
      );
    }

    room.turn = room.followOrigin;
    room.drawn = false;
    finishChain(room);
    return;
  }

  room.turn = (seat + 1) % 5;
  room.drawn = false;

  if (eligibleFollow(room).length) {
    scheduleTurn(room);
  } else {
    finishChain(room);
  }

  broadcast(room);
}

async function discard(room, p, cards, automatic = false) {
  if (!canAct(room, p, true, automatic) || room.chain) return;

  if (
    !Array.isArray(cards) ||
    !cards.length ||
    new Set(cards).size !== cards.length ||
    !cards.every(c => Number.isInteger(c) && p.hand.includes(c)) ||
    !cards.every(c => rank(c) === rank(cards[0]))
  ) {
    if (!automatic) fail(p.ws, "เลือกไพ่หน้าเดียวกันเท่านั้น");
    return;
  }

  const seat = room.turn;

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }

  tripleBonus(room, p, cards);
  room.last = { seat, cards };

  if (!p.hand.length) {
    clearChain(room);
    await finishRound(room, seat, "ทิ้งจนหมดมือ");
    return;
  }

  room.turn = (seat + 1) % 5;
  room.drawn = false;
  room.chain = true;
  room.followOrigin = seat;
  room.followers = [];

  if (eligibleFollow(room).length) {
    scheduleTurn(room);
  } else {
    finishChain(room);
  }

  broadcast(room);
}

function botDiscard(hand) {
  const groups = new Map();

  for (const c of hand) {
    const r = rank(c);
    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(c);
  }

  return [...groups.values()].sort(
    (a, b) => b.length - a.length ||
      points(b[0]) - points(a[0])
  )[0] || [];
}

function botAction(room) {
  const seat = room.turn;
  const p = room.players[seat];
  const helper = !p.bot;

  setTimer(room, 1800, async () => {
    if (room.players[seat] !== p) return;

    if (room.chain) {
      if (eligibleFollow(room).length) {
        await follow(room, p, true);
      } else {
        finishChain(room);
      }
      return;
    }

    if (!room.drawn) {
      if (!draw(room, p, true)) {
        await finishRound(room, lowestSeat(room), "กองจั่วหมด");
        return;
      }
    }

    setTimer(room, 1800, () =>
      discard(room, p, botDiscard(p.hand), true)
    );

    room.helper = helper;
    broadcast(room);
  });

  room.helper = helper;
  broadcast(room);
}

function recoverTurn(room) {
  if (room.phase !== "play") return;

  const p = room.players[room.turn];

  if (room.chain) {
    if (eligibleFollow(room).length) {
      void follow(room, p, true);
    } else {
      finishChain(room);
    }
  } else if (room.drawn) {
    void discard(room, p, botDiscard(p.hand), true);
  } else {
    botAction(room);
  }
}

function scheduleTurn(room) {
  clearTimer(room);

  if (room.phase !== "play") return;

  if (room.chain && !eligibleFollow(room).length) {
    finishChain(room);
    return;
  }

  const p = room.players[room.turn];

  if (p.bot) {
    botAction(room);
  } else {
    setTimer(
      room,
      p.online ? 90000 : 15000,
      () => botAction(room)
    );
    broadcast(room);
  }
}

function startRound(room, player) {
  if (
    room.phase === "play" ||
    room.saveBusy ||
    room.saveError
  ) return fail(player.ws, "ยังเริ่มรอบใหม่ไม่ได้");

  if (room.ownerId !== player.id) {
    return fail(player.ws, "เจ้าของห้องเท่านั้นที่เริ่มได้");
  }

  if (room.kind === "human" && humans(room).length !== 5) {
    return fail(
      player.ws,
      `รอผู้เล่นจริงครบ 5 คน ตอนนี้ ${humans(room).length}/5`
    );
  }

  const noCredit = humans(room).find(p => p.credits <= 0);

  if (noCredit) {
    return fail(
      player.ws,
      `${noCredit.name} ต้องเติมเครดิตก่อนเริ่ม`
    );
  }

  if (room.shuffleNext) {
    shuffle(room.players);
    room.shuffleNext = false;
  }

  if (room.lastWinnerId) {
    const seat = room.players.findIndex(
      p => p?.id === room.lastWinnerId
    );
    if (seat >= 0) room.dealer = seat;
  }

  room.round++;
  room.phase = "play";
  room.deck = shuffle(Array.from({ length: 52 }, (_, i) => i));
  room.turn = room.dealer;
  room.last = null;
  room.drawn = false;
  room.result = null;
  room.showdown = [];
  room.shuffleVotes = null;

  clearChain(room);

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
    p.delta = 0;
  }

  scheduleTurn(room);
  broadcast(room);
}

async function authenticate(ws, action, data) {
  if (action === "resume") {
    const token = String(data.token || "");

    if (!/^[a-f0-9]{48}$/.test(token)) {
      return fail(ws, "กรุณาเข้าสู่ระบบใหม่");
    }

    const r = await db.query(
      `SELECT m.id,m.username
       FROM sessions s JOIN members m ON m.id=s.member_id
       WHERE s.token_hash=$1 AND s.expires_at>NOW()`,
      [hash(token)]
    );

    if (!r.rowCount) {
      return fail(ws, "เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่");
    }

    ws.memberId = String(r.rows[0].id);
    ws.username = r.rows[0].username;

    send(ws, {
      type: "auth",
      username: ws.username,
      token
    });

    await profile(ws);
    return;
  }

  const username = String(data.username || "").trim();
  const password = String(data.password || "");

  if (
    !/^[A-Za-z0-9_]{3,24}$/.test(username) ||
    password.length < 8 ||
    password.length > 128
  ) {
    return fail(
      ws,
      "ชื่อผู้ใช้ 3–24 ตัว และรหัสผ่าน 8–128 ตัว"
    );
  }

  if (action === "register") {
    try {
      await db.query(
        `INSERT INTO members(username,password_hash,credits)
         VALUES($1,$2,0)`,
        [username, passwordHash(password)]
      );
    } catch (e) {
      if (e.code === "23505") {
        return fail(ws, "ชื่อผู้ใช้นี้มีแล้ว");
      }
      throw e;
    }
  }

  const r = await db.query(
    `SELECT id,username,password_hash
     FROM members WHERE username=$1`,
    [username]
  );

  const m = r.rows[0];

  if (!m || !checkPassword(password, m.password_hash)) {
    return fail(ws, "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง");
  }

  const token = uid();

  await db.query(
    `INSERT INTO sessions(token_hash,member_id,expires_at)
     VALUES($1,$2,NOW()+INTERVAL '30 days')`,
    [hash(token), m.id]
  );

  ws.memberId = String(m.id);
  ws.username = m.username;

  send(ws, {
    type: "auth",
    username,
    token
  });

  await profile(ws);
}

function attach(room, p, ws) {
  if (p.ws && p.ws !== ws) {
    const old = p.ws;
    p.ws = null;
    old.close();
  }

  p.online = true;
  p.ws = ws;
  ws.room = room;
  ws.player = p;

  if (p.memberId) {
    onlineMembers.set(String(p.memberId), room.code);
  }

  send(ws, {
    type: "identity",
    code: room.code,
    mode: room.mode,
    token: p.token
  });

  if (
    room.phase === "play" &&
    room.players[room.turn] === p
  ) scheduleTurn(room);

  broadcast(room);
}

function detach(ws, permanent = false) {
  const room = ws.room;
  const p = ws.player;

  if (!room || !p || p.ws !== ws) return;

  const seat = room.players.indexOf(p);

  p.online = false;
  p.ws = null;
  ws.room = null;
  ws.player = null;

  if (permanent && seat >= 0) {
    if (p.memberId) {
      onlineMembers.delete(String(p.memberId));
    }

    if (room.kind === "bot") {
      const bot = makePlayer("Bot " + (seat + 1), true);
      bot.credits = p.credits;
      bot.hand = [...p.hand];
      bot.discards = [...p.discards];
      room.players[seat] = bot;
    } else {
      room.players[seat] = null;
    }

    if (room.ownerId === p.id) {
      room.ownerId = humans(room)[0]?.id || null;
    }

    if (!humans(room).length) {
      clearTimer(room);
      rooms.delete(room.code);
      return;
    }
  }

  if (room.phase === "play" && room.turn === seat) {
    scheduleTurn(room);
  }

  broadcast(room);
}

async function joinRoom(ws, data) {
  const code = String(data.code || "").trim().toUpperCase();
  const room = rooms.get(code);

  if (!room) return fail(ws, "ไม่พบห้อง");

  if (room.mode === "member" && !ws.memberId) {
    return fail(ws, "กรุณาเข้าสู่ระบบสมาชิก");
  }

  if (room.mode === "guest" && ws.memberId) {
    return fail(ws, "กรุณาออกจากบัญชีสมาชิกก่อน");
  }

  const existing = room.players.find(p =>
    p && !p.bot &&
    (room.mode === "member"
      ? p.memberId === ws.memberId
      : p.token === data.token)
  );

  if (existing) {
    attach(room, existing, ws);
    return;
  }

  if (ws.memberId && onlineMembers.has(ws.memberId)) {
    return fail(ws, "บัญชีนี้อยู่ในห้องอื่นแล้ว");
  }

  if (
    room.phase === "play" ||
    room.saveBusy ||
    room.saveError
  ) {
    return fail(ws, "ต้องรอให้รอบนี้จบก่อนเข้าห้อง");
  }

  const seat = room.players.findIndex(p => !p || p.bot);

  if (seat < 0) return fail(ws, "ห้องเต็มแล้ว");

  const name = room.mode === "member"
    ? ws.username
    : String(data.name || "Guest").trim() || "Guest";

  const p = makePlayer(name, false, ws.memberId);

  if (p.memberId) {
    const r = await db.query(
      "SELECT credits FROM members WHERE id=$1",
      [p.memberId]
    );
    p.credits = Number(r.rows[0]?.credits ?? 0);
  }

  room.players[seat] = p;

  if (!room.ownerId) room.ownerId = p.id;

  attach(room, p, ws);
}

async function memberCredit(ws, action, data) {
  if (!ws.memberId) {
    return fail(ws, "กรุณาเข้าสู่ระบบ");
  }

  const room = ws.room;
  const p = ws.player;

  if (room?.saveBusy || room?.saveError) {
    return fail(ws, "กำลังบันทึกผล กรุณารอ");
  }

  if (action === "topup") {
    const amount = Number(data.amount);

    if (!CREDIT_OPTIONS.includes(amount)) {
      return fail(ws, "เติมได้ครั้งละ 100–500 เครดิต");
    }

    await updateMemberCredit(
      ws.memberId,
      amount,
      `เติมเครดิตจำลอง +${amount}`
    );

    if (p) {
      p.credits += amount;
      logCredit(room, "ระบบ", p.name, amount, "เติมเครดิต");
      broadcast(room);
    }

    await profile(ws);
    send(ws, {
      type: "notice",
      message: `เติม ${amount} เครดิตแล้ว`
    });
    return;
  }

  if (action === "reset") {
    if (p && room.phase === "play") {
      p.resetPending = true;
      broadcast(room);

      send(ws, {
        type: "notice",
        message: "เครดิตจะรีเซ็ตเป็น 0 หลังจบรอบ"
      });
      return;
    }

    await updateMemberCredit(
      ws.memberId,
      0,
      "รีเซ็ตเครดิตเป็น 0",
      true
    );

    if (p) {
      p.credits = 0;
      p.delta = 0;
      p.resetPending = false;
      broadcast(room);
    }

    await profile(ws);

    send(ws, {
      type: "notice",
      message: "รีเซ็ตเป็น 0 แล้ว กรุณาเติมเครดิตใหม่"
    });
  }
}

async function handle(ws, data) {
  if (!data || typeof data !== "object") return;

  const action = data.action;

  if (["login", "register", "resume"].includes(action)) {
    if (ws.room) return fail(ws, "กรุณาออกจากห้องก่อน");
    return authenticate(ws, action, data);
  }

  if (action === "profile") return profile(ws);

  if (action === "logout") {
    if (ws.room) return fail(ws, "กรุณาออกจากห้องก่อน");

    if (typeof data.token === "string") {
      await db.query(
        "DELETE FROM sessions WHERE token_hash=$1",
        [hash(data.token)]
      );
    }

    ws.memberId = null;
    ws.username = null;

    send(ws, { type: "loggedOut" });
    return;
  }

  if (action === "topup" || action === "reset") {
    return memberCredit(ws, action, data);
  }

  if (action === "create") {
    if (ws.room) {
      return fail(ws, "กรุณาออกจากห้องเดิมก่อน");
    }

    const mode = data.mode === "member" ? "member" : "guest";
    const kind = data.kind === "human" ? "human" : "bot";

    if (mode === "member" && !ws.memberId) {
      return fail(ws, "กรุณาเข้าสู่ระบบก่อน");
    }

    if (mode === "guest" && ws.memberId) {
      return fail(ws, "กรุณาออกจากสมาชิกก่อน");
    }

    if (mode === "guest" && kind === "human") {
      return fail(ws, "ห้องไม่มี Bot สำหรับสมาชิกเท่านั้น");
    }

    if (ws.memberId && onlineMembers.has(ws.memberId)) {
      return fail(ws, "บัญชีนี้อยู่ในห้องอื่นแล้ว");
    }

    const room = makeRoom(mode, kind);

    await joinRoom(ws, {
      code: room.code,
      name: data.name
    });
    return;
  }

  if (action === "join") {
    if (ws.room) return fail(ws, "กรุณาออกจากห้องก่อน");
    return joinRoom(ws, data);
  }

  if (action === "leave") {
    if (
      ws.room?.phase === "play" &&
      ws.room.mode === "member"
    ) {
      return fail(ws, "กรุณารอให้จบรอบก่อนออก");
    }

    detach(ws, true);
    send(ws, { type: "left" });
    return;
  }

  const room = ws.room;
  const p = ws.player;

  if (!room || !p || p.ws !== ws) {
    return fail(ws, "กรุณาเข้าห้องก่อน");
  }

  switch (action) {
    case "start":
    case "next":
      return startRound(room, p);

    case "kang":
      return kang(room, p);

    case "draw":
      if (!draw(room, p)) fail(ws, "จั่วไม่ได้");
      return;

    case "follow":
      if (!eligibleFollow(room).length) {
        return fail(ws, "ยังไหลไม่ได้");
      }
      return follow(room, p);

    case "discard":
      return discard(room, p, data.cards);

    case "chat": {
      const text = String(data.text || "").trim().slice(0, 300);

      if (text) {
        room.chat.push({ name: p.name, text });
        if (room.chat.length > 100) room.chat.shift();
        broadcast(room);
      }
      return;
    }

    case "guestCredit": {
      if (room.mode !== "guest") {
        return fail(ws, "ใช้ระบบเติมเครดิตสมาชิก");
      }

      const amount = Number(data.amount);

      if (CREDIT_OPTIONS.includes(amount)) {
        p.credits += amount;
        logCredit(
          room,
          "ระบบ",
          p.name,
          amount,
          "เติมเครดิต Guest"
        );
        broadcast(room);
      }
      return;
    }

    case "voteShuffle": {
      if (room.phase === "play") {
        return fail(ws, "ต้องรอจบรอบก่อน");
      }

      if (!room.shuffleVotes) {
        room.shuffleVotes = {
          approvals: [p.id],
          required: humans(room)
            .filter(x => x.online)
            .map(x => x.id)
        };
      } else if (!room.shuffleVotes.approvals.includes(p.id)) {
        room.shuffleVotes.approvals.push(p.id);
      }

      if (room.shuffleVotes.required.every(
        id => room.shuffleVotes.approvals.includes(id)
      )) {
        room.shuffleNext = true;
        room.shuffleVotes = null;
      }

      broadcast(room);
      return;
    }

    case "rejectShuffle":
      room.shuffleVotes = null;
      broadcast(room);
      return;

    case "retry":
      if (
        room.phase === "end" &&
        room.saveError &&
        !room.saveBusy
      ) return settle(room);
      return;

    default:
      return fail(ws, "ไม่รู้จักคำสั่ง");
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
    } catch (e) {
      console.error(e);
      res.writeHead(500);
      res.end("index.html missing");
    }
    return;
  }

  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("KANG ONLINE V14.4 OK");
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server });
let queue = Promise.resolve();

wss.on("connection", ws => {
  ws.memberId = null;
  ws.username = null;
  ws.room = null;
  ws.player = null;
  ws.alive = true;

  ws.on("pong", () => {
    ws.alive = true;
  });

  ws.on("message", raw => {
    queue = queue.then(async () => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        return fail(ws, "ข้อมูลไม่ถูกต้อง");
      }

      await handle(ws, data);
    }).catch(e => {
      console.error("Action error:", e);
      fail(ws, "ระบบขัดข้อง กรุณาลองใหม่");
    });
  });

  ws.on("close", () => detach(ws, false));
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) {
      ws.terminate();
      continue;
    }

    ws.alive = false;
    ws.ping();
  }
}, 15000);

await initDatabase();

server.listen(PORT, "0.0.0.0", () => {
  console.log("KANG ONLINE V14.4 listening on", PORT);
});
