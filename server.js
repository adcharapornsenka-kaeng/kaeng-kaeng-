import http from "node:http";
import { readFile } from "node:fs/promises";
import {
  randomBytes,
  createHash,
  scrypt as scryptCallback,
  timingSafeEqual
} from "node:crypto";
import { promisify } from "node:util";
import { WebSocketServer, WebSocket } from "ws";
import pg from "pg";

const scrypt = promisify(scryptCallback);
const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is required");
}

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DB_SSL === "false"
    ? false
    : { rejectUnauthorized: false }
});

const rooms = new Map();
const presence = new Map();

const FOLLOW_BONUS = 10;
const HUMAN_TIMEOUT = 90000;
const BOT_DELAY = 2300;
const START_CREDITS = 0;

const uid = () => randomBytes(24).toString("hex");

const hashToken = token =>
  createHash("sha256").update(token).digest("hex");

const rank = card => card % 13 + 1;
const suit = card => Math.floor(card / 13);
const points = card => Math.min(rank(card), 10);

const score = hand =>
  hand.reduce((total, card) => total + points(card), 0);

const creditOptions = [100, 200, 300, 400, 500];

function shuffle(items) {
  for (let i = items.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
  return items;
}

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS members (
      id BIGSERIAL PRIMARY KEY,
      username VARCHAR(24) UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      credits BIGINT NOT NULL DEFAULT 0,
      wins INTEGER NOT NULL DEFAULT 0,
      losses INTEGER NOT NULL DEFAULT 0,
      games INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE members
    ALTER COLUMN credits SET DEFAULT 0
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      token_hash TEXT PRIMARY KEY,
      member_id BIGINT NOT NULL
        REFERENCES members(id) ON DELETE CASCADE,
      expires_at TIMESTAMPTZ NOT NULL
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS match_history (
      id BIGSERIAL PRIMARY KEY,
      member_id BIGINT NOT NULL
        REFERENCES members(id) ON DELETE CASCADE,
      room_code VARCHAR(12) NOT NULL,
      round_number INTEGER NOT NULL,
      outcome VARCHAR(12) NOT NULL,
      credit_change BIGINT NOT NULL,
      reason TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS
      match_history_unique_round
    ON match_history(member_id, room_code, round_number)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS credit_history (
      id BIGSERIAL PRIMARY KEY,
      member_id BIGINT NOT NULL
        REFERENCES members(id) ON DELETE CASCADE,
      amount BIGINT NOT NULL,
      reason TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function makePasswordHash(password) {
  const salt = randomBytes(16).toString("hex");
  const key = await scrypt(password, salt, 64);
  return salt + ":" + key.toString("hex");
}

async function checkPassword(password, stored) {
  const [salt, hex] = String(stored).split(":");
  if (!salt || !hex) return false;

  const expected = Buffer.from(hex, "hex");
  const actual = await scrypt(password, salt, 64);

  return expected.length === actual.length &&
    timingSafeEqual(expected, actual);
}

function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function error(ws, message) {
  send(ws, { type: "error", message });
}

function player(name, bot = false, memberId = null) {
  return {
    id: uid(),
    token: uid(),
    name: String(name || "Player").slice(0, 24),
    bot,
    memberId,
    ws: null,
    online: bot,
    credits: START_CREDITS,
    roundDelta: 0,
    hand: [],
    discards: [],
    resetPending: false
  };
}

function createRoom(mode) {
  let code;

  do {
    code = randomBytes(3)
      .toString("hex")
      .toUpperCase();
  } while (rooms.has(code));

  const room = {
    code,
    mode,
    players: Array.from(
      { length: 5 },
      (_, i) => player(`Bot ${i + 1}`, true)
    ),
    phase: "lobby",
    round: 0,
    dealer: 0,
    turn: 0,
    deck: [],
    last: null,
    actionTaken: false,

    followChain: false,
    followOriginSeat: null,
    followParticipants: [],
    followFourPaid: false,

    champagne: 0,
    ledger: [],
    chat: [],
    result: null,
    showdown: [],
    timer: null,
    timerVersion: 0,
    deadline: null,
    helperSeat: -1,
    botStage: "",
    lastWinnerId: null,
    pendingShuffle: false,
    vote: null,
    saving: false,
    saveError: false
  };

  rooms.set(code, room);
  return room;
}

function classify(hand) {
  const counts = new Map();

  for (const card of hand) {
    const r = rank(card);
    counts.set(r, (counts.get(r) || 0) + 1);
  }

  const groups = [...counts.values()]
    .sort((a, b) => b - a);

  const sorted = hand.map(rank).sort((a, b) => a - b);

  const flush = hand.length === 5 &&
    hand.every(card => suit(card) === suit(hand[0]));

  const straight = hand.length === 5 &&
    (
      sorted.every(
        (r, i) =>
          i === 0 || r === sorted[i - 1] + 1
      ) ||
      sorted.join(",") === "1,10,11,12,13"
    );

  if (straight && flush)
    return { name: "สเตรทฟลัช", base: 60 };

  if (groups[0] === 4)
    return { name: "โฟร์การ์ด", base: 50 };

  if (groups[0] === 3 && groups[1] === 2)
    return { name: "ฟูลเฮาส์", base: 40 };

  if (straight)
    return { name: "สเตรท", base: 20 };

  if (flush)
    return { name: "ฟลัช", base: 20 };

  if (score(hand) === 50)
    return { name: "50 แต้ม", base: 20 };

  if (groups[0] === 3)
    return { name: "ตอง", base: 20 };

  return { name: "ธรรมดา", base: 10 };
}

function rate(hand) {
  return classify(hand).base +
    hand.filter(card => rank(card) === 1).length * 10;
}

function ledger(room, from, to, amount, reason) {
  room.ledger.push({
    round: room.round,
    from,
    to,
    amount,
    reason
  });

  if (room.ledger.length > 250) {
    room.ledger.shift();
  }
}

function changeCredits(p, amount) {
  p.credits += amount;
  p.roundDelta += amount;
}

function transfer(room, from, to, amount, reason) {
  if (
    !from ||
    !to ||
    from === to ||
    amount <= 0
  ) return;

  changeCredits(from, -amount);
  changeCredits(to, amount);

  ledger(
    room,
    from.name,
    to.name,
    amount,
    reason
  );
}

function resetFollow(room) {
  room.followChain = false;
  room.followOriginSeat = null;
  room.followParticipants = [];
  room.followFourPaid = false;
}

function clearTimer(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.timerVersion++;
  room.deadline = null;
  room.helperSeat = -1;
  room.botStage = "";
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

    Promise.resolve()
      .then(callback)
      .catch(e => {
        console.error(e);
        recoverTurn(room);
      });
  }, delay);
}

function eligibleFollow(room) {
  if (
    room.phase !== "play" ||
    !room.followChain ||
    room.actionTaken ||
    !room.last?.cards?.length ||
    room.followParticipants.length >= 4
  ) return [];

  if (
    room.last.player !==
    (room.turn + 4) % 5
  ) return [];

  const target = rank(room.last.cards[0]);

  return room.players[room.turn].hand.filter(
    card => rank(card) === target
  );
}

function publicState(room, p) {
  const seat = room.players.indexOf(p);

  const canPlay =
    room.phase === "play" &&
    seat === room.turn &&
    room.helperSeat !== room.turn;

  const followCards = canPlay
    ? eligibleFollow(room)
    : [];

  return {
    type: "state",
    code: room.code,
    mode: room.mode,
    phase: room.phase,
    round: room.round,
    seat,
    dealer: room.dealer,
    turn: room.turn,
    hand: [...p.hand],
    players: room.players.map(x => ({
      name: x.name,
      bot: x.bot,
      online: x.online,
      member: !!x.memberId,
      credits: x.credits,
      cardCount: x.hand.length,
      discards: [...x.discards],
      resetPending: x.resetPending
    })),
    deckCount: room.deck.length,
    last: room.last,
    actionTaken: room.actionTaken,
    followChain: room.followChain,
    followOriginSeat: room.followOriginSeat,
    followParticipants: [...room.followParticipants],
    canFollow: followCards.length > 0,
    followCards,
    champagne: room.champagne,
    ledger: room.ledger,
    chat: room.chat,
    result: room.result,
    showdown: room.showdown,
    vote: room.vote,
    pendingShuffle: room.pendingShuffle,
    deadline: room.deadline,
    helperSeat: room.helperSeat,
    botStage: room.botStage,
    saving: room.saving,
    saveError: room.saveError
  };
}

function broadcast(room) {
  for (const p of room.players) {
    if (!p.bot && p.online) {
      send(p.ws, publicState(room, p));
    }
  }
}

function removeCards(p, cards) {
  for (const card of cards) {
    const index = p.hand.indexOf(card);
    if (index < 0) return false;

    p.hand.splice(index, 1);
    p.discards.push(card);
  }
  return true;
}

function tripleBonus(room, p, cards) {
  if (cards.length !== 3) return;

  for (const other of room.players) {
    if (other !== p) {
      transfer(
        room,
        other,
        p,
        10,
        "โบนัสทิ้งตอง"
      );
    }
  }
}

function payFourFollowers(room) {
  if (
    room.followFourPaid ||
    room.followParticipants.length !== 4 ||
    !Number.isInteger(room.followOriginSeat)
  ) return;

  const origin = room.players[
    room.followOriginSeat
  ];

  for (const seat of room.followParticipants) {
    transfer(
      room,
      origin,
      room.players[seat],
      FOLLOW_BONUS,
      "โบนัสไหลครบ 4 คน"
    );
  }

  room.followFourPaid = true;
}

function payFollowWinner(room, winnerSeat) {
  const originSeat = room.followOriginSeat;

  if (
    !Number.isInteger(originSeat) ||
    originSeat === winnerSeat ||
    room.followFourPaid
  ) return false;

  transfer(
    room,
    room.players[originSeat],
    room.players[winnerSeat],
    FOLLOW_BONUS,
    "โบนัสไหลจนหมดมือ"
  );

  return true;
}

async function recordMemberCredit(
  memberId,
  amount,
  reason,
  reset = false
) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const updated = await client.query(
      reset
        ? `
          UPDATE members
          SET credits = 0
          WHERE id = $1
          RETURNING credits
        `
        : `
          UPDATE members
          SET credits = credits + $2
          WHERE id = $1
          RETURNING credits
        `,
      reset
        ? [memberId]
        : [memberId, amount]
    );

    await client.query(`
      INSERT INTO credit_history(
        member_id,amount,reason
      )
      VALUES($1,$2,$3)
    `, [
      memberId,
      amount,
      reason
    ]);

    await client.query("COMMIT");

    return Number(
      updated.rows[0]?.credits ?? 0
    );
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function applyReset(p, room = null) {
  const old = p.credits;

  if (p.memberId) {
    await recordMemberCredit(
      p.memberId,
      -old,
      "รีเซ็ตเครดิตเป็น 0",
      true
    );
  }

  p.credits = 0;
  p.roundDelta = 0;
  p.resetPending = false;

  if (room) {
    ledger(
      room,
      p.name,
      "ระบบ",
      old,
      "รีเซ็ตเครดิตเป็น 0"
    );
    broadcast(room);
  }
}

async function addMemberCredits(p, amount, room = null) {
  if (!creditOptions.includes(amount)) {
    throw new Error("จำนวนเครดิตไม่ถูกต้อง");
  }

  if (p.memberId) {
    await recordMemberCredit(
      p.memberId,
      amount,
      `เติมเครดิตจำลอง +${amount}`
    );
  }

  p.credits += amount;

  if (room) {
    ledger(
      room,
      "ระบบ",
      p.name,
      amount,
      "เติมเครดิตจำลอง"
    );
    broadcast(room);
  }
}

async function saveRound(room) {
  if (room.mode !== "member") return;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (let i = 0; i < 5; i++) {
      const p = room.players[i];
      if (!p.memberId) continue;

      const won =
        i === room.result.winnerSeat;

      const inserted = await client.query(`
        INSERT INTO match_history(
          member_id,room_code,round_number,
          outcome,credit_change,reason
        )
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(
          member_id,room_code,round_number
        )
        DO NOTHING
        RETURNING id
      `, [
        p.memberId,
        room.code,
        room.round,
        won ? "win" : "loss",
        p.roundDelta,
        room.result.reason
      ]);

      if (!inserted.rowCount) continue;

      await client.query(`
        UPDATE members
        SET credits = credits + $1,
            games = games + 1,
            wins = wins + $2,
            losses = losses + $3
        WHERE id = $4
      `, [
        p.roundDelta,
        won ? 1 : 0,
        won ? 0 : 1,
        p.memberId
      ]);

      await client.query(`
        INSERT INTO credit_history(
          member_id,amount,reason
        )
        VALUES($1,$2,$3)
      `, [
        p.memberId,
        p.roundDelta,
        `ห้อง ${room.code} รอบ ${room.round}`
      ]);
    }

    await client.query("COMMIT");

    for (const p of room.players) {
      p.roundDelta = 0;
    }
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function persistRound(room) {
  if (room.mode !== "member") return;

  room.saving = true;
  room.saveError = false;
  broadcast(room);

  try {
    await saveRound(room);

    for (const p of room.players) {
      if (p.memberId && p.resetPending) {
        await applyReset(p, room);
      }
    }

    room.saveError = false;
  } catch (e) {
    console.error("Save round error:", e);
    room.saveError = true;
  } finally {
    room.saving = false;
    broadcast(room);
  }
}

async function finishRound(
  room,
  winnerSeat,
  reason,
  callerSeat = null,
  followWin = false
) {
  if (room.phase !== "play") return;

  clearTimer(room);

  const winner = room.players[winnerSeat];
  const category = classify(winner.hand);
  const amount = rate(winner.hand);

  const failed =
    callerSeat !== null &&
    callerSeat !== winnerSeat;

  if (failed) {
    transfer(
      room,
      room.players[callerSeat],
      winner,
      amount * 4,
      "โดนทับ"
    );
  } else {
    room.players.forEach((p, i) => {
      if (i !== winnerSeat) {
        transfer(
          room,
          p,
          winner,
          amount,
          reason
        );
      }
    });
  }

  const originSeat = room.followOriginSeat;

  const followWinPaid = followWin
    ? payFollowWinner(room, winnerSeat)
    : false;

  changeCredits(winner, -2);
  room.champagne += 2;

  ledger(
    room,
    winner.name,
    "Champagne Money",
    2,
    "ค่าผู้ชนะ"
  );

  room.showdown = room.players.map(p => ({
    name: p.name,
    hand: [...p.hand],
    points: score(p.hand)
  }));

  room.phase = "end";
  room.dealer = winnerSeat;
  room.lastWinnerId = winner.id;

  room.result = {
    reason,
    message: failed
      ? `💀 ${room.players[callerSeat].name} โดนทับ! ${winner.name} ชนะ`
      : `🏆 ${winner.name} ชนะ! ${reason}`,
    category: category.name,
    amount: failed ? amount * 4 : amount,
    failed,
    winnerSeat,
    overlaidSeat: failed ? callerSeat : null,
    winnerName: winner.name,
    overlaidName: failed
      ? room.players[callerSeat].name
      : null,
    followWinBonus: followWinPaid ? 10 : 0,
    followBonusPayerName: followWinPaid
      ? room.players[originSeat].name
      : null,
    effectUntil: Date.now() + 5000
  };

  resetFollow(room);
  broadcast(room);

  await persistRound(room);
}

function canAct(room, p, taken, automatic = false) {
  const valid =
    room.phase === "play" &&
    room.players[room.turn] === p &&
    room.actionTaken === taken &&
    (
      automatic ||
      room.helperSeat !== room.turn
    );

  if (!valid && !automatic) {
    error(p.ws, "ยังไม่ถึงตาคุณ");
  }

  return valid;
}

function lowestSeat(room, excluded = null) {
  const seats = room.players
    .map((_, i) => i)
    .filter(i => i !== excluded);

  let best = seats[0];

  for (const i of seats.slice(1)) {
    if (
      score(room.players[i].hand) <
      score(room.players[best].hand)
    ) best = i;
  }

  return best;
}

async function kang(room, p, automatic = false) {
  if (
    !canAct(room, p, false, automatic) ||
    room.followChain
  ) return;

  const seat = room.turn;

  const lower = room.players.some(
    (other, i) =>
      i !== seat &&
      score(other.hand) < score(p.hand)
  );

  if (lower) {
    await finishRound(
      room,
      lowestSeat(room, seat),
      "โดนทับ",
      seat
    );
  } else {
    await finishRound(
      room,
      seat,
      "แคงสำเร็จ"
    );
  }
}

function draw(room, p, automatic = false) {
  if (
    !canAct(room, p, false, automatic) ||
    room.followChain
  ) return false;

  if (!room.deck.length) {
    if (!automatic) error(p.ws, "กองจั่วหมด");
    return false;
  }

  p.hand.push(room.deck.pop());
  room.actionTaken = true;

  if (!automatic) scheduleTurn(room);

  broadcast(room);
  return true;
}

async function follow(room, p, automatic = false) {
  if (!canAct(room, p, false, automatic)) return;

  const cards = eligibleFollow(room);

  if (!cards.length) {
    if (!automatic) error(p.ws, "ไม่มีไพ่ให้ไหล");
    return;
  }

  const seat = room.turn;

  removeCards(p, cards);
  tripleBonus(room, p, cards);

  room.followParticipants.push(seat);
  room.last = {
    player: seat,
    cards: [...cards]
  };

  if (!p.hand.length) {
    await finishRound(
      room,
      seat,
      "ไหลจนหมดมือ",
      null,
      true
    );
    return;
  }

  if (room.followParticipants.length === 4) {
    payFourFollowers(room);

    room.turn = room.followOriginSeat;
    room.actionTaken = false;
    resetFollow(room);

    if (!room.deck.length) {
      await finishRound(
        room,
        lowestSeat(room),
        "กองจั่วหมด"
      );
    } else {
      scheduleTurn(room);
    }

    broadcast(room);
    return;
  }

  room.turn = (seat + 1) % 5;
  room.actionTaken = false;
  room.followChain = true;

  if (eligibleFollow(room).length) {
    scheduleTurn(room);
  } else {
    endFollowChain(room);
  }

  broadcast(room);
}

function endFollowChain(room) {
  if (room.phase !== "play") return;

  resetFollow(room);
  room.actionTaken = false;

  if (!room.deck.length) {
    void finishRound(
      room,
      lowestSeat(room),
      "กองจั่วหมด"
    );
  } else {
    scheduleTurn(room);
    broadcast(room);
  }
}

async function discard(
  room,
  p,
  cards,
  automatic = false
) {
  if (
    !canAct(room, p, true, automatic) ||
    room.followChain
  ) return;

  if (
    !Array.isArray(cards) ||
    !cards.length ||
    new Set(cards).size !== cards.length ||
    !cards.every(
      c => Number.isInteger(c) && p.hand.includes(c)
    ) ||
    !cards.every(
      c => rank(c) === rank(cards[0])
    )
  ) {
    if (!automatic) {
      error(
        p.ws,
        "ต้องเลือกไพ่หน้าเดียวกันเท่านั้น"
      );
    }
    return;
  }

  const seat = room.turn;

  removeCards(p, cards);
  tripleBonus(room, p, cards);

  room.last = {
    player: seat,
    cards: [...cards]
  };

  if (!p.hand.length) {
    resetFollow(room);

    await finishRound(
      room,
      seat,
      "ทิ้งจนหมดมือ"
    );
    return;
  }

  room.turn = (seat + 1) % 5;
  room.actionTaken = false;

  room.followChain = true;
  room.followOriginSeat = seat;
  room.followParticipants = [];
  room.followFourPaid = false;

  if (eligibleFollow(room).length) {
    scheduleTurn(room);
  } else {
    resetFollow(room);

    if (!room.deck.length) {
      await finishRound(
        room,
        lowestSeat(room),
        "กองจั่วหมด"
      );
    } else {
      scheduleTurn(room);
    }
  }

  broadcast(room);
}

function botDiscard(hand) {
  const groups = new Map();

  for (const card of hand) {
    const r = rank(card);

    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(card);
  }

  return [...groups.values()].sort(
    (a, b) =>
      b.length - a.length ||
      points(b[0]) - points(a[0])
  )[0] || [];
}

function recoverTurn(room) {
  if (room.phase !== "play") return;

  const p = room.players[room.turn];

  if (room.followChain) {
    if (eligibleFollow(room).length) {
      void follow(room, p, true);
    } else {
      endFollowChain(room);
    }
  } else if (room.actionTaken) {
    void discard(
      room,
      p,
      botDiscard(p.hand),
      true
    );
  } else {
    botTurn(room, !p.bot);
  }
}

function botTurn(room, helper = false) {
  if (room.phase !== "play") return;

  const seat = room.turn;
  const p = room.players[seat];

  setTimer(room, BOT_DELAY, async () => {
    if (
      room.phase !== "play" ||
      room.players[seat] !== p
    ) return;

    if (room.followChain) {
      if (eligibleFollow(room).length) {
        await follow(room, p, true);
      } else {
        endFollowChain(room);
      }
      return;
    }

    if (!room.actionTaken) {
      if (
        score(p.hand) <= 9 &&
        Math.random() < 0.4
      ) {
        await kang(room, p, true);
        return;
      }

      if (!draw(room, p, true)) {
        await finishRound(
          room,
          lowestSeat(room),
          "กองจั่วหมด"
        );
        return;
      }
    }

    setTimer(room, BOT_DELAY, async () => {
      if (
        room.phase !== "play" ||
        room.players[seat] !== p
      ) return;

      await discard(
        room,
        p,
        botDiscard(p.hand),
        true
      );
    });

    room.botStage = "discarding";
    room.helperSeat = helper ? seat : -1;
    broadcast(room);
  });

  room.botStage = "thinking";
  room.helperSeat = helper ? seat : -1;
  broadcast(room);
}

function scheduleTurn(room) {
  clearTimer(room);

  if (room.phase !== "play") return;

  if (
    room.followChain &&
    !eligibleFollow(room).length
  ) {
    endFollowChain(room);
    return;
  }

  const p = room.players[room.turn];

  if (p.bot) {
    botTurn(room);
    return;
  }

  if (!p.online) {
    setTimer(room, 15000, () => {
      if (!p.online) botTurn(room, true);
    });
  } else {
    setTimer(
      room,
      HUMAN_TIMEOUT,
      () => botTurn(room, true)
    );
  }

  broadcast(room);
}

function startRound(room) {
  if (
    room.phase === "play" ||
    room.saving ||
    room.saveError
  ) return;

  // ผู้เล่นจริงต้องมีเครดิตมากกว่า 0
  // หลังรีเซ็ตจึงต้องเติมใหม่ก่อนเริ่มรอบ
  const noCredit = room.players.find(
    p => !p.bot && p.credits <= 0
  );

  if (noCredit) {
    for (const p of room.players) {
      if (!p.bot) {
        error(
          p.ws,
          `${noCredit.name} ต้องเติมเครดิตก่อนเริ่มเกม`
        );
      }
    }
    return;
  }

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
  room.deck = shuffle(
    Array.from({ length: 52 }, (_, i) => i)
  );
  room.turn = room.dealer;
  room.last = null;
  room.actionTaken = false;
  room.result = null;
  room.showdown = [];
  room.vote = null;

  resetFollow(room);

  for (const p of room.players) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
    p.roundDelta = 0;
  }

  scheduleTurn(room);
  broadcast(room);
}

async function memberByToken(token) {
  if (
    typeof token !== "string" ||
    !/^[a-f0-9]{48}$/.test(token)
  ) return null;

  const result = await pool.query(`
    SELECT m.id,m.username,m.credits,
           m.wins,m.losses,m.games
    FROM sessions s
    JOIN members m ON m.id=s.member_id
    WHERE s.token_hash=$1
      AND s.expires_at>NOW()
  `, [hashToken(token)]);

  return result.rows[0] || null;
}

async function sendProfile(ws) {
  if (!ws.memberId) return;

  const member = await pool.query(`
    SELECT id,username,credits,
           wins,losses,games
    FROM members
    WHERE id=$1
  `, [ws.memberId]);

  const matches = await pool.query(`
    SELECT room_code,round_number,
           outcome,credit_change,
           reason,created_at
    FROM match_history
    WHERE member_id=$1
    ORDER BY id DESC
    LIMIT 30
  `, [ws.memberId]);

  const credits = await pool.query(`
    SELECT amount,reason,created_at
    FROM credit_history
    WHERE member_id=$1
    ORDER BY id DESC
    LIMIT 40
  `, [ws.memberId]);

  send(ws, {
    type: "profile",
    member: member.rows[0],
    history: matches.rows,
    creditHistory: credits.rows
  });
}

async function authenticate(ws, action, data) {
  if (action === "resume") {
    const member = await memberByToken(data.token);

    if (!member) {
      error(ws, "เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่");
      return;
    }

    ws.memberId = String(member.id);
    ws.username = member.username;

    send(ws, {
      type: "auth",
      token: data.token,
      username: member.username
    });

    await sendProfile(ws);
    return;
  }

  const username =
    String(data.username || "").trim();

  const password =
    String(data.password || "");

  if (!/^[A-Za-z0-9_]{3,24}$/.test(username)) {
    error(
      ws,
      "ชื่อผู้ใช้ 3–24 ตัว ใช้ภาษาอังกฤษ ตัวเลข หรือ _"
    );
    return;
  }

  if (
    password.length < 8 ||
    password.length > 128
  ) {
    error(ws, "รหัสผ่านต้องมี 8–128 ตัวอักษร");
    return;
  }

  if (action === "register") {
    try {
      await pool.query(`
        INSERT INTO members(
          username,password_hash,credits
        )
        VALUES($1,$2,0)
      `, [
        username,
        await makePasswordHash(password)
      ]);
    } catch (e) {
      if (e.code === "23505") {
        error(ws, "ชื่อผู้ใช้นี้มีคนใช้แล้ว");
        return;
      }
      throw e;
    }
  }

  const result = await pool.query(`
    SELECT id,username,password_hash
    FROM members
    WHERE username=$1
  `, [username]);

  const member = result.rows[0];

  if (
    !member ||
    !await checkPassword(
      password,
      member.password_hash
    )
  ) {
    error(ws, "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง");
    return;
  }

  const token = uid();

  await pool.query(`
    INSERT INTO sessions(
      token_hash,member_id,expires_at
    )
    VALUES(
      $1,$2,NOW()+INTERVAL '30 days'
    )
  `, [hashToken(token), member.id]);

  ws.memberId = String(member.id);
  ws.username = member.username;

  send(ws, {
    type: "auth",
    token,
    username: member.username
  });

  await sendProfile(ws);
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
    presence.set(String(p.memberId), room.code);
  }

  send(ws, {
    type: "roomIdentity",
    code: room.code,
    mode: room.mode,
    token: p.token
  });

  if (
    room.phase === "play" &&
    room.players[room.turn] === p
  ) {
    scheduleTurn(room);
  }

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
    const bot = player(
      `Bot ${seat + 1}`,
      true
    );

    bot.hand = [...p.hand];
    bot.discards = [...p.discards];
    bot.credits = p.credits;

    room.players[seat] = bot;

    if (p.memberId) {
      presence.delete(String(p.memberId));
    }
  }

  if (
    room.phase === "play" &&
    room.turn === seat
  ) {
    scheduleTurn(room);
  }

  broadcast(room);
}

async function joinRoom(ws, data) {
  const code = String(data.code || "")
    .trim()
    .toUpperCase();

  const room = rooms.get(code);

  if (!room) {
    error(ws, "ไม่พบห้อง");
    return;
  }

  if (
    room.mode === "member" &&
    !ws.memberId
  ) {
    error(ws, "กรุณาเข้าสู่ระบบสมาชิก");
    return;
  }

  if (
    room.mode === "guest" &&
    ws.memberId
  ) {
    error(ws, "กรุณาออกจากบัญชีสมาชิกก่อน");
    return;
  }

  const existing = room.players.find(p =>
    !p.bot &&
    (
      room.mode === "member"
        ? p.memberId === ws.memberId
        : p.token === data.token
    )
  );

  if (existing) {
    attach(room, existing, ws);
    return;
  }

  if (
    ws.memberId &&
    presence.has(String(ws.memberId))
  ) {
    error(ws, "บัญชีนี้อยู่ในห้องอื่นแล้ว");
    return;
  }

  if (room.phase === "play") {
    error(ws, "ห้องกำลังเล่น กรุณารอจบรอบ");
    return;
  }

  if (room.saving || room.saveError) {
    error(ws, "ห้องกำลังบันทึกผล");
    return;
  }

  const seat = room.players.findIndex(p => p.bot);

  if (seat < 0) {
    error(ws, "ห้องเต็มแล้ว");
    return;
  }

  const name = room.mode === "member"
    ? ws.username
    : String(data.name || "Guest")
        .trim()
        .slice(0, 24);

  const p = player(
    name,
    false,
    ws.memberId || null
  );

  if (p.memberId) {
    const result = await pool.query(`
      SELECT credits
      FROM members
      WHERE id=$1
    `, [p.memberId]);

    p.credits = Number(
      result.rows[0]?.credits ?? 0
    );
  }

  room.players[seat] = p;
  attach(room, p, ws);
}

async function creditAction(ws, action, data) {
  const room = ws.room;
  const p = ws.player;

  const memberId = ws.memberId;

  if (!memberId) {
    error(ws, "กรุณาเข้าสู่ระบบสมาชิก");
    return;
  }

  if (room && room.mode !== "member") {
    error(ws, "โหมดไม่ถูกต้อง");
    return;
  }

  if (
    room &&
    (!p || p.memberId !== memberId)
  ) {
    error(ws, "ไม่พบบัญชีผู้เล่นในห้อง");
    return;
  }

  if (room?.saving || room?.saveError) {
    error(ws, "กรุณารอระบบบันทึกผลก่อน");
    return;
  }

  if (action === "memberTopup") {
    const amount = Number(data.amount);

    if (!creditOptions.includes(amount)) {
      error(ws, "เลือก 100–500 เครดิตเท่านั้น");
      return;
    }

    if (p) {
      await addMemberCredits(p, amount, room);
    } else {
      await recordMemberCredit(
        memberId,
        amount,
        `เติมเครดิตจำลอง +${amount}`
      );
    }

    await sendProfile(ws);

    send(ws, {
      type: "notice",
      message: `เติมเครดิต ${amount} สำเร็จ`
    });

    return;
  }

  if (action === "memberReset") {
    if (p && room.phase === "play") {
      p.resetPending = true;
      broadcast(room);

      send(ws, {
        type: "notice",
        message: "รอบนี้จบแล้ว เครดิตจะรีเซ็ตเป็น 0"
      });

      return;
    }

    if (p) {
      await applyReset(p, room);
    } else {
      const result = await pool.query(
        "SELECT credits FROM members WHERE id=$1",
        [memberId]
      );

      const old = Number(
        result.rows[0]?.credits ?? 0
      );

      await recordMemberCredit(
        memberId,
        -old,
        "รีเซ็ตเครดิตเป็น 0",
        true
      );
    }

    await sendProfile(ws);

    send(ws, {
      type: "notice",
      message: "รีเซ็ตเครดิตเป็น 0 แล้ว กรุณาเติมเครดิตใหม่"
    });
  }
}

async function handle(ws, data) {
  const action = data?.action;

  if (
    ["register","login","resume"].includes(action)
  ) {
    if (ws.room) {
      error(ws, "กรุณาออกจากห้องก่อน");
      return;
    }

    await authenticate(ws, action, data);
    return;
  }

  if (action === "logout") {
    if (ws.room) {
      error(ws, "กรุณาออกจากห้องก่อน");
      return;
    }

    if (typeof data.token === "string") {
      await pool.query(
        "DELETE FROM sessions WHERE token_hash=$1",
        [hashToken(data.token)]
      );
    }

    ws.memberId = null;
    ws.username = null;

    send(ws, { type: "loggedOut" });
    return;
  }

  if (action === "profile") {
    await sendProfile(ws);
    return;
  }

  if (
    action === "memberTopup" ||
    action === "memberReset"
  ) {
    await creditAction(ws, action, data);
    return;
  }

  if (action === "create") {
    if (ws.room) {
      error(ws, "กรุณาออกจากห้องเดิมก่อน");
      return;
    }

    const mode = data.mode === "member"
      ? "member"
      : "guest";

    if (mode === "member" && !ws.memberId) {
      error(ws, "กรุณาเข้าสู่ระบบสมาชิก");
      return;
    }

    if (mode === "guest" && ws.memberId) {
      error(ws, "กรุณาออกจากบัญชีสมาชิกก่อน");
      return;
    }

    if (
      ws.memberId &&
      presence.has(String(ws.memberId))
    ) {
      error(ws, "บัญชีนี้อยู่ในห้องอื่นแล้ว");
      return;
    }

    const room = createRoom(mode);

    await joinRoom(ws, {
      code: room.code,
      name: data.name
    });

    return;
  }

  if (action === "join") {
    if (ws.room) {
      error(ws, "กรุณาออกจากห้องเดิมก่อน");
      return;
    }

    await joinRoom(ws, data);
    return;
  }

  if (action === "leave") {
    if (
      ws.room?.phase === "play" &&
      ws.room.mode === "member"
    ) {
      error(
        ws,
        "กรุณารอจบรอบก่อนออกจากห้องสมาชิก"
      );
      return;
    }

    detach(ws, true);
    send(ws, { type: "left" });
    return;
  }

  const room = ws.room;
  const p = ws.player;

  if (
    !room ||
    !p ||
    p.ws !== ws ||
    !room.players.includes(p)
  ) {
    error(ws, "กรุณาเข้าห้องก่อน");
    return;
  }

  switch (action) {
    case "start":
      if (room.phase === "lobby") {
        startRound(room);
      }
      break;

    case "next":
      if (
        room.phase === "end" &&
        !room.saving &&
        !room.saveError
      ) {
        startRound(room);
      }
      break;

    case "kang":
      await kang(room, p);
      break;

    case "draw":
      draw(room, p);
      break;

    case "follow":
      await follow(room, p);
      break;

    case "discard":
      await discard(room, p, data.cards);
      break;

    case "chat": {
      const text = String(data.text || "")
        .trim()
        .slice(0, 300);

      if (text) {
        room.chat.push({
          name: p.name,
          text
        });

        if (room.chat.length > 100) {
          room.chat.shift();
        }

        broadcast(room);
      }
      break;
    }

    case "credit": {
      if (room.mode !== "guest") {
        error(
          ws,
          "สมาชิกต้องใช้ปุ่มเติมเครดิตสมาชิก"
        );
        return;
      }

      const amount = Number(data.amount);

      if (creditOptions.includes(amount)) {
        p.credits += amount;

        ledger(
          room,
          "ระบบ",
          p.name,
          amount,
          "เติมเครดิต Guest"
        );

        broadcast(room);
      }
      break;
    }

    case "requestSeats": {
      if (
        room.phase === "play" ||
        room.vote
      ) break;

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
      break;
    }

    case "approveSeats": {
      if (!room.vote) break;

      if (!room.vote.approvals.includes(p.id)) {
        room.vote.approvals.push(p.id);
      }

      if (
        room.vote.required.every(
          id => room.vote.approvals.includes(id)
        )
      ) {
        room.pendingShuffle = true;
        room.vote = null;
      }

      broadcast(room);
      break;
    }

    case "rejectSeats":
      room.vote = null;
      broadcast(room);
      break;

    case "retrySave":
      if (
        room.phase === "end" &&
        room.saveError &&
        !room.saving
      ) {
        await persistRound(room);
      }
      break;

    default:
      error(ws, "ไม่รู้จักคำสั่ง");
  }
}

const server = http.createServer(async (req, res) => {
  try {
    if (
      req.url === "/" ||
      req.url === "/index.html"
    ) {
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

      res.end("KANG ONLINE V14.3 OK");
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

// Serialize incoming actions to avoid credit/turn races.
let queue = Promise.resolve();

wss.on("connection", ws => {
  ws.memberId = null;
  ws.username = null;
  ws.room = null;
  ws.player = null;
  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", raw => {
    queue = queue
      .then(async () => {
        let data;

        try {
          data = JSON.parse(raw.toString());
        } catch {
          error(ws, "ข้อมูลไม่ถูกต้อง");
          return;
        }

        await handle(ws, data);
      })
      .catch(e => {
        console.error(e);
        error(ws, "เกิดข้อผิดพลาด กรุณาลองใหม่");
      });
  });

  ws.on("close", () => {
    detach(ws, false);
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

await initDB();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`KANG ONLINE V14.3 on port ${PORT}`);
});
