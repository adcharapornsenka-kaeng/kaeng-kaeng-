import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { randomBytes, createHash, scryptSync, timingSafeEqual } from 'node:crypto';
import { WebSocketServer, WebSocket } from 'ws';
import pg from 'pg';

const PORT = Number(process.env.PORT || 3000);
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
const db = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false }
});
const rooms = new Map();
const memberRoom = new Map();
const MAX_SEATS = 6;
const TOPUPS = [100, 200, 300, 400, 500];
const uid = () => randomBytes(24).toString('hex');
const sha = s => createHash('sha256').update(s).digest('hex');
const rnk = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const points = c => Math.min(rnk(c), 10);
const score = hand => hand.reduce((n, c) => n + points(c), 0);
const formatName = s => String(s || 'Guest').trim().slice(0, 24) || 'Guest';

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function initDatabase() {
  await db.query(`CREATE TABLE IF NOT EXISTS members (
    id BIGSERIAL PRIMARY KEY, username VARCHAR(24) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL, credits BIGINT NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0, losses INTEGER NOT NULL DEFAULT 0,
    games INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ DEFAULT NOW())`);
  await db.query('ALTER TABLE members ALTER COLUMN credits SET DEFAULT 0');
  await db.query(`CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY,
    member_id BIGINT NOT NULL REFERENCES members(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL)`);
  await db.query(`CREATE TABLE IF NOT EXISTS match_history (
    id BIGSERIAL PRIMARY KEY,
    member_id BIGINT NOT NULL REFERENCES members(id) ON DELETE CASCADE,
    room_code VARCHAR(12) NOT NULL, round_number INTEGER NOT NULL,
    outcome VARCHAR(12) NOT NULL, credit_change BIGINT NOT NULL,
    reason TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
  await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS kang_v145_unique_round
    ON match_history(member_id, room_code, round_number)`);
  await db.query(`CREATE TABLE IF NOT EXISTS credit_history (
    id BIGSERIAL PRIMARY KEY,
    member_id BIGINT NOT NULL REFERENCES members(id) ON DELETE CASCADE,
    amount BIGINT NOT NULL, reason TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW())`);
}

function passwordHash(password) {
  const salt = randomBytes(16).toString('hex');
  return `${salt}:${scryptSync(password, salt, 64).toString('hex')}`;
}
function verifyPassword(password, stored) {
  const [salt, hex] = String(stored).split(':');
  if (!salt || !/^[a-f\d]{128}$/i.test(hex || '')) return false;
  const actual = scryptSync(password, salt, 64);
  const expected = Buffer.from(hex, 'hex');
  return timingSafeEqual(actual, expected);
}
function send(ws, data) {
  if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
const fail = (ws, message) => send(ws, { type: 'error', message });
const notice = (ws, message) => send(ws, { type: 'notice', message });

function player(name, bot = false, memberId = null) {
  return {
    id: uid(), token: uid(), name: formatName(name), bot, memberId,
    ws: null, online: bot, credits: 0, roundDelta: 0,
    hand: [], discards: [], resetPending: false,
    afterResetTopups: 0
  };
}
function makeRoom(mode, kind) {
  let code;
  do { code = randomBytes(3).toString('hex').toUpperCase(); }
  while (rooms.has(code));
  const room = {
    code, mode, kind, ownerId: null,
    seats: Array.from({ length: MAX_SEATS }, (_, i) =>
      kind === 'bot' ? player(`Bot ${i + 1}`, true) : null),
    phase: 'lobby', round: 0, dealer: 0, turn: 0, deck: [],
    last: null, drawn: false, chain: false, origin: null, followers: [],
    ledger: [], chat: [], champagne: 0, result: null, showdown: [],
    timer: null, timerVersion: 0, deadline: 0, auto: false,
    saving: false, saveError: false, lastWinnerId: null,
    votes: null, shuffleNext: false
  };
  rooms.set(code, room);
  return room;
}
const activeSeats = room => room.seats.map((p, i) => p ? i : -1).filter(i => i >= 0);
const humans = room => room.seats.filter(p => p && !p.bot);
const aliveCount = room => activeSeats(room).length;
function nextSeat(room, from) {
  for (let step = 1; step <= MAX_SEATS; step++) {
    const i = (from + step) % MAX_SEATS;
    if (room.seats[i]) return i;
  }
  return from;
}
function clearTimer(room) {
  clearTimeout(room.timer);
  room.timer = null;
  room.deadline = 0;
  room.timerVersion++;
  room.auto = false;
}
// All WebSocket actions and turn callbacks share one queue.
let workQueue = Promise.resolve();
function enqueue(job) {
  workQueue = workQueue.then(job).catch(e => console.error('Game task:', e));
  return workQueue;
}
function setTimer(room, ms, fn) {
  clearTimer(room);
  const version = ++room.timerVersion;
  const seat = room.turn;
  const playerId = room.seats[seat]?.id;
  room.deadline = Date.now() + ms;
  room.timer = setTimeout(() => {
    enqueue(async () => {
      if (room.phase !== 'play' || room.timerVersion !== version ||
          room.turn !== seat || room.seats[seat]?.id !== playerId) return;
      await fn();
    });
  }, ms);
}
function clearChain(room) {
  room.chain = false;
  room.origin = null;
  room.followers = [];
}
function eligibleFollow(room) {
  if (room.phase !== 'play' || !room.chain || room.drawn ||
      !room.last?.cards?.length || room.followers.length >= aliveCount(room) - 1) return [];
  if (room.last.seat !== previousSeat(room, room.turn)) return [];
  const target = rnk(room.last.cards[0]);
  return room.seats[room.turn].hand.filter(c => rnk(c) === target);
}
function previousSeat(room, seat) {
  for (let d = 1; d <= MAX_SEATS; d++) {
    const i = (seat - d + MAX_SEATS) % MAX_SEATS;
    if (room.seats[i]) return i;
  }
  return seat;
}
function publicState(room, me) {
  const seat = room.seats.indexOf(me);
  const canPlay = room.phase === 'play' && room.turn === seat && !room.auto;
  return {
    type: 'state', code: room.code, mode: room.mode, kind: room.kind,
    myId: me.id, ownerId: room.ownerId, phase: room.phase,
    round: room.round, seat, dealer: room.dealer, turn: room.turn,
    hand: [...me.hand], players: room.seats.map(p => p ? {
      id: p.id, name: p.name, bot: p.bot, online: p.online,
      credits: p.credits, count: p.hand.length, discards: [...p.discards],
      resetPending: p.resetPending
    } : null),
    activeCount: aliveCount(room), humanCount: humans(room).length,
    deck: room.deck.length, last: room.last, drawn: room.drawn,
    chain: room.chain, origin: room.origin, followers: [...room.followers],
    followCards: canPlay ? eligibleFollow(room) : [],
    deadline: room.deadline, auto: room.auto,
    champagne: room.champagne, ledger: room.ledger.slice(-120),
    chat: room.chat.slice(-80), result: room.result,
    showdown: room.showdown, saving: room.saving,
    saveError: room.saveError, votes: room.votes
  };
}
function broadcast(room) {
  for (const p of humans(room)) if (p.online) send(p.ws, publicState(room, p));
}
function ledger(room, from, to, amount, reason) {
  room.ledger.push({ round: room.round, from, to, amount, reason });
  if (room.ledger.length > 300) room.ledger.shift();
}
function adjust(p, amount) {
  p.credits += amount;
  p.roundDelta += amount;
}
function transfer(room, a, b, amount, reason) {
  if (!a || !b || a === b || amount <= 0) return;
  adjust(a, -amount); adjust(b, amount);
  ledger(room, a.name, b.name, amount, reason);
}
function tripleBonus(room, p, cards) {
  if (cards.length !== 3) return;
  for (const q of room.seats) if (q && q !== p) {
    transfer(room, q, p, 10, 'โบนัสตอง');
  }
}
function classify(hand) {
  const counts = new Map();
  for (const c of hand) counts.set(rnk(c), (counts.get(rnk(c)) || 0) + 1);
  const groups = [...counts.values()].sort((a, b) => b - a);
  const values = hand.map(rnk).sort((a, b) => a - b);
  const flush = hand.length === 5 && hand.every(c => suit(c) === suit(hand[0]));
  const straight = hand.length === 5 &&
    (values.every((v, i) => !i || v === values[i - 1] + 1) ||
      values.join(',') === '1,10,11,12,13');
  if (straight && flush) return ['สเตรทฟลัช', 60];
  if (groups[0] === 4) return ['โฟร์การ์ด', 50];
  if (groups[0] === 3 && groups[1] === 2) return ['ฟูลเฮาส์', 40];
  if (straight) return ['สเตรท', 20];
  if (flush) return ['ฟลัช', 20];
  if (score(hand) === 50) return ['50 แต้ม', 20];
  if (groups[0] === 3) return ['ตอง', 20];
  return ['ธรรมดา', 10];
}
const paymentRate = hand => classify(hand)[1] + hand.filter(c => rnk(c) === 1).length * 10;
function lowestSeat(room, excluded = -1) {
  const seats = activeSeats(room).filter(i => i !== excluded);
  return seats.reduce((best, i) =>
    score(room.seats[i].hand) < score(room.seats[best].hand) ? i : best);
}

async function profile(ws) {
  if (!ws.memberId) return;
  const [member, matches, credits] = await Promise.all([
    db.query('SELECT id,username,credits,wins,losses,games FROM members WHERE id=$1', [ws.memberId]),
    db.query(`SELECT room_code,round_number,outcome,credit_change,reason,created_at
      FROM match_history WHERE member_id=$1 ORDER BY id DESC LIMIT 30`, [ws.memberId]),
    db.query(`SELECT amount,reason,created_at FROM credit_history
      WHERE member_id=$1 ORDER BY id DESC LIMIT 40`, [ws.memberId])
  ]);
  send(ws, { type: 'profile', member: member.rows[0],
    history: matches.rows, creditHistory: credits.rows });
}
async function setMemberCredits(id, amount, reason, reset = false) {
  const c = await db.connect();
  try {
    await c.query('BEGIN');
    const current = await c.query('SELECT credits FROM members WHERE id=$1 FOR UPDATE', [id]);
    if (!current.rowCount) throw new Error('Member not found');
    const previous = Number(current.rows[0].credits);
    const r = await c.query(reset
      ? 'UPDATE members SET credits=$2 WHERE id=$1 RETURNING credits'
      : 'UPDATE members SET credits=credits+$2 WHERE id=$1 RETURNING credits',
      [id, amount]);
    await c.query('INSERT INTO credit_history(member_id,amount,reason) VALUES($1,$2,$3)',
      [id, reset ? amount - previous : amount, reason]);
    await c.query('COMMIT');
    return Number(r.rows[0].credits);
  } catch (e) { await c.query('ROLLBACK'); throw e; }
  finally { c.release(); }
}
async function saveRound(room) {
  if (room.mode !== 'member') return;
  const c = await db.connect();
  try {
    await c.query('BEGIN');
    for (let i = 0; i < MAX_SEATS; i++) {
      const p = room.seats[i];
      if (!p?.memberId) continue;
      const won = i === room.result.winnerSeat;
      const inserted = await c.query(`INSERT INTO match_history
        (member_id,room_code,round_number,outcome,credit_change,reason)
        VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT (member_id,room_code,round_number) DO NOTHING RETURNING id`,
        [p.memberId, room.code, room.round, won ? 'win' : 'loss',
          p.roundDelta, room.result.reason]);
      if (!inserted.rowCount) continue;
      await c.query(`UPDATE members SET credits=credits+$1,
        games=games+1,wins=wins+$2,losses=losses+$3 WHERE id=$4`,
        [p.roundDelta, won ? 1 : 0, won ? 0 : 1, p.memberId]);
      await c.query(`INSERT INTO credit_history(member_id,amount,reason)
        VALUES($1,$2,$3)`, [p.memberId, p.roundDelta,
        `ห้อง ${room.code} รอบ ${room.round} ${room.result.reason}`]);
    }
    await c.query('COMMIT');
    for (const p of humans(room)) p.roundDelta = 0;
  } catch (e) { await c.query('ROLLBACK'); throw e; }
  finally { c.release(); }
}
async function settle(room) {
  if (room.mode !== 'member') return;
  room.saving = true; room.saveError = false; broadcast(room);
  try {
    await saveRound(room);
    // A top-up made after a pending reset stays credited after the reset.
    for (const p of humans(room)) {
      if (!p.resetPending) continue;
      const carry = p.afterResetTopups;
      await setMemberCredits(p.memberId, carry,
        `รีเซ็ตเครดิตเป็น 0 หลังจบรอบ (เติมใหม่ ${carry})`, true);
      p.credits = carry;
      p.roundDelta = 0;
      p.resetPending = false;
      p.afterResetTopups = 0;
      ledger(room, p.name, 'ระบบ', 0, `รีเซ็ตเครดิต เหลือ ${carry}`);
    }
  } catch (e) {
    console.error('Save round error:', e);
    room.saveError = true;
  } finally {
    room.saving = false; broadcast(room);
    for (const p of humans(room)) if (p.online) await profile(p.ws);
  }
}
async function finishRound(room, winnerSeat, reason, failedSeat = null, followWin = false) {
  if (room.phase !== 'play') return;
  clearTimer(room);
  const winner = room.seats[winnerSeat];
  const amount = paymentRate(winner.hand);
  const [category] = classify(winner.hand);
  const failed = failedSeat !== null && failedSeat !== winnerSeat;
  const origin = room.origin;

  if (failed) {
    transfer(room, room.seats[failedSeat], winner, amount * 4, 'โดนทับ');
  } else {
    for (let i = 0; i < MAX_SEATS; i++) {
      const p = room.seats[i];
      if (p && i !== winnerSeat) transfer(room, p, winner, amount, reason);
    }
  }
  let followBonus = 0;
  if (followWin && Number.isInteger(origin) && origin !== winnerSeat) {
    transfer(room, room.seats[origin], winner, 10, 'ไหลจนหมดมือ +10');
    followBonus = 10;
  }
  adjust(winner, -2); room.champagne += 2;
  ledger(room, winner.name, 'Champagne Money', 2, 'ค่าผู้ชนะ');
  room.showdown = room.seats.map(p => p ? {
    name: p.name, hand: [...p.hand], points: score(p.hand)
  } : null);
  room.phase = 'end'; room.dealer = winnerSeat; room.lastWinnerId = winner.id;
  room.result = { winnerSeat, winnerName: winner.name,
    overlaidSeat: failed ? failedSeat : null, failed,
    reason, category, amount: failed ? amount * 4 : amount,
    followBonus, originName: followBonus ? room.seats[origin].name : null,
    effectUntil: Date.now() + 5000 };
  clearChain(room); broadcast(room);
  await settle(room);
}
function canAct(room, p, drawn, auto = false) {
  return room.phase === 'play' && room.seats[room.turn] === p &&
    room.drawn === drawn && (auto || !room.auto);
}
async function kang(room, p, auto = false) {
  if (!canAct(room, p, false, auto) || room.chain) return;
  const seat = room.turn;
  const lower = activeSeats(room).some(i => i !== seat &&
    score(room.seats[i].hand) < score(p.hand));
  await finishRound(room, lower ? lowestSeat(room, seat) : seat,
    lower ? 'โดนทับ' : 'แคงสำเร็จ', lower ? seat : null);
}
function draw(room, p, auto = false) {
  if (!canAct(room, p, false, auto) || room.chain || !room.deck.length) return false;
  p.hand.push(room.deck.pop()); room.drawn = true;
  if (!auto) scheduleTurn(room);
  broadcast(room);
  return true;
}
async function endChain(room) {
  clearChain(room);
  if (!room.deck.length) await finishRound(room, lowestSeat(room), 'กองจั่วหมด');
  else { scheduleTurn(room); broadcast(room); }
}
async function follow(room, p, auto = false) {
  if (!canAct(room, p, false, auto)) return;
  const cards = eligibleFollow(room);
  if (!cards.length) return;
  const seat = room.turn;
  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }
  tripleBonus(room, p, cards);
  room.followers.push(seat);
  room.last = { seat, cards: [...cards] };

  if (!p.hand.length) {
    await finishRound(room, seat, 'ไหลจนหมดมือ', null, true);
    return;
  }
  // Full circle means every OTHER active player followed, not always four.
  if (room.followers.length === aliveCount(room) - 1) {
    const starter = room.seats[room.origin];
    for (const i of room.followers) {
      transfer(room, starter, room.seats[i], 10, 'ไหลครบวง +10');
    }
    room.turn = room.origin;
    room.drawn = false;
    await endChain(room);
    return;
  }
  room.turn = nextSeat(room, seat);
  room.drawn = false;
  if (eligibleFollow(room).length) scheduleTurn(room);
  else await endChain(room);
  broadcast(room);
}
async function discard(room, p, cards, auto = false) {
  if (!canAct(room, p, true, auto) || room.chain) return;
  if (!Array.isArray(cards) || !cards.length ||
      new Set(cards).size !== cards.length ||
      !cards.every(c => Number.isInteger(c) && p.hand.includes(c)) ||
      !cards.every(c => rnk(c) === rnk(cards[0]))) {
    if (!auto) fail(p.ws, 'เลือกไพ่หน้าเดียวกันเท่านั้น');
    return;
  }
  const seat = room.turn;
  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c), 1);
    p.discards.push(c);
  }
  tripleBonus(room, p, cards);
  room.last = { seat, cards: [...cards] };
  if (!p.hand.length) {
    clearChain(room);
    await finishRound(room, seat, 'ทิ้งจนหมดมือ');
    return;
  }
  room.turn = nextSeat(room, seat);
  room.drawn = false;
  room.chain = true;
  room.origin = seat;
  room.followers = [];
  if (eligibleFollow(room).length) scheduleTurn(room);
  else await endChain(room);
  broadcast(room);
}
function botDiscard(hand) {
  const groups = new Map();
  for (const c of hand) {
    const r = rnk(c);
    if (!groups.has(r)) groups.set(r, []);
    groups.get(r).push(c);
  }
  return [...groups.values()].sort((a, b) =>
    b.length - a.length || points(b[0]) - points(a[0]))[0] || [];
}
function botTurn(room) {
  if (room.phase !== 'play') return;
  const seat = room.turn;
  const p = room.seats[seat];
  setTimer(room, 1700, async () => {
    if (room.chain) {
      if (eligibleFollow(room).length) await follow(room, p, true);
      else await endChain(room);
      return;
    }
    if (!room.drawn && !draw(room, p, true)) {
      await finishRound(room, lowestSeat(room), 'กองจั่วหมด');
      return;
    }
    setTimer(room, 1700, () => discard(room, p, botDiscard(p.hand), true));
    room.auto = true;
    broadcast(room);
  });
  room.auto = true;
  broadcast(room);
}
function scheduleTurn(room) {
  clearTimer(room);
  if (room.phase !== 'play') return;
  if (room.chain && !eligibleFollow(room).length) {
    void endChain(room);
    return;
  }
  const p = room.seats[room.turn];
  if (p.bot) botTurn(room);
  else {
    setTimer(room, p.online ? 90000 : 15000, () => botTurn(room));
    broadcast(room);
  }
}
function startRound(room, p) {
  if (!['lobby', 'end'].includes(room.phase) || room.saving || room.saveError) {
    return fail(p.ws, 'ยังเริ่มรอบใหม่ไม่ได้');
  }
  if (room.ownerId !== p.id) return fail(p.ws, 'เจ้าของห้องเท่านั้นที่เริ่มเกมได้');
  if (room.kind === 'human' && humans(room).length < 2) {
    return fail(p.ws, `ห้องส่วนตัวต้องมีสมาชิกอย่างน้อย 2 คน (ตอนนี้ ${humans(room).length})`);
  }
  const noCredit = humans(room).find(x => x.credits <= 0);
  if (noCredit) return fail(p.ws, `${noCredit.name} ต้องเติมเครดิตก่อนเริ่ม`);

  if (room.shuffleNext) {
    shuffle(room.seats);
    room.shuffleNext = false;
  }
  if (room.lastWinnerId) {
    const winnerSeat = room.seats.findIndex(x => x?.id === room.lastWinnerId);
    if (winnerSeat >= 0) room.dealer = winnerSeat;
  }
  if (!room.seats[room.dealer]) room.dealer = activeSeats(room)[0];
  room.round++;
  room.phase = 'play';
  room.deck = shuffle(Array.from({ length: 52 }, (_, i) => i));
  room.turn = room.dealer;
  room.last = null; room.drawn = false;
  room.result = null; room.showdown = []; room.votes = null;
  clearChain(room);
  for (const p of room.seats) if (p) {
    p.hand = room.deck.splice(0, 5);
    p.discards = [];
    p.roundDelta = 0;
  }
  scheduleTurn(room); broadcast(room);
}

async function authenticate(ws, action, data) {
  if (action === 'resume') {
    const token = String(data.token || '');
    if (!/^[a-f0-9]{48}$/.test(token)) return fail(ws, 'เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่');
    const r = await db.query(`SELECT m.id,m.username FROM sessions s
      JOIN members m ON m.id=s.member_id
      WHERE s.token_hash=$1 AND s.expires_at>NOW()`, [sha(token)]);
    if (!r.rowCount) return fail(ws, 'เซสชันหมดอายุ กรุณาเข้าสู่ระบบใหม่');
    ws.memberId = String(r.rows[0].id);
    ws.username = r.rows[0].username;
    send(ws, { type: 'auth', token, username: ws.username });
    await profile(ws);
    return;
  }
  if (ws.authAttempts >= 10) return fail(ws, 'ลองเข้าสู่ระบบใหม่ภายหลัง');
  ws.authAttempts++;
  const username = String(data.username || '').trim();
  const password = String(data.password || '');
  if (!/^[A-Za-z0-9_]{3,24}$/.test(username) || password.length < 8 || password.length > 128) {
    return fail(ws, 'ชื่อผู้ใช้ 3–24 ตัว และรหัสผ่าน 8–128 ตัว');
  }
  if (action === 'register') {
    try {
      await db.query('INSERT INTO members(username,password_hash,credits) VALUES($1,$2,0)',
        [username, passwordHash(password)]);
    } catch (e) {
      if (e.code === '23505') return fail(ws, 'ชื่อผู้ใช้นี้มีคนใช้แล้ว');
      throw e;
    }
  }
  const r = await db.query('SELECT id,username,password_hash FROM members WHERE username=$1',
    [username]);
  const m = r.rows[0];
  if (!m || !verifyPassword(password, m.password_hash)) {
    return fail(ws, 'ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง');
  }
  const token = uid();
  await db.query(`INSERT INTO sessions(token_hash,member_id,expires_at)
    VALUES($1,$2,NOW()+INTERVAL '30 days')`, [sha(token), m.id]);
  ws.memberId = String(m.id);
  ws.username = m.username;
  ws.authAttempts = 0;
  send(ws, { type: 'auth', token, username });
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
  if (p.memberId) memberRoom.set(String(p.memberId), room.code);
  send(ws, { type: 'identity', code: room.code, mode: room.mode, token: p.token });
  if (room.phase === 'play' && room.seats[room.turn] === p) scheduleTurn(room);
  broadcast(room);
}
function detach(ws, permanent = false) {
  const room = ws.room;
  const p = ws.player;
  if (!room || !p || p.ws !== ws) return;
  const seat = room.seats.indexOf(p);
  p.ws = null;
  p.online = false;
  ws.room = null;
  ws.player = null;
  if (permanent && seat >= 0) {
    if (p.memberId) memberRoom.delete(String(p.memberId));
    if (room.kind === 'bot') {
      const bot = player(`Bot ${seat + 1}`, true);
      bot.credits = p.credits;
      room.seats[seat] = bot;
    } else {
      room.seats[seat] = null;
    }
    if (room.ownerId === p.id) room.ownerId = humans(room)[0]?.id || null;
    if (!humans(room).length) {
      clearTimer(room);
      rooms.delete(room.code);
      return;
    }
  }
  if (room.phase === 'play' && room.turn === seat) scheduleTurn(room);
  broadcast(room);
}
async function joinRoom(ws, data) {
  const code = String(data.code || '').trim().toUpperCase();
  const room = rooms.get(code);
  if (!room) return fail(ws, 'ไม่พบห้อง');
  if (room.mode === 'member' && !ws.memberId) return fail(ws, 'กรุณาเข้าสู่ระบบสมาชิก');
  if (room.mode === 'guest' && ws.memberId) return fail(ws, 'กรุณาออกจากบัญชีสมาชิกก่อน');
  const existing = room.seats.find(p => p && !p.bot &&
    (room.mode === 'member' ? p.memberId === ws.memberId : p.token === data.token));
  if (existing) return attach(room, existing, ws);
  if (ws.memberId && memberRoom.has(ws.memberId)) {
    return fail(ws, 'บัญชีนี้อยู่ในห้องอื่นแล้ว กรุณากลับเข้าห้องเดิม');
  }
  if (room.phase === 'play' || room.saving || room.saveError) {
    return fail(ws, 'ต้องรอให้รอบนี้จบก่อนเข้าห้อง');
  }
  const seat = room.seats.findIndex(p => !p || p.bot);
  if (seat < 0) return fail(ws, 'ห้องเต็มแล้ว (สูงสุด 6 คน)');
  const p = player(room.mode === 'member' ? ws.username : formatName(data.name),
    false, ws.memberId || null);
  if (p.memberId) {
    const r = await db.query('SELECT credits FROM members WHERE id=$1', [p.memberId]);
    p.credits = Number(r.rows[0]?.credits ?? 0);
  }
  room.seats[seat] = p;
  if (!room.ownerId) room.ownerId = p.id;
  attach(room, p, ws);
}
async function creditsAction(ws, action, data) {
  if (!ws.memberId) return fail(ws, 'กรุณาเข้าสู่ระบบสมาชิก');
  const room = ws.room, p = ws.player;
  if (room && room.mode !== 'member') return fail(ws, 'โหมดไม่ถูกต้อง');
  if (room?.saving || room?.saveError) return fail(ws, 'กรุณารอบันทึกผลก่อน');
  if (action === 'topup') {
    const amount = Number(data.amount);
    if (!TOPUPS.includes(amount)) return fail(ws, 'เติมได้ครั้งละ 100, 200, 300, 400 หรือ 500');
    await setMemberCredits(ws.memberId, amount, `เติมเครดิตจำลอง +${amount}`);
    if (p) {
      p.credits += amount;
      if (p.resetPending) p.afterResetTopups += amount;
      ledger(room, 'ระบบ', p.name, amount, 'เติมเครดิตจำลอง');
      broadcast(room);
    }
    await profile(ws);
    return notice(ws, `เติมเครดิต ${amount} สำเร็จ`);
  }
  if (p && room.phase === 'play') {
    // Keep all top-ups that happen AFTER the reset request.
    p.resetPending = true;
    p.afterResetTopups = 0;
    broadcast(room);
    return notice(ws, 'รับคำขอรีเซ็ตแล้ว: เมื่อจบรอบเครดิตจะเป็น 0 บวกยอดที่เติมใหม่หลังจากนี้');
  }
  await setMemberCredits(ws.memberId, 0, 'รีเซ็ตเครดิตเป็น 0', true);
  if (p) {
    p.credits = 0;
    p.roundDelta = 0;
    p.resetPending = false;
    p.afterResetTopups = 0;
    broadcast(room);
  }
  await profile(ws);
  notice(ws, 'รีเซ็ตเป็น 0 แล้ว กรุณาเติมเครดิตใหม่ก่อนเล่น');
}
async function handle(ws, data) {
  if (!data || typeof data !== 'object') return;
  const action = data.action;
  if (['register', 'login', 'resume'].includes(action)) {
    if (ws.room) return fail(ws, 'กรุณาออกจากห้องก่อน');
    return authenticate(ws, action, data);
  }
  if (action === 'profile') return profile(ws);
  if (action === 'logout') {
    if (ws.room) return fail(ws, 'กรุณาออกจากห้องก่อน');
    if (typeof data.token === 'string') {
      await db.query('DELETE FROM sessions WHERE token_hash=$1', [sha(data.token)]);
    }
    ws.memberId = null; ws.username = null;
    return send(ws, { type: 'loggedOut' });
  }
  if (action === 'topup' || action === 'reset') return creditsAction(ws, action, data);
  if (action === 'create') {
    if (ws.room) return fail(ws, 'กรุณาออกจากห้องเดิมก่อน');
    const mode = data.mode === 'member' ? 'member' : 'guest';
    const kind = data.kind === 'human' ? 'human' : 'bot';
    if (mode === 'member' && !ws.memberId) return fail(ws, 'กรุณาเข้าสู่ระบบก่อน');
    if (mode === 'guest' && ws.memberId) return fail(ws, 'กรุณาออกจากสมาชิกก่อน');
    if (mode === 'guest' && kind === 'human') return fail(ws, 'ห้องไม่มี Bot สำหรับสมาชิกเท่านั้น');
    if (ws.memberId && memberRoom.has(ws.memberId)) {
      return fail(ws, 'บัญชีนี้อยู่ในห้องอื่นแล้ว');
    }
    const room = makeRoom(mode, kind);
    return joinRoom(ws, { code: room.code, name: data.name });
  }
  if (action === 'join') {
    if (ws.room) return fail(ws, 'กรุณาออกจากห้องเดิมก่อน');
    return joinRoom(ws, data);
  }
  if (action === 'leave') {
    if (ws.room?.phase === 'play' || ws.room?.saving || ws.room?.saveError) {
      return fail(ws, 'กรุณารอจบรอบและบันทึกเครดิตให้เสร็จก่อนออก');
    }
    detach(ws, true);
    return send(ws, { type: 'left' });
  }
  const room = ws.room, p = ws.player;
  if (!room || !p || p.ws !== ws || !room.seats.includes(p)) {
    return fail(ws, 'กรุณาเข้าห้องก่อน');
  }
  switch (action) {
    case 'start': case 'next': return startRound(room, p);
    case 'kang': return kang(room, p);
    case 'draw': if (!draw(room, p)) fail(ws, 'จั่วไม่ได้'); return;
    case 'follow': if (!eligibleFollow(room).length) return fail(ws, 'ยังไหลไม่ได้');
      return follow(room, p);
    case 'discard': return discard(room, p, data.cards);
    case 'chat': {
      const txt = String(data.text || '').trim().slice(0, 300);
      if (txt) {
        room.chat.push({ name: p.name, text: txt });
        if (room.chat.length > 100) room.chat.shift();
        broadcast(room);
      }
      return;
    }
    case 'guestCredit': {
      if (room.mode !== 'guest') return fail(ws, 'ใช้ปุ่มเติมเครดิตสมาชิก');
      const amount = Number(data.amount);
      if (TOPUPS.includes(amount)) {
        p.credits += amount;
        ledger(room, 'ระบบ', p.name, amount, 'เติมเครดิต Guest');
        broadcast(room);
      }
      return;
    }
    case 'voteShuffle': {
      if (room.phase === 'play') return fail(ws, 'รอจบรอบก่อน');
      if (!room.votes) room.votes = {
        approvals: [p.id],
        required: humans(room).filter(x => x.online).map(x => x.id)
      };
      else if (!room.votes.approvals.includes(p.id)) room.votes.approvals.push(p.id);
      if (room.votes.required.every(id => room.votes.approvals.includes(id))) {
        room.shuffleNext = true; room.votes = null;
      }
      broadcast(room); return;
    }
    case 'rejectShuffle': room.votes = null; broadcast(room); return;
    case 'retry':
      if (room.phase === 'end' && room.saveError && !room.saving) return settle(room);
      return;
    default: fail(ws, 'ไม่รู้จักคำสั่ง');
  }
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    try {
      const html = await readFile(new URL('./index.html', import.meta.url));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store' });
      res.end(html);
    } catch (e) { console.error(e); res.writeHead(500); res.end('index.html missing'); }
    return;
  }
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('KANG ONLINE V14.5 OK');
    return;
  }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocketServer({ server, maxPayload: 65536 });
wss.on('connection', ws => {
  ws.memberId = null; ws.username = null;
  ws.room = null; ws.player = null;
  ws.alive = true; ws.authAttempts = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => enqueue(async () => {
    let data;
    try { data = JSON.parse(raw.toString()); }
    catch { return fail(ws, 'ข้อมูลไม่ถูกต้อง'); }
    try { await handle(ws, data); }
    catch (e) { console.error('Action:', e); fail(ws, 'ระบบขัดข้อง กรุณาลองใหม่'); }
  }));
  ws.on('close', () => enqueue(() => detach(ws, false)));
});
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 15000);
await initDatabase();
server.listen(PORT, '0.0.0.0', () => {
  console.log('KANG ONLINE V14.5 listening on', PORT);
});
