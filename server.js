
import http from "node:http";
import { readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const uid = () => randomBytes(10).toString("hex");
const rank = c => c % 13;
const suit = c => Math.floor(c / 13);
const value = c => Math.min(rank(c) + 1, 10);
const points = hand => hand.reduce((n,c) => n + value(c), 0);
const aces = hand => hand.filter(c => rank(c) === 0).length;

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i],a[j]] = [a[j],a[i]];
  }
  return a;
}

function send(ws,data) {
  if (ws?.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function makePlayer(name,bot=false) {
  return {
    id:uid(),
    token:bot ? null : uid(),
    name:bot ? name : String(name || "Player").slice(0,24),
    bot,
    online:bot,
    ws:null,
    credits:500,
    hand:[],
    discards:[]
  };
}

function makeRoom() {
  let code;
  do {
    code = randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));

  return {
    code,
    players:[],
    queue:[],
    phase:"lobby",
    round:0,
    turn:0,
    starter:0,
    deck:[],
    drawn:null,
    last:null,
    result:null,
    showdown:null,
    champagneMoney:0,
    log:[],
    ledger:[],
    vote:null,
    timer:null
  };
}

function seat(r,p) {
  return r.players.findIndex(x => x.id === p.id);
}

function note(r,msg) {
  r.log.unshift(msg);
  r.log = r.log.slice(0,60);
}

function record(r,from,to,amount,reason) {
  r.ledger.unshift({
    round:r.round,
    from,
    to,
    amount,
    reason
  });
  r.ledger = r.ledger.slice(0,100);
}

function transfer(r,from,to,amount,reason) {
  from.credits -= amount;
  to.credits += amount;
  record(r,from.name,to.name,amount,reason);
}

function error(p,msg) {
  send(p.ws,{type:"error",message:msg});
}

function state(r,p) {
  return {
    code:r.code,
    seat:seat(r,p),
    players:r.players.map(x => ({
      id:x.id,
      name:x.name,
      bot:x.bot,
      online:x.bot || x.online,
      credits:x.credits,
      cardCount:x.hand.length,
      discards:x.discards
    })),
    queue:r.queue.map(x => x.name),
    queuePosition:r.queue.findIndex(x => x.id === p.id) + 1,
    phase:r.phase,
    round:r.round,
    turn:r.turn,
    starter:r.starter,
    deckCount:r.deck.length,
    hand:seat(r,p) >= 0 ? p.hand : [],
    drawn:r.drawn?.playerId === p.id ? r.drawn.card : null,
    last:r.last,
    result:r.result,
    showdown:r.showdown,
    champagneMoney:r.champagneMoney,
    log:r.log,
    ledger:r.ledger.slice(0,30),
    vote:r.vote ? {
      requester:r.vote.requester,
      approvals:r.vote.approvals.size,
      required:r.vote.required.length,
      approved:r.vote.approvals.has(p.id),
      waiting:r.vote.waiting
    } : null
  };
}

function broadcast(r) {
  for (const p of [...r.players,...r.queue]) {
    send(p.ws,{type:"state",state:state(r,p)});
  }
}

function stopTimer(r) {
  if (r.timer) clearTimeout(r.timer);
  r.timer = null;
}

function fillBots(r) {
  while (r.players.length < 5) {
    r.players.push(makePlayer("Bot " + (r.players.length + 1),true));
  }
}

function promoteQueue(r) {
  for (let i=0;i<r.players.length && r.queue.length;i++) {
    if (r.players[i].bot) {
      r.players[i] = r.queue.shift();
    }
  }
}

function combination(hand) {
  const counts = {};
  for (const c of hand) {
    counts[rank(c)] = (counts[rank(c)] || 0) + 1;
  }

  const freq = Object.values(counts).sort((a,b) => b-a);
  const rr = hand.map(c => rank(c)+1).sort((a,b) => a-b);
  const five = hand.length === 5;
  const flush = five && hand.every(c => suit(c) === suit(hand[0]));
  const straight = five && new Set(rr).size === 5 &&
    (rr[4]-rr[0] === 4 || rr.join(",") === "1,10,11,12,13");

  const options = [{name:"ธรรมดา",price:10}];

  if (freq[0] >= 3) options.push({name:"ตอง",price:20});
  if (flush) options.push({name:"สี",price:20});
  if (five && points(hand) === 50) options.push({name:"เซียน",price:20});
  if (straight) options.push({name:"เรียง",price:20});
  if (five && freq[0] === 3 && freq[1] === 2) {
    options.push({name:"Full House",price:40});
  }
  if (freq[0] === 4) options.push({name:"Four of a Kind",price:50});
  if (straight && flush) {
    options.push({name:"Straight Flush",price:60});
  }

  return options.sort((a,b) => b.price-a.price)[0];
}

function champagne(r,winner) {
  winner.credits -= 2;
  r.champagneMoney += 2;
  record(r,winner.name,"Champagne Money",2,"ค่ารอบผู้ชนะ");
}

function shuffleSeats(r) {
  if (!r.vote) return;

  if (r.phase === "play") {
    r.vote.waiting = true;
    return;
  }

  shuffle(r.players);
  r.starter = Math.floor(Math.random()*r.players.length);
  r.vote = null;
  note(r,"🔀 จับที่นั่งใหม่สำเร็จ");
}

function checkVote(r) {
  if (!r.vote) return;

  if (r.vote.required.every(id => r.vote.approvals.has(id))) {
    if (r.phase === "play") {
      if (!r.vote.waiting) {
        r.vote.waiting = true;
        note(r,"ทุกคนยอมรับแล้ว รอจบรอบ");
      }
    } else {
      shuffleSeats(r);
    }
  }
  broadcast(r);
}

function requestSeats(r,p) {
  if (r.vote) return error(p,"มีคำขอจับที่นั่งใหม่อยู่แล้ว");
  if (seat(r,p) < 0) return error(p,"ต้องมีที่นั่งก่อน");

  const required = r.players
    .filter(x => !x.bot && x.online)
    .map(x => x.id);

  r.vote = {
    requester:p.name,
    required,
    approvals:new Set([p.id]),
    waiting:false
  };

  note(r,`${p.name} ขอจับที่นั่งใหม่`);
  checkVote(r);
}

function finishRound(r,caller) {
  stopTimer(r);

  const ci = seat(r,caller);
  const min = Math.min(...r.players.map(p => points(p.hand)));

  let winner = caller;
  for (let step=0;step<r.players.length;step++) {
    const p = r.players[(ci+step)%r.players.length];
    if (points(p.hand) === min) {
      winner = p;
      break;
    }
  }

  const successful = winner.id === caller.id;
  let amount = 0;

  if (successful) {
    const combo = combination(winner.hand);
    const unit = combo.price + aces(winner.hand)*10;

    for (const p of r.players) {
      if (p.id === winner.id) continue;
      transfer(r,p,winner,unit,"แคงชนะ: "+combo.name);
      amount += unit;
    }
  } else {
    amount = (10 + aces(winner.hand)*10)*(r.players.length-1);
    transfer(r,caller,winner,amount,"แคงไม่ผ่าน จ่ายแทนทั้งวง");
  }

  champagne(r,winner);

  r.phase = "end";
  r.drawn = null;
  r.showdown = r.players.map(p => ({
    name:p.name,
    hand:[...p.hand],
    points:points(p.hand)
  }));

  r.result = {
    winner:winner.name,
    successful,
    amount,
    message:successful
      ? `${winner.name} แคงชนะ รับ ${amount} เครดิต`
      : `${caller.name} แคงไม่ผ่าน! ${winner.name} รับ ${amount} เครดิต`
  };

  note(r,r.result.message);

  if (r.vote?.waiting) shuffleSeats(r);
  broadcast(r);
}

function startRound(r) {
  if (r.phase === "play") return;

  stopTimer(r);
  promoteQueue(r);
  fillBots(r);

  if (r.vote?.waiting) shuffleSeats(r);

  r.round++;
  r.phase = "play";
  r.deck = shuffle(Array.from({length:52},(_,i) => i));
  r.drawn = null;
  r.last = null;
  r.result = null;
  r.showdown = null;

  for (const p of r.players) {
    p.hand = r.deck.splice(0,5);
    p.discards = [];
  }

  r.starter = Math.floor(Math.random()*r.players.length);
  r.turn = r.starter;

  note(r,`🃏 เริ่มรอบ ${r.round}`);
  broadcast(r);
  scheduleBot(r);
}

function advance(r) {
  r.drawn = null;
  r.turn = (r.turn+1)%r.players.length;
  broadcast(r);
  scheduleBot(r);
}

function tripleBonus(r,p) {
  for (const other of r.players) {
    if (other.id !== p.id) {
      transfer(r,other,p,10,"โบนัสทิ้งตองระหว่างเล่น");
    }
  }
  note(r,`🎉 ${p.name} ทิ้งตอง รับ 40 เครดิต!`);
}

function discardCards(r,p,cards) {
  if (r.drawn?.playerId !== p.id) {
    return error(p,"ต้องจั่วก่อนทิ้งไพ่");
  }

  if (!Array.isArray(cards) || cards.length < 1 || cards.length > 3) {
    return error(p,"ทิ้งไพ่ได้ครั้งละ 1–3 ใบ");
  }

  if (new Set(cards).size !== cards.length) {
    return error(p,"เลือกไพ่ซ้ำไม่ได้");
  }

  if (cards.some(c => !Number.isInteger(c) || !p.hand.includes(c))) {
    return error(p,"ไพ่ที่เลือกไม่อยู่ในมือ");
  }

  if (cards.length > 1 &&
      !cards.every(c => rank(c) === rank(cards[0]))) {
    return error(p,"ทิ้งพร้อมกันต้องเป็นไพ่หน้าเดียวกัน");
  }

  // หลังจั่วมี 6 ใบ
  // ทิ้ง 2–3 ใบ แล้วจั่วชดเชยให้เหลือ 5 ใบ
  const needed = Math.max(0,5-(p.hand.length-cards.length));

  if (r.deck.length < needed) {
    return error(p,"ไพ่ในสำรับไม่พอ");
  }

  for (const c of cards) {
    p.hand.splice(p.hand.indexOf(c),1);
    p.discards.push(c);
  }

  r.last = {player:p.name,cards:[...cards]};
  note(r,`${p.name} ทิ้งไพ่ ${cards.length} ใบ`);

  if (cards.length === 3) {
    tripleBonus(r,p);
  }

  for (let i=0;i<needed;i++) {
    p.hand.push(r.deck.pop());
  }

  advance(r);
}

function scheduleBot(r) {
  stopTimer(r);
  if (r.phase !== "play") return;

  const p = r.players[r.turn];
  if (!p || (!p.bot && p.online)) return;

  r.timer = setTimeout(() => {
    if (r.phase !== "play" || r.players[r.turn]?.id !== p.id) return;

    if (!r.deck.length) {
      // สำรับหมด: จบโดยให้ผู้แต้มต่ำสุดชนะ
      const min = Math.min(...r.players.map(x => points(x.hand)));
      const winner = r.players.find(x => points(x.hand) === min);
      finishRound(r,winner);
      return;
    }

    const drawn = r.deck.pop();
    p.hand.push(drawn);

    // Bot เลือกทิ้งตองได้ถ้ามีไพ่หน้าเดียวกัน 3 ใบ
    let chosen = null;
    const groups = new Map();

    for (const c of p.hand) {
      const k = rank(c);
      if (!groups.has(k)) groups.set(k,[]);
      groups.get(k).push(c);
    }

    for (const group of groups.values()) {
      if (group.length >= 3) {
        chosen = group.slice(0,3);
        break;
      }
    }

    if (!chosen) {
      chosen = [p.hand.reduce((best,c) =>
        value(c)>value(best)?c:best
      )];
    }

    r.drawn = {playerId:p.id,card:drawn};
    discardCards(r,p,chosen);
  },1200);
}

function handle(r,p,m) {
  const action = m.action;

  if (action === "chat") {
    const msg = String(m.message||"").trim().slice(0,200);
    if (msg) note(r,`${p.name}: ${msg}`);
    broadcast(r);
    return;
  }

  if (action === "credit") {
    const n = Number(m.amount);
    if (![100,200,300,400,500].includes(n)) {
      return error(p,"จำนวนเครดิตไม่ถูกต้อง");
    }
    p.credits += n;
    record(r,"ระบบ",p.name,n,"เพิ่มเครดิตจำลอง");
    broadcast(r);
    return;
  }

  if (action === "shuffleSeats" || action === "requestSeats") {
    requestSeats(r,p);
    return;
  }

  if (action === "approveSeats") {
    if (!r.vote) return error(p,"ไม่มีคำขอ");
    if (!r.vote.required.includes(p.id)) {
      return error(p,"ไม่มีสิทธิ์ลงคะแนน");
    }
    r.vote.approvals.add(p.id);
    checkVote(r);
    return;
  }

  if (action === "rejectSeats") {
    if (!r.vote) return;
    if (!r.vote.required.includes(p.id)) return;
    note(r,`${p.name} ไม่ยอมรับการจับที่นั่งใหม่`);
    r.vote = null;
    broadcast(r);
    return;
  }

  if (action === "start" || action === "next") {
    if (seat(r,p) < 0) return error(p,"กำลังรอที่นั่ง");
    if (r.phase === "play") return error(p,"เกมกำลังเล่นอยู่");
    startRound(r);
    return;
  }

  if (r.phase !== "play") return error(p,"ยังไม่เริ่มเกม");
  if (seat(r,p) < 0) return error(p,"กำลังรอที่นั่ง");
  if (r.players[r.turn]?.id !== p.id) {
    return error(p,"ยังไม่ถึงตาคุณ");
  }

  if (action === "kang") {
    if (r.drawn) return error(p,"ต้องทิ้งไพ่ก่อนแคง");
    finishRound(r,p);
    return;
  }

  if (action === "draw") {
    if (r.drawn) return error(p,"คุณจั่วไพ่แล้ว");
    if (!r.deck.length) return error(p,"ไพ่หมดสำรับ");

    const card = r.deck.pop();
    p.hand.push(card);
    r.drawn = {playerId:p.id,card};
    broadcast(r);
    return;
  }

  if (action === "discard") {
    discardCards(r,p,m.cards);
    return;
  }

  if (action === "follow") {
    return error(p,"ระบบตามไพ่ยังไม่เปิดใช้งาน");
  }
}

const server = http.createServer((req,res) => {
  if (req.url === "/health") {
    res.writeHead(200,{"Content-Type":"text/plain"});
    res.end("KANG ONLINE V12.1 OK");
    return;
  }

  try {
    const html = readFileSync(new URL("./index.html",import.meta.url));
    res.writeHead(200,{
      "Content-Type":"text/html; charset=utf-8",
      "Cache-Control":"no-store"
    });
    res.end(html);
  } catch {
    res.writeHead(500);
    res.end("index.html not found");
  }
});

const wss = new WebSocketServer({server});

wss.on("connection",ws => {
  let room = null;
  let current = null;

  ws.on("message",raw => {
    let m;
    try {
      m = JSON.parse(String(raw));
    } catch {
      send(ws,{type:"error",message:"ข้อมูลไม่ถูกต้อง"});
      return;
    }

    if (m.action === "create" || m.action === "join") {
      if (current) {
        send(ws,{type:"error",message:"คุณอยู่ในห้องแล้ว"});
        return;
      }

      let r;
      if (m.action === "create") {
        r = makeRoom();
        rooms.set(r.code,r);
      } else {
        r = rooms.get(String(m.code||"").trim().toUpperCase());
      }

      if (!r) {
        send(ws,{type:"error",message:"ไม่พบห้อง"});
        return;
      }

      let p = null;

      if (m.token) {
        p = [...r.players,...r.queue].find(
          x => x.token === m.token
        );
      }

      if (!p) {
        p = makePlayer(m.name);

        if (r.players.length < 5) {
          r.players.push(p);
        } else {
          const bi = r.players.findIndex(x => x.bot);

          if (bi >= 0) {
            const old = r.players[bi];
            p.hand = [...old.hand];
            p.discards = [...old.discards];
            p.credits = old.credits;
            r.players[bi] = p;

            if (r.phase === "play" && r.turn === bi) {
              stopTimer(r);
            }
          } else {
            r.queue.push(p);
          }
        }
      }

      if (p.ws && p.ws !== ws) p.ws.close();

      p.ws = ws;
      p.online = true;
      room = r;
      current = p;

      send(ws,{
        type:"joined",
        code:r.code,
        token:p.token
      });

      note(r,`${p.name} เข้าห้อง`);
      broadcast(r);

      if (r.phase === "play") scheduleBot(r);
      return;
    }

    if (!room || !current) {
      send(ws,{type:"error",message:"กรุณาเข้าห้องก่อน"});
      return;
    }

    handle(room,current,m);
  });

  ws.on("close",() => {
    if (!room || !current || current.ws !== ws) return;

    current.online = false;
    current.ws = null;

    const qi = room.queue.findIndex(x => x.id === current.id);
    if (qi >= 0) room.queue.splice(qi,1);

    if (room.vote && !room.vote.waiting) {
      room.vote.required = room.vote.required.filter(
        id => id !== current.id
      );
      checkVote(room);
    }

    note(room,`${current.name} ออฟไลน์`);
    broadcast(room);

    if (room.phase === "play") scheduleBot(room);
  });

  ws.on("error",() => {});
});

server.listen(PORT,"0.0.0.0",() => {
  console.log("KANG ONLINE V12.1 running on port "+PORT);
});
