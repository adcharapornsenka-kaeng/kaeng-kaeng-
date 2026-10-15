
import http from "node:http";
import { readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();
const uid = () => randomBytes(10).toString("hex");
const rank = c => c % 13 + 1;
const suit = c => Math.floor(c / 13);
const point = c => Math.min(rank(c), 10);
const score = h => h.reduce((s,c) => s + point(c), 0);
const aceCount = h => h.filter(c => rank(c) === 1).length;

function shuffle(a) {
  for (let i=a.length-1;i>0;i--) {
    const j=Math.floor(Math.random()*(i+1));
    [a[i],a[j]]=[a[j],a[i]];
  }
  return a;
}
function makeDeck() {
  return shuffle(Array.from({length:52},(_,i)=>i));
}
function send(ws,obj) {
  if (ws?.readyState===WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}
function makePlayer(name,bot=false) {
  return {
    id:uid(),token:bot?null:uid(),
    name:bot?name:String(name||"Player").slice(0,24),
    bot,online:bot,ws:null,credits:500,
    hand:[],discards:[]
  };
}
function makeRoom() {
  let code;
  do {
    code=randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));
  return {
    code,players:[],queue:[],
    phase:"lobby",round:0,turn:0,starter:0,
    deck:[],drawn:null,last:null,
    result:null,showdown:null,
    champagneMoney:0,ledger:[],log:[],
    vote:null,botTimer:null
  };
}
function note(r,msg) {
  r.log.unshift(msg);
  r.log=r.log.slice(0,60);
}
function seatOf(r,p) {
  return r.players.findIndex(x=>x.id===p.id);
}
function publicState(r,p) {
  return {
    code:r.code,
    seat:seatOf(r,p),
    players:r.players.map(x=>({
      id:x.id,name:x.name,bot:x.bot,
      online:x.bot||x.online,
      credits:x.credits,
      cardCount:x.hand.length,
      discards:x.discards
    })),
    queue:r.queue.map(x=>x.name),
    queuePosition:r.queue.findIndex(x=>x.id===p.id)+1,
    phase:r.phase,round:r.round,
    turn:r.turn,starter:r.starter,
    deckCount:r.deck.length,
    hand:seatOf(r,p)>=0?p.hand:[],
    drawn:r.drawn?.playerId===p.id?r.drawn.card:null,
    last:r.last,
    result:r.result,
    showdown:r.showdown,
    champagneMoney:r.champagneMoney,
    ledger:r.ledger.slice(0,50),
    log:r.log,
    vote:r.vote?{
      requester:r.vote.requester,
      approvals:r.vote.approvals.size,
      required:r.vote.required.length,
      approved:r.vote.approvals.has(p.id),
      waiting:r.vote.waiting
    }:null
  };
}
function broadcast(r) {
  for (const p of [...r.players,...r.queue]) {
    send(p.ws,{type:"state",state:publicState(r,p)});
  }
}
function fail(p,msg) {
  send(p.ws,{type:"error",message:msg});
}
function ledger(r,from,to,amount,reason) {
  r.ledger.unshift({
    round:r.round,from,to,amount,reason
  });
  r.ledger=r.ledger.slice(0,100);
}
function transfer(r,from,to,amount,reason) {
  from.credits-=amount;
  to.credits+=amount;
  ledger(r,from.name,to.name,amount,reason);
}
function clearBotTimer(r) {
  if (r.botTimer) clearTimeout(r.botTimer);
  r.botTimer=null;
}
function addBots(r) {
  while (r.players.length<5) {
    r.players.push(makePlayer("Bot "+(r.players.length+1),true));
  }
}
function promoteQueue(r) {
  for (let i=0;i<r.players.length&&r.queue.length;i++) {
    if (r.players[i].bot) {
      const newcomer=r.queue.shift();
      newcomer.hand=[];
      newcomer.discards=[];
      r.players[i]=newcomer;
    }
  }
}
function combinations(h) {
  const counts=new Map();
  for (const c of h) {
    counts.set(rank(c),(counts.get(rank(c))||0)+1);
  }
  const groups=[...counts.values()].sort((a,b)=>b-a);
  const ranks=h.map(rank).sort((a,b)=>a-b);
  const five=h.length===5;
  const flush=five&&h.every(c=>suit(c)===suit(h[0]));
  const unique=[...new Set(ranks)];
  const straight=five&&unique.length===5&&(
    ranks[4]-ranks[0]===4 ||
    ranks.join(",")==="1,10,11,12,13"
  );

  const options=[{name:"ไพ่ธรรมดา",price:10}];
  if (groups[0]>=3) options.push({name:"ตอง",price:20});
  if (flush) options.push({name:"สี",price:20});
  if (five&&score(h)===50)
    options.push({name:"เซียน",price:20});
  if (straight) options.push({name:"เรียง",price:20});
  if (five&&groups[0]===3&&groups[1]===2)
    options.push({name:"Full House",price:40});
  if (groups[0]===4)
    options.push({name:"Four of a Kind",price:50});
  if (straight&&flush)
    options.push({name:"Straight Flush",price:60});

  return options.sort((a,b)=>b.price-a.price)[0];
}
function champagne(r,winner) {
  winner.credits-=2;
  r.champagneMoney+=2;
  ledger(r,winner.name,"Champagne Money",2,"ค่ารอบ");
}
function findLowest(r,caller) {
  const n=r.players.length;
  const ci=seatOf(r,caller);
  const minimum=Math.min(...r.players.map(p=>score(p.hand)));

  // แต้มเท่ากัน: คนที่อยู่ก่อนตามลำดับการเล่นชนะ
  // เริ่มตรวจจากคนประกาศแคง แล้ววนตามลำดับที่นั่ง
  for (let step=0;step<n;step++) {
    const p=r.players[(ci+step)%n];
    if (score(p.hand)===minimum) return p;
  }
  return caller;
}
function finishRound(r,caller) {
  clearBotTimer(r);
  const winner=findLowest(r,caller);
  const successful=winner.id===caller.id;
  let amount=0;
  let reason;

  if (successful) {
    const combo=combinations(winner.hand);
    const unit=combo.price+aceCount(winner.hand)*10;
    reason="แคงชนะ: "+combo.name;

    for (const p of r.players) {
      if (p.id===winner.id) continue;
      transfer(r,p,winner,unit,reason);
      amount+=unit;
    }
  } else {
    // แคงไม่ผ่าน: ผู้ประกาศจ่ายแทนทั้งวง
    // โบนัส A คิดจากผู้ชนะแต้มต่ำสุดเท่านั้น
    const unit=10+aceCount(winner.hand)*10;
    amount=unit*(r.players.length-1);
    reason="แคงไม่ผ่าน จ่ายแทนทั้งวง";
    transfer(r,caller,winner,amount,reason);
  }

  champagne(r,winner);
  r.phase="end";
  r.drawn=null;
  r.showdown=r.players.map(p=>({
    name:p.name,hand:[...p.hand],points:score(p.hand)
  }));
  r.result={
    winner:winner.name,
    winnerId:winner.id,
    caller:caller.name,
    successful,amount,
    message:successful
      ?`${winner.name} แคงชนะ รับ ${amount} เครดิต หัก Champagne Money 2`
      :`${caller.name} แคงไม่ผ่าน! ${winner.name} รับ ${amount} เครดิต หัก Champagne Money 2`
  };
  note(r,r.result.message);

  if (r.vote?.waiting) {
    shuffleSeats(r);
  }
  broadcast(r);
}
function shuffleSeats(r) {
  if (!r.vote) return;
  if (r.phase==="play") {
    r.vote.waiting=true;
    return;
  }
  shuffle(r.players);
  r.starter=Math.floor(Math.random()*r.players.length);
  r.vote=null;
  note(r,"ทุกคนยอมรับแล้ว จับที่นั่งใหม่สำเร็จ");
}
function voteStatus(r) {
  if (!r.vote) return;
  if (r.vote.required.every(id=>r.vote.approvals.has(id))) {
    if (r.phase==="play") {
      if (!r.vote.waiting) {
        r.vote.waiting=true;
        note(r,"อนุมัติครบ รอจบรอบเพื่อจับที่นั่งใหม่");
      }
    } else {
      shuffleSeats(r);
    }
  }
  broadcast(r);
}
function requestSeats(r,p) {
  if (r.vote) return fail(p,"มีคำขอจับที่นั่งใหม่อยู่แล้ว");
  if (seatOf(r,p)<0) return fail(p,"ผู้เข้าคิวต้องรอที่นั่งก่อน");
  const required=r.players
    .filter(x=>!x.bot&&x.online)
    .map(x=>x.id);

  r.vote={
    requester:p.name,
    required,
    approvals:new Set([p.id]),
    waiting:false
  };
  note(r,`${p.name} ขอจับที่นั่งใหม่`);
  voteStatus(r);
}
function startRound(r) {
  if (r.phase==="play") return;
  clearBotTimer(r);
  promoteQueue(r);
  addBots(r);

  if (r.vote?.waiting) shuffleSeats(r);

  r.round++;
  r.phase="play";
  r.deck=makeDeck();
  r.drawn=null;
  r.last=null;
  r.result=null;
  r.showdown=null;

  for (const p of r.players) {
    p.hand=r.deck.splice(0,5);
    p.discards=[];
  }

  r.starter=Math.floor(Math.random()*5);
  r.turn=r.starter;
  note(r,`รอบ ${r.round}: ${r.players[r.turn].name} เริ่มเล่น`);
  broadcast(r);
  scheduleBot(r);
}
function advance(r) {
  r.drawn=null;
  r.turn=(r.turn+1)%r.players.length;
  broadcast(r);
  scheduleBot(r);
}
function scheduleBot(r) {
  clearBotTimer(r);
  if (r.phase!=="play") return;
  const p=r.players[r.turn];
  if (!p || (!p.bot&&p.online)) return;

  r.botTimer=setTimeout(()=>{
    if (r.phase!=="play"||r.players[r.turn]?.id!==p.id) return;

    // Bot หรือผู้เล่นออฟไลน์ จั่วหนึ่งใบและทิ้งหนึ่งใบ
    if (!r.deck.length) {
      // สำรับหมด: ตัดสินผู้ชนะแต้มต่ำสุด
      const lowest=Math.min(...r.players.map(x=>score(x.hand)));
      const candidate=r.players.find(x=>score(x.hand)===lowest);
      finishRound(r,candidate);
      return;
    }

    const drawn=r.deck.pop();
    p.hand.push(drawn);
    const discarded=p.hand.pop();
    p.discards.push(discarded);
    r.last={player:p.name,cards:[discarded]};
    note(r,`${p.name} จั่วและทิ้งไพ่`);
    advance(r);
  },1100);
}
function handleAction(r,p,m) {
  const action=m.action;

  if (action==="chat") {
    const msg=String(m.message||"").trim().slice(0,200);
    if (msg) {
      note(r,`${p.name}: ${msg}`);
      broadcast(r);
    }
    return;
  }

  if (action==="credit") {
    const amount=Number(m.amount);
    if (![100,200,300,400,500].includes(amount)) {
      return fail(p,"เลือกเครดิต 100, 200, 300, 400 หรือ 500");
    }
    p.credits+=amount;
    ledger(r,"ระบบ",p.name,amount,"เพิ่มเครดิตจำลอง");
    note(r,`${p.name} เพิ่ม ${amount} เครดิต`);
    broadcast(r);
    return;
  }

  if (action==="shuffleSeats"||action==="requestSeats") {
    requestSeats(r,p);
    return;
  }

  if (action==="approveSeats") {
    if (!r.vote) return fail(p,"ไม่มีคำขอ");
    if (!r.vote.required.includes(p.id)) {
      return fail(p,"คุณไม่มีสิทธิ์ลงคะแนนคำขอนี้");
    }
    r.vote.approvals.add(p.id);
    voteStatus(r);
    return;
  }

  if (action==="rejectSeats") {
    if (!r.vote) return;
    if (!r.vote.required.includes(p.id)) return;
    note(r,`${p.name} ไม่ยอมรับคำขอจับที่นั่งใหม่`);
    r.vote=null;
    broadcast(r);
    return;
  }

  if (action==="start"||action==="next") {
    if (seatOf(r,p)<0) return fail(p,"ผู้เข้าคิวเริ่มเกมไม่ได้");
    if (r.phase==="play") return fail(p,"เกมกำลังเล่นอยู่");
    startRound(r);
    return;
  }

  if (r.phase!=="play") return fail(p,"ยังไม่เริ่มเกม");
  if (seatOf(r,p)<0) return fail(p,"กำลังรอที่นั่ง");
  if (r.players[r.turn]?.id!==p.id) {
    return fail(p,"ยังไม่ถึงตาคุณ");
  }

  if (action==="kang") {
    if (r.drawn) return fail(p,"ต้องทิ้งไพ่ที่จั่วก่อนแคง");
    finishRound(r,p);
    return;
  }

  if (action==="draw") {
    if (r.drawn) return fail(p,"คุณจั่วไพ่แล้ว");
    if (!r.deck.length) return fail(p,"ไพ่หมดสำรับ");
    const card=r.deck.pop();
    p.hand.push(card);
    r.drawn={playerId:p.id,card};
    broadcast(r);
    return;
  }

  if (action==="discard") {
    if (r.drawn?.playerId!==p.id) {
      return fail(p,"ต้องจั่วไพ่ก่อนทิ้ง");
    }
    const cards=Array.isArray(m.cards)?m.cards:[];
    if (cards.length!==1||!p.hand.includes(cards[0])) {
      return fail(p,"เลือกไพ่หนึ่งใบเพื่อทิ้ง");
    }
    const card=cards[0];
    p.hand.splice(p.hand.indexOf(card),1);
    p.discards.push(card);
    r.last={player:p.name,cards:[card]};
    note(r,`${p.name} ทิ้งไพ่`);
    advance(r);
    return;
  }

  if (action==="follow") {
    return fail(p,"ระบบตามไพ่ยังไม่เปิดใช้งานในเวอร์ชันนี้");
  }
}

const server=http.createServer((req,res)=>{
  if (req.url==="/health") {
    res.writeHead(200,{"Content-Type":"text/plain"});
    res.end("KANG ONLINE V12.0 OK");
    return;
  }

  try {
    const html=readFileSync(
      new URL("./index.html",import.meta.url)
    );
    res.writeHead(200,{
      "Content-Type":"text/html; charset=utf-8",
      "Cache-Control":"no-store"
    });
    res.end(html);
  } catch(e) {
    res.writeHead(500);
    res.end("index.html not found");
  }
});

const wss=new WebSocketServer({server});

wss.on("connection",ws=>{
  let currentRoom=null;
  let currentPlayer=null;

  ws.on("message",raw=>{
    let m;
    try {
      m=JSON.parse(String(raw));
    } catch {
      send(ws,{type:"error",message:"ข้อมูลไม่ถูกต้อง"});
      return;
    }

    if (m.action==="create"||m.action==="join") {
      if (currentPlayer) {
        send(ws,{type:"error",message:"คุณอยู่ในห้องแล้ว"});
        return;
      }

      let r;
      if (m.action==="create") {
        r=makeRoom();
        rooms.set(r.code,r);
      } else {
        r=rooms.get(String(m.code||"").trim().toUpperCase());
      }

      if (!r) {
        send(ws,{type:"error",message:"ไม่พบห้อง"});
        return;
      }

      let p=null;
      if (m.token) {
        p=[...r.players,...r.queue].find(
          x=>x.token===m.token
        );
      }

      if (!p) {
        p=makePlayer(m.name);
        if (r.players.length<5) {
          r.players.push(p);
        } else {
          const botIndex=r.players.findIndex(x=>x.bot);
          if (botIndex>=0) {
            const old=r.players[botIndex];
            p.hand=[...old.hand];
            p.discards=[...old.discards];
            p.credits=old.credits;
            r.players[botIndex]=p;
            if (r.phase==="play"&&r.turn===botIndex) {
              clearBotTimer(r);
            }
          } else {
            r.queue.push(p);
          }
        }
      }

      if (p.ws&&p.ws!==ws) {
        p.ws.close();
      }
      p.ws=ws;
      p.online=true;
      currentRoom=r;
      currentPlayer=p;

      send(ws,{
        type:"joined",
        code:r.code,
        token:p.token
      });
      note(r,`${p.name} เข้าห้อง`);
      broadcast(r);

      if (r.phase==="play") scheduleBot(r);
      return;
    }

    if (!currentRoom||!currentPlayer) {
      send(ws,{type:"error",message:"กรุณาเข้าห้องก่อน"});
      return;
    }

    handleAction(currentRoom,currentPlayer,m);
  });

  ws.on("close",()=>{
    const r=currentRoom;
    const p=currentPlayer;
    if (!r||!p||p.ws!==ws) return;

    p.online=false;
    p.ws=null;

    // ผู้ที่อยู่ในคิวและออกจากห้อง ถูกนำออกจากคิว
    const qi=r.queue.findIndex(x=>x.id===p.id);
    if (qi>=0) r.queue.splice(qi,1);

    if (r.vote&&!r.vote.waiting) {
      r.vote.required=r.vote.required.filter(
        id=>id!==p.id
      );
      voteStatus(r);
    }

    note(r,`${p.name} ออฟไลน์`);
    broadcast(r);
    if (r.phase==="play") scheduleBot(r);
  });

  ws.on("error",()=>{});
});

server.listen(PORT,"0.0.0.0",()=>{
  console.log(`KANG ONLINE V12.0 running on ${PORT}`);
});
