import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {WebSocketServer, WebSocket} from 'ws';
import {randomBytes} from 'node:crypto';

const PORT=Number(process.env.PORT||3000), rooms=new Map(), MAX_LOG=100;
const uid=()=>randomBytes(12).toString('hex');
const rank=c=>c%13+1, suit=c=>Math.floor(c/13), points=c=>Math.min(rank(c),10);
const score=hand=>hand.reduce((n,c)=>n+points(c),0);
const aces=hand=>hand.filter(c=>rank(c)===1).length;
const shuffle=a=>{for(let i=a.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[a[i],a[j]]=[a[j],a[i]];}return a;};
const clean=s=>String(s??'').trim().slice(0,32);
const roomCode=()=>{let s;do{s=randomBytes(3).toString('hex').toUpperCase()}while(rooms.has(s));return s;};
const human=(name,token)=>({id:uid(),token:token||uid(),name:clean(name)||'Player',bot:false,online:true,credits:0,hand:[],discards:[],ws:null});
const bot=i=>({id:uid(),token:null,name:`BOT ${i}`,bot:true,online:true,credits:0,hand:[],discards:[],ws:null});
const msg=(ws,data)=>{if(ws?.readyState===WebSocket.OPEN)ws.send(JSON.stringify(data));};
const error=(ws,text)=>msg(ws,{type:'error',message:text});
const record=(r,text)=>{r.log.push(text);if(r.log.length>MAX_LOG)r.log.shift();};
const ledger=(r,from,to,amount,reason)=>{r.ledger.push({round:r.round,from,to,amount,reason});if(r.ledger.length>250)r.ledger.shift();};
const transfer=(r,from,to,amount,reason)=>{if(amount<=0)return;from.credits-=amount;to.credits+=amount;ledger(r,from.name,to.name,amount,reason);};
const category=hand=>{
  const n=hand.length,rs=hand.map(rank),ss=hand.map(suit),counts=new Map();rs.forEach(x=>counts.set(x,(counts.get(x)||0)+1));
  const nums=[...counts.values()].sort((a,b)=>b-a),flush=n===5&&new Set(ss).size===1;
  const sorted=[...new Set(rs)].sort((a,b)=>a-b);
  const straight=n===5&&sorted.length===5&&(sorted[4]-sorted[0]===4||sorted.join(',')==='1,10,11,12,13');
  if(straight&&flush)return {name:'Straight Flush',rate:60};
  if(n===4&&nums[0]===4)return {name:'Four of a Kind',rate:50};
  if(n===5&&nums[0]===3&&nums[1]===2)return {name:'Full House',rate:40};
  if(n===5&&flush)return {name:'Flush (สี)',rate:20};
  if(n===5&&straight)return {name:'Straight (เรียง)',rate:20};
  if(n===5&&score(hand)===50)return {name:'เซียน 50 แต้ม',rate:20};
  if(n===3&&nums[0]===3)return {name:'ตอง',rate:20};
  return {name:'ธรรมดา',rate:10};
};
const rate=hand=>category(hand).rate+10*aces(hand);
const next=(i)=>(i+1)%5;
const inOrder=(r,ids,anchor)=>{for(let j=1;j<=5;j++){let i=(anchor+j)%5;if(ids.includes(i))return i;}return ids[0];};
function state(r,p){return {type:'state',code:r.code,seat:r.players.indexOf(p),phase:r.phase,round:r.round,turn:r.turn,starter:r.starter,deckCount:r.deck.length,hand:p.hand,players:r.players.map(x=>({id:x.id,name:x.name,bot:x.bot,online:x.online,credits:x.credits,cardCount:x.hand.length,discards:x.discards})),last:r.last,actionTaken:r.actionTaken,champagneMoney:r.champagneMoney,ledger:r.ledger.slice(-50),log:r.log.slice(-35),result:r.result,showdown:r.showdown,queue:r.queue.map(x=>x.name),vote:r.vote?{requester:r.vote.requester,approvals:[...r.vote.approvals],required:r.vote.required,waiting:r.vote.waiting}:null};}
function broadcast(r){for(const p of r.players)if(!p.bot)msg(p.ws,state(r,p));for(const q of r.queue)msg(q.ws,{type:'queue',code:r.code,position:r.queue.indexOf(q)+1});}
function stopBot(r){if(r.botTimer)clearTimeout(r.botTimer);r.botTimer=null;}
function botTurn(r){stopBot(r);if(r.phase!=='play'||!r.players[r.turn]?.bot)return;r.botTimer=setTimeout(()=>{if(r.phase!=='play'||!r.players[r.turn]?.bot)return;const p=r.players[r.turn];if(!r.actionTaken){const available=r.last?.player===((r.turn+4)%5)?r.last.cards:[];const good=available?.filter(c=>points(c)<=4)||[];if(good.length&&Math.random()<.65){follow(r,p,good.slice(0,Math.min(3,good.length)));}else if(r.deck.length){draw(r,p);}else if(available?.length){follow(r,p,[available[0]]);}else{finishDeck(r);return;}}if(r.phase!=='play')return;const groups=new Map();p.hand.forEach(c=>{const k=rank(c);groups.set(k,[...(groups.get(k)||[]),c]);});const triple=[...groups.values()].find(g=>g.length>=3);const pair=[...groups.values()].find(g=>g.length>=2);const chosen=triple?.slice(0,3)||pair?.slice(0,2)||[...p.hand].sort((a,b)=>points(b)-points(a)).slice(0,1);discard(r,p,chosen);},650);}
function finish(r,winner,reason,failed=false,caller=null){if(r.phase!=='play')return;stopBot(r);const w=r.players[winner];const before=r.players.map(p=>({name:p.name,hand:[...p.hand],points:score(p.hand),count:p.hand.length}));let amount;
  if(failed){amount=(10+10*aces(w.hand))*4;transfer(r,caller,w,amount,'แคงไม่สำเร็จ');}
  else{amount=rate(w.hand);for(const p of r.players)if(p!==w)transfer(r,p,w,amount,reason);}
  w.credits-=2;r.champagneMoney+=2;ledger(r,w.name,'Champagne Pool',2,'Champagne Money');
  r.result={winner,reason,failed,amount,category:category(w.hand).name,message:`🏆 ${w.name} ชนะ — ${reason}`};r.showdown=before;r.phase='end';r.actionTaken=false;record(r,r.result.message);broadcast(r);
  if(r.vote?.waiting&&r.vote.approvals.size===r.vote.required.length)r.pendingShuffle=true;
}
function finishDeck(r){if(r.phase!=='play')return;const min=Math.min(...r.players.map(p=>p.hand.length));const ids=r.players.map((p,i)=>p.hand.length===min?i:-1).filter(i=>i>=0);finish(r,ids.length===1?ids[0]:inOrder(r,ids,r.turn),'กองจั่วหมด');}
function begin(r){stopBot(r);if(r.pendingShuffle){shuffle(r.players);r.pendingShuffle=false;r.vote=null;record(r,'จับที่นั่งใหม่สำเร็จ');}r.round++;r.phase='play';r.deck=shuffle(Array.from({length:52},(_,i)=>i));r.players.forEach(p=>{p.hand=r.deck.splice(0,5);p.discards=[];});r.starter=Math.floor(Math.random()*5);r.turn=r.starter;r.last=null;r.actionTaken=false;r.result=null;r.showdown=null;record(r,`เริ่มรอบ ${r.round} — ${r.players[r.turn].name} เริ่ม`);broadcast(r);botTurn(r);}
function follow(r,p,cards){if(r.phase!=='play'||r.players[r.turn]!==p||r.actionTaken)return false;const prev=(r.turn+4)%5;if(r.last?.player!==prev||!r.last.cards.length)return false;if(!Array.isArray(cards)||cards.length<1||cards.length>3||new Set(cards).size!==cards.length||!cards.every(c=>r.last.cards.includes(c)))return false;const owner=r.players[prev];for(const c of cards){const i=owner.discards.indexOf(c);if(i<0)return false;}for(const c of cards){owner.discards.splice(owner.discards.indexOf(c),1);r.last.cards.splice(r.last.cards.indexOf(c),1);p.hand.push(c);}r.actionTaken=true;record(r,`${p.name} ตามไพ่ ${cards.length} ใบ`);broadcast(r);return true;}
function draw(r,p){if(r.phase!=='play'||r.players[r.turn]!==p||r.actionTaken||!r.deck.length)return false;p.hand.push(r.deck.pop());r.actionTaken=true;record(r,`${p.name} จั่วไพ่ 1 ใบ`);broadcast(r);return true;}
function discard(r,p,cards){if(r.phase!=='play'||r.players[r.turn]!==p||!r.actionTaken||!Array.isArray(cards)||cards.length<1||cards.length>3||new Set(cards).size!==cards.length||!cards.every(c=>p.hand.includes(c))||(cards.length>1&&new Set(cards.map(rank)).size!==1))return false;
  for(const c of cards)p.hand.splice(p.hand.indexOf(c),1);p.discards.push(...cards);r.last={player:r.turn,cards:[...cards]};
  if(cards.length===3){for(const other of r.players)if(other!==p)transfer(r,other,p,10,'โบนัสทิ้งตอง');record(r,`${p.name} ทิ้งตอง รับ 40 เครดิต`);}else record(r,`${p.name} ทิ้ง ${cards.length} ใบ`);
  if(p.hand.length===0){finish(r,r.turn,'ไพ่หมดมือ');return true;}
  r.turn=next(r.turn);r.actionTaken=false;
  if(r.deck.length===0){finishDeck(r);return true;}
  broadcast(r);botTurn(r);return true;}
function kang(r,p){if(r.phase!=='play'||r.players[r.turn]!==p||r.actionTaken)return false;const caller=r.turn,s=score(p.hand),lower=r.players.map((x,i)=>({i,s:score(x.hand)})).filter(x=>x.i!==caller&&x.s<s);if(lower.length){const min=Math.min(...lower.map(x=>x.s));const tied=lower.filter(x=>x.s===min).map(x=>x.i);finish(r,inOrder(r,tied,caller),'แคงไม่สำเร็จ',true,p);}else finish(r,caller,'แคงสำเร็จ');return true;}
function attach(r,p,ws){if(p.ws&&p.ws!==ws)try{p.ws.close();}catch{}p.ws=ws;p.online=true;ws.room=r;ws.player=p;broadcast(r);}
function promote(r){while(r.queue.length){const idx=r.players.findIndex(x=>x.bot);if(idx<0)break;const q=r.queue.shift(),b=r.players[idx];q.hand=b.hand;q.discards=b.discards;q.credits=b.credits;r.players[idx]=q;attach(r,q,q.ws);record(r,`${q.name} เข้ามาแทน ${b.name}`);}}
function voteStart(r,p){if(r.vote)return false;const required=r.players.filter(x=>!x.bot&&x.online).map(x=>x.id);r.vote={requester:p.name,approvals:new Set([p.id]),required,waiting:r.phase==='play'};if(r.vote.approvals.size===required.length){r.pendingShuffle=true;r.vote=null;}broadcast(r);return true;}
function handle(ws,data){const action=data?.action;if(action==='create'){const p=human(data.name,data.token),code=roomCode(),r={code,players:[p,...Array.from({length:4},(_,i)=>bot(i+1))],queue:[],phase:'lobby',round:0,turn:0,starter:0,deck:[],last:null,actionTaken:false,champagneMoney:0,ledger:[],log:[],result:null,showdown:null,vote:null,pendingShuffle:false,botTimer:null};rooms.set(code,r);attach(r,p,ws);msg(ws,{type:'identity',code,token:p.token});return;}
  if(action==='join'){const r=rooms.get(clean(data.code).toUpperCase());if(!r)return error(ws,'ไม่พบห้อง');const token=String(data.token||'');let p=r.players.find(x=>!x.bot&&x.token===token)||r.queue.find(x=>x.token===token);if(p){attach(r,p,ws);msg(ws,{type:'identity',code:r.code,token:p.token});return;}p=human(data.name);const idx=r.players.findIndex(x=>x.bot);if(idx>=0){const old=r.players[idx];p.hand=old.hand;p.discards=old.discards;p.credits=old.credits;r.players[idx]=p;attach(r,p,ws);record(r,`${p.name} เข้ามาแทน ${old.name}`);}else{r.queue.push(p);p.ws=ws;ws.room=r;ws.player=p;broadcast(r);}msg(ws,{type:'identity',code:r.code,token:p.token});return;}
  const r=ws.room,p=ws.player;if(!r||!p)return error(ws,'กรุณาเข้าห้องก่อน');if(!r.players.includes(p))return error(ws,'คุณอยู่ในคิวรอ');let ok=false;
  if(action==='start'||action==='next'){if((action==='start'&&r.phase==='lobby')||(action==='next'&&r.phase==='end')){begin(r);ok=true;}}
  else if(action==='draw')ok=draw(r,p);
  else if(action==='follow')ok=follow(r,p,data.cards);
  else if(action==='discard')ok=discard(r,p,data.cards);
  else if(action==='kang')ok=kang(r,p);
  else if(action==='credit'){const amount=Number(data.amount);if([100,200,300,400,500].includes(amount)){p.credits+=amount;ledger(r,'ระบบ',p.name,amount,'เติมเครดิตจำลอง');broadcast(r);ok=true;}}
  else if(action==='chat'){const t=String(data.text||'').trim().slice(0,250);if(t){record(r,`💬 ${p.name}: ${t}`);broadcast(r);ok=true;}}
  else if(action==='requestSeats')ok=voteStart(r,p);
  else if(action==='approveSeats'){if(r.vote&&r.vote.required.includes(p.id)){r.vote.approvals.add(p.id);if(r.vote.approvals.size===r.vote.required.length){r.pendingShuffle=true;record(r,'ทุกคนอนุมัติจับที่นั่งใหม่');r.vote=null;}broadcast(r);ok=true;}}
  else if(action==='rejectSeats'){if(r.vote&&r.vote.required.includes(p.id)){r.vote=null;record(r,`${p.name} ไม่อนุมัติจับที่นั่งใหม่`);broadcast(r);ok=true;}}
  if(!ok)error(ws,'ทำรายการไม่ได้ กรุณาตรวจสอบตาเล่นหรือเงื่อนไข');}
const server=http.createServer(async(req,res)=>{if(req.url==='/'||req.url==='/index.html'){try{const html=await readFile(new URL('./index.html',import.meta.url));res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});res.end(html);}catch{res.writeHead(500);res.end('Missing index.html');}}else if(req.url==='/health'){res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({ok:true}));}else{res.writeHead(404);res.end('Not found');}});
const wss=new WebSocketServer({server});wss.on('connection',ws=>{ws.on('message',raw=>{try{const data=JSON.parse(raw.toString());if(raw.length>20000)return error(ws,'ข้อความยาวเกินไป');handle(ws,data);}catch(e){error(ws,'รูปแบบข้อมูลไม่ถูกต้อง');}});ws.on('close',()=>{const r=ws.room,p=ws.player;if(!r||!p||p.ws!==ws)return;p.online=false;p.ws=null;record(r,`${p.name} ออฟไลน์`);broadcast(r);});});
server.listen(PORT,'0.0.0.0',()=>console.log(`KANG ONLINE V12.1 running on ${PORT}`));
