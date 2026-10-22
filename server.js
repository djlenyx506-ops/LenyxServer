// Servidor de Lenyx Live v2.1 (Con extracción de PNG de regalos)
// App (tu teléfono) <-> este servidor <-> WebSocket gestionado de Euler Stream <-> TikTok LIVE
import http from 'node:http';
import crypto from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 3000;
const APP_KEY = process.env.APP_KEY || '';               
const EULER_API_KEY = process.env.EULER_API_KEY || '';   
const ALLOWED = (process.env.ALLOWED_USERS || '')        
  .split(',').map((s) => s.trim().toLowerCase().replace(/^@/, '')).filter(Boolean);

// BEGIN_AUTH
const USERS = new Map();
for (const item of (process.env.USERS || '').split(',')) {
  const [u, p, exp] = item.trim().split(':');
  if (!u || !p) continue;
  const e = exp ? Date.parse(exp.trim() + 'T23:59:59Z') : 0;
  USERS.set(u.trim().toLowerCase(), { p: p.trim(), exp: Number.isNaN(e) ? 0 : e });
}

const SECRET = process.env.TOKEN_SECRET || APP_KEY || EULER_API_KEY || 'cambia-esto';
const b64 = (x) => Buffer.from(x).toString('base64url');
const hmac = (x) => crypto.createHmac('sha256', SECRET).update(x).digest('base64url');
const safeEq = (a, b) => { 
  const x = Buffer.from(String(a)), y = Buffer.from(String(b)); 
  return x.length === y.length && crypto.timingSafeEqual(x, y); 
};

function signToken(u, exp) { 
  const p = b64(JSON.stringify({ u, exp })); 
  return p + '.' + hmac(p); 
}

function verifyToken(t) {
  try {
    const [p, sig] = String(t || '').split('.');
    if (!p || !sig || !safeEq(sig, hmac(p))) return null;
    const { u, exp } = JSON.parse(Buffer.from(p, 'base64url').toString());
    if (!u || !exp || Date.now() > exp) return null;
    const rec = USERS.get(u);
    if (!rec || (rec.exp && Date.now() > rec.exp)) return null;
    return u;
  } catch { return null; }
}

const fails = new Map();
function blocked(ip) { const f = fails.get(ip); return !!(f && f.until && Date.now() < f.until); }
function fail(ip) { 
  const f = fails.get(ip) || { n: 0, until: 0 }; 
  f.n++; 
  if (f.n >= 5) { f.until = Date.now() + 60000; f.n = 0; } 
  fails.set(ip, f); 
}
// END_AUTH

// BEGIN_FRAME
const nm = (d) => d?.user?.uniqueId || d?.user?.nickname || d?.uniqueId || d?.nickname || 'alguien';
const seen = new Set();
const recent = new Set();
const lvl = (d) => d.level ?? d.newLevel ?? d.upgradeLevel ?? d.fansLevel ?? d.currentLevel ?? d.user?.level ?? 0;

function mapFrame(raw, send, dbg) {
  let obj;
  try { obj = JSON.parse(raw.toString()); } catch { if (dbg) dbg('frame no JSON'); return; }
  if (dbg) dbg('frame ' + JSON.stringify(obj).slice(0, 500));
  
  const list = Array.isArray(obj?.messages) ? obj.messages : (obj?.type || obj?.decodedData ? [obj] : []);
  
  for (const m of list) {
    const type = String(m.type ?? m.decodedData?.type ?? '');
    const d = m.data ?? m.decodedData?.data ?? m;
    const mid = d?.common?.msgId ?? d?.msgId;
    
    if (mid != null) {
      const k = String(mid);
      if (recent.has(k)) continue;
      recent.add(k);
      if (recent.size > 2000) recent.delete(recent.values().next().value);
    }
    
    if (/roomuser/i.test(type)) { send({ t: 'viewers', n: d.viewerCount || d.totalUser || 0 }); }
    else if (/level|upgrade|fansclub/i.test(type)) { send({ t: 'level', u: nm(d), n: lvl(d) }); }
    else if (/chat/i.test(type)) { send({ t: 'chat', u: nm(d), m: String(d.comment || d.content || '') }); }
    
    // 🎁 AQUÍ ESTÁ EL CAMBIO CLAVE PARA EXTRAER EL PNG DEL REGALO
    else if (/gift/i.test(type)) {
      const gd = d.giftDetails || {};
      if ((gd.giftType ?? d.giftType) === 1 && d.repeatEnd) continue; // fin de racha: ya se contó
      
      const g = gd.giftName || d.giftName || d.gift?.name || ('Regalo ' + (d.giftId ?? ''));
      const giftId = gd.giftId || d.giftId || d.gift?.id || d.id;
      
      // Extraer la URL de la imagen (PNG) de las diferentes posibles estructuras de Euler Stream
      const giftImg = (gd.image?.url_list?.[0]) || 
                      (d.gift?.image?.url_list?.[0]) || 
                      (d.image?.url_list?.[0]) || 
                      null;

      send({ 
        t: 'gift', 
        u: nm(d),  
        g, 
        n: d.repeatCount || d.comboCount || 1,
        id: giftId,
        img: giftImg // <-- URL DEL PNG LISTA PARA USAR EN LA APP
      });
    }
    
    else if (/like/i.test(type)) { send({ t: 'like', u: nm(d), n: d.likeCount || d.count || 1, total: d.totalLikeCount ?? d.totalLikes ?? 0 }); }
    else if (/member/i.test(type)) { send({ t: 'join', u: nm(d) }); }
    else if (/social/i.test(type)) {
      const kind = String(d.messageKind ?? d.displayType ?? d.common?.displayText?.key ?? d.action ?? '');
      if (/follow/i.test(kind) || kind === '1') send({ t: 'follow', u: nm(d) });
    }
    else if (/control/i.test(type)) {
      if (d.action === 3) send({ t: 'status', s: 'ended' });
    }
    else {
      const dt = d.common?.displayText || {};
      const text = [dt.key, dt.defaultPattern, d.content, d.text, d.message, d.comment].filter((x) => typeof x === 'string').join(' ');
      if (/level|lvl|upgrade|nivel/i.test(type + ' ' + text)) {
        const nums = [...text.matchAll(/(?:level|lvl|nivel)\D{0,8}(\d{1,3})/gi)].map((x) => +x[1]).filter((x) => x > 0);
        const num = nums.length ? nums[nums.length - 1] : 0;
        const pu = dt.piecesList?.find?.((p) => p?.userValue)?.userValue?.user;
        send({ t: 'level', u: pu?.uniqueId || pu?.nickname || nm(d), n: num || lvl(d) });
        send({ t: 'dbg', m: 'Nivel detectado (' + type + '): ' + text.slice(0, 160) });
      } else if (!seen.has(type)) {
        seen.add(type);
        let j = '';
        try { j = JSON.stringify(d).slice(0, 260); } catch {}
        if (dbg) dbg('tipo ignorado: ' + type);
        send({ t: 'dbg', m: 'Tipo sin usar: ' + type + ' ' + j });
      }
    }
  }
}
// END_FRAME

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Lenyx Live activo v2.1 🟢');
});

const wss = new WebSocketServer({ server });
wss.on('connection', (app, req) => {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?').split(',')[0].trim();
  let up = null;
  let got = false;
  let wantUp = false;
  let tries = 0;
  let endT = null;
  let user = '';
  let n = 0;
  
  app.isAlive = true;
  app.on('pong', () => (app.isAlive = true));
  
  const send = (o) => { if (app.readyState === 1) app.send(JSON.stringify(o)); };
  const dbg = (t) => { if (n++ < 8) console.log(t); };
  const clearEnd = () => { if (endT) { clearTimeout(endT); endT = null; } };
  
  const stop = () => {
    wantUp = false;
    clearEnd();
    if (up) { up.removeAllListeners(); up.on('error', () => {}); try { up.close(); } catch {} up = null; }
  };
  
  const out = (o) => {
    if (o.t === 'status' && o.s === 'ended') {
      if (!endT) endT = setTimeout(() => { send(o); stop(); }, 90000);
      return;
    }
    if (endT && ['chat', 'gift', 'like', 'join', 'follow'].includes(o.t)) clearEnd();
    send(o);
  };
  
  const openUp = () => {
    const url = 'wss://ws.eulerstream.com?uniqueId=' + encodeURIComponent(user) + '&apiKey=' + encodeURIComponent(EULER_API_KEY);
    const mine = new WebSocket(url);
    up = mine;
    let lastPong = Date.now(), pingT = null, stableT = null;
    const clearT = () => { clearInterval(pingT); clearTimeout(stableT); };
    
    mine.on('pong', () => { lastPong = Date.now(); });
    mine.on('open', () => {
      console.log('Conectado a Euler para @' + user + ' (sesión de ' + ip + ')');
      send({ t: 'status', s: 'connected' });
      lastPong = Date.now();
      stableT = setTimeout(() => { tries = 0; }, 45000);
      pingT = setInterval(() => {
        if (up !== mine) { clearT(); return; }
        if (Date.now() - lastPong > 75000) { console.log('Euler no responde: reiniciando la conexión'); mine.terminate(); return; }
        try { mine.ping(); } catch {}
      }, 25000);
    });
    
    mine.on('message', (data) => { 
      got = true; tries = 0; lastPong = Date.now(); 
      try { mapFrame(data, out, dbg); } catch (e) { console.error('Error al procesar:', e?.message || e); } 
    });
    
    mine.on('error', (e) => console.error('Error de Euler:', e?.message || e));
    mine.on('close', (code, reason) => {
      clearT();
      if (up !== mine) return;
      up = null;
      const why = reason ? reason.toString() : '';
      console.log('Euler cerró la conexión', code, why);
      if (!wantUp) return;
      if (!got) {
        wantUp = false;
        send({ t: 'status', s: 'error', msg: 'No se pudo conectar al live' + (why ? ': ' + why : '') + ' (código ' + code + '). ¿Estás en vivo?' });
        return;
      }
      if (tries < 30) {
        tries++;
        console.log('Reconectando con Euler, intento ' + tries);
        send({ t: 'status', s: 'reconnecting', why: 'Euler cerró la conexión (' + code + (why ? ': ' + why : '') + ')' });
        setTimeout(() => { if (wantUp && !up) openUp(); }, Math.min(1500 * tries, 15000));
      } else {
        wantUp = false;
        send({ t: 'status', s: 'ended' });
      }
    });
  };
  
  app.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    
    if (m.cmd === 'ping') { send({ t: 'pong' }); return; }
    
    if (m.cmd === 'login') {
      if (blocked(ip)) { send({ t: 'auth', ok: false, msg: 'Demasiados intentos. Espera un minuto.' }); return; }
      if (APP_KEY && m.key !== APP_KEY) { send({ t: 'auth', ok: false, msg: 'Clave del servidor incorrecta' }); return; }
      if (!USERS.size) { send({ t: 'auth', ok: false, msg: 'El servidor no tiene usuarios configurados' }); return; }
      
      const name = String(m.u || '').trim().toLowerCase();
      const rec = USERS.get(name);
      if (!rec || !safeEq(rec.p, String(m.p || ''))) {
        fail(ip);
        console.log('Inicio de sesión FALLIDO: ' + name + ' desde ' + ip);
        send({ t: 'auth', ok: false, msg: 'Usuario o contraseña incorrectos' });
        return;
      }
      if (rec.exp && Date.now() > rec.exp) { send({ t: 'auth', ok: false, msg: 'Tu acceso venció' }); return; }
      
      const exp = Math.min(Date.now() + 30 * 864e5, rec.exp || Infinity);
      console.log('Inicio de sesión: ' + name + ' desde ' + ip);
      send({ t: 'auth', ok: true, token: signToken(name, exp), u: name, exp });
      return;
    }
    
    if (m.cmd === 'disconnect') { stop(); return; }
    if (m.cmd !== 'connect') return;
    
    const who = verifyToken(m.token);
    if (!who) { send({ t: 'status', s: 'error', auth: false, msg: 'Tu acceso no está autorizado o venció. Inicia sesión de nuevo.' }); return; }
    if (APP_KEY && m.key !== APP_KEY) { send({ t: 'status', s: 'error', msg: 'Clave del servidor incorrecta' }); return; }
    if (!EULER_API_KEY) { send({ t: 'status', s: 'error', msg: 'Falta EULER_API_KEY en el servidor' }); return; }
    
    user = String(m.user || '').replace(/^@/, '').trim();
    if (!/^[\w.]{2,40}$/.test(user)) { send({ t: 'status', s: 'error', msg: 'Usuario no válido' }); return; }
    if (ALLOWED.length && !ALLOWED.includes(user.toLowerCase())) { send({ t: 'status', s: 'error', msg: 'Usuario no permitido en este servidor' }); return; }
    
    stop();
    seen.clear();
    wantUp = true; got = false; tries = 0; n = 0;
    openUp();
  });
  
  app.on('close', stop);
  app.on('error', stop);
});

setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.isAlive) return c.terminate();
    c.isAlive = false;
    c.ping();
  });
}, 25000);

server.listen(PORT, () => console.log('🚀 Servidor de Lenyx Live escuchando en el puerto ' + PORT));