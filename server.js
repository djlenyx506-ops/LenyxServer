// Servidor de Lenyx Live v2
// App (tu teléfono) <-> este servidor <-> WebSocket gestionado de Euler Stream <-> TikTok LIVE
// Tu clave de Euler queda guardada aquí, en el servidor, y no dentro de la app.
import http from 'node:http';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 3000;
const APP_KEY = process.env.APP_KEY || '';               // clave que pide la app
const EULER_API_KEY = process.env.EULER_API_KEY || '';   // clave gratis de eulerstream.com
const ALLOWED = (process.env.ALLOWED_USERS || '')        // opcional: usuarios permitidos, separados por coma
  .split(',').map((s) => s.trim().toLowerCase().replace(/^@/, '')).filter(Boolean);

// BEGIN_FRAME
const nm = (d) => d?.user?.uniqueId || d?.user?.nickname || d?.uniqueId || d?.nickname || 'alguien';
const seen = new Set();
const recent = new Set(); // ids de mensajes ya enviados (evita repetir al reconectar)
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
    else if (/gift/i.test(type)) {
      const gd = d.giftDetails || {};
      if ((gd.giftType ?? d.giftType) === 1 && d.repeatEnd) continue; // fin de racha: ya se contó cada regalo
      const g = gd.giftName || d.giftName || d.gift?.name || ('Regalo ' + (d.giftId ?? ''));
      send({ t: 'gift', u: nm(d), g, n: d.repeatCount || d.comboCount || 1 });
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
    else if (/level|upgrade/i.test(String(d.common?.displayText?.key ?? ''))) { send({ t: 'level', u: nm(d), n: lvl(d) }); }
    else if (!seen.has(type)) { seen.add(type); if (dbg) dbg('tipo ignorado: ' + type); }
  }
}
// END_FRAME

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Lenyx Live activo');
});
const wss = new WebSocketServer({ server });

wss.on('connection', (app) => {
  let up = null;       // conexión con Euler
  let got = false;     // ¿ya llegó algún mensaje?
  let wantUp = false;  // ¿la app quiere seguir conectada?
  let tries = 0;       // reintentos de reconexión
  let endT = null;     // temporizador de "fin de live"
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
  // El fin del live solo se confirma si en 20 s no llega ningún evento más
  const out = (o) => {
    if (o.t === 'status' && o.s === 'ended') {
      if (!endT) endT = setTimeout(() => { send(o); stop(); }, 20000);
      return;
    }
    if (endT && ['chat', 'gift', 'like', 'join', 'follow'].includes(o.t)) clearEnd();
    send(o);
  };

  const openUp = () => {
    const url = 'wss://ws.eulerstream.com?uniqueId=' + encodeURIComponent(user) + '&apiKey=' + encodeURIComponent(EULER_API_KEY);
    const mine = new WebSocket(url);
    up = mine;
    mine.on('open', () => { console.log('Conectado a Euler para @' + user); send({ t: 'status', s: 'connected' }); });
    mine.on('message', (data) => { got = true; tries = 0; try { mapFrame(data, out, dbg); } catch (e) { console.error('Error al procesar:', e?.message || e); } });
    mine.on('error', (e) => console.error('Error de Euler:', e?.message || e));
    mine.on('close', (code, reason) => {
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
      if (tries < 6) {
        tries++;
        console.log('Reconectando con Euler, intento ' + tries);
        send({ t: 'status', s: 'reconnecting' });
        setTimeout(() => { if (wantUp && !up) openUp(); }, Math.min(1500 * tries, 8000));
      } else {
        wantUp = false;
        send({ t: 'status', s: 'ended' });
      }
    });
  };

  app.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.cmd === 'disconnect') { stop(); return; }
    if (m.cmd !== 'connect') return;
    if (APP_KEY && m.key !== APP_KEY) { send({ t: 'status', s: 'error', msg: 'Clave del servidor incorrecta' }); return; }
    if (!EULER_API_KEY) { send({ t: 'status', s: 'error', msg: 'Falta EULER_API_KEY en el servidor' }); return; }
    user = String(m.user || '').replace(/^@/, '').trim();
    if (!/^[\w.]{2,40}$/.test(user)) { send({ t: 'status', s: 'error', msg: 'Usuario no válido' }); return; }
    if (ALLOWED.length && !ALLOWED.includes(user.toLowerCase())) { send({ t: 'status', s: 'error', msg: 'Usuario no permitido en este servidor' }); return; }
    stop();
    wantUp = true; got = false; tries = 0; n = 0;
    openUp();
  });

  app.on('close', stop);
  app.on('error', stop);
});

// Mantiene vivas las conexiones con la app
setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.isAlive) return c.terminate();
    c.isAlive = false;
    c.ping();
  });
}, 25000);

server.listen(PORT, () => console.log('Servidor escuchando en el puerto ' + PORT));
