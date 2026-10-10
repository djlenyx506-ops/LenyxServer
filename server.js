// Servidor de Lenyx Live: escucha un live de TikTok y reenvía los eventos a la app.
import http from 'node:http';
import * as TLC from 'tiktok-live-connector';
import { WebSocketServer } from 'ws';

const { TikTokLiveConnection, WebcastEvent, ControlEvent } = TLC;
const PORT = process.env.PORT || 3000;
const APP_KEY = process.env.APP_KEY || '';            // clave que pedirá la app
const SIGN_API_KEY = process.env.SIGN_API_KEY || '';  // opcional (eulerstream.com)
const EULER_ROOMID = process.env.EULER_ROOMID === '1'; // opcional, para IPs de la nube

const ev = (k, fallback) => (WebcastEvent && WebcastEvent[k]) || fallback;
const ctl = (k, fallback) => (ControlEvent && ControlEvent[k]) || fallback;
const nm = (d) => d?.user?.uniqueId || d?.uniqueId || d?.user?.nickname || d?.nickname || 'alguien';

function friendly(e) {
  const t = String(e?.message || e);
  if (e?.name === 'UserOfflineError' || /offline|not live|isn't live|not currently/i.test(t)) {
    return 'Ese usuario no está en vivo ahora mismo';
  }
  return 'No se pudo conectar: ' + t.slice(0, 120);
}

function wire(c, send) {
  c.on(ev('CHAT', 'chat'), (d) => send({ t: 'chat', u: nm(d), m: String(d.comment || '') }));
  c.on(ev('GIFT', 'gift'), (d) => {
    const type = d.giftType ?? d.giftDetails?.giftType;
    if (type === 1 && d.repeatEnd) return; // fin de racha: ya se contó cada regalo
    const g = d.giftName || d.giftDetails?.giftName || d.extendedGiftInfo?.name || 'Regalo ' + d.giftId;
    send({ t: 'gift', u: nm(d), g, n: d.repeatCount || 1 });
  });
  c.on(ev('FOLLOW', 'follow'), (d) => send({ t: 'follow', u: nm(d) }));
  c.on(ev('MEMBER', 'member'), (d) => send({ t: 'join', u: nm(d) }));
  c.on(ev('LIKE', 'like'), (d) => send({ t: 'like', u: nm(d), n: d.likeCount || 1 }));
  c.on(ev('ROOM_USER', 'roomUser'), (d) => send({ t: 'viewers', n: d.viewerCount || 0 }));
  c.on(ctl('STREAM_END', 'streamEnd'), () => send({ t: 'status', s: 'ended' }));
  c.on(ctl('ERROR', 'error'), (e) => console.error('Error de TikTok:', e?.message || e));
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Lenyx Live activo');
});
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let conn = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
  const stop = () => { if (conn) { try { conn.disconnect(); } catch {} conn = null; } };

  ws.on('message', async (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.cmd === 'disconnect') { stop(); return; }
    if (m.cmd !== 'connect') return;
    if (APP_KEY && m.key !== APP_KEY) {
      send({ t: 'status', s: 'error', msg: 'Clave del servidor incorrecta' });
      return;
    }
    const user = String(m.user || '').replace(/^@/, '').trim();
    if (!/^[\w.]{2,40}$/.test(user)) {
      send({ t: 'status', s: 'error', msg: 'Usuario no válido' });
      return;
    }
    stop();
    const opts = { enableExtendedGiftInfo: true, processInitialData: false };
    if (SIGN_API_KEY) opts.signApiKey = SIGN_API_KEY;
    if (EULER_ROOMID) opts.connectWithUniqueId = true;
    conn = new TikTokLiveConnection(user, opts);
    wire(conn, send);
    try {
      const state = await conn.connect();
      send({ t: 'status', s: 'connected' });
      const f = state?.roomInfo?.owner?.follow_info?.follower_count;
      if (f) send({ t: 'info', f });
    } catch (e) {
      console.error('Fallo al conectar:', e?.message || e);
      send({ t: 'status', s: 'error', msg: friendly(e) });
      stop();
    }
  });

  ws.on('close', stop);
  ws.on('error', stop);
});

// Mantiene vivas las conexiones (evita cortes por inactividad)
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

server.listen(PORT, () => console.log('Servidor escuchando en el puerto ' + PORT));
