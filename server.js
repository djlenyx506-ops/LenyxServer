// Servidor de Lenyx Live v2.3 (Corregido: sin errores de sintaxis + extracción de PNG)
// App (tu teléfono) <-> este servidor <-> WebSocket gestionado de Euler Stream <-> TikTok LIVE
import http from 'node:http';
import crypto from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = process.env.PORT || 3000;
const APP_KEY = process.env.APP_KEY || '';
const EULER_API_KEY = process.env.EULER_API_KEY || '';
const ALLOWED = (process.env.ALLOWED_USERS || '')
  .split(',')
  .map((s) => s.trim().toLowerCase().replace(/^@/, ''))
  .filter(Boolean);

// Límites configurables
const MAX_SESSIONS_PER_USER = Number(process.env.MAX_SESSIONS_PER_USER || 2);
const MAX_CONN_PER_IP = Number(process.env.MAX_CONN_PER_IP || 10);
const AUTH_TIMEOUT_MS = 30000;
const MAX_PAYLOAD = 16 * 1024;

// Red de seguridad
process.on('uncaughtException', (e) => console.error('uncaughtException:', e));
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));

// BEGIN_AUTH
// TOKEN_SECRET: si no existe, usa un fallback (para no romper tu configuración actual)
const SECRET = process.env.TOKEN_SECRET || APP_KEY || EULER_API_KEY || 'lenyx-default-secret-16ch';
if (SECRET.length < 16) {
  console.warn('️ TOKEN_SECRET muy corto. Se recomienda uno de al menos 16 caracteres.');
}

const USERS = new Map();
for (const item of (process.env.USERS || '').split(',')) {
  const raw = item.trim();
  if (!raw) continue;
  const parts = raw.split(':');
  if (parts.length < 2 || parts.length > 3) {
    console.warn('USERS: entrada ignorada (formato inválido): ' + raw);
    continue;
  }
  const u = parts[0].trim().toLowerCase();
  const p = parts[1].trim();
  if (!u || !p) {
    console.warn('USERS: entrada ignorada (usuario o clave vacíos)');
    continue;
  }
  let e = 0;
  if (parts.length === 3) {
    const ds = parts[2].trim();
    e = /^\d{4}-\d{2}-\d{2}$/.test(ds) ? Date.parse(ds + 'T23:59:59Z') : NaN;
    if (Number.isNaN(e)) {
      console.warn('USERS: usuario "' + u + '" ignorado (fecha de vencimiento inválida)');
      continue;
    }
  }
  USERS.set(u, { p, exp: e });
}

const b64 = (x) => Buffer.from(x).toString('base64url');
const hmac = (x) => crypto.createHmac('sha256', SECRET).update(x).digest('base64url');
const sha = (x) => crypto.createHash('sha256').update(String(x)).digest();
const safeEq = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));

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
  } catch {
    return null;
  }
}

// Anti fuerza bruta por IP
const fails = new Map();
function blocked(ip) {
  const f = fails.get(ip);
  return !!(f && f.until && Date.now() < f.until);
}
function fail(ip) {
  const f = fails.get(ip) || { n: 0, until: 0 };
  f.n++;
  if (f.n >= 5) {
    f.until = Date.now() + 60000;
    f.n = 0;
  }
  f.last = Date.now();
  fails.set(ip, f);
}

// Limpieza periódica del mapa de fallos
setInterval(() => {
  const now = Date.now();
  for (const [ip, f] of fails) {
    if (now > (f.until || 0) && now - (f.last || 0) > 10 * 60000) {
      fails.delete(ip);
    }
  }
}, 5 * 60000).unref();

function clientIp(req) {
  const xff = String(req.headers['x-forwarded-for'] || '');
  if (xff) {
    const last = xff.split(',').pop().trim();
    if (last) return last;
  }
  return req.socket.remoteAddress || '?';
}
// END_AUTH

// BEGIN_FRAME
const nm = (d) =>
  d?.user?.uniqueId || d?.user?.nickname || d?.uniqueId || d?.nickname || 'alguien';
const lvl = (d) =>
  d.level ?? d.newLevel ?? d.upgradeLevel ?? d.fansLevel ?? d.currentLevel ?? d.user?.level ?? 0;

// Normaliza "WebcastGiftMessage" -> "gift"
const normType = (t) =>
  String(t || '').replace(/^webcast/i, '').replace(/message$/i, '').toLowerCase();

// 🎁 FUNCIÓN CLAVE: extrae la URL del PNG del regalo
function pickImg(...cands) {
  for (const c of cands) {
    const list = Array.isArray(c) ? c : c?.url_list || c?.urlList;
    if (Array.isArray(list) && list.length) {
      return list.find((u) => /.png(?|$)/i.test(u)) || list[0];
    }
    if (typeof c === 'string' && c) return c;
  }
  return null;
}

function mapFrame(raw, send, dbg, ctx) {
  let obj;
  try {
    obj = JSON.parse(raw.toString());
  } catch {
    dbg('frame no JSON');
    return;
  }
  dbg(() => 'frame ' + JSON.stringify(obj).slice(0, 500));
  if (!obj || typeof obj !== 'object') return;

  const list = Array.isArray(obj.messages)
    ? obj.messages
    : obj.type || obj.decodedData
    ? [obj]
    : [];

  for (const m of list) {
    if (!m || typeof m !== 'object') continue;
    const rawType = String(m.type ?? m.decodedData?.type ?? '');
    const type = normType(rawType);
    const d = m.data ?? m.decodedData?.data ?? m;
    if (!d || typeof d !== 'object') continue;
    const mid = d.common?.msgId ?? d.msgId;

    if (mid != null) {
      const k = String(mid);
      if (ctx.recent.has(k)) continue;
      ctx.recent.add(k);
      if (ctx.recent.size > 2000) {
        ctx.recent.delete(ctx.recent.values().next().value);
      }
    }

    if (type.startsWith('roomuser')) {
      send({ t: 'viewers', n: d.viewerCount || d.totalUser || 0 });
    } else if (/level|upgrade|fansclub/.test(type)) {
      send({ t: 'level', u: nm(d), n: lvl(d) });
    } else if (type === 'chat') {
      const text = String(d.comment || d.content || '').trim();
      if (text) send({ t: 'chat', u: nm(d), m: text });
    }
    // 🎁 DETECTAR REGALOS Y EXTRAER URL DEL PNG
    else if (type === 'gift') {
      const gd = d.giftDetails || {};
      const isStreak = (gd.giftType ?? d.giftType) === 1;
      const giftId = gd.giftId || d.giftId || d.gift?.id || d.id;
      const g =
        gd.giftName || d.giftName || d.gift?.name || 'Regalo ' + (giftId ?? '');
      const user = nm(d);
      const count = Math.max(1, Number(d.repeatCount || d.comboCount || 1));
      let n = count;

      // Rachas: enviar solo el delta (lo nuevo)
      if (isStreak) {
        const key = user + '|' + giftId;
        const last = ctx.streaks.get(key) || 0;
        n = count >= last ? count - last : count;
        if (d.repeatEnd) {
          ctx.streaks.delete(key);
        } else {
          ctx.streaks.set(key, count);
          if (ctx.streaks.size > 500) {
            ctx.streaks.delete(ctx.streaks.keys().next().value);
          }
        }
        if (n <= 0) continue;
      }

      // Extraer la URL del PNG
      const giftImg = pickImg(
        gd.image?.url_list,
        d.gift?.image?.url_list,
        d.image?.url_list,
        gd.giftPictureUrl,
        d.giftPictureUrl,
        d.gift?.image
      );

      // ✅ ENVÍO COMPLETO CON IMAGEN PNG E ID
      send({
        t: 'gift',
        u: user,
        g,
        n,
        total: count,
        id: giftId,
        img: giftImg,
      });
    } else if (type === 'like') {
      send({
        t: 'like',
        u: nm(d),
        n: d.likeCount || d.count || 1,
        total: d.totalLikeCount ?? d.totalLikes ?? 0,
      });
    } else if (type === 'member') {
      send({ t: 'join', u: nm(d) });
    } else if (type === 'social') {
      const kind = String(
        d.messageKind ??
          d.displayType ??
          d.common?.displayText?.key ??
          d.action ??
          ''
      );
      if (/follow/i.test(kind) || kind === '1') {
        send({ t: 'follow', u: nm(d) });
      }
    } else if (type === 'control') {
      if (d.action === 3) send({ t: 'status', s: 'ended' });
    } else {
      const dt = d.common?.displayText || {};
      const text = [
        dt.key,
        dt.defaultPattern,
        d.content,
        d.text,
        d.message,
        d.comment,
      ]
        .filter((x) => typeof x === 'string')
        .join(' ');
      if (/level|lvl|upgrade|nivel/i.test(rawType + ' ' + text)) {
        const nums = [...text.matchAll(/(?:level|lvl|nivel)\D{0,8}(\d{1,3})/gi)]
          .map((x) => +x[1])
          .filter((x) => x > 0);
        const num = nums.length ? nums[nums.length - 1] : 0;
        const pu = dt.piecesList?.find?.((p) => p?.userValue)?.userValue?.user;
        send({
          t: 'level',
          u: pu?.uniqueId || pu?.nickname || nm(d),
          n: num || lvl(d),
        });
        send({
          t: 'dbg',
          m: 'Nivel detectado (' + rawType + '): ' + text.slice(0, 160),
        });
      } else if (!ctx.seen.has(rawType)) {
        ctx.seen.add(rawType);
        let j = '';
        try {
          j = JSON.stringify(d).slice(0, 260);
        } catch {}
        dbg('tipo ignorado: ' + rawType);
        send({ t: 'dbg', m: 'Tipo sin usar: ' + rawType + ' ' + j });
      }
    }
  }
}
// END_FRAME

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor de Lenyx Live activo v2.3 🟢');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });
const connsByIp = new Map();
const sessionsByUser = new Map();

wss.on('connection', (app, req) => {
  const ip = clientIp(req);
  const open = (connsByIp.get(ip) || 0) + 1;
  if (open > MAX_CONN_PER_IP) {
    app.close(1013, 'Demasiadas conexiones');
    return;
  }
  connsByIp.set(ip, open);

  let up = null;
  let got = false;
  let wantUp = false;
  let tries = 0;
  let endT = null;
  let reT = null;
  let user = '';
  let who = '';
  let counted = false;
  let n = 0;
  const ctx = { seen: new Set(), recent: new Set(), streaks: new Map() };

  let authT = setTimeout(() => {
    try {
      app.close(4001, 'Tiempo de autenticación agotado');
    } catch {}
  }, AUTH_TIMEOUT_MS);
  const clearAuth = () => {
    if (authT) {
      clearTimeout(authT);
      authT = null;
    }
  };

  app.isAlive = true;
  app.on('pong', () => (app.isAlive = true));

  const send = (o) => {
    if (app.readyState === 1) app.send(JSON.stringify(o));
  };
  const dbg = (t) => {
    if (n < 8) {
      n++;
      console.log(typeof t === 'function' ? t() : t);
    }
  };
  const clearEnd = () => {
    if (endT) {
      clearTimeout(endT);
      endT = null;
    }
  };

  const release = () => {
    if (!counted) return;
    counted = false;
    const c = (sessionsByUser.get(who) || 1) - 1;
    if (c <= 0) sessionsByUser.delete(who);
    else sessionsByUser.set(who, c);
  };

  const stop = () => {
    wantUp = false;
    clearEnd();
    if (reT) {
      clearTimeout(reT);
      reT = null;
    }
    if (up) {
      up.removeAllListeners();
      up.on('error', () => {});
      try {
        up.close();
      } catch {}
      up = null;
    }
    release();
  };

  const out = (o) => {
    if (o.t === 'status' && o.s === 'ended') {
      if (!endT) {
        endT = setTimeout(() => {
          send(o);
          stop();
        }, 90000);
      }
      return;
    }
    if (
      endT &&
      ['chat', 'gift', 'like', 'join', 'follow'].includes(o.t)
    ) {
      clearEnd();
    }
    send(o);
  };

  const openUp = () => {
    const url =
      'wss://ws.eulerstream.com?uniqueId=' +
      encodeURIComponent(user) +
      '&apiKey=' +
      encodeURIComponent(EULER_API_KEY);
    const mine = new WebSocket(url);
    up = mine;
    let lastPong = Date.now();
    let pingT = null;
    let stableT = null;
    const clearT = () => {
      clearInterval(pingT);
      clearTimeout(stableT);
    };

    mine.on('pong', () => {
      lastPong = Date.now();
    });
    mine.on('open', () => {
      console.log('Conectado a Euler para @' + user + ' (sesión de ' + ip + ')');
      send({ t: 'status', s: 'connected' });
      lastPong = Date.now();
      stableT = setTimeout(() => {
        tries = 0;
      }, 45000);
      pingT = setInterval(() => {
        if (up !== mine) {
          clearT();
          return;
        }
        if (Date.now() - lastPong > 75000) {
          console.log('Euler no responde: reiniciando la conexión');
          mine.terminate();
          return;
        }
        try {
          mine.ping();
        } catch {}
      }, 25000);
    });
    mine.on('message', (data) => {
      got = true;
      lastPong = Date.now();
      try {
        mapFrame(data, out, dbg, ctx);
      } catch (e) {
        console.error('Error al procesar:', e?.message || e);
      }
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
        stop();
        send({
          t: 'status',
          s: 'error',
          msg:
            'No se pudo conectar al live' +
            (why ? ': ' + why : '') +
            ' (código ' +
            code +
            '). ¿Estás en vivo?',
        });
        return;
      }
      if (tries < 30) {
        tries++;
        console.log('Reconectando con Euler, intento ' + tries);
        send({
          t: 'status',
          s: 'reconnecting',
          why:
            'Euler cerró la conexión (' +
            code +
            (why ? ': ' + why : '') +
            ')',
        });
        reT = setTimeout(() => {
          reT = null;
          if (wantUp && !up) openUp();
        }, Math.min(1500 * tries, 15000));
      } else {
        stop();
        send({ t: 'status', s: 'ended' });
      }
    });
  };

  app.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!m || typeof m !== 'object' || Array.isArray(m)) return;

    if (m.cmd === 'ping') {
      send({ t: 'pong' });
      return;
    }

    if (m.cmd === 'login') {
      if (blocked(ip)) {
        send({ t: 'auth', ok: false, msg: 'Demasiados intentos. Espera un minuto.' });
        return;
      }
      if (APP_KEY && !safeEq(m.key ?? '', APP_KEY)) {
        send({ t: 'auth', ok: false, msg: 'Clave del servidor incorrecta' });
        return;
      }
      if (!USERS.size) {
        send({ t: 'auth', ok: false, msg: 'El servidor no tiene usuarios configurados' });
        return;
      }
      const name = String(m.u || '').trim().toLowerCase();
      const rec = USERS.get(name);
      if (!rec || !safeEq(rec.p, String(m.p || ''))) {
        fail(ip);
        console.log('Inicio de sesión FALLIDO: ' + name.slice(0, 40) + ' desde ' + ip);
        send({ t: 'auth', ok: false, msg: 'Usuario o contraseña incorrectos' });
        return;
      }
      if (rec.exp && Date.now() > rec.exp) {
        send({ t: 'auth', ok: false, msg: 'Tu acceso venció' });
        return;
      }
      const exp = Math.min(Date.now() + 30 * 864e5, rec.exp || Infinity);
      console.log('Inicio de sesión: ' + name + ' desde ' + ip);
      clearAuth();
      send({ t: 'auth', ok: true, token: signToken(name, exp), u: name, exp });
      return;
    }

    if (m.cmd === 'disconnect') {
      stop();
      return;
    }
    if (m.cmd !== 'connect') return;

    const w = verifyToken(m.token);
    if (!w) {
      send({
        t: 'status',
        s: 'error',
        auth: false,
        msg: 'Tu acceso no está autorizado o venció. Inicia sesión de nuevo.',
      });
      return;
    }
    clearAuth();
    if (APP_KEY && !safeEq(m.key ?? '', APP_KEY)) {
      send({ t: 'status', s: 'error', msg: 'Clave del servidor incorrecta' });
      return;
    }
    if (!EULER_API_KEY) {
      send({ t: 'status', s: 'error', msg: 'Falta EULER_API_KEY en el servidor' });
      return;
    }

    const nuevo = String(m.user || '').replace(/^@/, '').trim();
    if (!/^[\w.]{2,40}$/.test(nuevo)) {
      send({ t: 'status', s: 'error', msg: 'Usuario no válido' });
      return;
    }
    if (ALLOWED.length && !ALLOWED.includes(nuevo.toLowerCase())) {
      send({ t: 'status', s: 'error', msg: 'Usuario no permitido en este servidor' });
      return;
    }

    stop();
    if ((sessionsByUser.get(w) || 0) >= MAX_SESSIONS_PER_USER) {
      send({
        t: 'status',
        s: 'error',
        msg:
          'Tu cuenta ya tiene ' +
          MAX_SESSIONS_PER_USER +
          ' sesiones activas. Cierra una para continuar.',
      });
      return;
    }

    who = w;
    user = nuevo;
    sessionsByUser.set(who, (sessionsByUser.get(who) || 0) + 1);
    counted = true;
    ctx.seen.clear();
    ctx.recent.clear();
    ctx.streaks.clear();
    wantUp = true;
    got = false;
    tries = 0;
    n = 0;
    openUp();
  });

  const cleanup = () => {
    clearAuth();
    stop();
    const c = (connsByIp.get(ip) || 1) - 1;
    if (c <= 0) connsByIp.delete(ip);
    else connsByIp.set(ip, c);
  };
  let cleaned = false;
  const cleanOnce = () => {
    if (cleaned) return;
    cleaned = true;
    cleanup();
  };
  app.on('close', cleanOnce);
  app.on('error', cleanOnce);
});

const hb = setInterval(() => {
  wss.clients.forEach((c) => {
    if (!c.isAlive) return c.terminate();
    c.isAlive = false;
    c.ping();
  });
}, 25000);

function shutdown() {
  console.log('Cerrando servidor...');
  clearInterval(hb);
  wss.clients.forEach((c) => {
    try {
      c.close(1001, 'Servidor reiniciando');
    } catch {}
  });
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, () =>
  console.log('🚀 Servidor de Lenyx Live escuchando en el puerto ' + PORT)
);
