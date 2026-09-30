/* ═══════════════════════════════════════════════════════════
   خادم الشات — WebSocket خام برسائل JSON
   مبنيّ على عقد الأحداث الذي تتوقّعه الواجهة حرفيًّا:
     يرسل العميل : msg · pic · setprofile · rleave · logout · turn · bitrate
     ويستقبل     : me · here · join · leave · msg · rooms · rjoin
                   · rleave · wall · pm · kicked · ustate · err · ok
   ═══════════════════════════════════════════════════════════ */
/* ══════════════════════════════════════════════════════════════
   قواعد العمل — تُقرأ قبل أيّ تعديل
   ١ · لا يُنشأ مجلّدٌ ولا تعريفٌ ولا دالّةٌ ولا نظامٌ ولا بنيةٌ
       جديدة إلّا إن لم تكن موجودةً أصلًا. كلّ عملٍ تصحيحٌ على
       القديم وتحديثٌ له، مع حذف التكرارات والترسّبات وتنظيفها.
   ٢ · يُمنع منعًا باتًّا تعديلُ أيّ تصميم أو استحداثُ تصميمٍ جديد
       أو تغييرُ أيّ ملمح أو إضافةُ شيءٍ أو حذفُه من التصاميم
       والهيكل والبناء — إلّا بطلبٍ صريحٍ من صاحب المشروع.
   ══════════════════════════════════════════════════════════════ */

'use strict';

const http = require('http');
const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT     = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUBLIC   = path.join(__dirname, 'public');
const SECRET   = process.env.SECRET || crypto.randomBytes(32).toString('hex');

/* ══ الغرف ══ */
/* v4.87 — ROOMS كانت const ثابتة تمامًا، بلا حفظٍ على القرص ولا
   حقول (صورة/لون/وصف/صوتيّة) — فحتى الغرف الأربع الأصليّة لم تكن
   تبثّ about أو voice قطّ رغم أنّ العميل يقرؤهما منذ البداية
   (buildRooms: r.about، r.voice). الآن: let لا const، وتُحمَّل من
   القرص إن وُجدت (تُعرَّف loadJSON لاحقًا في الملفّ لكنّ رفعها
   دالّةٌ لا قيمة، فيُتاح استدعاؤها هنا رغم ترتيب النصّ). */
let ROOMS = loadJSON('rooms.json', [
  { id: '1', name: 'الغرفه العامه', cap: 60 },
  { id: '2', name: 'غرفة الترحيب',  cap: 40 },
  { id: '3', name: 'غرفة الشعر',    cap: 30 },
  { id: '4', name: 'غرفة الصوتيات', cap: 25 }
]);

/* ══ الرتب: الرقم سلّمٌ تصاعديّ ══ */
const RANKS = {
  guest:  { n: 0, label: 'زائر'  },
  member: { n: 1, label: 'عضو'   },
  mod:    { n: 2, label: 'مشرف'  },
  admin:  { n: 3, label: 'إداري' },
  owner:  { n: 4, label: 'مالك'  }
};
/* ما تحتاجه كل أداة من رتبة */
const NEED = { kick: 2, ban: 3, mute: 2, msgdel: 2, bc: 3, redit: 3, cp: 4, mkr: 1 };

/* ══ الحفظ على القرص ══ */
function loadJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), 'utf8')); }
  catch (e) { return fallback; }
}
function saveJSON(file, val) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(path.join(DATA_DIR, file), JSON.stringify(val));
  } catch (e) { console.error('حفظ ' + file + ':', e.message); }
}

let members = loadJSON('members.json', {});   /* اسم -> {salt, hash, rank, pic, colors} */
let bans    = loadJSON('bans.json', {});      /* اسم -> {until, why} */
let wall    = loadJSON('wall.json', []);
let stories = loadJSON('stories.json', []);
let history = loadJSON('history.json', {});
/* v4.88 — سجلّ الخاصّ: pm.log/pms كانا مُرسَلَين من العميل منذ
   البداية (فتح محادثةٍ يطلب سجلّها، وفتح لوح الخاصّ يطلب قائمة
   المحادثات) ولا معالج لهما في الخادم إطلاقًا — والأخطر: case
   'pm' نفسها لم تكن تُخزّن الرسالة في أيّ مكانٍ بعد إرسالها، فحتى
   لو بُني المعالجان لم يكن هناك ما يُعيدانه. والمفتاح اسمٌ لا
   معرّفٌ: uid() عشوائيّةٌ بالكامل مع كل اتصال (سطر uid أعلاه)، فلا
   قيمة لربط سجلٍّ بمعرّفٍ يتغيّر في كل دخول. */
let pms = loadJSON('pms.json', {});           /* "اسم1|اسم2" -> [{from,to,text,at}] */
function pmKey(a, b) { return [String(a).toLowerCase(), String(b).toLowerCase()].sort().join('|'); }
ROOMS.forEach(r => { if (!Array.isArray(history[r.id])) history[r.id] = []; });
const micSeats = {};                  /* roomId -> [٥ مقاعد: null أو {id,name,pic,muted}] */
ROOMS.forEach(r => { micSeats[r.id] = [null, null, null, null, null]; });
function micStateOf(room) { return { type: 'mic.state', room, seats: micSeats[room] || [] }; }
function releaseSeat(userId) {
  /* يُنادى عند الخروج أو الانتقال بين الغرف: لا يبقى مقعدٌ لعضوٍ غائب */
  let changedRoom = null;
  Object.keys(micSeats).forEach(rid => {
    const seats = micSeats[rid];
    const idx = seats.findIndex(x => x && x.id === userId);
    if (idx > -1) { seats[idx] = null; changedRoom = rid; }
  });
  if (changedRoom) broadcast(micStateOf(changedRoom), inRoom(changedRoom));
}

const HISTORY_MAX = 60;
const STORY_MAX = 60;                    /* أقصى عدد محفوظ */
const STORY_MAX_BYTES = 400 * 1024;      /* حدّ الصورة الواحدة */
const STORY_TTL = 24 * 3600e3;           /* يسقط بعد يوم كما هي عادة الاستوري */

/* يُسقط ما مضى عليه يومٌ ويعيد القائمة الحيّة */
function liveStories() {
  const now = Date.now();
  const before = stories.length;
  stories = stories.filter(s => now - s.at < STORY_TTL);
  if (stories.length !== before) persistSoon();
  return stories.map(s => ({
    id: s.id, uid: s.uid, name: s.name, pic: s.pic,
    media: s.media, kind: s.kind, at: s.at,
    likes: s.likes.length, liked: s.likes
  }));
}
function persist() {
  saveJSON('members.json', members);
  saveJSON('bans.json', bans);
  saveJSON('wall.json', wall.slice(0, 100));
  saveJSON('stories.json', stories);
  saveJSON('rooms.json', ROOMS);
  saveJSON('pms.json', pms);
  const slim = {};
  Object.keys(history).forEach(k => {
    slim[k] = history[k].slice(-HISTORY_MAX).map(m => {
      const c = Object.assign({}, m);
      if (c.img) c.img = null;          /* الصور لا تُحفظ على القرص */
      if (c.vid) c.vid = null;          /* ولا الفيديو — أثقل منها */
      return c;
    });
  });
  saveJSON('history.json', slim);
}
setInterval(persist, 30000);
/* حفظٌ مؤجَّلٌ بعد التغيير مباشرةً: يُجمّع التغييرات المتلاحقة في
   كتابةٍ واحدة بعد ثانيةٍ وربع، فلا يُنتظر دورُ الثلاثين ثانية ولا
   يُكتب القرصُ مع كلّ حدث. */
let _pt = null;
function persistSoon(){ clearTimeout(_pt); _pt = setTimeout(persist, 1200); }
['SIGINT', 'SIGTERM'].forEach(s => process.on(s, () => { persist(); process.exit(0); }));

/* ══ أدوات ══ */
const uid = () => 'u' + crypto.randomBytes(6).toString('hex');
function clean(v, max) {
  return String(v == null ? '' : v)
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .trim().slice(0, max || 200);
}
function hashPass(pass, salt) {
  return crypto.scryptSync(String(pass), salt, 32).toString('hex');
}
function isColor(v) {
  if (typeof v !== 'string') return false;
  v = v.trim();
  return v.length <= 40 && (
    /^#[0-9a-fA-F]{3,8}$/.test(v) ||
    /^(rgb|rgba|hsl|hsla)\([\d\s.,%\/]+\)$/.test(v) ||
    /^[a-zA-Z]{3,20}$/.test(v));
}
function banned(name) {
  const b = bans[name.toLowerCase()];
  if (!b) return null;
  if (b.until && b.until < Date.now()) { delete bans[name.toLowerCase()]; return null; }
  return b;
}

/* ══ الحالة الحيّة ══ */
const clients = new Map();     /* ws -> user */
const dashLog = [];                          /* سجلّ الأحداث للوحة التحكّم */
function logEvent(kind, u, extra) {
  dashLog.push(Object.assign({
    kind, name: u.name, ip: u.ip, topic: u.topic || '', at: Date.now()
  }, extra || {}));
  if (dashLog.length > 500) dashLog.shift();
}
function usersArr() { return [...clients.values()]; }
function byId(id)   { return usersArr().find(u => u.id === id); }

function sendTo(ws, obj) {
  if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }
}
function broadcast(obj, filter) {
  for (const [ws, u] of clients) {
    if (filter && !filter(u)) continue;
    sendTo(ws, obj);
  }
}
function inRoom(roomId) { return u => String(u.room) === String(roomId); }

function pub(u) {
  return {
    id: u.id, name: u.name, rank: u.rank, flag: u.flag || 'sa',
    pic: u.pic || '', topic: u.topic || '', room: u.room,
    color: u.color || '', bg: u.bg || '', textColor: u.textColor || ''
  };
}
function roomList() {
  return ROOMS.map(r => ({
    id: r.id, name: r.name, cap: r.cap,
    n: usersArr().filter(inRoom(r.id)).length,
    /* v4.87 — about/voice/pic/color لم تكن تُبثّ قطّ، فبقيت
       الغرف بلا وصفٍ ولا صورةٍ ولا تمييز صوتيّ للأبد — حتى
       الأربع الأصليّة. lk/lkv/hi/pass تبقى داخليّةً، لا تخصّ
       العرض العامّ. */
    about: r.about || '', voice: !!r.voice, pic: r.pic || '', color: r.color || ''
  }));
}

/* ══ حدّ المعدّل ══ */
function allowed(u) {
  const now = Date.now();
  u._hits = (u._hits || []).filter(t => now - t < 3000);
  if (u._hits.length >= 6) return false;
  u._hits.push(now);
  return true;
}

/* ══ خادم الملفّات ══ */
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.gif': 'image/gif',
  '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.woff2': 'font/woff2'
};
const server = http.createServer((req, res) => {
  let p = decodeURIComponent(req.url.split('?')[0]);
  if (p === '/' || p === '') p = '/index.html';
  if (p === '/health') { res.writeHead(200); return res.end('ok'); }
  const file = path.join(PUBLIC, path.normalize(p).replace(/^(\.\.[\/\\])+/, ''));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end('forbidden'); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(buf);
  });
});

/* ══ WebSocket ══ */
const wss = new WebSocketServer({ server, maxPayload: 6 * 1024 * 1024 });

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '')
             .split(',')[0].trim();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.type !== 'string') return;
    const u = clients.get(ws);

    /* ── الدخول ── */
    if (m.type === 'login' || m.type === 'join' || m.type === 'hello') {
      if (u) return;
      const name = clean(m.name, 24);
      if (!name) return sendTo(ws, { type: 'err', why: 'الاسم مطلوب' });
      const b = banned(name);
      if (b) return sendTo(ws, { type: 'err', why: 'محظور: ' + (b.why || '') });
      if (usersArr().some(x => x.name === name))
        return sendTo(ws, { type: 'err', why: 'الاسم مستعمل الآن' });

      const key = name.toLowerCase();
      let rank = 'guest';
      let rec = members[key];
      if (m.pass) {
        if (rec) {
          if (hashPass(m.pass, rec.salt) !== rec.hash)
            return sendTo(ws, { type: 'err', why: 'كلمة المرور خطأ' });
          rank = rec.rank;
        } else {
          const salt = crypto.randomBytes(8).toString('hex');
          rec = members[key] = {
            name, salt, hash: hashPass(m.pass, salt),
            rank: Object.keys(members).length === 0 ? 'owner' : 'member',
            pic: '', color: '', bg: '', textColor: ''
          };
          rank = rec.rank;
          persist();
        }
      }
      const user = {
        id: uid(), name, rank, ip, ws,
        room: ROOMS[0].id, flag: m.flag || 'sa',
        pic: (rec && rec.pic) || '', topic: '',
        color: (rec && rec.color) || '', bg: (rec && rec.bg) || '',
        textColor: (rec && rec.textColor) || '',
        stealth: !!m.stealth
      };
      clients.set(ws, user);

      sendTo(ws, { type: 'me', id: user.id, name: user.name, rank: user.rank,
                   ranks: RANKS, need: NEED, room: user.room });
      /* الداخلُ يُدرَج في قائمته أيضًا: العميل يضيف نفسه محلّيًّا في
         الوضع بلا خادم فقط، فمع الخادم لا يرى نفسه ما لم يصله here. */
      usersArr().forEach(o => { if (!o.stealth || o.id === user.id)
        sendTo(ws, Object.assign({ type: 'here' }, pub(o))); });
      sendTo(ws, { type: 'rooms', rooms: roomList() });
      sendTo(ws, { type: 'wall', wall: wall.slice(0, 30) });
      sendTo(ws, { type: 'stories', stories: liveStories() });
      sendTo(ws, micStateOf(user.room));
      (history[user.room] || []).slice(-40).forEach(h => sendTo(ws, Object.assign({}, h, { type: 'msg', old: 1 })));
      /* تنبيه دخولي يُرسَل إليّ وحدي رسالةَ نظام.
         لا يصحّ إرساله بـrjoin: العميل يستثني نفسه منه عمدًا
         (يقارن t.id بـ NET.me.id) لأنّ بناء الغرفة محلّيًّا يضع
         التنبيه مسبقًا — وذلك البناء لا يقع مع خادم. فيُبعث هنا
         msg بصنف hmsg، وهو الصنف نفسه الذي يرسمه المستمع. */
      /* v4.78 — الحقل bid لا mid: العميل يقرأ m.bid في كل موضعٍ
         (الردّ، الحذف، الإعجاب — 15 استعمالًا)، وكان الخادم يبعثه
         باسم mid فيبقى m.bid غير معرَّفٍ أبدًا لأيّ رسالةٍ حقيقيّة
         واصلة من خادم — فلا data-bid يُكتب على الصفّ، ولا زرّ ردٍّ
         يظهر إطلاقًا. كشفه اختبارٌ بمتصفّحين مستقلّين حقيقيّين لا
         نداءً مصطنعًا يزوّد bid يدويًّا كما فعلت كلّ اختباراتي قبله. */
      sendTo(ws, {
        type: 'msg', bid: crypto.randomBytes(8).toString('hex'),
        id: user.id, name: user.name, rank: user.rank, room: user.room,
        text: 'هذا المستخدم قد دخل', kind: 'hmsg', at: Date.now(), self: 1
      });
      if (!user.stealth) broadcast(Object.assign({ type: 'join' }, pub(user)), o => o.id !== user.id);
      logEvent('دخول', user, { src: 'دخول' });
      broadcast({ type: 'rooms', rooms: roomList() });
      return;
    }

    if (!u) return sendTo(ws, { type: 'err', why: 'لم تدخل بعد' });

    switch (m.type) {
      /* ── رسالة ── */
      case 'msg': {
        if (!allowed(u)) return sendTo(ws, { type: 'err', why: 'تمهّل قليلًا' });
        if (u.muted) return sendTo(ws, { type: 'err', why: 'أنت مكتوم' });
        const text = clean(m.text, 700);
        const img  = (typeof m.img === 'string' && m.img.startsWith('data:image/') &&
                      m.img.length < 4 * 1024 * 1024) ? m.img : null;
        if (!text && !img) return;
        /* v4.82 — reto لم يكن يُقرأ من العميل ولا يُبثّ أصلًا: كل ردٍّ
           حقيقيّ عبر خادمٍ فعليّ كان يفقد ربطه بأصله فور مروره هنا،
           فتبقى قائمة «التعليقات» «لا ردود بعد» دومًا مهما رُدّ فعلًا.
           كل اختباراتي السابقة نجحت بالخطأ: كانت تُطلق reto مباشرةً
           عبر NET._handlers.msg مصطنعةً، متجاوزةً هذا المسار كليًّا. */
        const reto = (typeof m.reto === 'string' && m.reto.length < 64) ? m.reto : null;
        const out = {
          type: 'msg', bid: crypto.randomBytes(8).toString('hex'),
          id: u.id, name: u.name, rank: u.rank, room: u.room,
          text, img, reto, at: Date.now(),
          color: u.color, bg: u.bg, textColor: u.textColor, pic: u.pic
        };
        history[u.room] = (history[u.room] || []).concat(out).slice(-HISTORY_MAX);
        broadcast(out, inRoom(u.room));
        return;
      }
      case 'pic': {
        if (!allowed(u)) return;
        const img = (typeof m.img === 'string' && m.img.startsWith('data:image/') &&
                     m.img.length < 4 * 1024 * 1024) ? m.img : null;
        if (!img) return sendTo(ws, { type: 'err', why: 'صورة غير صالحة' });
        const out = { type: 'msg', bid: crypto.randomBytes(8).toString('hex'),
          id: u.id, name: u.name, rank: u.rank, room: u.room,
          text: '', img, at: Date.now(),
          color: u.color, bg: u.bg, textColor: u.textColor, pic: u.pic };
        history[u.room] = (history[u.room] || []).concat(out).slice(-HISTORY_MAX);
        broadcast(out, inRoom(u.room));
        return;
      }
      /* v4.80 — فيديو: نفس معالج pic حرفًا بحدٍّ أكبر (8MB لا 4) —
         الفيديو أثقل من الصورة، ولا سبيل لضغطه في المتصفّح كضغط
         الصور بإعادة الرسم على canvas. كُشف هذا الزرّ («عرض الفيديو»)
         في نافذة التعليقات على khaleejchat عبر uix-replybox، فبنيته
         بمعالجٍ مستقلٍّ لا حقلًا إضافيًّا على msg — يطابق pic تمامًا
         في كل شيءٍ إلّا نوع البيانات وحدّها. */
      case 'vid': {
        if (!allowed(u)) return;
        const vid = (typeof m.vid === 'string' && m.vid.startsWith('data:video/') &&
                     m.vid.length < 8 * 1024 * 1024) ? m.vid : null;
        if (!vid) return sendTo(ws, { type: 'err', why: 'فيديو غير صالح أو أكبر من الحدّ' });
        const out = { type: 'msg', bid: crypto.randomBytes(8).toString('hex'),
          id: u.id, name: u.name, rank: u.rank, room: u.room,
          text: '', vid, at: Date.now(),
          color: u.color, bg: u.bg, textColor: u.textColor, pic: u.pic };
        history[u.room] = (history[u.room] || []).concat(out).slice(-HISTORY_MAX);
        broadcast(out, inRoom(u.room));
        return;
      }

      /* ── الإعجاب والحذف ──
         v4.83 — لم يكن لأيٍّ منها معالجٌ في الخادم إطلاقًا: العميل
         يرسل like/msgdel/likebc منذ أوّل بناءٍ للشات، والخادم لا
         يعرفها فتضيع صامتةً. زرّا الإعجاب والحذف في الغرفة الرئيسيّة
         نفسها — لا الحائط وحده — لم يعملا قطّ عبر خادمٍ حقيقيّ طوال
         هذه المحادثة، لأنّ كل اختبارٍ سابقٍ فحص وجود الزرّ وقابليّته
         للنقر لا أثره الفعليّ بعد جولةٍ كاملة عبر خادم. الردّ نفسه
         رسالةٌ عاديّةٌ (بـreto) تعيش في history نفسه، فـlikebc تشارك
         آلة like ذاتها؛ الاختلاف في أيّ عنصر DOM يُحدَّث عند العميل
         وحده، لا في المنطق. */
      case 'like': {
        const arr = history[u.room] || [];
        const msg = arr.find(x => x.bid === m.bid);
        if (!msg) return;
        msg.likes = (msg.likes || 0) + 1;
        broadcast({ type: 'like', bid: m.bid, likes: msg.likes }, inRoom(u.room));
        return;
      }
      case 'likebc': {
        if (!m.bid) return;   /* القلب العائم العامّ بلا bid لم يُطلَب بعد، لا نبنيه بمعزلٍ عن طلب */
        const arr = history[u.room] || [];
        const msg = arr.find(x => x.bid === m.bid);
        if (!msg) return;
        msg.likes = (msg.likes || 0) + 1;
        broadcast({ type: 'likebc', bid: m.bid, likes: msg.likes }, inRoom(u.room));
        return;
      }
      case 'msgdel': {
        const arr = history[u.room] || [];
        const idx = arr.findIndex(x => x.bid === m.bid);
        if (idx < 0) return;
        const msg = arr[idx];
        const canDel = msg.id === u.id || RANKS[u.rank].n >= (NEED.msgdel || 2);
        if (!canDel) return sendTo(ws, { type: 'err', why: 'ليس لك حذف هذه الرسالة' });
        arr.splice(idx, 1);
        broadcast({ type: 'msgdel', bid: m.bid }, inRoom(u.room));
        return;
      }

      /* ── الملفّ الشخصيّ: الصورة والألوان والحالة ── */
      case 'setprofile': {
        if (typeof m.pic === 'string') {
          if (m.pic === '') u.pic = '';
          else if (m.pic.startsWith('data:image/') && m.pic.length < 3 * 1024 * 1024) u.pic = m.pic;
        }
        if (isColor(m.color))     u.color = m.color.trim();
        if (isColor(m.bg))        u.bg = m.bg.trim();
        if (isColor(m.textColor)) u.textColor = m.textColor.trim();
        if (m.topic != null)      u.topic = clean(m.topic, 40);
        if (m.name) {
          const nn = clean(m.name, 24);
          if (nn && nn !== u.name && !usersArr().some(x => x.name === nn)) u.name = nn;
        }
        const rec = members[u.name.toLowerCase()];
        if (rec) {
          rec.pic = u.pic; rec.color = u.color; rec.bg = u.bg; rec.textColor = u.textColor;
          persist();
        }
        broadcast(Object.assign({ type: 'ustate' }, pub(u)));
        if (u.room && micSeats[u.room]) {
          /* اسمٌ أو صورةٌ تغيّرت وأنا على المايك: يتحدّث مقعدي معها */
          const seats = micSeats[u.room];
          const idx = seats.findIndex(x => x && x.id === u.id);
          if (idx > -1) {
            seats[idx].name = u.name; seats[idx].pic = u.pic || '';
            broadcast(micStateOf(u.room), inRoom(u.room));
          }
        }
        return;
      }

      /* ── الغرف ── */
      case 'rjoin': {
        const r = ROOMS.find(x => String(x.id) === String(m.room));
        if (!r) return sendTo(ws, { type: 'err', why: 'غرفة غير موجودة' });
        if (usersArr().filter(inRoom(r.id)).length >= r.cap)
          return sendTo(ws, { type: 'err', why: 'الغرفة ممتلئة' });
        const old = u.room;
        releaseSeat(u.id);   /* لا يبقى على المايك في غرفةٍ غادرها */
        u.room = r.id;
        broadcast({ type: 'rleave', id: u.id, name: u.name, room: old }, inRoom(old));
        broadcast({ type: 'rjoin', id: u.id, name: u.name, room: r.id, rank: u.rank });
        (history[r.id] || []).slice(-40).forEach(h => sendTo(ws, Object.assign({}, h, { type: 'msg', old: 1 })));
        sendTo(ws, micStateOf(r.id));
        broadcast({ type: 'rooms', rooms: roomList() });
        return;
      }
      case 'rleave': {
        const old = u.room;
        releaseSeat(u.id);
        u.room = null;
        broadcast({ type: 'rleave', id: u.id, name: u.name, room: old });
        broadcast({ type: 'rooms', rooms: roomList() });
        return;
      }

      /* ── الخاصّ ── */
      /* v4.88 — 'pm' مباشرةً لا مُستدعًى إطلاقًا: كل مسارات الإرسال
         الحقيقية (cwSend ← NET.pmSend، والإعلان ← NET.act('pmsg'))
         تمرّ عبر NET.act فتصل بصيغة {type:'act', act:X, target,...}
         لا {type:'pm'} مباشرة — فمهما بُني معالج 'pm' هنا لا يصله
         شيءٌ من الواجهة الحقيقية أبدًا. الحلّ: معالجٌ واحدٌ لـ'act'
         يُفرّق بحقل act، لا حالتان متوازيتان إحداهما ميتة. */
      case 'act': {
        if (m.act === 'pm') {
          if (!allowed(u)) return;
          const to = byId(m.target) || usersArr().find(x => x.name === m.target);
          if (!to) return sendTo(ws, { type: 'err', why: 'المستخدم غير موجود' });
          const out = { type: 'pm', from: u.name, fromId: u.id, to: to.name,
                        text: clean(m.text, 700), at: Date.now() };
          sendTo(to.ws, out);
          sendTo(ws, Object.assign({}, out, { mine: 1 }));
          const key = pmKey(u.name, to.name);
          pms[key] = (pms[key] || []).concat({ from: u.name, to: to.name,
            text: out.text, at: out.at }).slice(-200);
          persistSoon();
          return;
        }
        if (m.act === 'pmsg') {
          if (RANKS[u.rank].n < NEED.bc) return sendTo(ws, { type: 'err', why: 'للإدارة فقط' });
          broadcast({ type: 'msg', bid: crypto.randomBytes(8).toString('hex'),
            id: u.id, name: u.name, rank: 'admin', text: clean(m.text, 400),
            at: Date.now(), bc: 1, color: u.color });
          return;
        }
        return;
      }
      case 'pm.log': {
        const name = clean(m.name, 24);
        if (!name) return;
        const key = pmKey(u.name, name);
        sendTo(ws, { type: 'pm.log', name, log: pms[key] || [] });
        return;
      }
      case 'pms': {
        const mine = u.name.toLowerCase();
        const list = [];
        Object.keys(pms).forEach(key => {
          const parts = key.split('|');
          if (parts.indexOf(mine) < 0) return;
          const msgs = pms[key];
          if (!msgs.length) return;
          const last = msgs[msgs.length - 1];
          const otherName = last.from.toLowerCase() === mine ? last.to : last.from;
          list.push({ name: otherName, text: last.text, at: last.at, n: msgs.length });
        });
        list.sort((a, b) => b.at - a.at);
        sendTo(ws, { type: 'pms', list });
        return;
      }

      /* ── المايك: مطالبة مقعد وتركه وكتمه ──
         خمسة مقاعد لكلّ غرفة. مطالبةٌ صريحةٌ بمقعدٍ بعينه أو أوّل
         شاغر، وتركٌ يُحرّره، وكتمٌ فرديّ يحتاج رتبة، وكتمٌ جماعيّ
         يبدّل حالة الجميع دفعةً واحدة. */
      case 'mic.take': {
        const room = u.room;
        if (!room || !micSeats[room]) return;
        const seats = micSeats[room];
        if (seats.some(x => x && x.id === u.id)) return;   /* جالسٌ أصلًا */
        let idx = Number(m.seat);
        if (!(idx >= 0 && idx < seats.length) || seats[idx]) {
          idx = seats.findIndex(x => !x);
        }
        if (idx < 0) return sendTo(ws, { type: 'err', why: 'لا مقاعد شاغرة' });
        seats[idx] = { id: u.id, name: u.name, pic: u.pic || '', muted: false };
        broadcast(micStateOf(room), inRoom(room));
        return;
      }
      case 'mic.leave': {
        const room = u.room;
        if (!room || !micSeats[room]) return;
        const seats = micSeats[room];
        const idx = seats.findIndex(x => x && x.id === u.id);
        if (idx < 0) return;
        seats[idx] = null;
        broadcast(micStateOf(room), inRoom(room));
        return;
      }
      case 'mic.mute': {
        const room = u.room;
        if (!room || !micSeats[room]) return;
        const seats = micSeats[room];
        const targetId = m.id || u.id;
        const idx = seats.findIndex(x => x && x.id === targetId);
        if (idx < 0) return;
        if (targetId !== u.id && RANKS[u.rank].n < NEED.mute)
          return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        seats[idx].muted = !seats[idx].muted;
        broadcast(micStateOf(room), inRoom(room));
        return;
      }
      case 'mic.muteall': {
        if (RANKS[u.rank].n < NEED.mute) return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        const room = u.room;
        if (!room || !micSeats[room]) return;
        const seats = micSeats[room];
        const anyUnmuted = seats.some(x => x && !x.muted);
        seats.forEach(x => { if (x) x.muted = anyUnmuted; });
        broadcast(micStateOf(room), inRoom(room));
        return;
      }

      /* ── لوحة التحكم: تقارير الأقسام ──
         الواجهة تُرسل {type:'dash', section} وتنتظر {type:'dash',
         section, rows}. البنية كاملةٌ فيها (DASH_TABLES وrebuildDash)
         وكان الخادم لا يردّ، فتبقى الجداول فارغةً. الأقسام الخمسة:
         users · rooms · bans · states · log. وكلّها للإدارة فقط. */
      case 'dash': {
        if (RANKS[u.rank].n < NEED.cp)
          return sendTo(ws, { type: 'err', why: 'لوحة التحكّم للمالك' });
        const sec = String(m.section || '');
        let rows = [];
        const arr = usersArr();
        if (sec === 'users') {
          /* الأعضاء المسجّلون لا المتصلون فقط */
          rows = Object.keys(members).map(k => {
            const rec = members[k];
            const on = arr.find(x => x.name.toLowerCase() === k);
            return {
              'العضو': rec.name,
              'الزخرفه': (on && on.topic) || '',
              'الآي بي': on ? on.ip : '—',
              'الجهاز': '—',
              'صلاحيات': (RANKS[rec.rank] || {}).label || rec.rank,
              'لايكات': rec.likes || 0,
              'آخر تواجد': on ? 'الآن' : (rec.seen ? new Date(rec.seen).toLocaleString('ar') : '—'),
              'التسجيل': rec.created ? new Date(rec.created).toLocaleDateString('ar') : '—',
              _id: (on && on.id) || '', _name: rec.name
            };
          });
        } else if (sec === 'rooms') {
          rows = ROOMS.map(r => ({
            'الغرفه': r.name,
            'صاحب الغرفه': r.owner || '—',
            'اعدادات': usersArr().filter(inRoom(r.id)).length + '/' + r.cap,
            _id: r.id
          }));
        } else if (sec === 'bans') {
          rows = Object.keys(bans).map(name => {
            const b = bans[name];
            return {
              'العضو': name,
              'الحاله': b.until ? (b.until > Date.now() ? 'محظور' : 'منتهٍ') : 'مؤبد',
              'اسم الحظر': name,
              'رقم الحظر': b.id || '—',
              'الجهاز': b.device || '—',
              'الدوله': b.country || '—',
              'الآي بي': b.ip || '—',
              _name: name
            };
          });
        } else if (sec === 'states') {
          rows = arr.map(x => ({
            'الحاله': x.muted ? 'مكتوم' : (x.room ? 'في غرفة' : 'في الشات'),
            'العضو': x.name,
            'العضو الثاني': '—',
            'الغرفه': (ROOMS.find(r => String(r.id) === String(x.room)) || {}).name || '—',
            'الاي بي': x.ip,
            'الوقت': new Date(x.at || Date.now()).toLocaleTimeString('ar'),
            _id: x.id
          }));
        } else if (sec === 'log') {
          /* سجلٌّ حيٌّ لآخر الأحداث — يُجمّع في dashLog */
          rows = dashLog.slice(-200).reverse().map(e => ({
            'الحاله': e.kind,
            'العضو': e.name,
            'الزخرفه': e.topic || '',
            'الآي بي': e.ip || '—',
            'الدوله': e.country || '—',
            'الجهاز': e.device || '—',
            'المصدر': e.src || '—',
            'الدعوه': e.ref || '—',
            'الوقت': new Date(e.at).toLocaleString('ar')
          }));
        } else {
          return sendTo(ws, { type: 'err', why: 'قسم غير معروف' });
        }
        sendTo(ws, { type: 'dash', section: sec, rows });
        return;
      }

      /* ── الاستوري ──
         الواجهة تُرسل story.add و story.like و story.del وتنتظر
         حدث stories بالقائمة. كانت المعالجات مفقودةً في الخادم،
         فتبقى القائمة فارغةً أبدًا والشريط مطويًّا — وزرُّ الإضافة
         نفسه داخل الشريط، فلا سبيل لنشر أوّل استوري. حلقةٌ مغلقة. */
      case 'story.add': {
        if (!allowed(u)) return sendTo(ws, { type: 'err', why: 'تمهّل قليلًا' });
        const media = m.media;
        if (typeof media !== 'string' || !media.startsWith('data:image/') ||
            media.length > STORY_MAX_BYTES)
          return sendTo(ws, { type: 'err', why: 'الصورة غير صالحة أو أكبر من الحدّ' });
        stories.unshift({
          id: crypto.randomBytes(6).toString('hex'),
          uid: u.id, name: u.name, pic: u.pic || '',
          media, kind: m.kind === 'video' ? 'video' : 'image',
          at: Date.now(), likes: []
        });
        if (stories.length > STORY_MAX) stories.length = STORY_MAX;
        persistSoon();
        broadcast({ type: 'stories', stories: liveStories() });
        return;
      }
      case 'story.like': {
        const st = stories.find(x => x.id === m.id);
        if (!st) return;
        const i = st.likes.indexOf(u.id);
        if (i < 0) st.likes.push(u.id); else st.likes.splice(i, 1);
        persistSoon();
        broadcast({ type: 'stories', stories: liveStories() });
        return;
      }
      case 'story.del': {
        const st = stories.find(x => x.id === m.id);
        if (!st) return;
        /* صاحبه يحذفه، ومن رتبته تكفي للإشراف */
        if (st.uid !== u.id && RANKS[u.rank].n < NEED.msgdel)
          return sendTo(ws, { type: 'err', why: 'ليس لك حذفه' });
        stories = stories.filter(x => x.id !== m.id);
        persistSoon();
        broadcast({ type: 'stories', stories: liveStories() });
        return;
      }

      /* ── الحائط ── */
      /* v4.88 — أُعيد تسميتها من 'wall' إلى 'wall.add': هذا ما ترسله
         الواجهة الحقيقية فعلًا (wallSend ← NET.wallAdd) — لا 'wall'
         التي ظننتها سابقًا. اختباراتي القديمة نجحت بنداءٍ يدويٍّ
         مباشر (NET.send('wall',{text})) يتجاوز غلاف NET.wallAdd
         الحقيقي، فأخفى أنّ النشر عبر الواجهة الفعليّة كان لا يعمل
         قطّ. والصلاحية أُنزلت من NEED.bc (إداريّ) إلى NEED.mkr
         (عضوٌ فأعلى، كإنشاء الغرف): NEED.bc خاصّةٌ بميزة الإعلانات
         المنفصلة كليًّا (case 'bc' أدناه)، لا الحائط — والزرّ نفسه
         لا يفرض أيّ رتبةٍ في الواجهة. */
      case 'wall.add': {
        if (RANKS[u.rank].n < NEED.mkr) return sendTo(ws, { type: 'err', why: 'يلزم تسجيل عضويّة' });
        const text = clean(m.text, 300);
        if (!text) return;
        wall.unshift({ id: crypto.randomBytes(6).toString('hex'), name: u.name, text, at: Date.now(),
          rank: u.rank, pic: u.pic, likes: 0 });
        wall = wall.slice(0, 100);
        persist();
        broadcast({ type: 'wall', wall: wall.slice(0, 30) });
        return;
      }
      /* NET.wallAsk ترسل هذه فعلًا (600مل.ث بعد الاتصال) لجلب
         القائمة الأوليّة — طلب قراءةٍ لا إنشاء. كانت تصطدم بمعالج
         الإنشاء القديم بلا نصٍّ فتُتجاهَل بصمت، فتبقى اللوحة فارغةً
         حتى ينشر أحدٌ شيئًا جديدًا. */
      case 'wall': {
        sendTo(ws, { type: 'wall', wall: wall.slice(0, 30) });
        return;
      }
      /* v4.83 — كذلك: NET.wallLike/wallDel يرسلان wall.like/wall.del
         منذ بنائهما، والخادم لا يعرفهما فيضيعان صامتَين — زرّا
         الإعجاب والحذف في الحائط معطَّلان تمامًا رغم ظهورهما ورغم
         قابليّتهما للنقر. يُعاد بثّ القائمة كاملةً كما تفعل 'wall'
         تمامًا، فتُعيد wallSync رسمها بلا بروتوكولٍ جزئيٍّ جديد. */
      case 'wall.like': {
        const w = wall.find(x => x.id === m.id);
        if (!w) return;
        w.likes = (w.likes || 0) + 1;
        persist();
        broadcast({ type: 'wall', wall: wall.slice(0, 30) });
        return;
      }
      case 'wall.del': {
        const idx = wall.findIndex(x => x.id === m.id);
        if (idx < 0) return;
        const w = wall[idx];
        const canDel = w.name === u.name || RANKS[u.rank].n >= NEED.bc;
        if (!canDel) return sendTo(ws, { type: 'err', why: 'ليس لك حذف هذا المنشور' });
        wall.splice(idx, 1);
        persist();
        broadcast({ type: 'wall', wall: wall.slice(0, 30) });
        return;
      }

      /* ── الغرف: إنشاءٌ وتعديل ──
         v4.87 — كان العميل يرسل mkr/redit كاملةً (اسمٌ، وصفٌ، صورةٌ،
         لونٌ، سعةٌ، صوتيّةٌ) منذ أوّل بناءٍ لنافذة «غرفه جديدة»،
         والخادم لا يعرف النوعين إطلاقًا فتضيع الرسالة صامتة — لا
         غرفة تُنشأ ولا تُعدَّل ولا صورتها تتغيّر ولا اسمها، رغم أنّ
         الواجهة كاملةً وتعمل ظاهريًّا بلا خطإٍ واحد. نفس عائلة عطب
         wall.like/msgdel تمامًا: بناءٌ أماميّ تامّ بلا استقبالٍ خلفيّ. */
      case 'mkr': {
        if (RANKS[u.rank].n < NEED.mkr) return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        const name = clean(m.name, 40);
        if (!name) return sendTo(ws, { type: 'err', why: 'اسم الغرفه مطلوب' });
        const cap = Math.max(2, Math.min(200, (+m.cap || 30)));
        const pic = (typeof m.pic === 'string' && m.pic.startsWith('data:image/') &&
                     m.pic.length < 2 * 1024 * 1024) ? m.pic : '';
        const id = crypto.randomBytes(4).toString('hex');
        const room = {
          id, name, cap,
          about: clean(m.about, 100),
          hi: clean(m.hi, 100),
          pass: clean(m.pass, 40),
          lk: +m.lk || 0, lkv: +m.lkv || 0,
          voice: !!m.voice,
          color: (typeof m.color === 'string' && /^#[0-9a-fA-F]{3,8}$/.test(m.color)) ? m.color : '',
          pic
        };
        ROOMS.push(room);
        history[id] = [];
        micSeats[id] = [null, null, null, null, null];
        persist();
        broadcast({ type: 'rooms', rooms: roomList() });
        return;
      }
      case 'redit': {
        if (RANKS[u.rank].n < NEED.redit) return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        const room = ROOMS.find(r => String(r.id) === String(m.room));
        if (!room) return sendTo(ws, { type: 'err', why: 'غرفة غير موجودة' });
        if (typeof m.name === 'string' && m.name.trim()) room.name = clean(m.name, 40);
        if (typeof m.about === 'string') room.about = clean(m.about, 100);
        if (typeof m.hi === 'string') room.hi = clean(m.hi, 100);
        if (typeof m.pass === 'string') room.pass = clean(m.pass, 40);
        if (m.cap != null) room.cap = Math.max(2, Math.min(200, (+m.cap || room.cap)));
        if (m.lk != null) room.lk = +m.lk || 0;
        if (m.lkv != null) room.lkv = +m.lkv || 0;
        if (m.voice != null) room.voice = !!m.voice;
        if (typeof m.color === 'string')
          room.color = /^#[0-9a-fA-F]{3,8}$/.test(m.color) ? m.color : '';
        /* pic: نصٌّ فارغٌ يحذف الصورة، data: تستبدلها، أيّ شيءٍ آخر
           (بما فيه الغياب) يُبقيها كما هي — كما وثّق العميل نفسه. */
        if (typeof m.pic === 'string') {
          room.pic = (m.pic === '' || (m.pic.startsWith('data:image/') && m.pic.length < 2 * 1024 * 1024))
            ? m.pic : room.pic;
        }
        persist();
        broadcast({ type: 'rooms', rooms: roomList() });
        return;
      }

      /* ── الإشراف ── */
      case 'kick':
      case 'ban': {
        const need = m.type === 'ban' ? NEED.ban : NEED.kick;
        if (RANKS[u.rank].n < need) return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        const t = byId(m.id) || usersArr().find(x => x.name === m.name);
        if (!t) return sendTo(ws, { type: 'err', why: 'غير موجود' });
        if (RANKS[t.rank].n >= RANKS[u.rank].n)
          return sendTo(ws, { type: 'err', why: 'لا يمكن على رتبةٍ مثلك أو أعلى' });
        if (m.type === 'ban') {
          const hours = Math.min(720, Math.max(1, Number(m.hours) || 24));
          bans[t.name.toLowerCase()] = { until: Date.now() + hours * 3600e3, why: clean(m.why, 80) };
          persist();
        }
        broadcast({ type: 'kicked', id: t.id, name: t.name, ban: m.type === 'ban', by: u.name });
        sendTo(t.ws, { type: 'kicked', id: t.id, name: t.name, ban: m.type === 'ban', me: 1 });
        try { t.ws.close(); } catch (e) {}
        return;
      }
      case 'mute': {
        if (RANKS[u.rank].n < NEED.mute) return sendTo(ws, { type: 'err', why: 'صلاحيّة غير كافية' });
        const t = byId(m.id);
        if (!t) return;
        t.muted = !t.muted;
        broadcast(Object.assign({ type: 'ustate', muted: !!t.muted }, pub(t)));
        return;
      }
      case 'rank': {
        if (RANKS[u.rank].n < RANKS.owner.n) return sendTo(ws, { type: 'err', why: 'للمالك فقط' });
        const t = byId(m.id);
        if (!t || !RANKS[m.rank]) return;
        t.rank = m.rank;
        const rec = members[t.name.toLowerCase()];
        if (rec) { rec.rank = m.rank; persist(); }
        broadcast({ type: 'rankchanged', id: t.id, rank: t.rank });
        return;
      }
      /* v4.88 — case 'bc' حُذفت: كانت مكرّرةً ميتة — لا شيء يرسل
         type:'bc' مباشرةً؛ المسار الحقيقي الوحيد (زرّ «إعلان» ←
         NET.act('pmsg')) صار مُعالَجًا أعلاه ضمن case 'act'. */

      case 'who':
        sendTo(ws, { type: 'rooms', rooms: roomList() });
        usersArr().forEach(o => { if (o.id !== u.id && !o.stealth) sendTo(ws, Object.assign({ type: 'here' }, pub(o))); });
        return;
      /* NET.askRooms ترسلها لتحديث القائمة يدويًّا؛ الغرف تصل أصلًا
         عند الدخول ومع كل تغييرٍ، فهذا رخيصٌ ومكمّلٌ لا حرجٌ حقيقي. */
      case 'rooms':
        sendTo(ws, { type: 'rooms', rooms: roomList() });
        return;

      case 'turn':
      case 'bitrate':
        return;   /* إعداداتٌ محلّيّة، تُقبل بلا أثرٍ على الخادم */

      case 'logout':
        try { ws.close(); } catch (e) {}
        return;

      default:
        return;
    }
  });

  ws.on('close', () => {
    const u = clients.get(ws);
    clients.delete(ws);
    if (u) {
      logEvent('خروج', u, { src: 'خروج' });
      releaseSeat(u.id);   /* اتّصالٌ مقطوعٌ لا يبقى صاحبه على مقعدٍ حيّ */
      broadcast({ type: 'leave', id: u.id, name: u.name });
      broadcast({ type: 'rooms', rooms: roomList() });
    }
  });
  ws.on('error', () => {});
});

/* نبضٌ يُسقط الاتّصالات الميّتة — بلا هذا تتراكم على الاستضافات المجّانيّة */
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 30000);

server.listen(PORT, () => {
  console.log('الخادم يعمل على المنفذ ' + PORT);
  console.log('الغرف: ' + ROOMS.map(r => r.name).join(' · '));
});
