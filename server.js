/**
 * ============================================================
 *  سرور بازی نقاشی آنلاین
 *  Node.js + Express + Socket.io + MongoDB
 *  دو حالت: امتیازی (۴ نفره) و حذفی (۶ نفره)
 * ============================================================
 */

const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

// ============================================================
//  ۱) راه‌اندازی Express و Socket.io
// ============================================================
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

// مسیر پینگ برای Render
app.get('/health', (req, res) => res.send('OK'));
app.get('/', (req, res) => res.send('Drawing game server is running'));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 2e6, // حداکثر ۲ مگابایت برای هر پیام (نقاشی ضبط‌شده)
});

const PORT = process.env.PORT || 3000;

// ============================================================
//  ۲) تنظیمات و قوانین بازی
// ============================================================
const MODES = {
  score: {
    max: 4,              // تعداد بازیکن
    totalRounds: 4,      // تعداد دور
    optionsCount: 10,    // تعداد گزینه‌ها
    roundTime: 30000,    // ۳۰ ثانیه نقاشی زنده
    coins: 100,          // جایزه سکه برنده
    bonusPoints: 20,     // جایزه امتیاز برنده
  },
  knockout: {
    max: 6,
    totalRounds: 999,
    optionsCount: 6,
    drawTime: 30000,     // زمان نقاشی کشیدن (ضبط)
    guessTime: 20000,    // ۲۰ ثانیه وقت حدس
    coins: 200,
    bonusPoints: 50,
  },
};

// امتیازدهی حالت امتیازی
const SCORE_GUESS_POINTS = [6, 5, 4];            // حدس اول، دوم، سوم
const SCORE_DRAWER_POINTS = { 0: 0, 1: 2, 2: 3, 3: 4 }; // نقاش بر اساس تعداد حدس درست

// امتیازدهی حالت حذفی
const KO_CORRECT_POINTS = 5;   // هر حدس درست
const KO_SURVIVE_POINTS = 3;   // هر دور موندن
const KO_WIN_POINTS = 30;      // برد نهایی
const KO_SPEED_RULE_ROUND = 15; // از این دور به بعد قانون سرعت فعال میشه

const NEXT_ROUND_DELAY = 8000; // فاصله بین دورها (اگه میزبان زودتر نزنه)
const MAX_STROKES = 6000;      // سقف تعداد خط‌های ضبط‌شده (ضد سوءاستفاده)

// ============================================================
//  ۳) لیست کلمات
// ============================================================
const WORDS = [...new Set([
  'گربه', 'سگ', 'خانه', 'درخت', 'ماشین', 'خورشید', 'ماه', 'ستاره', 'گل', 'پرنده',
  'ماهی', 'کتاب', 'میز', 'صندلی', 'در', 'پنجره', 'تخت', 'ساعت', 'کلید', 'چتر',
  'کفش', 'کلاه', 'عینک', 'هندوانه', 'سیب', 'موز', 'پرتقال', 'انگور', 'هویج',
  'سیب‌زمینی', 'گوجه', 'خیار', 'نان', 'پنیر', 'تخم‌مرغ', 'شیر', 'چای', 'قهوه',
  'بستنی', 'کیک', 'شکلات', 'آدامس', 'توپ', 'راکت', 'دوچرخه', 'موتور', 'هواپیما',
  'قطار', 'کشتی', 'راکت فضایی', 'ماهواره', 'تلویزیون', 'رادیو', 'موبایل', 'لپ‌تاپ',
  'دوربین', 'چراغ', 'شمع', 'آینه', 'شانه', 'مسواک', 'صابون', 'حوله', 'تیشرت', 'شلوار',
  'کاپشن', 'جوراب', 'دستکش', 'شال', 'کیف', 'کوله', 'چمدان', 'پول', 'سکه', 'کارت',
  'نامه', 'پاکت', 'مداد', 'خودکار', 'دفتر', 'کاغذ', 'قیچی', 'چسب', 'خط‌کش', 'پاک‌کن',
  'مداد رنگی', 'آبرنگ', 'قلم‌مو', 'بوم', 'مجسمه', 'عکس', 'فرش', 'پرده', 'بالش', 'تشک',
  'گلدان', 'آباژور', 'فرفره', 'بادبادک', 'توپ فوتبال', 'دروازه', 'سبد', 'تور',
  'قلب', 'ستاره دریایی', 'صدف', 'خرچنگ', 'اختاپوس', 'دلفین', 'نهنگ', 'کوسه',
  'پنگوئن', 'زرافه', 'فیل', 'ببر', 'خرس', 'میمون', 'خرگوش', 'موش', 'لاک‌پشت',
  'مار', 'عقاب', 'جغد', 'طوطی', 'پروانه', 'زنبور', 'مورچه', 'عنکبوت', 'کفشدوزک',
])];

// ============================================================
//  ۴) MongoDB + جدول بهترین‌ها (با fallback به حافظه)
// ============================================================
const COLLECTIONS = {
  score: 'leaderboard_score',
  knockout: 'leaderboard_knockout',
};

let db = null; // اگه null باشه از حافظه استفاده میشه
const memBoards = { score: new Map(), knockout: new Map() };

async function initMongo() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.log('⚠️  MONGODB_URI تنظیم نشده؛ جدول بهترین‌ها در حافظه سرور نگهداری میشه.');
    return;
  }
  try {
    const client = new MongoClient(uri);
    await client.connect();
    db = client.db(process.env.MONGODB_DB || 'drawing_game');
    for (const name of Object.values(COLLECTIONS)) {
      await db.collection(name).createIndex({ uid: 1 }, { unique: true });
      await db.collection(name).createIndex({ totalScore: -1 });
    }
    console.log('✅ به MongoDB وصل شد.');
  } catch (err) {
    console.error('❌ اتصال MongoDB ناموفق بود، از حافظه استفاده میشه:', err.message);
    db = null;
  }
}

// آپدیت خودکار بعد هر بازی
async function updateLeaderboard(mode, { uid, name, photo, finalScore, won }) {
  const now = new Date();
  if (db) {
    try {
      await db.collection(COLLECTIONS[mode]).updateOne(
        { uid },
        {
          $inc: { totalScore: finalScore, wins: won ? 1 : 0, gamesPlayed: 1 },
          $max: { bestScore: finalScore },
          $set: { name, photo: photo || '', mode, lastGame: now },
        },
        { upsert: true }
      );
      return;
    } catch (err) {
      console.error('خطا در آپدیت لیدربرد Mongo، fallback به حافظه:', err.message);
    }
  }
  // fallback حافظه
  const board = memBoards[mode];
  const cur = board.get(uid) || {
    uid, name, photo: photo || '', mode,
    totalScore: 0, wins: 0, gamesPlayed: 0, bestScore: 0, lastGame: now,
  };
  cur.name = name;
  cur.photo = photo || cur.photo;
  cur.totalScore += finalScore;
  cur.wins += won ? 1 : 0;
  cur.gamesPlayed += 1;
  cur.bestScore = Math.max(cur.bestScore, finalScore);
  cur.lastGame = now;
  board.set(uid, cur);
}

// ۱۰ نفر اول
async function getTop(mode, limit = 10) {
  if (db) {
    try {
      return await db
        .collection(COLLECTIONS[mode])
        .find({}, { projection: { _id: 0 } })
        .sort({ totalScore: -1 })
        .limit(limit)
        .toArray();
    } catch (err) {
      console.error('خطا در خواندن لیدربرد Mongo:', err.message);
    }
  }
  return [...memBoards[mode].values()]
    .sort((a, b) => b.totalScore - a.totalScore)
    .slice(0, limit);
}

// ============================================================
//  ۵) توابع کمکی
// ============================================================
const rooms = {};

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// ساخت کد اتاق ۶ کاراکتری یکتا
function generateRoomId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let id;
  do {
    id = Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
  } while (rooms[id]);
  return id;
}

function cleanName(name) {
  return String(name || '').trim().slice(0, 20) || 'بازیکن';
}

// اطلاعات عمومی بازیکن‌ها (بدون هیچ چیز حساسی)
function publicPlayers(room) {
  return room.players.map((p) => ({
    id: p.id,
    uid: p.uid,
    name: p.name,
    photo: p.photo,
    score: room.scores[p.id] || 0,
    isDrawer: p.isDrawer,
    eliminated: p.eliminated,
    left: p.left,
  }));
}

function addScore(room, playerId, pts) {
  room.scores[playerId] = (room.scores[playerId] || 0) + pts;
}

function clearTimer(room) {
  if (room.timer) {
    clearTimeout(room.timer);
    room.timer = null;
  }
}

function destroyRoom(room) {
  clearTimer(room);
  io.in(room.id).socketsLeave(room.id);
  delete rooms[room.id];
}

function activePlayers(room) {
  return room.players.filter((p) => !p.eliminated && !p.left);
}

function getCtx(socket, roomId) {
  const room = rooms[String(roomId || '').toUpperCase()];
  const player = room ? room.players.find((p) => p.id === socket.id) : null;
  return { room, player };
}

function sendError(socket, message) {
  socket.emit('error-message', { message });
}

// انتخاب کلمه + گزینه‌ها (کلمه همیشه بین گزینه‌هاست)
function pickWordAndOptions(room, count) {
  let pool = WORDS.filter((w) => !room.usedWords.includes(w));
  if (pool.length === 0) {
    room.usedWords = [];
    pool = [...WORDS];
  }
  const word = pool[Math.floor(Math.random() * pool.length)];
  room.usedWords.push(word);
  const others = shuffle(WORDS.filter((w) => w !== word)).slice(0, count - 1);
  return { word, options: shuffle([word, ...others]) };
}

// ============================================================
//  ۶) شروع دور
// ============================================================
function startRound(room, replay = false) {
  if (!rooms[room.id] || room.status !== 'playing') return;
  clearTimer(room);

  const cfg = MODES[room.mode];
  if (!replay) room.round += 1;

  // انتخاب نقاش
  if (room.mode === 'score') {
    room.drawerIndex = room.round - 1;
  } else {
    const n = room.players.length;
    room.drawerIndex = -1;
    for (let i = 1; i <= n; i++) {
      const idx = (((room.prevDrawerIndex ?? -1) + i) % n + n) % n;
      const p = room.players[idx];
      if (!p.eliminated && !p.left) { room.drawerIndex = idx; break; }
    }
    if (room.drawerIndex === -1) { finishGame(room, null); return; }
    room.prevDrawerIndex = room.drawerIndex;
  }

  const drawer = room.players[room.drawerIndex];
  room.players.forEach((p) => { p.isDrawer = p.id === drawer.id; });
  room.drawerId = drawer.id;

  // آماده‌سازی اطلاعات دور
  const { word, options } = pickWordAndOptions(room, cfg.optionsCount);
  room.word = word;
  room.options = options;
  room.wordRevealed = false;
  room.guesses = [];
  room.strokes = [];
  room.drawing = null;
  room.roundGuessers = room.players
    .filter((p) => p.id !== drawer.id && !p.eliminated && !p.left)
    .map((p) => p.id);

  const now = Date.now();
  room.startTime = now;

  if (room.mode === 'score') {
    room.phase = 'live';
    room.endTime = now + cfg.roundTime;
    room.timer = setTimeout(() => endRound(room), cfg.roundTime);
  } else {
    room.phase = 'drawing'; // اول نقاش می‌کشه، بعد پخش میشه
    room.drawStart = now;
    room.endTime = now + cfg.drawTime;
    room.timer = setTimeout(() => startGuessing(room), cfg.drawTime);
  }

  io.to(room.id).emit('round-started', {
    mode: room.mode,
    round: room.round,
    totalRounds: room.mode === 'score' ? cfg.totalRounds : null,
    drawer: { id: drawer.id, name: drawer.name },
    wordLength: [...word].length,
    options, // کلمه بین گزینه‌هاست ولی مشخص نیست کدومه
    phase: room.phase,
    startTime: room.startTime,
    endTime: room.endTime,
    serverTime: Date.now(),
    duration: (room.endTime - room.startTime) / 1000,
    players: publicPlayers(room),
  });

  // کلمه فقط برای نقاش (ضد تقلب ۱)
  io.to(drawer.id).emit('word-for-drawer', { word, round: room.round });
}

// حالت حذفی: شروع پخش نقاشی ضبط‌شده + شروع زمان حدس
function startGuessing(room) {
  if (!rooms[room.id] || room.phase !== 'drawing') return;
  clearTimer(room);
  const cfg = MODES.knockout;

  room.phase = 'guessing';
  room.startTime = Date.now();
  room.endTime = room.startTime + cfg.guessTime;
  room.timer = setTimeout(() => endRound(room), cfg.guessTime);

  io.to(room.id).emit('drawing-preview', {
    round: room.round,
    drawing: room.drawing,   // تصویر نهایی (اگه نقاش فرستاده باشه)
    strokes: room.strokes,   // خط‌های ضبط‌شده با زمان برای پخش مجدد
    startTime: room.startTime,
    endTime: room.endTime,
    serverTime: Date.now(),
    duration: cfg.guessTime / 1000,
  });
}

// اگه همه حدس زدن دور زودتر تموم بشه
function checkAllGuessed(room) {
  if (room.phase !== 'live' && room.phase !== 'guessing') return;
  const expected = room.roundGuessers
    .map((id) => room.players.find((p) => p.id === id))
    .filter((p) => p && !p.left);
  const allDone = expected.every((p) => room.guesses.some((g) => g.playerId === p.id));
  if (allDone) endRound(room);
}

// ============================================================
//  ۷) پایان دور
// ============================================================
function endRound(room) {
  if (!rooms[room.id]) return;
  if (room.phase !== 'live' && room.phase !== 'guessing') return;
  clearTimer(room);
  room.phase = 'between';
  room.wordRevealed = true;

  if (room.mode === 'score') endRoundScore(room);
  else endRoundKnockout(room);
}

// ---------- حالت امتیازی ----------
function endRoundScore(room) {
  const gains = {};
  room.players.forEach((p) => { gains[p.id] = 0; });

  // تماشاچی‌ها: ۶ / ۵ / ۴ بر اساس ترتیب حدس درست
  const correct = room.guesses.filter((g) => g.correct);
  correct.forEach((g, i) => { gains[g.playerId] += SCORE_GUESS_POINTS[i] || 0; });

  // نقاش: بسته به تعداد حدس درست
  gains[room.drawerId] += SCORE_DRAWER_POINTS[Math.min(correct.length, 3)] || 0;

  Object.entries(gains).forEach(([id, pts]) => addScore(room, id, pts));

  io.to(room.id).emit('round-ended', {
    mode: 'score',
    round: room.round,
    word: room.word,
    scores: { ...room.scores },
    gains,
    eliminated: [],
  });

  if (room.round >= MODES.score.totalRounds) {
    room.timer = setTimeout(() => finishGame(room, null), 5000);
  } else {
    scheduleNextRound(room);
  }
}

// ---------- حالت حذفی ----------
function endRoundKnockout(room) {
  const guessers = room.roundGuessers
    .map((id) => room.players.find((p) => p.id === id))
    .filter((p) => p && !p.left);

  const correctGuesses = [];
  const wrongPlayers = [];
  guessers.forEach((p) => {
    const g = room.guesses.find((x) => x.playerId === p.id);
    if (g && g.correct) correctGuesses.push(g);
    else wrongPlayers.push(p); // غلط یا اصلاً حدس نزده
  });

  // امتیاز هر حدس درست
  correctGuesses.forEach((g) => addScore(room, g.playerId, KO_CORRECT_POINTS));

  // تعیین حذف‌شده‌ها
  let toEliminate = [];
  if (guessers.length > 0) {
    if (correctGuesses.length === 0) {
      toEliminate = [...guessers];                // همه غلط → همه حذف
    } else if (wrongPlayers.length > 0) {
      toEliminate = wrongPlayers;                 // غلط‌ها حذف
    } else if (room.round >= KO_SPEED_RULE_ROUND) {
      // همه درست + از دور ۱۵ به بعد → دیرترین حذف
      const slowest = correctGuesses.reduce((a, b) => (b.elapsed > a.elapsed ? b : a));
      toEliminate = [room.players.find((p) => p.id === slowest.playerId)];
    }
  }

  toEliminate.forEach((p) => {
    p.eliminated = true;
    p.elimRound = room.round;
    room.eliminated.push(p.id);
    io.to(room.id).emit('player-eliminated', { playerId: p.id });
  });

  // امتیاز موندن در دور (نقاش هم اگه موند)
  const survivors = activePlayers(room);
  survivors.forEach((p) => addScore(room, p.id, KO_SURVIVE_POINTS));

  io.to(room.id).emit('round-ended', {
    mode: 'knockout',
    round: room.round,
    word: room.word,
    scores: { ...room.scores },
    eliminated: toEliminate.map((p) => p.id),
    allEliminated: [...room.eliminated],
    players: publicPlayers(room),
  });

  if (survivors.length <= 1) {
    // اگه هیچ‌کس نمونده (حالت نادر)، بهترین امتیازِ حذف‌شده‌های همین دور برنده‌ست
    let winner = survivors[0] || null;
    if (!winner && toEliminate.length) {
      winner = toEliminate.reduce((a, b) => ((room.scores[b.id] || 0) > (room.scores[a.id] || 0) ? b : a));
    }
    room.timer = setTimeout(() => finishGame(room, winner), 5000);
  } else {
    scheduleNextRound(room);
  }
}

function scheduleNextRound(room) {
  room.awaitingNext = true;
  room.timer = setTimeout(() => {
    room.awaitingNext = false;
    startRound(room);
  }, NEXT_ROUND_DELAY);
}

// ============================================================
//  ۸) پایان بازی
// ============================================================
function finishGame(room, knockoutWinner) {
  if (!rooms[room.id] || room.status === 'finished') return;
  clearTimer(room);
  room.phase = 'finished';
  room.status = 'finished';
  room.finishedAt = Date.now();

  const cfg = MODES[room.mode];
  let winnerIds = [];

  if (room.mode === 'knockout') {
    if (knockoutWinner) {
      winnerIds = [knockoutWinner.id];
      addScore(room, knockoutWinner.id, KO_WIN_POINTS); // برد نهایی +۳۰
    }
  } else {
    // برنده: بیشترین امتیاز (مساوی = هر دو برنده)
    const max = Math.max(...room.players.map((p) => room.scores[p.id] || 0));
    if (max > 0) {
      winnerIds = room.players.filter((p) => (room.scores[p.id] || 0) === max).map((p) => p.id);
    }
  }

  // رتبه‌بندی
  const ranked = [...room.players].sort((a, b) => {
    const wa = winnerIds.includes(a.id) ? 1 : 0;
    const wb = winnerIds.includes(b.id) ? 1 : 0;
    if (wa !== wb) return wb - wa;
    if (room.mode === 'knockout') {
      const ea = a.elimRound || 0;
      const eb = b.elimRound || 0;
      if (ea !== eb) return eb - ea; // دیرتر حذف‌شده بهتره
    }
    return (room.scores[b.id] || 0) - (room.scores[a.id] || 0);
  });

  const results = ranked.map((p, i) => {
    const won = winnerIds.includes(p.id);
    return {
      id: p.id,
      uid: p.uid,
      name: p.name,
      photo: p.photo,
      score: room.scores[p.id] || 0,
      rank: i + 1,
      won,                                   // برای محاسبه سطح بر اساس برد/باخت
      coins: won ? cfg.coins : 0,
      bonusPoints: won ? cfg.bonusPoints : 0,
    };
  });

  const winnerPlayer = room.players.find((p) => p.id === winnerIds[0]);

  io.to(room.id).emit('game-ended', {
    mode: room.mode,
    winner: winnerPlayer
      ? { id: winnerPlayer.id, uid: winnerPlayer.uid, name: winnerPlayer.name, photo: winnerPlayer.photo }
      : null,
    winners: winnerIds,
    scores: { ...room.scores },
    results,
    rewards: { coins: cfg.coins, points: cfg.bonusPoints },
  });

  // آپدیت جدول بهترین‌ها (فقط سرور)
  Promise.all(
    results.map((r) =>
      updateLeaderboard(room.mode, {
        uid: r.uid,
        name: r.name,
        photo: r.photo,
        finalScore: r.score + r.bonusPoints,
        won: r.won,
      })
    )
  )
    .then(() => getTop(room.mode))
    .then((list) => io.emit('leaderboard-updated', { mode: room.mode, list }))
    .catch((err) => console.error('خطا در آپدیت لیدربرد:', err.message));
}

// ============================================================
//  ۹) خروج بازیکن / قطع اتصال
// ============================================================
function handleLeave(socket) {
  const roomId = socket.data.roomId;
  if (!roomId) return;
  socket.leave(roomId);
  socket.data.roomId = null;

  const room = rooms[roomId];
  if (!room) return;
  const idx = room.players.findIndex((p) => p.id === socket.id);
  if (idx === -1) return;
  const player = room.players[idx];

  // --- در لابی یا بعد از پایان: حذف ساده ---
  if (room.status === 'waiting' || room.status === 'finished') {
    room.players.splice(idx, 1);
    delete room.scores[socket.id];
    if (room.players.length === 0) { destroyRoom(room); return; }
    if (room.host === socket.id) room.host = room.players[0].id;
    io.to(roomId).emit('player-left', {
      players: publicPlayers(room), host: room.host, playerId: socket.id,
    });
    return;
  }

  // --- وسط بازی امتیازی: بازی کنسل میشه (نیاز به ۴ نفر) ---
  if (room.mode === 'score') {
    io.to(roomId).emit('player-left', {
      players: publicPlayers(room).filter((p) => p.id !== socket.id),
      host: room.host,
      playerId: socket.id,
    });
    io.to(roomId).emit('game-ended', {
      mode: 'score', winner: null, winners: [], scores: { ...room.scores },
      results: [], cancelled: true, reason: 'player-left',
    });
    destroyRoom(room);
    return;
  }

  // --- وسط بازی حذفی: بازیکن حذف میشه ---
  player.left = true;
  const wasDrawer = room.drawerId === player.id;
  if (!player.eliminated) {
    player.eliminated = true;
    player.elimRound = room.round;
    room.eliminated.push(player.id);
  }

  if (room.players.every((p) => p.left)) { destroyRoom(room); return; }
  if (room.host === player.id) {
    const nh = room.players.find((p) => !p.left);
    if (nh) room.host = nh.id;
  }

  io.to(roomId).emit('player-left', {
    players: publicPlayers(room), host: room.host, playerId: player.id,
  });
  io.to(roomId).emit('player-eliminated', { playerId: player.id });

  const active = activePlayers(room);
  if (active.length <= 1) {
    finishGame(room, active[0] || null);
    return;
  }

  if (wasDrawer && room.phase === 'drawing') {
    // نقاش رفت قبل از اتمام نقاشی → دور با نقاش بعدی تکرار میشه
    clearTimer(room);
    room.phase = 'between';
    room.timer = setTimeout(() => startRound(room, true), 2000);
  } else if (room.phase === 'guessing') {
    checkAllGuessed(room);
  }
}

// ============================================================
//  ۱۰) رویدادهای Socket.io
// ============================================================
io.on('connection', (socket) => {
  socket.data.roomId = null;

  // ---------- ساخت اتاق ----------
  // { name, mode, uid?, photo? }
  socket.on('create-room', (payload = {}) => {
    const { mode, uid, photo } = payload;
    if (!MODES[mode]) return sendError(socket, 'حالت بازی نامعتبر است');
    handleLeave(socket);

    const name = cleanName(payload.name);
    const roomId = generateRoomId();
    const room = {
      id: roomId,
      host: socket.id,
      mode,
      players: [],
      status: 'waiting',
      phase: 'lobby',
      round: 0,
      totalRounds: MODES[mode].totalRounds,
      drawerIndex: 0,
      drawerId: null,
      word: null,
      options: [],
      wordRevealed: false,
      startTime: null,
      endTime: null,
      drawing: null,
      strokes: [],
      guesses: [],
      scores: {},
      eliminated: [],
      usedWords: [],
      roundGuessers: [],
      timer: null,
      awaitingNext: false,
      createdAt: Date.now(),
    };
    room.players.push({
      id: socket.id,
      uid: uid ? String(uid) : `guest:${name}`,
      name,
      photo: photo ? String(photo).slice(0, 500) : '',
      isDrawer: false,
      eliminated: false,
      left: false,
      elimRound: 0,
    });
    room.scores[socket.id] = 0;
    rooms[roomId] = room;

    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.emit('room-created', {
      roomId, mode, host: room.host, players: publicPlayers(room), maxPlayers: MODES[mode].max,
    });
  });

  // ---------- ورود به اتاق ----------
  // { roomId, name, uid?, photo? }
  socket.on('join-room', (payload = {}) => {
    const roomId = String(payload.roomId || '').toUpperCase();
    const room = rooms[roomId];
    if (!room) return sendError(socket, 'اتاقی با این کد پیدا نشد');
    if (room.status !== 'waiting') return sendError(socket, 'بازی در این اتاق شروع شده');
    if (room.players.some((p) => p.id === socket.id)) return;
    if (room.players.length >= MODES[room.mode].max) return sendError(socket, 'اتاق پر است');

    handleLeave(socket);
    const name = cleanName(payload.name);
    room.players.push({
      id: socket.id,
      uid: payload.uid ? String(payload.uid) : `guest:${name}`,
      name,
      photo: payload.photo ? String(payload.photo).slice(0, 500) : '',
      isDrawer: false,
      eliminated: false,
      left: false,
      elimRound: 0,
    });
    room.scores[socket.id] = 0;

    socket.join(roomId);
    socket.data.roomId = roomId;
    io.to(roomId).emit('player-joined', {
      roomId,
      mode: room.mode,
      host: room.host,
      maxPlayers: MODES[room.mode].max,
      players: publicPlayers(room),
    });
  });

  // ---------- شروع بازی (فقط میزبان) ----------
  socket.on('start-game', ({ roomId } = {}) => {
    const { room } = getCtx(socket, roomId);
    if (!room) return sendError(socket, 'اتاق پیدا نشد');
    if (room.host !== socket.id) return sendError(socket, 'فقط میزبان می‌تواند بازی را شروع کند');
    if (room.status !== 'waiting') return;
    const cfg = MODES[room.mode];
    if (room.players.length !== cfg.max) {
      return sendError(socket, `برای شروع باید دقیقاً ${cfg.max} بازیکن در اتاق باشند`);
    }

    room.status = 'playing';
    room.phase = 'between';
    room.round = 0;
    room.prevDrawerIndex = -1;
    room.usedWords = [];
    room.eliminated = [];
    room.players.forEach((p) => {
      room.scores[p.id] = 0;
      p.eliminated = false;
      p.isDrawer = false;
      p.elimRound = 0;
    });

    io.to(room.id).emit('game-started', {
      roomId: room.id, mode: room.mode, players: publicPlayers(room),
    });
    room.timer = setTimeout(() => startRound(room), 2000);
  });

  // ---------- نقاشی ----------
  // حالت امتیازی: زنده برای بقیه ارسال میشه
  // حالت حذفی: فقط ضبط میشه و بعداً پخش میشه
  socket.on('draw', ({ roomId, data } = {}) => {
    const { room } = getCtx(socket, roomId);
    if (!room || room.status !== 'playing' || room.drawerId !== socket.id) return;

    if (room.mode === 'score') {
      if (room.phase !== 'live' || Date.now() > room.endTime + 300) return;
      socket.to(room.id).emit('draw-update', { data });
    } else {
      if (room.phase !== 'drawing') return;
      if (room.strokes.length < MAX_STROKES) {
        room.strokes.push({ t: Date.now() - room.drawStart, data });
      }
    }
  });

  // حالت حذفی: نقاش نقاشی تموم‌شده رو (تصویر base64) می‌فرسته و زودتر مرحله حدس شروع میشه
  socket.on('submit-drawing', ({ roomId, drawing } = {}) => {
    const { room } = getCtx(socket, roomId);
    if (!room || room.mode !== 'knockout' || room.phase !== 'drawing') return;
    if (room.drawerId !== socket.id) return;
    if (typeof drawing === 'string' && drawing.length < 1.5e6) room.drawing = drawing;
    startGuessing(room);
  });

  // ---------- پاک کردن بوم (فقط نقاش) ----------
  socket.on('clear-canvas', ({ roomId } = {}) => {
    const { room } = getCtx(socket, roomId);
    if (!room || room.status !== 'playing' || room.drawerId !== socket.id) return;
    if (room.mode === 'score') {
      if (room.phase !== 'live') return;
      socket.to(room.id).emit('canvas-cleared', {});
    } else {
      if (room.phase !== 'drawing') return;
      room.strokes = [];
      room.drawing = null;
    }
  });

  // ---------- حدس (همه چیز سمت سرور چک میشه) ----------
  socket.on('guess', ({ roomId, optionIndex } = {}) => {
    const { room, player } = getCtx(socket, roomId);
    if (!room || !player || room.status !== 'playing') return;

    const guessPhase = room.mode === 'score' ? 'live' : 'guessing';
    if (room.phase !== guessPhase) return;
    if (!room.roundGuessers.includes(socket.id)) return;     // نقاش/حذف‌شده نمی‌تونه حدس بزنه
    if (!Number.isInteger(optionIndex) || optionIndex < 0 || optionIndex >= room.options.length) return;
    if (room.guesses.some((g) => g.playerId === socket.id)) return; // فقط یک حدس
    const now = Date.now();
    if (now > room.endTime + 300) return;                    // تایمر سرور

    const elapsed = Math.round(((now - room.startTime) / 1000) * 100) / 100;
    const correct = room.options[optionIndex] === room.word;
    room.guesses.push({ playerId: socket.id, optionIndex, correct, elapsed });

    // نتیجه فقط برای خود حدس‌زننده
    socket.emit('guess-result', { playerId: socket.id, correct });

    if (correct) {
      const position = room.guesses.filter((g) => g.correct).length;
      io.to(room.id).emit('correct-guess', {
        playerId: socket.id, name: player.name, elapsed, position,
      });
    }

    checkAllGuessed(room);
  });

  // ---------- دور بعدی (میزبان) ----------
  socket.on('next-round', ({ roomId } = {}) => {
    const { room } = getCtx(socket, roomId);
    if (!room || room.host !== socket.id || !room.awaitingNext) return;
    clearTimer(room);
    room.awaitingNext = false;
    startRound(room);
  });

  // ---------- خروج ----------
  socket.on('leave-room', () => handleLeave(socket));
  socket.on('disconnect', () => handleLeave(socket));

  // ---------- جدول بهترین‌ها ----------
  socket.on('get-leaderboard', async ({ mode } = {}) => {
    if (!MODES[mode]) return sendError(socket, 'حالت نامعتبر است');
    const list = await getTop(mode);
    socket.emit('leaderboard-data', { mode, list });
  });
});

// پاک‌سازی اتاق‌های تمام‌شده قدیمی (هر ۱۰ دقیقه)
setInterval(() => {
  const limit = Date.now() - 30 * 60 * 1000;
  Object.values(rooms).forEach((room) => {
    if (room.status === 'finished' && room.finishedAt && room.finishedAt < limit) destroyRoom(room);
  });
}, 10 * 60 * 1000);

// ============================================================
//  ۱۱) استارت سرور + Keep-Alive
// ============================================================
initMongo().then(() => {
  server.listen(PORT, () => {
    console.log(`🚀 سرور روی پورت ${PORT} اجرا شد`);

    // Keep-Alive: هر ۱۴ دقیقه خودش رو پینگ می‌کنه (Render)
    const url = process.env.RENDER_EXTERNAL_URL;
    if (url) {
      setInterval(() => {
        fetch(url + '/health').catch(() => {});
      }, 14 * 60 * 1000);
    }
  });
});
