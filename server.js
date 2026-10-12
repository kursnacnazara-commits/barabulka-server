// 1. Підключаємо необхідні бібліотеки
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const path = require('path');
const cors = require('cors');
const fs = require('fs');
const mongoose = require('mongoose');

// 2. Ініціалізуємо додаток Express та HTTP-сервер
const app = express();
const server = http.createServer(app);

// 3. Підключення до MongoDB Atlas (хмара)
// ВАЖЛИВО: адреса бази з паролем більше НЕ зберігається в коді (репозиторій публічний!).
// Її треба додати в Railway → сервіс barabulka-server → Variables → змінна MONGO_URI.
// Прибираємо випадкові пробіли, лапки чи "MONGO_URI=" на початку, якщо таке вставилось разом зі значенням
const MONGO_URI = (process.env.MONGO_URI || '')
  .trim()
  .replace(/^MONGO_URI\s*=\s*/, '')
  .replace(/^["']|["']$/g, '')
  .trim();

// Діагностика без розкриття пароля: лише довжина і чи правильний початок
console.log(`[DB] MONGO_URI задано: ${MONGO_URI ? 'так' : 'НІ'}, довжина: ${MONGO_URI.length}, ` +
  `формат: ${/^mongodb(\+srv)?:\/\//.test(MONGO_URI) ? 'правильний' : 'НЕПРАВИЛЬНИЙ (має починатися з mongodb+srv://)'}`);

// Якщо бази немає — запити до неї одразу повертають помилку, а не висять 10 секунд
mongoose.set('bufferCommands', false);

let dbReady = false;
mongoose.connection.on('connected', () => { dbReady = true; console.log('✅ Успішно підключено до бази даних MongoDB Atlas!'); });
mongoose.connection.on('disconnected', () => { dbReady = false; console.log('⚠️ Зв\'язок з базою втрачено'); });

function connectDb() {
  if (!MONGO_URI) {
    console.error('❌ Не задано змінну MONGO_URI у Railway → Variables. Чат працює, але історія зберігається лише до перезапуску сервера.');
    return;
  }
  mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 10000 })
    .catch(err => {
      console.error('❌ Помилка бази даних:', err.message);
      setTimeout(connectDb, 30000); // пробуємо ще раз через 30 с
    });
}
connectDb();

// Запасна історія в пам'яті: працює навіть коли база недоступна
const MEMORY_LIMIT = 500;
const memoryHistory = new Map(); // room -> [повідомлення]
function rememberMessage(msg) {
  const list = memoryHistory.get(msg.room) || [];
  list.push(msg);
  if (list.length > MEMORY_LIMIT) list.splice(0, list.length - MEMORY_LIMIT);
  memoryHistory.set(msg.room, list);
}
function forgetTexts(room, texts) {
  const list = memoryHistory.get(room);
  if (list) memoryHistory.set(room, list.filter(m => !texts.includes(m.text)));
}

// Схема та модель повідомлення з підтримкою кімнат (room)
const messageSchema = new mongoose.Schema({
    room: { type: String, default: 'default_room' },
    sender: String,
    text: String,
    isMedia: { type: Boolean, default: false },
    mediaType: String,
    timestamp: { type: Date, default: Date.now }
});

const Message = mongoose.model('Message', messageSchema);

// Дії партнера (фон, улюбленці, квести, карта, віджети...).
// Зберігаємо їх, щоб телефон, який був офлайн, отримав пропущене після підключення.
// Старі записи MongoDB сама видаляє через 90 днів.
const actionSchema = new mongoose.Schema({
    room: String,
    sender: String,
    type: String,
    text: String,
    timestamp: { type: Date, default: Date.now, expires: 60 * 60 * 24 * 90 }
});
actionSchema.index({ room: 1, timestamp: 1 });
const Action = mongoose.model('Action', actionSchema);

// Які дії НЕ зберігати: дзвінки. Старий дзвінок через годину нікому не потрібен.
// Усе інше (фон, улюбленці, квести, карта, віджети, записки і майбутні нові функції) зберігаємо.
function isPersistentAction(type) {
  if (!type) return false;
  if (type.startsWith('WEBRTC_')) return false;
  // TYPING («друкує…») — миттєвий сигнал, зберігати немає сенсу
  return !['INCOMING_CALL', 'ACCEPT_CALL', 'DECLINE_CALL', 'END_CALL', 'TYPING', 'KISS_TOUCH'].includes(type);
}
// Для цих дій важливий лише останній стан — старі копії замінюємо новою
// (улюбленці синхронізуються кожні 45 секунд, нема сенсу зберігати тисячі копій).
const LATEST_ONLY_ACTIONS = new Set([
  'PETS_SYNC', 'POINTS_SYNC', 'SET_BACKGROUND', 'WIDGET_PHOTO_SYNC', 'WIDGET_NOTE_SYNC', 'MOOD_SYNC', 'MEETING_SYNC', 'READ_UP_TO'
]);

const MEMORY_ACTIONS_LIMIT = 1000;
const memoryActions = new Map(); // room -> [дії]

async function storeAction(rec) {
  let list = memoryActions.get(rec.room) || [];
  if (LATEST_ONLY_ACTIONS.has(rec.type)) {
    list = list.filter(a => !(a.type === rec.type && a.sender === rec.sender));
  }
  list.push(rec);
  if (list.length > MEMORY_ACTIONS_LIMIT) list.splice(0, list.length - MEMORY_ACTIONS_LIMIT);
  memoryActions.set(rec.room, list);

  if (!dbReady) return;
  try {
    if (LATEST_ONLY_ACTIONS.has(rec.type)) {
      await Action.deleteMany({ room: rec.room, type: rec.type, sender: rec.sender });
    }
    await new Action(rec).save();
  } catch (err) {
    console.error('Помилка збереження дії:', err.message);
  }
}

async function loadMissedActions(room, username, since) {
  if (dbReady) {
    try {
      return await Action.find({ room: room, sender: { $ne: username }, timestamp: { $gt: new Date(since) } })
        .sort({ timestamp: 1 }).limit(MEMORY_ACTIONS_LIMIT).lean();
    } catch (err) {
      console.error('Помилка завантаження пропущених дій:', err.message);
    }
  }
  return (memoryActions.get(room) || [])
    .filter(a => a.sender !== username && new Date(a.timestamp).getTime() > since);
}

// Middleware
app.use(cors());
app.use(express.json());
// Railway працює через проксі: так req.protocol буде "https", а не "http"
app.set('trust proxy', true);

// Папка для завантажених фото/відео/голосових.
// На Railway до неї підключено Volume "barabulka-server-volume" (шлях /app/uploads),
// тому файли НЕ зникають при оновленні сервера. Змінну UPLOAD_DIR задавати не потрібно.
const uploadDir = process.env.UPLOAD_DIR || path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Робимо папку uploads публічною
app.use('/uploads', express.static(uploadDir));

// Налаштування Multer для збереження файлів
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, uniqueSuffix + path.extname(file.originalname));
  }
});
const upload = multer({ storage: storage, limits: { fileSize: 200 * 1024 * 1024 } }); // до 200 МБ

const gallery = []; // Тимчасова галерея

// 4. Налаштування Socket.IO
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// 5. Обробка підключень по Socket.IO
io.on('connection', async (socket) => {
  console.log(`[Socket] Нове підключення: ${socket.id}`);

  // Приєднання до конкретної спільної кімнати за паролем
  socket.on('join_room', async (data) => {
    const room = data.room || 'default_room';
    socket.join(room);
    socket.room = room;
    socket.username = data.username;
    console.log(`[Socket] Користувач ${data.username} зайшов у кімнату: ${room}`);

    let history = memoryHistory.get(room) || [];
    if (dbReady) {
      try {
        history = await Message.find({ room: room }).sort({ timestamp: 1 }).lean();
      } catch (err) {
        console.error('Помилка завантаження історії кімнати:', err.message);
      }
    }
    socket.emit('initial_messages', history);
    socket.emit('chat_history', history); // назва, яку чекала стара версія додатка

    // Нова версія додатка надсилає "since" — час останньої дії партнера, яку телефон уже отримав.
    // Досилаємо все, що партнер зробив після цього, поки телефон був офлайн.
    if (data.since !== undefined) {
      const now = Date.now();
      const since = Number(data.since) || 0;
      const actions = since > 0 ? await loadMissedActions(room, data.username, since) : [];
      socket.emit('missed_actions', { actions: actions, now: now });
      if (actions.length) console.log(`[Socket] Дослано пропущених дій: ${actions.length}`);
    }
  });

  // Обробка отримання та збереження нового повідомлення
  socket.on('send_message', async (data) => {
    // Не пишемо в логи сам текст — це приватне листування
    console.log(`[Socket] Повідомлення від ${data.sender}${data.isMedia ? ' (' + data.mediaType + ')' : ''}`);
    const room = data.room || socket.room || 'default_room';
    
    // Якщо це службова дія (улюбленці, карта - починається з __ACTION__),
    // миттєво розсилаємо її в кімнату, НЕ зберігаючи в базу даних чату
    if (data.text && data.text.startsWith('__ACTION__:')) {
      let action = null;
      try {
        action = JSON.parse(data.text.slice('__ACTION__:'.length));
      } catch (e) {
        console.error('Помилка обробки дії:', e.message);
      }
      const type = action && action.type;

      // Кожна дія отримує номер і час — так телефон не обробить її двічі
      // і знатиме, з якого моменту просити пропущене.
      const rec = {
        _id: new mongoose.Types.ObjectId(),
        room: room,
        sender: data.sender,
        type: type || '',
        text: data.text,
        isMedia: false,
        mediaType: null,
        timestamp: new Date()
      };
      io.to(room).emit('receive_message', rec);

      // Видалення повідомлень — прибираємо їх і з бази, щоб не поверталися в історії
      if (type === 'DELETE_MESSAGES' && Array.isArray(action.texts) && action.texts.length) {
        forgetTexts(room, action.texts);
        if (dbReady) {
          try {
            await Message.deleteMany({ room: room, text: { $in: action.texts } });
          } catch (e) {
            console.error('Помилка видалення повідомлень:', e.message);
          }
        }
      }

      if (isPersistentAction(type)) {
        await storeAction({ _id: rec._id, room: room, sender: rec.sender, type: type, text: rec.text, timestamp: rec.timestamp });
      }
      return;
    }

    // СПОЧАТКУ розсилаємо повідомлення партнеру, і лише ПОТІМ зберігаємо в базу.
    // Раніше було навпаки: якщо база не відповідала, повідомлення взагалі не доходило.
    const msg = {
      _id: new mongoose.Types.ObjectId(),
      room: room,
      sender: data.sender,
      text: data.text,
      isMedia: data.isMedia || false,
      mediaType: data.mediaType || null,
      timestamp: new Date()
    };
    io.to(room).emit('receive_message', msg);
    rememberMessage(msg);

    if (dbReady) {
      try {
        await new Message(msg).save();
      } catch (err) {
        console.error('Помилка збереження повідомлення:', err.message);
      }
    }
  });

  // ==========================================
  // 📞 СИГНАЛІЗАЦІЯ ДЛЯ ДЗВІНКІВ (WebRTC)
  // ==========================================
  socket.on('call_user', (data) => {
    io.to(data.to).emit('call_incoming', {
      offer: data.offer,
      from: data.from
    });
  });

  socket.on('make_answer', (data) => {
    io.to(data.to).emit('answer_made', {
      answer: data.answer,
      from: data.from
    });
  });

  socket.on('add_ice_candidate', (data) => {
    io.to(data.to).emit('candidate_added', {
      candidate: data.candidate
    });
  });
  // ==========================================

  // Відключення користувача
  socket.on('disconnect', () => {
    console.log(`[Socket] Користувач відключився: ${socket.id}`);
  });
});

// 6. REST API Ендпоінти

// Завантаження медіафайлу (фото/відео)
app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'Файл не завантажено' });
  }

  const fileUrl = `${req.protocol}://${req.get('host')}/uploads/${req.file.filename}`;
  
  const mediaItem = {
    url: fileUrl,
    filename: req.file.filename,
    type: req.file.mimetype.startsWith('image/') ? 'image'
        : req.file.mimetype.startsWith('audio/') ? 'audio' : 'video',
    createdAt: new Date()
  };

  gallery.push(mediaItem);

  res.json({
    message: 'Файл успішно завантажено',
    url: fileUrl,
    type: mediaItem.type
  });
});

// Сервери для відеодзвінків (STUN + TURN). TURN потрібен, коли обидва телефони за мобільним інтернетом/NAT.
// Можна задати свої через змінні Railway: ICE_SERVERS (JSON) або TURN_HOST + TURN_SECRET.
app.get('/api/ice', (req, res) => {
  const servers = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.ICE_SERVERS) {
    try { return res.json({ iceServers: JSON.parse(process.env.ICE_SERVERS) }); } catch (e) { /* невалідний JSON — беремо типові */ }
  }
  const secret = process.env.TURN_SECRET || 'openrelayprojectsecret';
  const host = process.env.TURN_HOST || 'staticauth.openrelay.metered.ca';
  const username = `${Math.floor(Date.now() / 1000) + 24 * 3600}:barabulka`;
  const credential = require('crypto').createHmac('sha1', secret).update(username).digest('base64');
  servers.push({
    urls: [`turn:${host}:80`, `turn:${host}:80?transport=tcp`, `turn:${host}:443`, `turns:${host}:443?transport=tcp`],
    username, credential
  });
  res.json({ iceServers: servers });
});

// Альбом
app.get('/api/gallery', (req, res) => {
  res.json(gallery);
});

// Пошук по повідомленнях у базі
app.get('/api/search', async (req, res) => {
  const query = req.query.q ? req.query.q.toLowerCase() : '';
  const room = req.query.room;
  try {
    const filter = { text: { $regex: query, $options: 'i' } };
    if (room) filter.room = room;
    
    const results = await Message.find(filter);
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: 'Помилка пошуку' });
  }
});

// Запуск сервера
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`=================================`);
  console.log(`🚀 Сервер успішно запущено!`);
  console.log(`🌐 Порт: ${PORT}`);
  console.log(`💻 Локальна адреса: http://localhost:${PORT}`);
  console.log(`=================================`);
});
