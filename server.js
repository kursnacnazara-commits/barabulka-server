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
const MONGO_URI = "mongodb+srv://nazar:Barabulka123@cluster0.ayxyg57.mongodb.net/?retryWrites=true&w=majority&authSource=admin";

mongoose.connect(MONGO_URI)
    .then(() => console.log('✅ Успішно підключено до бази даних MongoDB Atlas!'))
    .catch(err => console.error('❌ Помилка бази даних:', err));

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

// Middleware
app.use(cors());
app.use(express.json());

// Перевіряємо та створюємо папку для завантажень uploads
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
}

// Робимо папку uploads публічною
app.use('/uploads', express.static(uploadDir));

// Налаштування Multer для збереження файлів
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'uploads/');
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
    cb(null, uniqueSuffix + path.extname(file.originalname));
  }
});
const upload = multer({ storage: storage });

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

    try {
      // Відправляємо історію повідомлень тільки для цієї кімнати
      const history = await Message.find({ room: room }).sort({ timestamp: 1 });
      socket.emit('initial_messages', history);
    } catch (err) {
      console.error('Помилка завантаження історії кімнати:', err);
    }
  });

  // Обробка отримання та збереження нового повідомлення
  socket.on('send_message', async (data) => {
    console.log('[Socket] Отримано повідомлення:', data);
    const room = data.room || socket.room || 'default_room';
    
    // Якщо це службова дія (улюбленці, карта - починається з __ACTION__),
    // миттєво розсилаємо її в кімнату, НЕ зберігаючи в базу даних чату
    if (data.text && data.text.startsWith('__ACTION__:')) {
      io.to(room).emit('receive_message', data);
      return;
    }

    try {
      // Створюємо та зберігаємо об'єкт у MongoDB для конкретної кімнати
      const newMessage = new Message({
        room: room,
        sender: data.sender,
        text: data.text,
        isMedia: data.isMedia || false,
        mediaType: data.mediaType || null,
        timestamp: new Date()
      });

      await newMessage.save();

      // Розсилаємо збережене повідомлення всім учасникам цієї кімнати
      io.to(room).emit('receive_message', newMessage);
    } catch (err) {
      console.error('Помилка збереження повідомлення:', err);
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

  const fileUrl = `http://${req.headers.host}/uploads/${req.file.filename}`;
  
  const mediaItem = {
    url: fileUrl,
    filename: req.file.filename,
    type: req.file.mimetype.startsWith('image/') ? 'image' : 'video',
    createdAt: new Date()
  };

  gallery.push(mediaItem);

  res.json({
    message: 'Файл успішно завантажено',
    url: fileUrl,
    type: mediaItem.type
  });
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