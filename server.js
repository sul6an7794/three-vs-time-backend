const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: process.env.FRONTEND_URL || "http://localhost:3000",
    methods: ["GET", "POST"]
  }
});

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// MongoDB Connection
const mongoUri = process.env.MONGODB_URI || "mongodb+srv://admin:password@cluster.mongodb.net/three-vs-time?retryWrites=true&w=majority";

mongoose.connect(mongoUri)
  .then(() => console.log('MongoDB Connected'))
  .catch(err => console.log('MongoDB Error:', err));

// ==================== Models ====================

// User Model
const userSchema = new mongoose.Schema({
  email: { type: String, unique: true, required: true },
  password: { type: String, required: true },
  username: String,
  createdAt: { type: Date, default: Date.now }
});

const User = mongoose.model('User', userSchema);

// Question Model
const questionSchema = new mongoose.Schema({
  question: { type: String, required: true },
  description: String,
  correctAnswer: { type: String, required: true },
  image1: String,
  image2: String,
  image3: String,
  roundId: mongoose.Schema.Types.ObjectId,
  createdAt: { type: Date, default: Date.now }
});

const Question = mongoose.model('Question', questionSchema);

// Round Model
const roundSchema = new mongoose.Schema({
  name: { type: String, required: true },
  questions: [mongoose.Schema.Types.ObjectId],
  createdAt: { type: Date, default: Date.now }
});

const Round = mongoose.model('Round', roundSchema);

// Game Session Model
const gameSessionSchema = new mongoose.Schema({
  roundId: mongoose.Schema.Types.ObjectId,
  players: [
    {
      userId: String,
      username: String,
      playerNumber: Number,
      socketId: String
    }
  ],
  startTime: { type: Date, default: Date.now },
  endTime: Date,
  answer: String,
  status: { type: String, enum: ['waiting', 'playing', 'finished'], default: 'waiting' },
  currentQuestionIndex: { type: Number, default: 0 },
  failedAttempts: { type: Number, default: 0 }
});

const GameSession = mongoose.model('GameSession', gameSessionSchema);

// ==================== Global Variables ====================

let activeSessions = new Map();
let playerSockets = new Map();

// ==================== Utility Functions ====================

function cleanAnswer(text) {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^؀-ۿa-z0-9]/g, '')
    .replace(/\s+/g, '');
}

function isSimilarAnswer(userAnswer, correctAnswer, threshold = 0.85) {
  const cleaned = cleanAnswer(userAnswer);
  const correct = cleanAnswer(correctAnswer);

  if (cleaned === correct) return true;

  let matches = 0;
  const minLen = Math.min(cleaned.length, correct.length);
  for (let i = 0; i < minLen; i++) {
    if (cleaned[i] === correct[i]) matches++;
  }

  const similarity = matches / Math.max(cleaned.length, correct.length);
  return similarity >= threshold;
}

// ==================== Socket.io Events ====================

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('join-game', async (data) => {
    try {
      const { roundId, playerId, username } = data;
      let session = activeSessions.get(roundId);

      if (!session) {
        session = {
          roundId,
          players: [],
          questions: [],
          startTime: Date.now(),
          currentQuestionIndex: 0,
          status: 'waiting'
        };
        activeSessions.set(roundId, session);

        const round = await Round.findById(roundId);
        if (round) {
          session.questions = await Question.find({ _id: { $in: round.questions } });
        }
      }

      if (session.players.length < 3) {
        const playerNumber = session.players.length + 1;
        session.players.push({
          userId: playerId,
          username,
          playerNumber,
          socketId: socket.id
        });

        playerSockets.set(socket.id, {
          roundId,
          playerId,
          playerNumber,
          username
        });

        io.to(roundId).emit('player-joined', {
          playerNumber,
          totalPlayers: session.players.length,
          username
        });

        if (session.players.length === 3) {
          socket.to(roundId).emit('all-players-ready');
          setTimeout(() => {
            startGame(roundId);
          }, 2000);
        }
      }

      socket.join(roundId);
      socket.emit('game-joined', {
        playerNumber: session.players.length,
        totalPlayers: session.players.length
      });
    } catch (error) {
      console.error('Join error:', error);
    }
  });

  socket.on('submit-answer', async (data) => {
    try {
      const playerData = playerSockets.get(socket.id);
      if (!playerData || playerData.playerNumber !== 1) return;

      const { answer, questionId } = data;
      const roundId = playerData.roundId;
      const session = activeSessions.get(roundId);

      if (!session) return;

      const currentQuestion = session.questions[session.currentQuestionIndex];

      if (isSimilarAnswer(answer, currentQuestion.correctAnswer)) {
        const elapsedTime = Math.floor((Date.now() - session.startTime) / 1000);

        if (session.currentQuestionIndex < session.questions.length - 1) {
          session.currentQuestionIndex++;
          const nextQuestion = session.questions[session.currentQuestionIndex];

          session.players.forEach((player) => {
            io.to(player.socketId).emit('next-question', {
              question: {
                id: nextQuestion._id,
                text: nextQuestion.question,
                description: nextQuestion.description
              },
              [`image${player.playerNumber}`]: nextQuestion[`image${player.playerNumber}`],
              playerNumber: player.playerNumber,
              elapsedTime
            });
          });
        } else {
          session.status = 'finished';
          session.endTime = Date.now();

          io.to(roundId).emit('game-finished', {
            finalTime: elapsedTime,
            totalPlayers: session.players.length
          });

          const gameRecord = new GameSession({
            roundId,
            players: session.players,
            startTime: new Date(session.startTime),
            endTime: new Date(session.endTime),
            answer,
            status: 'finished'
          });
          await gameRecord.save();

          setTimeout(() => {
            activeSessions.delete(roundId);
          }, 5000);
        }
      } else {
        session.failedAttempts++;
        io.to(roundId).emit('incorrect-answer', {
          attemptNumber: session.failedAttempts
        });
      }
    } catch (error) {
      console.error('Submit answer error:', error);
    }
  });

  socket.on('disconnect', () => {
    const playerData = playerSockets.get(socket.id);
    if (playerData) {
      io.to(playerData.roundId).emit('player-disconnected', {
        playerNumber: playerData.playerNumber,
        username: playerData.username
      });
      playerSockets.delete(socket.id);
    }
    console.log('User disconnected:', socket.id);
  });
});

function startGame(roundId) {
  const session = activeSessions.get(roundId);
  if (!session) return;

  session.status = 'playing';
  session.startTime = Date.now();

  if (session.questions.length > 0) {
    const question = session.questions[0];
    session.players.forEach((player) => {
      io.to(player.socketId).emit('game-start', {
        question: {
          id: question._id,
          text: question.question,
          description: question.description
        },
        [`image${player.playerNumber}`]: question[`image${player.playerNumber}`],
        playerNumber: player.playerNumber
      });
    });
  }
}

// ==================== REST API ====================

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email });

    if (!user || user.password !== password) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    res.json({
      userId: user._id,
      email: user.email,
      username: user.username
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, username } = req.body;

    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(400).json({ error: 'User already exists' });
    }

    const user = new User({ email, password, username });
    await user.save();

    res.json({
      userId: user._id,
      email: user.email,
      username: user.username
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/rounds', async (req, res) => {
  try {
    const rounds = await Round.find().populate('questions');
    res.json(rounds);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/admin/questions', async (req, res) => {
  try {
    const adminPassword = req.headers['x-admin-password'];
    if (adminPassword !== process.env.ADMIN_PASSWORD) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { question, description, correctAnswer, roundId, image1, image2, image3 } = req.body;

    const newQuestion = new Question({
      question,
      description,
      correctAnswer,
      image1,
      image2,
      image3,
      roundId
    });

    await newQuestion.save();

    if (roundId) {
      await Round.findByIdAndUpdate(
        roundId,
        { $push: { questions: newQuestion._id } }
      );
    }

    res.json(newQuestion);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/admin/rounds', async (req, res) => {
  try {
    const adminPassword = req.headers['x-admin-password'];
    if (adminPassword !== process.env.ADMIN_PASSWORD) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { name } = req.body;
    const round = new Round({ name, questions: [] });
    await round.save();

    res.json(round);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.put('/api/admin/questions/:id', async (req, res) => {
  try {
    const adminPassword = req.headers['x-admin-password'];
    if (adminPassword !== process.env.ADMIN_PASSWORD) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const question = await Question.findByIdAndUpdate(req.params.id, req.body, { new: true });
    res.json(question);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.delete('/api/admin/questions/:id', async (req, res) => {
  try {
    const adminPassword = req.headers['x-admin-password'];
    if (adminPassword !== process.env.ADMIN_PASSWORD) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await Question.findByIdAndDelete(req.params.id);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/statistics', async (req, res) => {
  try {
    const sessions = await GameSession.find();
    const fastestTime = sessions.length > 0
      ? Math.min(...sessions.map(s => (s.endTime - s.startTime) / 1000))
      : 0;

    res.json({
      totalGames: sessions.length,
      fastestTime: Math.floor(fastestTime),
      averageTime: sessions.length > 0
        ? Math.floor(sessions.reduce((acc, s) => acc + (s.endTime - s.startTime) / 1000, 0) / sessions.length)
        : 0
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ==================== Server Start ====================

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});