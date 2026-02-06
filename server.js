const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// 存储在线用户 { ws -> { username, room } }
const clients = new Map();

wss.on('connection', (ws) => {
  let currentUser = null;
  let currentRoom = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      // 用户注册（带房间号）
      if (msg.type === 'register') {
        currentUser = msg.username;
        currentRoom = msg.room;
        clients.set(ws, { username: currentUser, room: currentRoom });
        broadcastUserList(currentRoom);
        console.log(`[上线] ${currentUser} -> 房间 ${currentRoom}`);
      }

      // 发送心情（带附加文字）
      if (msg.type === 'mood') {
        const { to, mood, emoji, from, message } = msg;
        console.log(`[心情] ${from} -> ${to}: ${mood} ${emoji} ${message || ''}`);

        // 找到同房间的目标用户并推送
        for (const [clientWs, info] of clients) {
          if (info.username === to && info.room === currentRoom && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(JSON.stringify({
              type: 'mood_notification',
              from,
              mood,
              emoji,
              message: message || '',
              timestamp: new Date().toISOString()
            }));
          }
        }
      }
    } catch (e) {
      console.error('消息解析错误:', e);
    }
  });

  ws.on('close', () => {
    if (currentUser) {
      console.log(`[下线] ${currentUser}`);
      clients.delete(ws);
      if (currentRoom) broadcastUserList(currentRoom);
    }
  });
});

// 只广播同一房间的在线用户列表
function broadcastUserList(room) {
  const roomUsers = [];
  const roomClients = [];
  for (const [clientWs, info] of clients) {
    if (info.room === room) {
      roomUsers.push(info.username);
      roomClients.push(clientWs);
    }
  }
  const msg = JSON.stringify({ type: 'user_list', users: roomUsers });
  for (const clientWs of roomClients) {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(msg);
    }
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Moodlink 已启动: http://localhost:${PORT}`);
});
