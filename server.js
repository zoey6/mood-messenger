const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// 存储在线用户 { ws, username }
const clients = new Map();

wss.on('connection', (ws) => {
  let currentUser = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      // 用户注册
      if (msg.type === 'register') {
        currentUser = msg.username;
        clients.set(ws, currentUser);
        broadcastUserList();
        console.log(`[上线] ${currentUser}`);
      }

      // 发送心情
      if (msg.type === 'mood') {
        const { to, mood, emoji, from } = msg;
        console.log(`[心情] ${from} -> ${to}: ${mood} ${emoji}`);

        // 找到目标用户并推送
        for (const [clientWs, username] of clients) {
          if (username === to && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(JSON.stringify({
              type: 'mood_notification',
              from,
              mood,
              emoji,
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
      broadcastUserList();
    }
  });
});

// 广播在线用户列表
function broadcastUserList() {
  const userList = Array.from(clients.values());
  const msg = JSON.stringify({ type: 'user_list', users: userList });
  for (const [clientWs] of clients) {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(msg);
    }
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 心情信使服务已启动: http://localhost:${PORT}`);
});
