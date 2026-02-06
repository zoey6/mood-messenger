const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// 照片上传 - 接收 base64 JSON
app.use(express.json({ limit: '5mb' }));

// 存储在线用户 { ws -> { username, room } }
const clients = new Map();
// 消息 ID 计数器
let msgIdCounter = 0;

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
        // 通知房间内其他人：有人上线
        broadcastToRoom(currentRoom, {
          type: 'user_online',
          username: currentUser
        }, ws);
        broadcastUserList(currentRoom);
        console.log(`[上线] ${currentUser} -> 房间 ${currentRoom}`);
      }

      // 发送心情（带附加文字和可选照片）
      if (msg.type === 'mood') {
        const { to, mood, emoji, from, message, photo } = msg;
        const msgId = ++msgIdCounter;
        console.log(`[心情] ${from} -> ${to}: ${mood} ${emoji} ${message || ''} ${photo ? '[有照片]' : ''}`);

        // 找到同房间的目标用户并推送
        for (const [clientWs, info] of clients) {
          if (info.username === to && info.room === currentRoom && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(JSON.stringify({
              type: 'mood_notification',
              msgId,
              from,
              mood,
              emoji,
              message: message || '',
              photo: photo || '',
              timestamp: new Date().toISOString()
            }));
          }
        }

        // 回传给发送者确认（含 msgId）
        ws.send(JSON.stringify({
          type: 'mood_sent_confirm',
          msgId,
          to,
          mood,
          emoji,
          message: message || '',
          photo: photo || '',
          timestamp: new Date().toISOString()
        }));
      }

      // 已读回执
      if (msg.type === 'mark_read') {
        const { msgId, reader } = msg;
        // 通知消息发送者
        for (const [clientWs, info] of clients) {
          if (info.room === currentRoom && clientWs.readyState === WebSocket.OPEN && clientWs !== ws) {
            clientWs.send(JSON.stringify({
              type: 'read_receipt',
              msgId,
              reader
            }));
          }
        }
      }

      // 消息撤回
      if (msg.type === 'recall') {
        const { msgId } = msg;
        console.log(`[撤回] ${currentUser} 撤回消息 ${msgId}`);
        // 通知房间内所有人
        broadcastToRoom(currentRoom, {
          type: 'msg_recalled',
          msgId,
          by: currentUser
        }, null);
      }
    } catch (e) {
      console.error('消息解析错误:', e);
    }
  });

  ws.on('close', () => {
    if (currentUser) {
      console.log(`[下线] ${currentUser}`);
      clients.delete(ws);
      if (currentRoom) {
        // 通知房间内其他人：有人下线
        broadcastToRoom(currentRoom, {
          type: 'user_offline',
          username: currentUser
        }, null);
        broadcastUserList(currentRoom);
      }
    }
  });
});

// 广播给房间内所有人（可选排除某个ws）
function broadcastToRoom(room, data, excludeWs) {
  const msg = JSON.stringify(data);
  for (const [clientWs, info] of clients) {
    if (info.room === room && clientWs.readyState === WebSocket.OPEN && clientWs !== excludeWs) {
      clientWs.send(msg);
    }
  }
}

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
