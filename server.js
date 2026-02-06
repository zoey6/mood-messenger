const express = require('express');
const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// WxPusher 配置
const WXPUSHER_APP_TOKEN = process.env.WXPUSHER_APP_TOKEN || 'AT_cMB7gxksDNsH4XYTwpImIlw9PD6IMY3d';

// 存储 WxPusher UID 绑定 { "room:username" -> wxpusherUid }
const wxBindingsFile = path.join(__dirname, 'wx-bindings.json');
let wxBindings = {};
try {
  if (fs.existsSync(wxBindingsFile)) {
    wxBindings = JSON.parse(fs.readFileSync(wxBindingsFile, 'utf8'));
  }
} catch(e) { wxBindings = {}; }

function saveWxBindings() {
  fs.writeFileSync(wxBindingsFile, JSON.stringify(wxBindings, null, 2));
}

// 静态文件服务
app.use(express.static(path.join(__dirname, 'public')));

// JSON 解析
app.use(express.json({ limit: '5mb' }));

// ===== WxPusher API 路由 =====

// 绑定微信 UID
app.post('/api/wx/bind', (req, res) => {
  const { room, username, uid } = req.body;
  if (!room || !username || !uid) return res.json({ ok: false, msg: '参数不完整' });
  const key = `${room}:${username}`;
  wxBindings[key] = uid;
  saveWxBindings();
  console.log(`[微信绑定] ${key} -> ${uid}`);
  res.json({ ok: true });
});

// 解绑微信
app.post('/api/wx/unbind', (req, res) => {
  const { room, username } = req.body;
  const key = `${room}:${username}`;
  delete wxBindings[key];
  saveWxBindings();
  console.log(`[微信解绑] ${key}`);
  res.json({ ok: true });
});

// 查询绑定状态
app.get('/api/wx/status', (req, res) => {
  const { room, username } = req.query;
  const key = `${room}:${username}`;
  res.json({ ok: true, bound: !!wxBindings[key] });
});

// 创建关注二维码（带参数，用于自动绑定）
app.post('/api/wx/qrcode', (req, res) => {
  const { room, username } = req.body;
  if (!room || !username) return res.json({ ok: false, msg: '参数不完整' });
  const extra = JSON.stringify({ room, username });
  const postData = JSON.stringify({
    appToken: WXPUSHER_APP_TOKEN,
    extra,
    validTime: 1800 // 30分钟有效
  });

  const options = {
    hostname: 'wxpusher.zjiecode.com',
    path: '/api/fun/create/qrcode',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(postData) }
  };

  const apiReq = https.request(options, (apiRes) => {
    let body = '';
    apiRes.on('data', d => body += d);
    apiRes.on('end', () => {
      try {
        const result = JSON.parse(body);
        res.json(result);
      } catch(e) { res.json({ ok: false, msg: '解析失败' }); }
    });
  });
  apiReq.on('error', (e) => res.json({ ok: false, msg: e.message }));
  apiReq.write(postData);
  apiReq.end();
});

// WxPusher 回调（用户扫码关注后自动绑定）
app.get('/api/wx/callback', (req, res) => {
  const { action, data } = req.query;
  console.log('[WxPusher回调]', action, data);
  try {
    if (action === 'app_subscribe') {
      const parsed = JSON.parse(data);
      const { uid, extra } = parsed;
      if (extra && uid) {
        const { room, username } = JSON.parse(extra);
        if (room && username) {
          const key = `${room}:${username}`;
          wxBindings[key] = uid;
          saveWxBindings();
          console.log(`[自动绑定] ${key} -> ${uid}`);
          // 通知在线的该用户
          for (const [clientWs, info] of clients) {
            if (info.username === username && info.room === room && clientWs.readyState === WebSocket.OPEN) {
              clientWs.send(JSON.stringify({ type: 'wx_bindstatus', bound: true }));
            }
          }
        }
      }
    }
  } catch(e) { console.error('回调解析错误:', e); }
  res.send('success');
});

// WxPusher 推送函数
function wxPushMessage(uid, title, content) {
  const postData = JSON.stringify({
    appToken: WXPUSHER_APP_TOKEN,
    content,
    summary: title,
    contentType: 1,
    uids: [uid]
  });

  const options = {
    hostname: 'wxpusher.zjiecode.com',
    path: '/api/send/message',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(postData) }
  };

  const apiReq = https.request(options, (apiRes) => {
    let body = '';
    apiRes.on('data', d => body += d);
    apiRes.on('end', () => {
      try {
        const result = JSON.parse(body);
        console.log(`[微信推送] ${result.code === 1000 ? '成功' : '失败'}: ${title}`);
      } catch(e) {}
    });
  });
  apiReq.on('error', (e) => console.error('[微信推送错误]', e.message));
  apiReq.write(postData);
  apiReq.end();
}

// 存储在线用户 { ws -> { username, room } }
const clients = new Map();
// 消息 ID 计数器
let msgIdCounter = 0;

// 心跳检测：每25秒ping一次，防止代理/平台超时断开
const HEARTBEAT_INTERVAL = 25000;
const heartbeatTimer = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('[心跳] 客户端无响应，断开');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

wss.on('close', () => clearInterval(heartbeatTimer));

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

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

        // WxPusher 微信推送（如果目标用户绑定了微信）
        const wxKey = `${currentRoom}:${to}`;
        if (wxBindings[wxKey]) {
          const wxTitle = `${emoji} ${from} 发来一份心情`;
          let wxContent = `${from} 现在的心情是「${mood}」${emoji}`;
          if (message) wxContent += `\n💬 "${message}"`;
          wxPushMessage(wxBindings[wxKey], wxTitle, wxContent);
        }
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

      // 心跳响应（客户端主动发的ping）
      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
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
