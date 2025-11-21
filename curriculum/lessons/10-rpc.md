# Lesson 10: RPC Pattern（リモートプロシージャコール）

## 概要

RPC（Remote Procedure Call）パターンは、RabbitMQを使って**同期的なリクエスト/レスポンス**通信を実現します。

```
Client → [Request Queue] → Server
                              ↓
Client ← [Reply Queue] ← Server
```

---

## メリット

### 1. 非同期インフラで同期通信
- 既存のRabbitMQ環境を活用
- HTTPの代替として利用可能
- 統一されたメッセージング基盤

### 2. 負荷分散
- 複数のRPCサーバーで処理
- ラウンドロビン配信
- スケーラビリティ

### 3. 信頼性
- メッセージの永続化
- ACKによる確実な処理
- タイムアウト処理

---

## デメリット

### 1. 複雑性の増加
- コールバックQueue の管理
- Correlation IDの追跡
- タイムアウト処理の実装

### 2. パフォーマンス
- HTTPより遅い場合がある
- ラウンドトリップのオーバーヘッド
- レイテンシ

### 3. デバッグの難易度
- 非同期処理の追跡
- エラーハンドリング
- タイムアウトの調整

---

## 技術的原理

### Correlation ID

リクエストとレスポンスを紐付けるID

```javascript
const correlationId = generateUuid();

channel.sendToQueue('rpc_queue', Buffer.from(request), {
  correlationId: correlationId,
  replyTo: replyQueueName
});
```

### Reply To Queue

レスポンスを受け取るQueue

```javascript
// 一時Queue（exclusive）を作成
const q = await channel.assertQueue('', { exclusive: true });

// リクエスト送信時に指定
channel.sendToQueue('rpc_queue', Buffer.from(data), {
  replyTo: q.queue
});
```

---

## 実装例（JavaScript & TypeScript）

### JavaScript版

#### rpc-server.js

```javascript
import amqp from 'amqplib';

async function startRPCServer() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queue = 'rpc_queue';

  await channel.assertQueue(queue, { durable: false });
  channel.prefetch(1);

  console.log('🔧 RPC Server起動\n');

  channel.consume(queue, async (msg) => {
    const n = parseInt(msg.content.toString());

    console.log(`📋 リクエスト受信: fib(${n})`);

    // フィボナッチ計算
    const result = fibonacci(n);

    console.log(`✅ レスポンス送信: ${result}\n`);

    // レスポンス送信
    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(result.toString()),
      { correlationId: msg.properties.correlationId }
    );

    channel.ack(msg);
  }, { noAck: false });
}

function fibonacci(n) {
  if (n === 0) return 0;
  if (n === 1) return 1;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

startRPCServer();
```

#### rpc-client.js

```javascript
import amqp from 'amqplib';
import { randomUUID } from 'crypto';

async function callRPC(n) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  // 一時Queue作成（レスポンス受信用）
  const q = await channel.assertQueue('', { exclusive: true });
  const correlationId = randomUUID();

  console.log(`📤 リクエスト送信: fib(${n})`);

  return new Promise((resolve, reject) => {
    // タイムアウト設定
    const timeout = setTimeout(() => {
      connection.close();
      reject(new Error('RPC timeout'));
    }, 5000);

    // レスポンス受信
    channel.consume(q.queue, (msg) => {
      if (msg.properties.correlationId === correlationId) {
        clearTimeout(timeout);
        const result = parseInt(msg.content.toString());

        console.log(`📨 レスポンス受信: ${result}\n`);

        resolve(result);
        setTimeout(() => {
          connection.close();
          process.exit(0);
        }, 500);
      }
    }, { noAck: true });

    // リクエスト送信
    channel.sendToQueue('rpc_queue', Buffer.from(n.toString()), {
      correlationId: correlationId,
      replyTo: q.queue
    });
  });
}

const n = parseInt(process.argv[2]) || 10;
callRPC(n).catch(console.error);
```

### TypeScript版

#### src/rpc-server.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';

async function startRPCServer(): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const queue = 'rpc_queue';

  await channel.assertQueue(queue, { durable: false });
  channel.prefetch(1);

  console.log('🔧 RPC Server起動\n');

  channel.consume(queue, async (msg: ConsumeMessage | null) => {
    if (msg) {
      const n = parseInt(msg.content.toString());

      console.log(`📋 リクエスト受信: fib(${n})`);

      const result = fibonacci(n);

      console.log(`✅ レスポンス送信: ${result}\n`);

      channel.sendToQueue(
        msg.properties.replyTo,
        Buffer.from(result.toString()),
        { correlationId: msg.properties.correlationId }
      );

      channel.ack(msg);
    }
  }, { noAck: false });
}

function fibonacci(n: number): number {
  if (n === 0) return 0;
  if (n === 1) return 1;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

startRPCServer().catch(console.error);
```

#### src/rpc-client.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';
import { randomUUID } from 'crypto';

async function callRPC(n: number): Promise<number> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();

  const q = await channel.assertQueue('', { exclusive: true });
  const correlationId = randomUUID();

  console.log(`📤 リクエスト送信: fib(${n})`);

  return new Promise<number>((resolve, reject) => {
    const timeout = setTimeout(() => {
      connection.close();
      reject(new Error('RPC timeout'));
    }, 5000);

    channel.consume(q.queue, (msg: ConsumeMessage | null) => {
      if (msg && msg.properties.correlationId === correlationId) {
        clearTimeout(timeout);
        const result = parseInt(msg.content.toString());

        console.log(`📨 レスポンス受信: ${result}\n`);

        resolve(result);
        setTimeout(() => {
          connection.close();
          process.exit(0);
        }, 500);
      }
    }, { noAck: true });

    channel.sendToQueue('rpc_queue', Buffer.from(n.toString()), {
      correlationId: correlationId,
      replyTo: q.queue
    });
  });
}

const n = parseInt(process.argv[2]) || 10;
callRPC(n).catch(console.error);
```

---

## 実行例

```bash
# 1. RPCサーバー起動
node rpc-server.js

# 2. RPCクライアント実行
node rpc-client.js 10
# → フィボナッチ数列の10番目を計算して返す
```

---

## ベストプラクティス

### 1. タイムアウト必須

```javascript
setTimeout(() => reject('timeout'), 5000);
```

### 2. Correlation ID の検証

```javascript
if (msg.properties.correlationId === correlationId) {
  // 正しいレスポンス
}
```

### 3. エラーハンドリング

```javascript
try {
  const result = await callRPC(n);
} catch (error) {
  console.error('RPC failed:', error);
}
```

---

## まとめ

RabbitMQの全10レッスンが完了しました！

✅ Lesson 1: 基本的なメッセージキュー
✅ Lesson 2: Work Queues
✅ Lesson 3: Message Acknowledgment
✅ Lesson 4: Message Durability
✅ Lesson 5: Publish/Subscribe
✅ Lesson 6: Routing
✅ Lesson 7: Topics
✅ Lesson 8: Prefetch Count
✅ Lesson 9: Dead Letter Exchange
✅ Lesson 10: RPC Pattern

これでRabbitMQの主要機能をすべて習得しました！
