# Lesson 8: Prefetch Count（QoS設定）

## 概要

Prefetch Count（プリフェッチ数）は、Consumer が**同時に処理できるメッセージの数**を制限する機能です。QoS（Quality of Service）設定によって、負荷の均等分散を実現します。

**問題**: デフォルトのラウンドロビンでは負荷が偏る
```
Worker 1: [軽][軽][軽][軽] → 4秒で完了
Worker 2: [重][重] → 20秒で完了
```

**解決**: Prefetch で「処理完了したら次を取得」方式に
```
Worker 1: [軽][軽][軽][軽][重] → バランス良く分散
Worker 2: [重][軽][軽][軽] → バランス良く分散
```

---

## メリット

### 1. 均等な負荷分散
- 処理能力に応じた配信
- アイドル時間の削減
- スループット向上

### 2. メモリ管理
- 大量のメッセージを一度に受信しない
- OOMエラーの防止
- 安定したパフォーマンス

### 3. フェアディスパッチ
- 速いWorkerにより多くのタスク
- 遅いWorkerは無理をしない
- 全体の処理時間短縮

---

## デメリット

### 1. レイテンシの増加
- メッセージ取得のラウンドトリップ
- 若干の遅延
- リアルタイム性への影響

### 2. 設定の難しさ
- 適切なprefetch値の決定が困難
- 環境によって最適値が異なる
- チューニングが必要

### 3. 初期配信の遅延
- 最初のメッセージ取得まで時間がかかる
- バッチ処理では不利な場合も

---

## 技術的原理

### Prefetch の動作

```javascript
// Prefetch を設定
await channel.prefetch(1);  // 1つずつ処理

channel.consume(queueName, async (msg) => {
  await processMessage(msg);  // 処理
  channel.ack(msg);           // ACK送信 → 次のメッセージを取得
}, { noAck: false });
```

### Prefetch値の決定

| Prefetch | 動作 | 用途 |
|----------|------|------|
| **1** | 1つずつ処理 | 重い処理、完全な均等分散 |
| **10** | 10個までバッファ | バランス型 |
| **100** | 100個までバッファ | 軽い処理、高スループット |
| **無制限** | すべて事前取得 | デフォルト（非推奨） |

### メッセージフロー

```
Prefetch = 1 の場合:

1. Worker起動 → Queue から 1個取得
2. 処理実行
3. ACK送信
4. 次の 1個を取得
5. 繰り返し

→ Workerは常に1個のみ保持
```

---

## 実装例（JavaScript & TypeScript）

### JavaScript版

#### worker-with-prefetch.js

```javascript
import amqp from 'amqplib';

async function startWorker(workerId, prefetchCount) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queue = 'task_queue';

  await channel.assertQueue(queue, { durable: true });

  // Prefetch設定（重要！）
  channel.prefetch(prefetchCount);

  console.log(`🔧 [${workerId}] 起動 (prefetch: ${prefetchCount})\n`);

  channel.consume(queue, async (msg) => {
    const task = JSON.parse(msg.content.toString());

    console.log(`📋 [${workerId}] 受信: Task ${task.id} (${task.weight}秒)`);

    // タスク処理（重さに応じて時間がかかる）
    await new Promise(resolve => setTimeout(resolve, task.weight * 1000));

    console.log(`✅ [${workerId}] 完了: Task ${task.id}\n`);
    channel.ack(msg);
  }, { noAck: false });
}

const workerId = process.argv[2] || 'Worker-1';
const prefetchCount = parseInt(process.argv[3]) || 1;

startWorker(workerId, prefetchCount);
```

#### producer-heavy-tasks.js

```javascript
import amqp from 'amqplib';

async function sendHeavyTasks() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queue = 'task_queue';

  await channel.assertQueue(queue, { durable: true });

  // 軽いタスクと重いタスクを混ぜる
  const tasks = [
    { id: 1, weight: 1 },   // 軽い
    { id: 2, weight: 5 },   // 重い
    { id: 3, weight: 1 },
    { id: 4, weight: 5 },
    { id: 5, weight: 1 },
    { id: 6, weight: 5 },
    { id: 7, weight: 1 },
    { id: 8, weight: 5 },
  ];

  for (const task of tasks) {
    channel.sendToQueue(queue, Buffer.from(JSON.stringify(task)), {
      persistent: true
    });
    console.log(`📤 送信: Task ${task.id} (${task.weight}秒)`);
  }

  console.log('\n✅ すべてのタスクを送信しました\n');

  setTimeout(() => { connection.close(); process.exit(0); }, 500);
}

sendHeavyTasks();
```

### TypeScript版

#### src/worker-with-prefetch.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';

interface Task {
  id: number;
  weight: number;
}

async function startWorker(workerId: string, prefetchCount: number): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const queue = 'task_queue';

  await channel.assertQueue(queue, { durable: true });

  channel.prefetch(prefetchCount);

  console.log(`🔧 [${workerId}] 起動 (prefetch: ${prefetchCount})\n`);

  channel.consume(queue, async (msg: ConsumeMessage | null) => {
    if (msg) {
      const task: Task = JSON.parse(msg.content.toString());

      console.log(`📋 [${workerId}] 受信: Task ${task.id} (${task.weight}秒)`);

      await new Promise(resolve => setTimeout(resolve, task.weight * 1000));

      console.log(`✅ [${workerId}] 完了: Task ${task.id}\n`);
      channel.ack(msg);
    }
  }, { noAck: false });
}

const workerId = process.argv[2] || 'Worker-1';
const prefetchCount = parseInt(process.argv[3]) || 1;

startWorker(workerId, prefetchCount).catch(console.error);
```

---

## 実行例

### Prefetchなし（問題のデモ）

```bash
# Worker 1, 2起動（prefetchなし = 無制限）
node worker-with-prefetch.js "Worker-1" 0
node worker-with-prefetch.js "Worker-2" 0

# タスク送信
node producer-heavy-tasks.js

# 結果: Worker間で負荷が偏る
```

### Prefetch=1（推奨）

```bash
# Worker 1, 2起動（prefetch=1）
node worker-with-prefetch.js "Worker-1" 1
node worker-with-prefetch.js "Worker-2" 1

# タスク送信
node producer-heavy-tasks.js

# 結果: 均等に分散される
```

---

## ベストプラクティス

### 1. 重い処理では prefetch=1

```javascript
// ✅ 推奨（重い処理）
channel.prefetch(1);
```

### 2. 軽い処理では prefetch=10-100

```javascript
// ✅ バランス型
channel.prefetch(10);
```

### 3. Manual ACK必須

```javascript
// ✅ Prefetchには Manual ACK が必須
channel.consume(queue, handler, { noAck: false });
```

---

## 次のステップ

[Lesson 9: Dead Letter Exchange](09-dead-letter.md) でエラーハンドリングを学びましょう。
