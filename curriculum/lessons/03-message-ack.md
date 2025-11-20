# Lesson 3: Message Acknowledgment（確認応答）

## 概要

Message Acknowledgment（確認応答）は、Consumerがメッセージの処理完了をRabbitMQに通知する仕組みです。これにより、処理中のクラッシュやエラーでメッセージが失われることを防ぎます。

```
Queue → Consumer (メッセージ受信)
          ↓
       処理実行
          ↓
       処理完了
          ↓
Queue ← Consumer (ACK送信)
  ↓
メッセージ削除
```

---

## メリット

### 1. メッセージの損失防止
- Workerクラッシュ時も自動再配信
- 処理完了を保証
- システムの信頼性向上

**例**: Worker がタスク処理中にクラッシュ → メッセージは他のWorkerに再配信

### 2. At-least-once配信保証
- メッセージは必ず最低1回処理される
- ネットワーク障害にも対応
- データの完全性を維持

### 3. 処理失敗時の再試行
- エラー時にメッセージを拒否（Nack）
- 自動的に再キューイング
- リトライロジックの実装が容易

### 4. 柔軟なエラーハンドリング
- `ack`: 処理成功
- `nack`: 処理失敗（再キュー可）
- `reject`: メッセージ拒否

---

## デメリット

### 1. 重複処理のリスク
- ACKの送信遅延で重複配信の可能性
- **冪等性**の実装が必要
- 処理済みフラグの管理が重要

**例**:
```
1. Worker が処理完了
2. ACK送信前にネットワーク障害
3. RabbitMQが再配信
4. 同じメッセージが2回処理される
```

### 2. パフォーマンスの低下
- ACKの送受信でオーバーヘッド
- ネットワークトラフィックの増加
- スループットの若干の低下

### 3. タイムアウト管理の複雑性
- 長時間処理のタイムアウト設定が必要
- `consumer_timeout` の調整
- デフォルト30分

### 4. デバッグの難易度上昇
- ACK/Nackの状態追跡が必要
- メッセージの再配信回数の監視
- ログの充実が必須

---

## 技術的原理

### Acknowledgment の種類

#### 1. Auto Ack（自動確認）

```javascript
channel.consume(queueName, callback, {
  noAck: true  // 自動確認ON
});
```

- メッセージ受信時に**即座に削除**
- 処理の成否に関わらず削除
- **危険**: Worker クラッシュでメッセージ損失

#### 2. Manual Ack（手動確認）

```javascript
channel.consume(queueName, (msg) => {
  try {
    // 処理
    channel.ack(msg);  // 処理成功
  } catch (error) {
    channel.nack(msg, false, true);  // 処理失敗・再キュー
  }
}, {
  noAck: false  // 手動確認（デフォルト）
});
```

- 処理完了後に**明示的に確認**
- Worker クラッシュ時は自動再配信
- **推奨**: 本番環境では必須

### ACK/Nack/Reject の違い

| メソッド | 説明 | パラメータ | 用途 |
|---------|------|-----------|------|
| `ack(msg)` | 処理成功を通知 | - | 正常処理完了 |
| `ack(msg, true)` | 複数メッセージを一括確認 | `multiple` | バッチ処理 |
| `nack(msg, false, true)` | 処理失敗・再キュー | `multiple`, `requeue` | リトライ |
| `nack(msg, false, false)` | 処理失敗・破棄 | - | 回復不能エラー |
| `reject(msg, true)` | メッセージ拒否・再キュー | `requeue` | `nack`の単一版 |
| `reject(msg, false)` | メッセージ拒否・破棄 | - | 不正データ |

### メッセージの状態遷移

```
[Queue] → [Unacked] → [Acked] → 削除
             ↓
          Nack/Reject
             ↓
        [Queue] (再キュー)
             or
          削除/DLX
```

### Delivery Tag

各メッセージには**一意のDelivery Tag**が付与されます：

```javascript
msg.fields.deliveryTag  // 例: 1, 2, 3, ...
```

- チャネル内で単調増加
- ACK/Nackで使用
- 複数メッセージの一括確認に利用

```javascript
// Delivery Tag 1-5 のメッセージを一括確認
channel.ack(msg, true);  // multiple = true
```

### Redelivered フラグ

再配信されたメッセージには**redelivered フラグ**が立ちます：

```javascript
if (msg.fields.redelivered) {
  console.log('これは再配信されたメッセージです');
  // 特別な処理（ログ記録、アラート等）
}
```

### Consumer Timeout

長時間処理のタイムアウト設定：

```javascript
// RabbitMQ 3.8.15以降
// デフォルト: 30分
// 設定: rabbitmq.conf
consumer_timeout = 3600000  // 1時間（ミリ秒）
```

タイムアウト後、RabbitMQは：
1. Consumer接続を切断
2. 未確認メッセージを再キューイング
3. 他のWorkerに配信

---

## ユースケース

### 1. 決済処理システム

**シナリオ**: クレジットカード決済の処理

```javascript
channel.consume('payment_queue', async (msg) => {
  const payment = JSON.parse(msg.content.toString());

  try {
    // 決済API呼び出し
    await processPayment(payment);

    // 成功時のみACK
    channel.ack(msg);
    console.log(`✅ 決済完了: ${payment.id}`);

  } catch (error) {
    if (error.type === 'TEMPORARY') {
      // 一時的エラー（ネットワーク等）→ 再試行
      channel.nack(msg, false, true);
      console.log(`🔄 再試行: ${payment.id}`);
    } else {
      // 永続的エラー（無効カード等）→ 破棄
      channel.nack(msg, false, false);
      console.log(`❌ 決済失敗: ${payment.id}`);
    }
  }
}, { noAck: false });
```

### 2. 注文処理ワークフロー

**シナリオ**: ECサイトの注文確定処理

```javascript
channel.consume('order_queue', async (msg) => {
  const order = JSON.parse(msg.content.toString());

  try {
    // 在庫確認
    await checkInventory(order);

    // 決済処理
    await chargeCustomer(order);

    // 発送手配
    await createShipment(order);

    // すべて成功したらACK
    channel.ack(msg);

  } catch (error) {
    // エラー時は再キュー
    channel.nack(msg, false, true);

    // ロールバック処理
    await rollbackOrder(order);
  }
}, { noAck: false });
```

### 3. データベース更新の信頼性保証

**シナリオ**: ユーザーデータの更新

```javascript
channel.consume('user_update_queue', async (msg) => {
  const update = JSON.parse(msg.content.toString());

  try {
    await db.transaction(async (trx) => {
      await trx('users').where('id', update.userId).update(update.data);

      // DB更新成功後にACK
      channel.ack(msg);
    });

  } catch (error) {
    // DB接続エラー等 → 再試行
    console.error('DB更新失敗:', error);
    channel.nack(msg, false, true);
  }
}, { noAck: false });
```

### 4. 外部API呼び出しのリトライ

**シナリオ**: サードパーティAPIへのデータ送信

```javascript
channel.consume('api_sync_queue', async (msg) => {
  const data = JSON.parse(msg.content.toString());
  const retryCount = data.retryCount || 0;
  const MAX_RETRIES = 3;

  try {
    await sendToExternalAPI(data);
    channel.ack(msg);

  } catch (error) {
    if (retryCount < MAX_RETRIES) {
      // リトライ回数を増やして再キュー
      data.retryCount = retryCount + 1;
      channel.nack(msg, false, false);  // 現在のメッセージは破棄

      // 新しいメッセージとして再送信（リトライカウント付き）
      channel.sendToQueue('api_sync_queue', Buffer.from(JSON.stringify(data)));

    } else {
      // 最大リトライ回数超過 → Dead Letter Queueへ
      channel.nack(msg, false, false);
      console.error(`最大リトライ回数超過: ${data.id}`);
    }
  }
}, { noAck: false });
```

---

## 実装例

### ディレクトリ構成

```
curriculum/examples/03-message-ack/
├── package.json
├── producer.js              # タスク送信
├── worker-with-ack.js       # ACK付きワーカー
├── worker-auto-ack.js       # 自動ACK（比較用）
└── worker-error-handling.js # エラーハンドリング例
```

### package.json

```json
{
  "name": "03-message-ack",
  "version": "1.0.0",
  "description": "RabbitMQ Message Acknowledgment Example",
  "scripts": {
    "producer": "node producer.js",
    "worker-ack": "node worker-with-ack.js",
    "worker-auto": "node worker-auto-ack.js",
    "worker-error": "node worker-error-handling.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

### producer.js

```javascript
const amqp = require('amqplib');

async function sendTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    // いくつかのタスクを送信
    const tasks = [
      { id: 1, type: 'normal', data: 'Task 1' },
      { id: 2, type: 'normal', data: 'Task 2' },
      { id: 3, type: 'error', data: 'Task 3 - This will fail' },
      { id: 4, type: 'normal', data: 'Task 4' },
      { id: 5, type: 'crash', data: 'Task 5 - This will crash worker' },
      { id: 6, type: 'normal', data: 'Task 6' },
    ];

    console.log('📤 タスクを送信中...\n');

    for (const task of tasks) {
      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));
      console.log(`✓ 送信: Task ${task.id} (${task.type})`);
    }

    console.log(`\n✅ ${tasks.length}個のタスクを送信しました`);

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

sendTasks();
```

### worker-with-ack.js - Manual Ack の実装

```javascript
const amqp = require('amqplib');

const workerId = process.argv[2] || 'Worker-ACK';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（Manual ACK モード）`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    // Manual Ack モード
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`   Type: ${task.type}`);
        console.log(`   Redelivered: ${msg.fields.redelivered}`);

        try {
          // タスクの種類に応じて処理
          if (task.type === 'normal') {
            // 通常処理
            await new Promise(resolve => setTimeout(resolve, 1000));

            // 処理成功 → ACK
            channel.ack(msg);
            console.log(`✅ [${workerId}] Task ${task.id} 完了（ACK送信）\n`);

          } else if (task.type === 'error') {
            // エラーをシミュレート
            throw new Error('処理エラー');

          } else if (task.type === 'crash') {
            // クラッシュをシミュレート
            console.log(`💥 [${workerId}] クラッシュをシミュレート！`);
            console.log(`⚠️  ACKを送信せずにプロセス終了...\n`);

            // ACKを送信せずに終了
            // → RabbitMQは自動的に再キューイング
            setTimeout(() => process.exit(1), 500);
          }

        } catch (error) {
          console.log(`❌ [${workerId}] Task ${task.id} エラー: ${error.message}`);
          console.log(`🔄 [${workerId}] 再キューイング（Nack送信）\n`);

          // エラー時は再キュー
          channel.nack(msg, false, true);
          // パラメータ: (msg, multiple=false, requeue=true)
        }
      }
    }, {
      noAck: false  // Manual ACK モード
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
```

### worker-auto-ack.js - Auto Ack の比較

```javascript
const amqp = require('amqplib');

const workerId = 'Worker-AUTO-ACK';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（Auto ACK モード）`);
    console.log(`⚠️  警告: メッセージは受信時に即座に削除されます`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    // Auto Ack モード（危険）
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`⚠️  メッセージは既にQueueから削除されています`);

        try {
          if (task.type === 'normal') {
            await new Promise(resolve => setTimeout(resolve, 1000));
            console.log(`✅ [${workerId}] Task ${task.id} 完了\n`);

          } else if (task.type === 'crash') {
            console.log(`💥 [${workerId}] クラッシュ！`);
            console.log(`❌ メッセージは既に削除されているため、損失します！\n`);
            setTimeout(() => process.exit(1), 500);
          }

        } catch (error) {
          console.log(`❌ [${workerId}] エラー: ${error.message}`);
          console.log(`❌ メッセージは既に削除されているため、再試行できません！\n`);
        }
      }
    }, {
      noAck: true  // Auto ACK モード（危険）
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
```

### worker-error-handling.js - 高度なエラーハンドリング

```javascript
const amqp = require('amqplib');

const workerId = 'Worker-ERROR-HANDLER';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（高度なエラーハンドリング）\n`);

    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        // 再配信回数をチェック
        const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;
        const MAX_RETRIES = 3;

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`   Redelivered: ${msg.fields.redelivered}`);
        console.log(`   Retry Count: ${retryCount}/${MAX_RETRIES}`);

        try {
          // タスク処理
          if (task.type === 'error') {
            throw new Error('一時的なエラー');
          }

          await new Promise(resolve => setTimeout(resolve, 1000));
          channel.ack(msg);
          console.log(`✅ [${workerId}] Task ${task.id} 完了\n`);

        } catch (error) {
          console.log(`❌ [${workerId}] エラー: ${error.message}`);

          if (retryCount < MAX_RETRIES) {
            // リトライ
            console.log(`🔄 [${workerId}] リトライします (${retryCount + 1}/${MAX_RETRIES})\n`);

            // 現在のメッセージを拒否
            channel.nack(msg, false, false);

            // リトライカウントを増やして再送信
            const headers = {
              'x-retry-count': retryCount + 1
            };

            channel.sendToQueue(queueName, msg.content, {
              headers: headers
            });

          } else {
            // 最大リトライ回数超過
            console.log(`⛔ [${workerId}] 最大リトライ回数超過 - メッセージを破棄\n`);
            channel.nack(msg, false, false);

            // Dead Letter Queue に送信するのが理想的（Lesson 9で実装）
          }
        }
      }
    }, {
      noAck: false
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
```

---

## 実行方法

### 1. Manual ACK のデモ

**ターミナル1 - Manual ACK ワーカー起動:**
```bash
npm run worker-ack
```

**ターミナル2 - タスク送信:**
```bash
npm run producer
```

**期待される動作:**
- 通常タスク → 処理成功・ACK
- エラータスク → Nack・再キュー・再処理
- クラッシュタスク → Worker終了・自動再キュー

### 2. Auto ACK との比較

**Auto ACK ワーカー（危険）:**
```bash
npm run worker-auto
```

**Manual ACK ワーカー（安全）:**
```bash
npm run worker-ack
```

タスク送信後、両者の違いを確認してください。

### 3. 高度なエラーハンドリング

```bash
npm run worker-error
npm run producer
```

---

## 理解度チェック

### 質問

1. `noAck: true` と `noAck: false` の違いは？
2. Worker がクラッシュしたとき、メッセージはどうなりますか？
3. `nack` の3つのパラメータの意味は？
4. 再配信されたメッセージをどう識別しますか？

### 回答

1. **true**: 自動確認（受信時に削除）、**false**: 手動確認（ACK必要）
2. **Manual ACKの場合**: 自動再キューイング、**Auto ACKの場合**: メッセージ損失
3. `(msg, multiple, requeue)`: メッセージ、一括確認フラグ、再キューフラグ
4. **`msg.fields.redelivered`** フラグで識別

---

## ベストプラクティス

### 1. 必ず Manual ACK を使用
```javascript
// ✅ 推奨
channel.consume(queue, callback, { noAck: false });

// ❌ 非推奨（開発環境のみ）
channel.consume(queue, callback, { noAck: true });
```

### 2. 処理完了後にのみ ACK
```javascript
// ✅ 正しい
await processTask(task);
channel.ack(msg);

// ❌ 間違い
channel.ack(msg);
await processTask(task);  // 失敗してもメッセージは削除済み
```

### 3. try-catch でエラーハンドリング
```javascript
try {
  await processTask(task);
  channel.ack(msg);
} catch (error) {
  channel.nack(msg, false, true);
}
```

### 4. 冪等性の実装
```javascript
// 重複処理を防ぐ
const processed = await checkIfProcessed(task.id);
if (processed) {
  channel.ack(msg);  // 既に処理済み
  return;
}

await processTask(task);
await markAsProcessed(task.id);
channel.ack(msg);
```

---

## 次のステップ

Message Acknowledgment を理解できたら、次は [Lesson 4: Message Durability（永続化）](04-durability.md) でサーバー再起動時のデータ保護を学びましょう。

### 学んだこと

✅ Manual ACK で信頼性向上
✅ Nack/Reject によるエラーハンドリング
✅ Worker クラッシュ時の自動再配信
✅ 重複処理のリスクと対策
