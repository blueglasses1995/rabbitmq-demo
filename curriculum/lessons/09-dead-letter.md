# Lesson 9: Dead Letter Exchange（DLX）

## 概要

Dead Letter Exchange（DLX）は、**処理できなかったメッセージ**を別のExchangeに転送する仕組みです。エラーハンドリング、リトライ、監視に活用されます。

**メッセージがDead Letterになる条件**:
1. Consumer がメッセージを **reject/nack**（requeue=false）
2. メッセージの **TTL（有効期限）** が切れた
3. Queue の **長さ制限** を超えた

---

## メリット

### 1. エラーハンドリング
- 失敗したメッセージを別途処理
- 自動リトライの実装
- エラーログの集約

### 2. デバッグの容易化
- 問題のあるメッセージを分析
- 再処理や手動介入が可能
- トラブルシューティング

### 3. システムの安定性
- メインQueueにエラーが蓄積しない
- 処理継続が可能
- 監視とアラート

---

## デメリット

### 1. 設定の複雑さ
- Exchange、Queue、Bindingの追加設定
- 理解と実装の難易度
- ドキュメント化が重要

### 2. 無限ループのリスク
- 不適切なリトライ設定でループ
- リトライ上限の設定が必須
- 監視が必要

### 3. ストレージの消費
- Dead Letterが蓄積
- 定期的なクリーンアップが必要
- 容量監視

---

## 技術的原理

### DLXの設定

```javascript
// メインQueue にDLXを設定
await channel.assertQueue('main_queue', {
  durable: true,
  arguments: {
    'x-dead-letter-exchange': 'dlx_exchange',
    'x-dead-letter-routing-key': 'dead_letter'  // オプション
  }
});

// Dead Letter Exchange を作成
await channel.assertExchange('dlx_exchange', 'direct', { durable: true });

// Dead Letter Queue を作成
await channel.assertQueue('dead_letter_queue', { durable: true });

// バインディング
await channel.bindQueue('dead_letter_queue', 'dlx_exchange', 'dead_letter');
```

### TTL（Time To Live）

```javascript
// メッセージごとのTTL
channel.sendToQueue('queue', Buffer.from(msg), {
  expiration: '10000'  // 10秒後に期限切れ
});

// QueueのデフォルトTTL
await channel.assertQueue('queue', {
  arguments: {
    'x-message-ttl': 10000  // 10秒
  }
});
```

### リトライ戦略

```javascript
const MAX_RETRIES = 3;

channel.consume('main_queue', async (msg) => {
  const retryCount = (msg.properties.headers?.['x-retry-count'] || 0);

  try {
    await processMessage(msg);
    channel.ack(msg);
  } catch (error) {
    if (retryCount < MAX_RETRIES) {
      // リトライ
      channel.nack(msg, false, false);  // DLXへ

      // 遅延後に再キュー
      setTimeout(() => {
        channel.sendToQueue('main_queue', msg.content, {
          headers: { 'x-retry-count': retryCount + 1 }
        });
      }, 5000);  // 5秒待機
    } else {
      // 最終的にDLXへ
      channel.nack(msg, false, false);
    }
  }
}, { noAck: false });
```

---

## 実装例（JavaScript & TypeScript）

### JavaScript版

#### setup-dlx.js

```javascript
import amqp from 'amqplib';

async function setupDLX() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  // Dead Letter Exchange
  await channel.assertExchange('dlx', 'direct', { durable: true });

  // Dead Letter Queue
  await channel.assertQueue('dead_letter_queue', { durable: true });
  await channel.bindQueue('dead_letter_queue', 'dlx', 'dead_letter');

  // メインQueue（DLX設定付き）
  await channel.assertQueue('main_queue', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx',
      'x-dead-letter-routing-key': 'dead_letter'
    }
  });

  console.log('✅ DLX設定完了');

  await connection.close();
}

setupDLX();
```

#### producer-with-error.js

```javascript
import amqp from 'amqplib';

async function sendMessages() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  const messages = [
    { id: 1, type: 'valid', data: 'OK' },
    { id: 2, type: 'error', data: 'This will fail' },
    { id: 3, type: 'valid', data: 'OK' },
  ];

  for (const msg of messages) {
    channel.sendToQueue('main_queue', Buffer.from(JSON.stringify(msg)), {
      persistent: true
    });
    console.log(`📤 送信: ${JSON.stringify(msg)}`);
  }

  setTimeout(() => { connection.close(); process.exit(0); }, 500);
}

sendMessages();
```

#### consumer-with-dlx.js

```javascript
import amqp from 'amqplib';

async function startConsumer() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  console.log('🔧 Consumer起動\n');

  channel.consume('main_queue', async (msg) => {
    const message = JSON.parse(msg.content.toString());
    const retryCount = msg.properties.headers?.['x-retry-count'] || 0;

    console.log(`📋 受信: ${JSON.stringify(message)} (retry: ${retryCount})`);

    try {
      if (message.type === 'error') {
        throw new Error('Processing error');
      }

      // 正常処理
      await new Promise(resolve => setTimeout(resolve, 1000));
      console.log(`✅ 完了: ${message.id}\n`);
      channel.ack(msg);

    } catch (error) {
      console.log(`❌ エラー: ${error.message}`);

      if (retryCount < 3) {
        console.log(`🔄 リトライ (${retryCount + 1}/3)\n`);
        channel.nack(msg, false, false);

        setTimeout(() => {
          channel.sendToQueue('main_queue', msg.content, {
            persistent: true,
            headers: { 'x-retry-count': retryCount + 1 }
          });
        }, 2000);
      } else {
        console.log(`⛔ 最大リトライ超過 → DLXへ\n`);
        channel.nack(msg, false, false);
      }
    }
  }, { noAck: false });
}

startConsumer();
```

#### monitor-dlx.js

```javascript
import amqp from 'amqplib';

async function monitorDLX() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  console.log('👁️  Dead Letter Queue を監視中\n');

  channel.consume('dead_letter_queue', (msg) => {
    const message = JSON.parse(msg.content.toString());
    const retryCount = msg.properties.headers?.['x-retry-count'] || 0;

    console.log('💀 Dead Letter受信:');
    console.log(`   メッセージ: ${JSON.stringify(message)}`);
    console.log(`   リトライ回数: ${retryCount}`);
    console.log(`   理由: ${msg.properties.headers?.['x-death']?.[0]?.reason || 'unknown'}`);
    console.log('');

    // 手動で分析・対応
    channel.ack(msg);
  }, { noAck: false });
}

monitorDLX();
```

### TypeScript版

```typescript
// 同様の実装をTypeScriptで型安全に実装
interface Message {
  id: number;
  type: 'valid' | 'error';
  data: string;
}

interface MessageHeaders {
  'x-retry-count'?: number;
  'x-death'?: Array<{
    reason: string;
    queue: string;
    time: Date;
  }>;
}
```

---

## 実行例

```bash
# 1. DLX設定
node setup-dlx.js

# 2. DLXモニター起動
node monitor-dlx.js

# 3. Consumer起動
node consumer-with-dlx.js

# 4. メッセージ送信
node producer-with-error.js
```

---

## ベストプラクティス

### 1. リトライ上限を設定

```javascript
const MAX_RETRIES = 3;
```

### 2. DLXを必ず監視

```javascript
// アラート、ログ記録、手動介入
```

### 3. 定期的なクリーンアップ

```javascript
// 古いDead Letterを削除
```

---

## 次のステップ

[Lesson 10: RPC Pattern](10-rpc.md) で同期的な通信を学びましょう。
