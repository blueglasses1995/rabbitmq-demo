# Lesson 4: Message Durability（永続化）

## 概要

Message Durability（永続化）は、RabbitMQサーバーの再起動時にもQueue とメッセージを保持する機能です。これにより、システム障害時でもメッセージの損失を防ぎます。

永続化には**3つの要素**が必要です：
1. **Durable Queue**: Queueの永続化
2. **Persistent Message**: メッセージの永続化
3. **Publisher Confirms**: 送信確認（オプションだが推奨）

---

## メリット

### 1. データ損失の防止
- サーバー再起動でもメッセージを保持
- インフラ障害に強い
- ビジネスクリティカルなデータを保護

### 2. 信頼性の向上
- 重要な処理の確実な実行
- トランザクション的な処理が可能
- SLAの達成

### 3. 災害復旧（DR）対策
- サーバークラッシュからの復旧
- メンテナンス時の安全性
- データの整合性維持

### 4. ビジネス連続性
- ダウンタイム中もメッセージを保持
- サービス再開時に処理再開
- ユーザー影響の最小化

---

## デメリット

### 1. パフォーマンスの低下
- ディスクI/Oによる遅延
- スループットが約10分の1に
- レイテンシの増加（数ms〜数十ms）

**ベンチマーク例**:
```
非永続化: 50,000 msg/sec
永続化:    5,000 msg/sec
```

### 2. ディスク容量の消費
- メッセージがディスクに保存される
- ディスクフルでシステム停止
- 監視と容量管理が必須

### 3. 完全な保証はない
- ディスク書き込み前のクラッシュでは損失
- fsyncタイミングによる
- 100%の保証ではない点に注意

### 4. 運用の複雑化
- バックアップ戦略の必要性
- ディスク障害対策（RAID等）
- モニタリングの強化

---

## 技術的原理

### 3つの永続化要素

#### 1. Durable Queue（永続Queue）

```javascript
channel.assertQueue(queueName, {
  durable: true  // Queue を永続化
});
```

**効果**:
- RabbitMQ 再起動後も Queue が存在
- Queue の設定も保持される
- メタデータがディスクに保存

**注意**: 既存の非永続Queueを永続化に変更する場合は削除が必要
```bash
# 既存Queueを削除
rabbitmqadmin delete queue name=my_queue

# または
channel.deleteQueue('my_queue');
```

#### 2. Persistent Message（永続メッセージ）

```javascript
channel.sendToQueue(queueName, Buffer.from(message), {
  persistent: true  // メッセージを永続化
});
```

**効果**:
- メッセージがディスクに保存
- サーバー再起動後も保持
- `deliveryMode: 2` が内部的に設定される

#### 3. Publisher Confirms（送信確認）

```javascript
// チャネルを confirm モードに
await channel.assertQueue(queueName, { durable: true });
await channel.assertExchange(exchangeName, 'direct', { durable: true });

// メッセージ送信後の確認
channel.sendToQueue(queueName, Buffer.from(message), {
  persistent: true
});

// 確認を待つ
await channel.waitForConfirms();
console.log('メッセージがディスクに書き込まれました');
```

### 永続化の内部フロー

```
1. Producer がメッセージ送信（persistent: true）
   ↓
2. RabbitMQ がメモリに保存
   ↓
3. ディスクへ非同期書き込み
   ↓
4. fsync（ディスクへの物理書き込み）
   ↓
5. Publisher Confirm を返送（confirm モード時）
   ↓
6. Producer が確認を受信
```

### ディスク書き込みのタイミング

RabbitMQは以下のタイミングでディスクに書き込みます：

1. **定期的な間隔**: デフォルト数百ミリ秒
2. **メモリ圧迫時**: メモリ使用量が高い場合
3. **明示的なfsync**: `vm_memory_high_watermark` 到達時

**注意**: 書き込み前のクラッシュではメッセージ損失の可能性あり

### Lazy Queue（レイジーキュー）

メモリ使用量を抑えた永続化モード：

```javascript
channel.assertQueue(queueName, {
  durable: true,
  arguments: {
    'x-queue-mode': 'lazy'  // Lazy Queue モード
  }
});
```

**特徴**:
- メッセージを積極的にディスクに書き込み
- メモリ使用量を最小化
- 大量のメッセージを保持可能
- レイテンシはやや高い

**用途**:
- 大量のメッセージを保持するQueue
- メモリが限られた環境
- バッチ処理

---

## ユースケース

### 1. 金融取引システム

**シナリオ**: 決済トランザクションの処理

```javascript
// 決済メッセージを永続化
channel.sendToQueue('payment_queue', Buffer.from(JSON.stringify({
  transactionId: 'TXN_12345',
  amount: 10000,
  currency: 'JPY',
  timestamp: new Date().toISOString()
})), {
  persistent: true  // 絶対に失われてはいけない
});

await channel.waitForConfirms();
console.log('決済リクエストを永続化しました');
```

**重要性**: サーバー障害でも決済データを保護

### 2. 注文管理システム

**シナリオ**: ECサイトの注文処理

```javascript
// 注文を永続Queue に保存
await channel.assertQueue('order_queue', { durable: true });

channel.sendToQueue('order_queue', Buffer.from(JSON.stringify({
  orderId: 'ORD_67890',
  customerId: 'CUST_123',
  items: [...],
  totalAmount: 50000
})), {
  persistent: true
});
```

**メリット**:
- システムメンテナンス中も注文を受け付け
- 再起動後に注文処理を再開

### 3. ログ収集システム

**シナリオ**: アプリケーションログの集約

```javascript
// 重要なログを永続化
channel.sendToQueue('critical_logs', Buffer.from(JSON.stringify({
  level: 'ERROR',
  service: 'payment-service',
  message: 'Payment gateway timeout',
  timestamp: Date.now()
})), {
  persistent: true  // エラーログは失われてはいけない
});
```

### 4. タスクスケジューラー

**シナリオ**: 定期実行タスクの管理

```javascript
// スケジュールされたタスクを永続化
await channel.assertQueue('scheduled_tasks', {
  durable: true,
  arguments: {
    'x-queue-mode': 'lazy'  // 大量のタスクを保持
  }
});

channel.sendToQueue('scheduled_tasks', Buffer.from(JSON.stringify({
  taskId: 'TASK_001',
  executeAt: '2024-01-01T00:00:00Z',
  action: 'send_reminder_email',
  params: {...}
})), {
  persistent: true
});
```

---

## 実装例

### ディレクトリ構成

```
curriculum/examples/04-durability/
├── package.json
├── producer-durable.js      # 永続化メッセージ送信
├── producer-non-durable.js  # 非永続化（比較用）
├── consumer-durable.js      # 永続Queue Consumer
└── test-restart.sh          # サーバー再起動テスト
```

### package.json

```json
{
  "name": "04-durability",
  "version": "1.0.0",
  "description": "RabbitMQ Message Durability Example",
  "scripts": {
    "producer-durable": "node producer-durable.js",
    "producer-non-durable": "node producer-non-durable.js",
    "consumer": "node consumer-durable.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

### producer-durable.js - 永続化メッセージ

```javascript
const amqp = require('amqplib');

async function sendDurableMessages() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'durable_queue';

    // 永続Queue を宣言
    await channel.assertQueue(queueName, {
      durable: true  // ✅ Queue を永続化
    });

    console.log(`✅ 永続Queue "${queueName}" を作成しました\n`);

    // 10個のメッセージを送信
    for (let i = 1; i <= 10; i++) {
      const message = JSON.stringify({
        id: i,
        type: 'important_task',
        data: `Durable Message ${i}`,
        timestamp: new Date().toISOString()
      });

      channel.sendToQueue(queueName, Buffer.from(message), {
        persistent: true  // ✅ メッセージを永続化
      });

      console.log(`📤 送信: Message ${i} (永続化)`);
    }

    // Publisher Confirms を使った確実な送信
    await channel.waitForConfirms();
    console.log('\n✅ すべてのメッセージがディスクに書き込まれました');

    console.log('\n💡 RabbitMQを再起動しても、これらのメッセージは保持されます');
    console.log('   テスト: docker restart rabbitmq\n');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

sendDurableMessages();
```

### producer-non-durable.js - 非永続化メッセージ（比較用）

```javascript
const amqp = require('amqplib');

async function sendNonDurableMessages() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'non_durable_queue';

    // 非永続Queue を宣言
    await channel.assertQueue(queueName, {
      durable: false  // ❌ Queue は非永続化
    });

    console.log(`⚠️  非永続Queue "${queueName}" を作成しました\n`);

    // 10個のメッセージを送信
    for (let i = 1; i <= 10; i++) {
      const message = JSON.stringify({
        id: i,
        type: 'temporary_task',
        data: `Non-Durable Message ${i}`,
        timestamp: new Date().toISOString()
      });

      channel.sendToQueue(queueName, Buffer.from(message), {
        persistent: false  // ❌ メッセージは非永続化
      });

      console.log(`📤 送信: Message ${i} (非永続化)`);
    }

    console.log('\n⚠️  これらのメッセージはRabbitMQ再起動で失われます');
    console.log('   テスト: docker restart rabbitmq\n');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

sendNonDurableMessages();
```

### consumer-durable.js - 永続Queue Consumer

```javascript
const amqp = require('amqplib');

const workerId = process.argv[2] || 'Consumer-DURABLE';

async function startConsumer() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'durable_queue';

    // 永続Queue を宣言（Producer と同じ設定が必要）
    await channel.assertQueue(queueName, {
      durable: true  // ✅ 永続Queue
    });

    console.log(`🔧 [${workerId}] 起動しました`);
    console.log(`📦 Queue: ${queueName} (永続化)`);
    console.log(`⏳ メッセージを待機中...\n`);

    // Manual ACK で処理
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] メッセージ受信:`);
        console.log(`   ID: ${task.id}`);
        console.log(`   Data: ${task.data}`);
        console.log(`   Timestamp: ${task.timestamp}`);

        // 処理をシミュレート
        await new Promise(resolve => setTimeout(resolve, 1000));

        // 処理完了後にACK
        channel.ack(msg);
        console.log(`✅ [${workerId}] 処理完了\n`);
      }
    }, {
      noAck: false  // Manual ACK
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startConsumer();
```

### test-restart.sh - 再起動テストスクリプト

```bash
#!/bin/bash

echo "🧪 RabbitMQ 永続化テスト"
echo "=========================="
echo ""

# 1. メッセージを送信
echo "1️⃣  永続化メッセージを送信中..."
node producer-durable.js
sleep 2

echo ""
echo "2️⃣  非永続化メッセージを送信中..."
node producer-non-durable.js
sleep 2

# 2. RabbitMQを再起動
echo ""
echo "3️⃣  RabbitMQを再起動中..."
docker restart rabbitmq
sleep 10

# 3. メッセージを確認
echo ""
echo "4️⃣  Consumerを起動してメッセージを確認..."
echo "    - 永続化メッセージは残っているはず ✅"
echo "    - 非永続化メッセージは消えているはず ❌"
echo ""

node consumer-durable.js
```

---

## 実行方法

### 1. 基本的な永続化デモ

**永続化メッセージを送信:**
```bash
npm run producer-durable
```

**Consumer起動:**
```bash
npm run consumer
```

**RabbitMQ再起動:**
```bash
docker restart rabbitmq
```

**Consumer再起動:**
```bash
npm run consumer
# → メッセージは残っている！
```

### 2. 非永続化との比較

**非永続化メッセージを送信:**
```bash
npm run producer-non-durable
```

**RabbitMQ再起動:**
```bash
docker restart rabbitmq
```

**結果:**
- 永続化メッセージ: 残っている ✅
- 非永続化メッセージ: 消えている ❌

### 3. 自動テストスクリプト

```bash
chmod +x test-restart.sh
./test-restart.sh
```

---

## 実行結果例

### 永続化メッセージの場合

```
# メッセージ送信
npm run producer-durable
✅ 永続Queue "durable_queue" を作成しました
📤 送信: Message 1 (永続化)
📤 送信: Message 2 (永続化)
...
✅ すべてのメッセージがディスクに書き込まれました

# RabbitMQ再起動
docker restart rabbitmq
rabbitmq

# Consumer起動
npm run consumer
🔧 [Consumer-DURABLE] 起動しました
📋 [Consumer-DURABLE] メッセージ受信:
   ID: 1
   Data: Durable Message 1
✅ [Consumer-DURABLE] 処理完了

→ メッセージは保持されている！✅
```

### 非永続化メッセージの場合

```
# メッセージ送信
npm run producer-non-durable
⚠️  非永続Queue "non_durable_queue" を作成しました
📤 送信: Message 1 (非永続化)
...

# RabbitMQ再起動
docker restart rabbitmq

# Consumer起動
npm run consumer
⏳ メッセージを待機中...
(何も受信されない)

→ メッセージは失われた！❌
```

---

## 理解度チェック

### 質問

1. 永続化に必要な3つの要素は？
2. `durable: true` と `persistent: true` の違いは？
3. 永続化でもメッセージが失われる場合は？
4. Lazy Queue とは何ですか？

### 回答

1. **Durable Queue**、**Persistent Message**、**Publisher Confirms**（推奨）
2. **durable**: Queue の永続化、**persistent**: メッセージの永続化
3. **ディスク書き込み前のクラッシュ**、**ディスク障害**、**設定ミス**
4. **メモリ使用量を抑えた永続化モード**。メッセージを積極的にディスクに保存。

---

## ベストプラクティス

### 1. 重要なメッセージは必ず永続化

```javascript
// ✅ 正しい
await channel.assertQueue('payment_queue', { durable: true });
channel.sendToQueue('payment_queue', msg, { persistent: true });
await channel.waitForConfirms();

// ❌ 間違い（重要データが失われる可能性）
await channel.assertQueue('payment_queue', { durable: false });
channel.sendToQueue('payment_queue', msg);
```

### 2. Publisher Confirms を使用

```javascript
// ✅ 確実な送信
channel.sendToQueue(queue, msg, { persistent: true });
await channel.waitForConfirms();
console.log('ディスクに書き込まれました');
```

### 3. Consumer も durable Queue を宣言

```javascript
// Producer と Consumer で同じ設定
await channel.assertQueue('my_queue', { durable: true });
```

### 4. Lazy Queue で大量メッセージを扱う

```javascript
// 数百万件のメッセージを保持する場合
await channel.assertQueue('huge_queue', {
  durable: true,
  arguments: { 'x-queue-mode': 'lazy' }
});
```

---

## 次のステップ

Message Durability を理解できたら、次は [Lesson 5: Publish/Subscribe（Fanout Exchange）](05-pubsub.md) でブロードキャスト配信を学びましょう。

### 学んだこと

✅ Durable Queue でQueue を永続化
✅ Persistent Message でメッセージを永続化
✅ Publisher Confirms で確実な送信
✅ サーバー再起動時のデータ保護
✅ Lazy Queue で大量メッセージを扱う
