# Lesson 1: 基本的なメッセージキュー

## 概要

基本的なメッセージキューは、RabbitMQの最もシンプルな使い方です。Producer（送信者）がメッセージをQueueに送信し、Consumer（受信者）がそのメッセージを取り出して処理します。

```
Producer → Queue → Consumer
```

---

## メリット

### 1. 疎結合なアーキテクチャ
- ProducerとConsumerが独立して動作
- 一方が停止していても、もう一方は影響を受けない
- サービスの変更が容易

### 2. 非同期処理
- Producerは送信後すぐに次の処理に移れる
- レスポンス時間の短縮
- ユーザー体験の向上

### 3. バッファリング
- 一時的な負荷スパイクを吸収
- Consumerの処理速度に合わせて配信
- システム全体の安定性向上

### 4. シンプルな実装
- 最小限の設定で動作
- 学習コストが低い
- 素早くプロトタイプを作成可能

---

## デメリット

### 1. レイテンシの増加
- メッセージの中継により遅延が発生
- リアルタイム性が求められる処理には不向き
- 直接呼び出しに比べて数ミリ秒〜数十ミリ秒の遅延

### 2. 複雑性の増加
- システムコンポーネントが増える
- デバッグが難しくなる
- 監視ツールの必要性

### 3. 順序保証の制限
- 基本的には送信順に配信されるが、完全な保証はない
- ネットワーク障害時の再送で順序が変わる可能性
- 厳密な順序が必要な場合は追加の制御が必要

### 4. 単一障害点
- RabbitMQサーバーがダウンするとメッセージングが停止
- クラスタリングやHA構成が必要
- 運用コストの増加

---

## 技術的原理

### メッセージフロー

1. **接続の確立**
   ```
   Producer → TCP接続 → RabbitMQ Server
   ```
   - AMQP プロトコルで接続
   - 認証とチャネルの作成

2. **Queueの宣言**
   ```javascript
   channel.assertQueue(queueName, {durable: false})
   ```
   - Queue が存在しない場合は作成
   - 存在する場合は何もしない（べき等性）

3. **メッセージの送信**
   ```javascript
   channel.sendToQueue(queueName, Buffer.from(message))
   ```
   - メッセージはバイト配列として送信
   - デフォルトExchange（''）を使用
   - ルーティングキーはQueue名

4. **メッセージの受信**
   ```javascript
   channel.consume(queueName, (msg) => {
     // 処理
   })
   ```
   - ポーリングではなくプッシュ型
   - メッセージが届いたらコールバックが実行

### デフォルトExchange

基本的なキューでは **デフォルトExchange** を使用します：

- 名前: `''` (空文字列)
- タイプ: Direct
- 特徴: ルーティングキー = Queue名で自動バインディング

```
sendToQueue(queueName, msg)
    ↓
publish('', queueName, msg)
    ↓
Default Exchange [routing_key=queueName]
    ↓
Queue [name=queueName]
```

### メッセージの構造

```javascript
{
  content: Buffer,      // メッセージ本体（バイト配列）
  fields: {
    deliveryTag: 1,     // 配信ID
    redelivered: false, // 再配信フラグ
    exchange: '',       // Exchange名
    routingKey: 'task_queue' // ルーティングキー
  },
  properties: {
    contentType: 'application/json',
    contentEncoding: 'utf-8',
    headers: {},
    deliveryMode: 1,    // 1=非永続, 2=永続
    priority: 0,
    correlationId: null,
    replyTo: null,
    expiration: null,
    messageId: null,
    timestamp: null,
    type: null,
    userId: null,
    appId: null
  }
}
```

---

## ユースケース

### 1. バックグラウンドジョブの実行

**シナリオ**: ユーザー登録後のウェルカムメール送信

```
Web App → [新規登録リクエスト]
   ↓
   即座にレスポンス「登録完了」
   ↓
   メール送信タスクをQueueに追加
   ↓
Worker が非同期でメール送信
```

**メリット**:
- ユーザーはメール送信を待たずに次の画面へ
- メールサーバーの遅延がユーザー体験に影響しない

### 2. ログの集約

**シナリオ**: マイクロサービスからのログ収集

```
Service A ─┐
Service B ─┼→ [Log Queue] → Log Processor → Database
Service C ─┘
```

**メリット**:
- 各サービスはログを送信するだけ
- ログ処理の遅延がサービスに影響しない
- ログの集中管理

### 3. データ処理パイプライン

**シナリオ**: 画像のアップロードと処理

```
Upload API → [Image Queue] → Worker → リサイズ・圧縮 → S3保存
```

**メリット**:
- アップロード完了後すぐにユーザーに返答
- 重い画像処理を非同期で実行
- 処理の進行状況を別途管理可能

### 4. 注文処理システム

**シナリオ**: ECサイトの注文処理

```
注文API → [Order Queue] → 在庫確認 → 決済処理 → 発送手配
```

**メリット**:
- 注文の取りこぼしがない
- ピーク時の負荷を平準化
- 処理の各ステップを分離

---

## 実装例

### ディレクトリ構成

```
curriculum/examples/01-basic-queue/
├── package.json
├── producer.js      # メッセージ送信
└── consumer.js      # メッセージ受信
```

### package.json

```json
{
  "name": "01-basic-queue",
  "version": "1.0.0",
  "description": "RabbitMQ Basic Queue Example",
  "main": "index.js",
  "scripts": {
    "producer": "node producer.js",
    "consumer": "node consumer.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

### producer.js - メッセージ送信

```javascript
const amqp = require('amqplib');

async function sendMessage() {
  try {
    // 1. RabbitMQサーバーに接続
    const connection = await amqp.connect('amqp://localhost');
    console.log('✓ RabbitMQに接続しました');

    // 2. チャネルを作成
    const channel = await connection.createChannel();
    console.log('✓ チャネルを作成しました');

    // 3. Queueを宣言（存在しない場合は作成）
    const queueName = 'hello_queue';
    await channel.assertQueue(queueName, {
      durable: false  // サーバー再起動時に削除される
    });
    console.log(`✓ Queue "${queueName}" を宣言しました`);

    // 4. メッセージを送信
    const message = 'Hello World!';
    channel.sendToQueue(queueName, Buffer.from(message));
    console.log(`✓ メッセージを送信しました: "${message}"`);

    // 5. 接続をクリーンアップ
    setTimeout(() => {
      connection.close();
      console.log('✓ 接続を閉じました');
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラーが発生しました:', error.message);
    process.exit(1);
  }
}

// 複数メッセージを送信する例
async function sendMultipleMessages() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, { durable: false });

    // 10個のタスクを送信
    for (let i = 1; i <= 10; i++) {
      const task = {
        id: i,
        type: 'process_data',
        data: `Task ${i}`,
        timestamp: new Date().toISOString()
      };

      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));
      console.log(`✓ タスク ${i} を送信: ${message}`);
    }

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

// コマンドライン引数で実行モードを選択
const mode = process.argv[2] || 'single';

if (mode === 'multiple') {
  sendMultipleMessages();
} else {
  sendMessage();
}
```

### consumer.js - メッセージ受信

```javascript
const amqp = require('amqplib');

async function receiveMessages() {
  try {
    // 1. RabbitMQサーバーに接続
    const connection = await amqp.connect('amqp://localhost');
    console.log('✓ RabbitMQに接続しました');

    // 2. チャネルを作成
    const channel = await connection.createChannel();
    console.log('✓ チャネルを作成しました');

    // 3. Queueを宣言（Producerと同じ設定が必要）
    const queueName = 'hello_queue';
    await channel.assertQueue(queueName, {
      durable: false
    });
    console.log(`✓ Queue "${queueName}" を宣言しました`);
    console.log('⏳ メッセージを待機中...\n');

    // 4. メッセージを受信
    channel.consume(queueName, (msg) => {
      if (msg !== null) {
        const content = msg.content.toString();
        console.log(`📨 メッセージを受信: "${content}"`);

        // メッセージの詳細情報を表示
        console.log('   詳細情報:');
        console.log(`   - Delivery Tag: ${msg.fields.deliveryTag}`);
        console.log(`   - Routing Key: ${msg.fields.routingKey}`);
        console.log(`   - Exchange: ${msg.fields.exchange || '(default)'}`);
        console.log(`   - Redelivered: ${msg.fields.redelivered}`);
        console.log('');

        // メッセージを確認（Queueから削除）
        // 注: この例では自動確認を使用しているため、実際には不要
        // channel.ack(msg);
      }
    }, {
      noAck: true  // 自動確認モード（メッセージ受信時に自動削除）
    });

  } catch (error) {
    console.error('✗ エラーが発生しました:', error.message);
    process.exit(1);
  }
}

// タスク処理の例（JSON形式のメッセージ）
async function processTaskQueue() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, { durable: false });
    console.log(`✓ Queue "${queueName}" からタスクを受信中...\n`);

    channel.consume(queueName, (msg) => {
      if (msg !== null) {
        const content = msg.content.toString();
        const task = JSON.parse(content);

        console.log(`📋 タスクを受信:`);
        console.log(`   ID: ${task.id}`);
        console.log(`   Type: ${task.type}`);
        console.log(`   Data: ${task.data}`);
        console.log(`   Timestamp: ${task.timestamp}`);

        // タスクを処理（シミュレーション）
        console.log(`   ⚙️  処理中...`);
        setTimeout(() => {
          console.log(`   ✅ タスク ${task.id} 完了\n`);
        }, 1000);
      }
    }, {
      noAck: true
    });

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

// コマンドライン引数で実行モードを選択
const mode = process.argv[2] || 'simple';

if (mode === 'task') {
  processTaskQueue();
} else {
  receiveMessages();
}
```

### 実行方法

#### 1. セットアップ

```bash
cd curriculum/examples/01-basic-queue
npm install
```

#### 2. RabbitMQの起動（Dockerを使用）

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

#### 3. シンプルな例の実行

**ターミナル1 - Consumer起動:**
```bash
npm run consumer
```

**ターミナル2 - Producerでメッセージ送信:**
```bash
npm run producer
```

#### 4. 複数タスクの例

**ターミナル1 - タスクワーカー起動:**
```bash
node consumer.js task
```

**ターミナル2 - タスク送信:**
```bash
node producer.js multiple
```

### 実行結果例

**Producer側:**
```
✓ RabbitMQに接続しました
✓ チャネルを作成しました
✓ Queue "hello_queue" を宣言しました
✓ メッセージを送信しました: "Hello World!"
✓ 接続を閉じました
```

**Consumer側:**
```
✓ RabbitMQに接続しました
✓ チャネルを作成しました
✓ Queue "hello_queue" を宣言しました
⏳ メッセージを待機中...

📨 メッセージを受信: "Hello World!"
   詳細情報:
   - Delivery Tag: 1
   - Routing Key: hello_queue
   - Exchange: (default)
   - Redelivered: false
```

---

## 理解度チェック

### 質問

1. デフォルトExchangeのルーティングキーは何になりますか？
2. `assertQueue` の `durable: false` の意味は？
3. `noAck: true` にすると何が起こりますか？
4. ProducerとConsumerのどちらを先に起動すべきですか？

### 回答

1. **Queue名**がルーティングキーになります
2. **サーバー再起動時にQueueが削除される**（非永続化）
3. **メッセージ受信時に自動的に確認される**（手動確認不要）
4. **どちらでもOK**。Queueは双方で宣言されるため、先に起動した方が作成します

---

## 次のステップ

基本的なメッセージキューを理解できたら、次は複数のワーカーで負荷を分散する [Lesson 2: Work Queues](02-work-queues.md) に進みましょう。

### 改善ポイント

現在の実装には以下の問題があります（次のレッスンで改善）：

1. ❌ メッセージの処理失敗時に再送されない → **Lesson 3で解決**
2. ❌ サーバー再起動でメッセージが消える → **Lesson 4で解決**
3. ❌ 1つのConsumerしか動作しない → **Lesson 2で解決**
4. ❌ 負荷分散が最適化されていない → **Lesson 8で解決**
