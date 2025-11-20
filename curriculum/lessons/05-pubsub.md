# Lesson 5: Publish/Subscribe（Fanout Exchange）

## 概要

Publish/Subscribe パターンは、1つのメッセージを**複数のConsumer に同時配信**する仕組みです。これを実現するために **Fanout Exchange** を使用します。

```
              ┌─→ Queue A → Consumer A
Producer → Fanout Exchange ┼─→ Queue B → Consumer B
              └─→ Queue C → Consumer C
```

すべてのバインドされたQueueに**ブロードキャスト**されます。

---

## メリット

### 1. ブロードキャスト配信
- 同じメッセージを複数のサービスに配信
- サービス間の疎結合
- 新しいConsumerの追加が容易

### 2. イベント駆動アーキテクチャ
- イベント発行/購読モデルの実装
- マイクロサービス間の連携
- リアルタイム通知

### 3. 水平スケーリング
- Consumer を独立してスケール
- 各サービスが独自のペースで処理
- 負荷分散

### 4. 柔軟な拡張性
- 新しい機能を後から追加
- 既存システムへの影響なし
- プラグイン的な構成

---

## デメリット

### 1. メッセージの重複処理
- 複数Consumerがすべて受信
- 不要なConsumerも受信する可能性
- フィルタリングロジックが必要な場合も

### 2. 整合性の管理が複雑
- すべてのConsumerが成功するとは限らない
- 一部失敗時の対応が必要
- トランザクション的な処理が困難

### 3. デバッグの難易度
- メッセージがどこに配信されたか追跡が必要
- ログの相関が重要
- 分散トレーシングの必要性

### 4. パフォーマンスへの影響
- メッセージの複製コスト
- ネットワークトラフィックの増加
- メモリ使用量の増加

---

## 技術的原理

### Fanout Exchange の動作

**Fanout Exchange** は、ルーティングキーを**完全に無視**し、バインドされたすべてのQueueにメッセージをコピーします。

```javascript
// Exchange を宣言
await channel.assertExchange('logs', 'fanout', {
  durable: false
});

// メッセージを発行（ルーティングキーは無視される）
channel.publish('logs', '', Buffer.from(message));
//                      ↑
//                  空文字でOK
```

### バインディングの仕組み

```javascript
// Queue を作成
await channel.assertQueue('queue_A');
await channel.assertQueue('queue_B');
await channel.assertQueue('queue_C');

// Exchange に バインド
await channel.bindQueue('queue_A', 'logs', '');
await channel.bindQueue('queue_B', 'logs', '');
await channel.bindQueue('queue_C', 'logs', '');
//                                       ↑
//                              ルーティングキーは不要
```

### Temporary Queue（一時キュー）

Consumer が切断されたら自動削除されるQueue：

```javascript
// 一時Queue を作成（名前はランダム）
const q = await channel.assertQueue('', {
  exclusive: true,  // この接続専用
  autoDelete: true  // 接続切断時に自動削除
});

console.log(`一時Queue作成: ${q.queue}`);  // 例: amq.gen-Xu4...
```

**用途**:
- ログ受信など、Consumer起動中のみ必要な場合
- 各Consumerが独立したQueueを持つ
- Queue名の管理不要

### メッセージフロー

```
1. Producer が Exchange に publish
   ↓
2. Exchange が バインドされたすべてのQueue にコピー
   ↓
3. 各Queue が 対応する Consumer に配信
   ↓
4. すべてのConsumer が並列に処理
```

---

## ユースケース

### 1. ログ配信システム

**シナリオ**: アプリケーションログを複数の宛先に配信

```
Application
   ↓
Log Exchange (Fanout)
   ├─→ File Writer Queue → ファイル保存
   ├─→ ElasticSearch Queue → 検索インデックス
   └─→ Alert Queue → 異常検知・アラート
```

**メリット**:
- ログの複数処理が並列実行
- 新しい処理の追加が容易
- 既存処理への影響なし

### 2. 通知システム

**シナリオ**: ユーザーへの通知を複数チャネルで送信

```
Notification Event
   ↓
Notification Exchange (Fanout)
   ├─→ Email Queue → メール送信
   ├─→ SMS Queue → SMS送信
   ├─→ Push Queue → プッシュ通知
   └─→ In-App Queue → アプリ内通知
```

### 3. データ同期

**シナリオ**: マスターデータの更新を複数のサービスに通知

```
User Update Event
   ↓
User Data Exchange (Fanout)
   ├─→ Analytics Service → 分析データ更新
   ├─→ Cache Service → キャッシュ無効化
   ├─→ Search Service → 検索インデックス更新
   └─→ Audit Service → 監査ログ記録
```

### 4. リアルタイムダッシュボード

**シナリオ**: メトリクスデータを複数のダッシュボードに配信

```
Metrics Event
   ↓
Metrics Exchange (Fanout)
   ├─→ Dashboard A Queue → 運用ダッシュボード
   ├─→ Dashboard B Queue → 経営ダッシュボード
   └─→ Monitoring Queue → アラート監視
```

---

## 実装例

### ディレクトリ構成

```
curriculum/examples/05-pubsub/
├── package.json
├── publisher.js          # イベント発行
├── subscriber-file.js    # ファイル保存
├── subscriber-console.js # コンソール出力
└── subscriber-alert.js   # アラート処理
```

### package.json

```json
{
  "name": "05-pubsub",
  "version": "1.0.0",
  "description": "RabbitMQ Publish/Subscribe (Fanout Exchange) Example",
  "scripts": {
    "publisher": "node publisher.js",
    "sub-file": "node subscriber-file.js",
    "sub-console": "node subscriber-console.js",
    "sub-alert": "node subscriber-alert.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

### publisher.js - イベント発行

```javascript
const amqp = require('amqplib');

async function publishLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    // Fanout Exchange を宣言
    await channel.assertExchange(exchangeName, 'fanout', {
      durable: false
    });

    // ログメッセージを送信
    const logs = [
      { level: 'INFO', message: 'Application started', service: 'api-server' },
      { level: 'DEBUG', message: 'Database connection established', service: 'api-server' },
      { level: 'WARN', message: 'High memory usage detected', service: 'worker' },
      { level: 'ERROR', message: 'Failed to process payment', service: 'payment-service' },
      { level: 'INFO', message: 'User logged in', service: 'auth-service' },
    ];

    console.log(`📡 Exchange "${exchangeName}" にログを発行中...\n`);

    for (const log of logs) {
      const message = JSON.stringify({
        ...log,
        timestamp: new Date().toISOString()
      });

      // Fanout なのでルーティングキーは空文字でOK
      channel.publish(exchangeName, '', Buffer.from(message));

      console.log(`📤 発行: [${log.level}] ${log.message}`);
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log('\n✅ すべてのログを発行しました');
    console.log('💡 すべての購読者がこのログを受信します\n');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

publishLogs();
```

### subscriber-file.js - ファイル保存

```javascript
const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');

async function subscribeToLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    // Exchange を宣言
    await channel.assertExchange(exchangeName, 'fanout', {
      durable: false
    });

    // 一時Queue を作成（exclusive: この接続専用）
    const q = await channel.assertQueue('', {
      exclusive: true
    });

    console.log('📁 [File Subscriber] 起動しました');
    console.log(`   Queue: ${q.queue}`);

    // Exchange と Queue をバインド
    await channel.bindQueue(q.queue, exchangeName, '');

    console.log('⏳ ログを待機中...\n');

    // ログをファイルに保存
    channel.consume(q.queue, (msg) => {
      if (msg !== null) {
        const log = JSON.parse(msg.content.toString());

        console.log(`📝 [File Subscriber] ログ受信: [${log.level}] ${log.message}`);

        // ファイルに追記
        const logLine = `[${log.timestamp}] [${log.level}] [${log.service}] ${log.message}\n`;
        fs.appendFileSync('logs.txt', logLine);

        console.log(`   → logs.txt に保存しました\n`);

        channel.ack(msg);
      }
    }, {
      noAck: false
    });

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

subscribeToLogs();
```

### subscriber-console.js - コンソール出力

```javascript
const amqp = require('amqplib');

async function subscribeToLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    await channel.assertExchange(exchangeName, 'fanout', {
      durable: false
    });

    const q = await channel.assertQueue('', {
      exclusive: true
    });

    console.log('🖥️  [Console Subscriber] 起動しました');
    console.log(`   Queue: ${q.queue}`);

    await channel.bindQueue(q.queue, exchangeName, '');

    console.log('⏳ ログを待機中...\n');

    // コンソールに色付きで表示
    channel.consume(q.queue, (msg) => {
      if (msg !== null) {
        const log = JSON.parse(msg.content.toString());

        // ログレベルに応じた色付け
        const colors = {
          'INFO': '\x1b[32m',    // 緑
          'DEBUG': '\x1b[36m',   // シアン
          'WARN': '\x1b[33m',    // 黄色
          'ERROR': '\x1b[31m',   // 赤
        };
        const reset = '\x1b[0m';

        const color = colors[log.level] || '';
        console.log(`${color}🖥️  [Console Subscriber] ${log.timestamp}${reset}`);
        console.log(`${color}   [${log.level}] [${log.service}]${reset}`);
        console.log(`${color}   ${log.message}${reset}\n`);

        channel.ack(msg);
      }
    }, {
      noAck: false
    });

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

subscribeToLogs();
```

### subscriber-alert.js - アラート処理

```javascript
const amqp = require('amqplib');

async function subscribeToLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    await channel.assertExchange(exchangeName, 'fanout', {
      durable: false
    });

    const q = await channel.assertQueue('', {
      exclusive: true
    });

    console.log('🚨 [Alert Subscriber] 起動しました');
    console.log(`   Queue: ${q.queue}`);
    console.log('   ERROR レベルのログを監視します');

    await channel.bindQueue(q.queue, exchangeName, '');

    console.log('⏳ ログを待機中...\n');

    // ERROR レベルのログのみアラート
    channel.consume(q.queue, (msg) => {
      if (msg !== null) {
        const log = JSON.parse(msg.content.toString());

        if (log.level === 'ERROR' || log.level === 'WARN') {
          console.log('🚨 [Alert Subscriber] ⚠️  アラート発生！');
          console.log(`   レベル: ${log.level}`);
          console.log(`   サービス: ${log.service}`);
          console.log(`   メッセージ: ${log.message}`);
          console.log(`   時刻: ${log.timestamp}`);

          // 実際にはここで通知を送信
          // - Slack webhook
          // - PagerDuty API
          // - メール送信
          console.log('   📧 管理者に通知を送信しました\n');
        }

        channel.ack(msg);
      }
    }, {
      noAck: false
    });

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

subscribeToLogs();
```

---

## 実行方法

### 1. 複数の Subscriber を起動

**ターミナル1 - File Subscriber:**
```bash
npm run sub-file
```

**ターミナル2 - Console Subscriber:**
```bash
npm run sub-console
```

**ターミナル3 - Alert Subscriber:**
```bash
npm run sub-alert
```

### 2. ログを発行

**ターミナル4 - Publisher:**
```bash
npm run publisher
```

### 3. 結果確認

すべての Subscriber が**同じログ**を受信します：
- File Subscriber → ファイルに保存
- Console Subscriber → 色付きで表示
- Alert Subscriber → ERROR/WARN のみアラート

---

## 実行結果例

### Publisher

```
📡 Exchange "logs" にログを発行中...

📤 発行: [INFO] Application started
📤 発行: [DEBUG] Database connection established
📤 発行: [WARN] High memory usage detected
📤 発行: [ERROR] Failed to process payment
📤 発行: [INFO] User logged in

✅ すべてのログを発行しました
💡 すべての購読者がこのログを受信します
```

### File Subscriber

```
📁 [File Subscriber] 起動しました
   Queue: amq.gen-Xu4...
⏳ ログを待機中...

📝 [File Subscriber] ログ受信: [INFO] Application started
   → logs.txt に保存しました

📝 [File Subscriber] ログ受信: [WARN] High memory usage detected
   → logs.txt に保存しました
...
```

### Console Subscriber

```
🖥️  [Console Subscriber] 起動しました
   Queue: amq.gen-Pq8...
⏳ ログを待機中...

🖥️  [Console Subscriber] 2024-01-01T00:00:00.000Z
   [INFO] [api-server]
   Application started
...
```

### Alert Subscriber

```
🚨 [Alert Subscriber] 起動しました
   Queue: amq.gen-Rz2...
   ERROR レベルのログを監視します
⏳ ログを待機中...

🚨 [Alert Subscriber] ⚠️  アラート発生！
   レベル: ERROR
   サービス: payment-service
   メッセージ: Failed to process payment
   時刻: 2024-01-01T00:00:03.000Z
   📧 管理者に通知を送信しました
```

---

## 理解度チェック

### 質問

1. Fanout Exchange の特徴は？
2. 一時Queue（exclusive）の用途は？
3. すべての購読者が同じメッセージを受け取りますか？
4. 新しい購読者を追加する場合、既存システムの変更は必要ですか？

### 回答

1. **ルーティングキーを無視して、バインドされたすべてのQueueに配信**
2. **Consumer 起動中のみ必要なQueue。接続切断時に自動削除**
3. **はい**。すべての購読者が独立してメッセージを受信
4. **不要**。新しい購読者を起動してバインドするだけ

---

## 次のステップ

Publish/Subscribe パターンを理解できたら、次は [Lesson 6: Routing（Direct Exchange）](06-routing.md) で選択的な配信を学びましょう。

### 学んだこと

✅ Fanout Exchange でブロードキャスト配信
✅ 複数の独立したConsumerの実装
✅ 一時Queue の活用
✅ イベント駆動アーキテクチャの基礎
