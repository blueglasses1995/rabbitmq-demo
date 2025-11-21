# Lesson 7: Topics（Topic Exchange）

## 概要

Topic Exchange は、**パターンマッチング**でメッセージをルーティングする最も柔軟な方式です。ワイルドカードを使った複雑な配信条件を設定できます。

```
ルーティングキー: "service.log.error"
パターン:          "service.*.error"  → マッチ ✅
パターン:          "service.#"        → マッチ ✅
パターン:          "*.log.*"          → マッチ ✅
```

---

## メリット

### 1. 柔軟なルーティング
- ワイルドカードでパターンマッチング
- 複雑な条件設定が可能
- Direct Exchangeより汎用的

### 2. スケーラブルな設計
- 新しいサービス追加が容易
- ルーティングルールの動的変更
- 階層的なカテゴリ管理

### 3. マイクロサービスに最適
- サービス名、環境、ログレベルなどを組み合わせ
- 柔軟なメッセージング
- イベント駆動アーキテクチャの実現

---

## デメリット

### 1. パフォーマンス
- パターンマッチングのオーバーヘッド
- Direct Exchangeより遅い
- 大量のバインディングで影響

### 2. 設定の複雑さ
- ルーティングキーの命名規則が重要
- ワイルドカードの理解が必要
- デバッグが困難

### 3. 意図しない配信
- パターンミスで予期しない配信
- テストの重要性
- ドキュメント化必須

---

## 技術的原理

### ワイルドカード

- `*`（アスタリスク）: **1つの単語**に一致
- `#`（ハッシュ）: **0個以上の単語**に一致

```
ルーティングキー: "api.user.created"

パターン: "api.*.created"      → ✅ マッチ
パターン: "api.#"              → ✅ マッチ
パターン: "#.created"          → ✅ マッチ
パターン: "*.user.*"           → ✅ マッチ
パターン: "api.user.#"         → ✅ マッチ
パターン: "#"                  → ✅ マッチ（すべて）

パターン: "api.*.deleted"      → ❌ マッチしない
パターン: "web.*.created"      → ❌ マッチしない
```

### 命名規則

推奨形式: `<service>.<category>.<action>`

```
例:
auth.user.login
auth.user.logout
payment.order.created
payment.order.completed
notification.email.sent
```

---

## ユースケース

### 1. マイクロサービスイベント

```typescript
// イベント発行
publish('events', 'payment.order.created', orderData);
publish('events', 'inventory.stock.updated', stockData);

// 購読パターン
'payment.#'           → すべての決済イベント
'*.order.*'           → すべての注文イベント
'#.created'           → すべての作成イベント
```

### 2. ログ管理システム

```typescript
// ログ発行
publish('logs', 'api.error.database', errorLog);
publish('logs', 'worker.warning.memory', warningLog);

// 購読パターン
'*.error.*'           → すべてのエラー
'api.#'               → APIのすべてのログ
'#.database'          → データベース関連
```

---

## 実装例（JavaScript & TypeScript）

### JavaScript版

#### emit-topic.js

```javascript
import amqp from 'amqplib';

async function emitTopic(routingKey, message) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'topic_logs';

  await channel.assertExchange(exchange, 'topic', { durable: false });

  channel.publish(exchange, routingKey, Buffer.from(message));
  console.log(`📤 [${routingKey}] ${message}`);

  setTimeout(() => { connection.close(); process.exit(0); }, 500);
}

// 使用例: node emit-topic.js "api.user.login" "User logged in"
const routingKey = process.argv[2] || 'anonymous.info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitTopic(routingKey, message);
```

#### receive-topic.js

```javascript
import amqp from 'amqplib';

async function receiveTopic(patterns) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'topic_logs';

  await channel.assertExchange(exchange, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });

  for (const pattern of patterns) {
    await channel.bindQueue(q.queue, exchange, pattern);
    console.log(`🔗 バインド: ${pattern}`);
  }

  console.log('\n⏳ メッセージを待機中...\n');

  channel.consume(q.queue, (msg) => {
    console.log(`📨 [${msg.fields.routingKey}] ${msg.content.toString()}`);
  }, { noAck: true });
}

// 使用例: node receive-topic.js "*.error.*" "#.critical"
const patterns = process.argv.slice(2);
if (patterns.length === 0) {
  console.log('Usage: node receive-topic.js [pattern1] [pattern2] ...');
  process.exit(1);
}

receiveTopic(patterns);
```

### TypeScript版

#### src/emit-topic.ts

```typescript
import amqp, { Connection, Channel } from 'amqplib';

async function emitTopic(routingKey: string, message: string): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'topic_logs';

  await channel.assertExchange(exchange, 'topic', { durable: false });

  channel.publish(exchange, routingKey, Buffer.from(message));
  console.log(`📤 [${routingKey}] ${message}`);

  setTimeout(() => { connection.close(); process.exit(0); }, 500);
}

const routingKey = process.argv[2] || 'anonymous.info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitTopic(routingKey, message).catch(console.error);
```

#### src/receive-topic.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';

async function receiveTopic(patterns: string[]): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'topic_logs';

  await channel.assertExchange(exchange, 'topic', { durable: false });
  const q = await channel.assertQueue('', { exclusive: true });

  for (const pattern of patterns) {
    await channel.bindQueue(q.queue, exchange, pattern);
    console.log(`🔗 バインド: ${pattern}`);
  }

  console.log('\n⏳ メッセージを待機中...\n');

  channel.consume(q.queue, (msg: ConsumeMessage | null) => {
    if (msg) {
      console.log(`📨 [${msg.fields.routingKey}] ${msg.content.toString()}`);
    }
  }, { noAck: true });
}

const patterns = process.argv.slice(2);
if (patterns.length === 0) {
  console.log('Usage: node dist/receive-topic.js [pattern1] [pattern2] ...');
  process.exit(1);
}

receiveTopic(patterns).catch(console.error);
```

---

## 実行例

```bash
# ターミナル1: すべてのエラーを受信
node receive-topic.js "*.error.*"

# ターミナル2: API関連をすべて受信
node receive-topic.js "api.#"

# ターミナル3: メッセージ送信
node emit-topic.js "api.user.error" "User authentication failed"
node emit-topic.js "api.payment.info" "Payment processed"
node emit-topic.js "worker.task.warning" "High memory usage"
```

---

## 理解度チェック

### 質問

1. `*` と `#` の違いは？
2. `api.#` はどのキーにマッチしますか？
3. Topic Exchange が最も適しているユースケースは？

### 回答

1. `*`: 1つの単語、`#`: 0個以上の単語
2. `api`、`api.user`、`api.user.login` など、`api.`で始まるすべて
3. マイクロサービス、階層的なログ管理、複雑なイベントルーティング

---

## 次のステップ

[Lesson 8: Prefetch Count](08-prefetch.md) で負荷分散の最適化を学びましょう。
