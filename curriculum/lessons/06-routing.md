# Lesson 6: Routing（Direct Exchange）

## 概要

Routing パターンは、**ルーティングキー**を使ってメッセージを選択的に配信する仕組みです。**Direct Exchange** を使用して、特定の条件に合致するConsumerにのみメッセージを送ります。

```
                    ┌─→ Queue A (routing key: error) → Consumer A
Producer → Direct Exchange ┼─→ Queue B (routing key: info) → Consumer B
                    └─→ Queue C (routing key: warning) → Consumer C
```

---

## メリット

### 1. 選択的な配信
- 必要なConsumerにのみ配信
- 不要なメッセージ処理を削減
- リソースの効率化

### 2. 明確な責任分離
- ログレベル、優先度、カテゴリ別の処理
- 各Consumerが特定のタスクに特化
- 保守性の向上

### 3. 柔軟な拡張
- 新しいルーティングキーの追加が容易
- 既存システムへの影響最小
- 動的なルーティング設定

### 4. パフォーマンス向上
- Fanoutより効率的
- 必要なQueueにのみメッセージをコピー
- ネットワーク・メモリの節約

---

## デメリット

### 1. 完全一致のみ
- ルーティングキーの**完全一致**が必要
- パターンマッチング不可（→ Topic Exchangeで解決）
- 柔軟性に欠ける

### 2. 設定の複雑化
- ルーティングキーの管理が必要
- バインディングの設定ミスのリスク
- ドキュメント化が重要

### 3. デバッグの難易度
- メッセージがどこに配信されたか追跡が必要
- ルーティングミスの発見が困難
- ログとモニタリングが必須

---

## 技術的原理

### Direct Exchange の動作

```javascript
// Exchange宣言
await channel.assertExchange('direct_logs', 'direct', { durable: false });

// バインディング（ルーティングキー指定）
await channel.bindQueue(queueName, 'direct_logs', 'error');

// メッセージ発行（ルーティングキー指定）
channel.publish('direct_logs', 'error', Buffer.from(message));
//                              ↑
//                        完全一致が必要
```

### ルーティングのルール

1. **完全一致**: バインディングキーとルーティングキーが完全に一致
2. **複数バインディング**: 1つのQueueに複数のキーをバインド可能
3. **複数Queue**: 同じキーを複数のQueueにバインド可能

```
Queue A: ['error', 'warning']  // error と warning を受信
Queue B: ['error']              // error のみ受信
Queue C: ['info', 'debug']      // info と debug を受信
```

### メッセージフロー

```
Publisher:
  publish('direct_logs', 'error', msg)
    ↓
Direct Exchange:
  routing key = 'error' を探す
    ↓
  Queue A (bound to 'error') ✅
  Queue B (bound to 'error') ✅
  Queue C (bound to 'info')  ❌
    ↓
Queue A と Queue B にコピー
```

---

## ユースケース

### 1. ログレベル別処理

```
ERROR   → Error Handler (即座に対応)
WARNING → Monitoring System (監視)
INFO    → Log Archiver (保存のみ)
DEBUG   → Development Console (開発環境のみ)
```

### 2. 優先度別タスク処理

```
high     → Priority Workers (優先処理)
medium   → Standard Workers (通常処理)
low      → Background Workers (バックグラウンド)
```

### 3. 地域別配信

```
jp → Japan Servers
us → US Servers
eu → Europe Servers
```

### 4. 部署別通知

```
sales     → Sales Team Queue
engineering → Engineering Team Queue
marketing   → Marketing Team Queue
```

---

## 実装例（JavaScript & TypeScript）

### ディレクトリ構成

```
curriculum/examples/06-routing/
├── javascript/
│   ├── package.json
│   ├── emit-log.js          # ログ発行
│   ├── receive-error.js     # ERRORのみ受信
│   └── receive-all.js       # すべて受信
└── typescript/
    ├── package.json
    ├── tsconfig.json
    ├── src/
    │   ├── emit-log.ts
    │   ├── receive-error.ts
    │   └── receive-all.ts
    └── dist/
```

### JavaScript版

#### package.json

```json
{
  "name": "06-routing-js",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "emit": "node emit-log.js",
    "receive-error": "node receive-error.js",
    "receive-all": "node receive-all.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

#### emit-log.js

```javascript
import amqp from 'amqplib';

async function emitLog(severity, message) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  channel.publish(exchange, severity, Buffer.from(message));
  console.log(`📤 [${severity}] ${message}`);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

// コマンドライン引数: node emit-log.js error "Error occurred"
const severity = process.argv[2] || 'info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitLog(severity, message);
```

#### receive-error.js

```javascript
import amqp from 'amqplib';

async function receiveErrors() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  const q = await channel.assertQueue('', { exclusive: true });

  // ERRORのみバインド
  await channel.bindQueue(q.queue, exchange, 'error');

  console.log('🚨 [Error Handler] ERRORログのみ受信します\n');

  channel.consume(q.queue, (msg) => {
    console.log(`🚨 [ERROR] ${msg.content.toString()}`);
  }, { noAck: true });
}

receiveErrors();
```

#### receive-all.js

```javascript
import amqp from 'amqplib';

async function receiveAll() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  const q = await channel.assertQueue('', { exclusive: true });

  // 複数のルーティングキーをバインド
  const severities = ['error', 'warning', 'info', 'debug'];

  for (const severity of severities) {
    await channel.bindQueue(q.queue, exchange, severity);
  }

  console.log('📝 [All Logs] すべてのログを受信します\n');

  channel.consume(q.queue, (msg) => {
    const severity = msg.fields.routingKey;
    const content = msg.content.toString();
    console.log(`📝 [${severity.toUpperCase()}] ${content}`);
  }, { noAck: true });
}

receiveAll();
```

### TypeScript版

#### package.json

```json
{
  "name": "06-routing-ts",
  "version": "1.0.0",
  "scripts": {
    "build": "tsc",
    "emit": "npm run build && node dist/emit-log.js",
    "receive-error": "npm run build && node dist/receive-error.js",
    "receive-all": "npm run build && node dist/receive-all.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  },
  "devDependencies": {
    "@types/amqplib": "^0.10.1",
    "@types/node": "^20.0.0",
    "typescript": "^5.0.0"
  }
}
```

#### tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules"]
}
```

#### src/emit-log.ts

```typescript
import amqp, { Connection, Channel } from 'amqplib';

type Severity = 'error' | 'warning' | 'info' | 'debug';

async function emitLog(severity: Severity, message: string): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  channel.publish(exchange, severity, Buffer.from(message));
  console.log(`📤 [${severity}] ${message}`);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

const severity = (process.argv[2] as Severity) || 'info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitLog(severity, message).catch(console.error);
```

#### src/receive-error.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';

async function receiveErrors(): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  const q = await channel.assertQueue('', { exclusive: true });

  await channel.bindQueue(q.queue, exchange, 'error');

  console.log('🚨 [Error Handler] ERRORログのみ受信します\n');

  channel.consume(q.queue, (msg: ConsumeMessage | null) => {
    if (msg) {
      console.log(`🚨 [ERROR] ${msg.content.toString()}`);
    }
  }, { noAck: true });
}

receiveErrors().catch(console.error);
```

#### src/receive-all.ts

```typescript
import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';

type Severity = 'error' | 'warning' | 'info' | 'debug';

async function receiveAll(): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  const q = await channel.assertQueue('', { exclusive: true });

  const severities: Severity[] = ['error', 'warning', 'info', 'debug'];

  for (const severity of severities) {
    await channel.bindQueue(q.queue, exchange, severity);
  }

  console.log('📝 [All Logs] すべてのログを受信します\n');

  channel.consume(q.queue, (msg: ConsumeMessage | null) => {
    if (msg) {
      const severity = msg.fields.routingKey;
      const content = msg.content.toString();
      console.log(`📝 [${severity.toUpperCase()}] ${content}`);
    }
  }, { noAck: true });
}

receiveAll().catch(console.error);
```

---

## 実行方法

### JavaScript版

```bash
cd curriculum/examples/06-routing/javascript

# Receiver起動
npm run receive-error  # ターミナル1
npm run receive-all    # ターミナル2

# ログ発行
node emit-log.js error "Database connection failed"
node emit-log.js warning "High memory usage"
node emit-log.js info "User logged in"
```

### TypeScript版

```bash
cd curriculum/examples/06-routing/typescript

npm install
npm run build

# Receiver起動
npm run receive-error  # ターミナル1
npm run receive-all    # ターミナル2

# ログ発行
node dist/emit-log.js error "Payment processing error"
node dist/emit-log.js info "Application started"
```

---

## 理解度チェック

### 質問

1. Direct Exchange と Fanout Exchange の違いは？
2. 1つのQueueに複数のルーティングキーをバインドできますか？
3. ルーティングキーが一致しない場合、メッセージはどうなりますか？
4. TypeScript版のメリットは？

### 回答

1. **Direct**: ルーティングキーで選択的配信、**Fanout**: すべてに配信
2. **はい**。複数回 `bindQueue` を呼び出すことで可能
3. **破棄されます**（どのQueueにも配信されない）
4. **型安全性**、コンパイル時エラー検出、IDE補完の向上

---

## 次のステップ

[Lesson 7: Topics（Topic Exchange）](07-topics.md) でパターンマッチングによる柔軟なルーティングを学びましょう。

### 学んだこと

✅ Direct Exchange で選択的配信
✅ ルーティングキーの活用
✅ 複数バインディング
✅ JavaScript/TypeScript両対応の実装
