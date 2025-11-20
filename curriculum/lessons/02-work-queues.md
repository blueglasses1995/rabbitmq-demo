# Lesson 2: Work Queues（タスク分散）

## 概要

Work Queuesは、複数のワーカー（Consumer）間でタスクを分散する仕組みです。時間のかかる処理を複数のワーカーで並列実行することで、スループットを向上させます。

```
                    ┌─→ Worker 1
Producer → Queue ───┼─→ Worker 2
                    └─→ Worker 3
```

RabbitMQはデフォルトで**ラウンドロビン方式**でメッセージを配信します。

---

## メリット

### 1. 水平スケーラビリティ
- ワーカー数を増やすだけで処理能力が向上
- インフラの変更不要
- 動的にワーカーを追加・削除可能

**例**: ピーク時にワーカーを5台→10台に増やして処理能力を倍増

### 2. 高可用性
- 1つのワーカーが停止しても他のワーカーが処理継続
- システム全体の障害に強い
- ダウンタイムの最小化

### 3. 負荷の平準化
- タスクを複数のワーカーに分散
- サーバーへの負荷集中を回避
- リソースの効率的な利用

### 4. 処理速度の向上
- 並列処理によるスループット向上
- タスク完了時間の短縮
- ユーザー体験の改善

---

## デメリット

### 1. 不均等な負荷分散（デフォルト設定）
- ラウンドロビンは**メッセージ数**で分散
- 処理時間は考慮されない
- タスクの重さが異なると偏りが発生

**問題例**:
```
Worker 1: [軽いタスク][軽いタスク][軽いタスク][軽いタスク] → 4秒で完了
Worker 2: [重いタスク][重いタスク] → 10秒で完了
```

**解決策**: Prefetch設定（Lesson 8で詳細）

### 2. タスクの順序保証なし
- 複数ワーカーが並列処理するため順序が入れ替わる
- 先に送信したタスクが後で完了する可能性
- 順序が重要な場合は別の設計が必要

### 3. 結果の集約が必要
- 各ワーカーの処理結果を統合する仕組みが必要
- 実装の複雑性が増加
- 別のQueue や Database での管理が必要

### 4. リソース管理の複雑化
- ワーカー数の適切な設定が必要
- 多すぎると無駄、少なすぎると処理遅延
- モニタリングとチューニングが重要

---

## 技術的原理

### ラウンドロビン配信

RabbitMQは、デフォルトでメッセージを**順番に各Consumerに配信**します。

```
メッセージ送信順:
Task 1, Task 2, Task 3, Task 4, Task 5, Task 6

配信パターン（2ワーカーの場合）:
Worker 1: Task 1, Task 3, Task 5
Worker 2: Task 2, Task 4, Task 6
```

#### 配信のタイミング

**重要**: メッセージは**Consumer接続時**に事前配信されます

```javascript
// Worker 1が起動（Queueに6個のタスクがある）
channel.consume(queueName, callback)
  ↓
即座に Task 1, 3, 5 を Worker 1 に配信
  ↓
Worker 1 のメモリバッファに保持
  ↓
実際の処理は後から実行
```

これが**不均等な負荷分散**の原因になります。

### メッセージの配信フロー

```
1. Producer が 10個のタスクを送信
   Queue: [T1][T2][T3][T4][T5][T6][T7][T8][T9][T10]

2. Worker 1 が接続
   Queue: [T2][T4][T6][T8][T10]
   Worker 1: [T1][T3][T5][T7][T9] ← 5個割り当て

3. Worker 2 が接続
   Queue: []
   Worker 1: [T1][T3][T5][T7][T9]
   Worker 2: [T2][T4][T6][T8][T10] ← 5個割り当て

4. 各Workerが順次処理
   ※ 処理時間はタスクによって異なる
```

### フェアディスパッチの問題

```javascript
// ラウンドロビンの問題例

// Task の処理時間
Task 1: 1秒   → Worker 1
Task 2: 10秒  → Worker 2
Task 3: 1秒   → Worker 1
Task 4: 10秒  → Worker 2

// 結果
Worker 1: 2秒で完了（Task 1 + Task 3）
Worker 2: 20秒で完了（Task 2 + Task 4）

// Worker 1 は18秒間アイドル状態！
```

**解決策**: `prefetch` 設定で「処理完了したら次のタスク」方式に変更（Lesson 8）

### タスクの属性

実際の運用では、タスクに属性を付けて管理します：

```javascript
const task = {
  id: 'task_001',
  type: 'image_processing',
  priority: 'high',
  data: {
    imageUrl: 'https://example.com/image.jpg',
    operations: ['resize', 'compress']
  },
  metadata: {
    createdAt: '2024-01-01T00:00:00Z',
    retryCount: 0,
    timeout: 30000
  }
}
```

---

## ユースケース

### 1. 画像処理サービス

**シナリオ**: ユーザーがアップロードした画像を複数サイズにリサイズ

```
Upload API
   ↓
[Image Queue]
   ↓
┌─────────┬─────────┬─────────┐
│Worker 1 │Worker 2 │Worker 3 │
└─────────┴─────────┴─────────┘
   ↓          ↓          ↓
Resize    Compress   Thumbnail
```

**メリット**:
- アップロード完了後すぐにレスポンス
- 複数画像を並列処理
- ピーク時にワーカー追加で対応

### 2. メール配信システム

**シナリオ**: ニュースレターの大量配信

```
Admin Panel
   ↓
[Email Queue] (10,000通)
   ↓
┌─────────┬─────────┬─────────┬─────────┬─────────┐
│Worker 1 │Worker 2 │Worker 3 │Worker 4 │Worker 5 │
└─────────┴─────────┴─────────┴─────────┴─────────┘
   ↓
SMTP Server
```

**メリット**:
- 大量のメールを短時間で配信
- メール送信失敗時の再送が容易
- 配信速度を動的に調整可能

### 3. データ分析パイプライン

**シナリオ**: ログファイルの解析

```
Log Collector
   ↓
[Analysis Queue]
   ↓
┌─────────┬─────────┬─────────┐
│Worker 1 │Worker 2 │Worker 3 │
│Parse    │Parse    │Parse    │
│Analyze  │Analyze  │Analyze  │
│Aggregate│Aggregate│Aggregate│
└─────────┴─────────┴─────────┘
   ↓
Database
```

**メリット**:
- 大量のログを並列解析
- 処理時間の大幅短縮
- リソースの効率的な利用

### 4. Webスクレイピング

**シナリオ**: 複数サイトからのデータ収集

```
Scheduler
   ↓
[Scraping Queue]
   ↓
┌─────────┬─────────┬─────────┐
│Worker 1 │Worker 2 │Worker 3 │
│Site A   │Site B   │Site C   │
└─────────┴─────────┴─────────┘
   ↓
Data Store
```

**メリット**:
- 複数サイトを同時スクレイピング
- レート制限を回避
- エラー時の再試行が容易

### 5. 動画エンコーディング

**シナリオ**: アップロードされた動画の変換

```
Upload Service
   ↓
[Encoding Queue]
   ↓
┌──────────┬──────────┬──────────┐
│Worker 1  │Worker 2  │Worker 3  │
│1080p変換 │720p変換  │480p変換  │
└──────────┴──────────┴──────────┘
   ↓
CDN
```

**メリット**:
- 重い処理を並列実行
- 複数解像度を同時生成
- GPU搭載マシンに処理を振り分け

---

## 実装例

### ディレクトリ構成

```
curriculum/examples/02-work-queues/
├── package.json
├── producer.js         # タスク送信
├── worker.js           # タスク処理（複数起動）
└── worker-slow.js      # 遅いワーカー（ラウンドロビンの問題デモ）
```

### package.json

```json
{
  "name": "02-work-queues",
  "version": "1.0.0",
  "description": "RabbitMQ Work Queues Example",
  "main": "index.js",
  "scripts": {
    "producer": "node producer.js",
    "worker": "node worker.js",
    "worker-slow": "node worker-slow.js"
  },
  "dependencies": {
    "amqplib": "^0.10.3"
  }
}
```

### producer.js - タスク送信

```javascript
const amqp = require('amqplib');

async function sendTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, {
      durable: false
    });

    // タスクの重さを変えて送信（デモ用）
    const tasks = [
      { id: 1, name: 'Task 1', processingTime: 1000 },   // 1秒
      { id: 2, name: 'Task 2', processingTime: 5000 },   // 5秒
      { id: 3, name: 'Task 3', processingTime: 1000 },   // 1秒
      { id: 4, name: 'Task 4', processingTime: 5000 },   // 5秒
      { id: 5, name: 'Task 5', processingTime: 1000 },   // 1秒
      { id: 6, name: 'Task 6', processingTime: 5000 },   // 5秒
      { id: 7, name: 'Task 7', processingTime: 1000 },   // 1秒
      { id: 8, name: 'Task 8', processingTime: 5000 },   // 5秒
    ];

    console.log('📤 タスクを送信中...\n');

    for (const task of tasks) {
      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));

      console.log(`✓ 送信: ${task.name} (処理時間: ${task.processingTime}ms)`);
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

// 大量タスク送信のデモ
async function sendManyTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log('📤 100個のタスクを送信中...\n');

    for (let i = 1; i <= 100; i++) {
      const task = {
        id: i,
        name: `Task ${i}`,
        processingTime: Math.floor(Math.random() * 3000) + 1000, // 1-4秒
        data: {
          timestamp: new Date().toISOString(),
          payload: `Data for task ${i}`
        }
      };

      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));

      if (i % 10 === 0) {
        console.log(`✓ ${i}個のタスクを送信完了`);
      }
    }

    console.log('\n✅ すべてのタスクを送信しました');

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
const mode = process.argv[2] || 'default';

if (mode === 'many') {
  sendManyTasks();
} else {
  sendTasks();
}
```

### worker.js - タスク処理ワーカー

```javascript
const amqp = require('amqplib');

// ワーカーIDを生成（複数ワーカーを識別するため）
const workerId = process.argv[2] || `Worker-${Math.floor(Math.random() * 1000)}`;

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, {
      durable: false
    });

    console.log(`🔧 [${workerId}] 起動しました`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    // タスクを受信して処理
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: ${task.name}`);
        console.log(`   処理時間: ${task.processingTime}ms`);

        // タスクを処理（シミュレーション）
        const startTime = Date.now();
        await new Promise(resolve => setTimeout(resolve, task.processingTime));
        const endTime = Date.now();

        console.log(`✅ [${workerId}] ${task.name} 完了 (実際の処理時間: ${endTime - startTime}ms)\n`);
      }
    }, {
      noAck: true  // 自動確認（Lesson 3で改善）
    });

    // 統計情報を定期的に表示
    let processedCount = 0;
    setInterval(() => {
      console.log(`📊 [${workerId}] 処理済みタスク数: ${processedCount}`);
    }, 10000);

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
```

### worker-slow.js - 遅いワーカー（デモ用）

```javascript
const amqp = require('amqplib');

const workerId = 'Worker-SLOW';

async function startSlowWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, {
      durable: false
    });

    console.log(`🐌 [${workerId}] 起動しました（意図的に遅いワーカー）`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: ${task.name}`);

        // 意図的に10倍遅く処理
        const slowProcessingTime = task.processingTime * 10;
        console.log(`   処理時間: ${slowProcessingTime}ms (通常の10倍)`);

        const startTime = Date.now();
        await new Promise(resolve => setTimeout(resolve, slowProcessingTime));
        const endTime = Date.now();

        console.log(`✅ [${workerId}] ${task.name} 完了 (${endTime - startTime}ms)\n`);
      }
    }, {
      noAck: true
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startSlowWorker();
```

---

## 実行方法

### 1. セットアップ

```bash
cd curriculum/examples/02-work-queues
npm install
```

### 2. RabbitMQの起動

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

### 3. 基本的な実行（2ワーカー）

**ターミナル1 - Worker 1起動:**
```bash
node worker.js "Worker-1"
```

**ターミナル2 - Worker 2起動:**
```bash
node worker.js "Worker-2"
```

**ターミナル3 - タスク送信:**
```bash
npm run producer
```

### 4. ラウンドロビンの問題デモ

**ターミナル1 - 通常のワーカー:**
```bash
node worker.js "Worker-FAST"
```

**ターミナル2 - 遅いワーカー:**
```bash
npm run worker-slow
```

**ターミナル3 - タスク送信:**
```bash
npm run producer
```

### 5. 大量タスクの並列処理

**3つのワーカーを起動:**
```bash
# ターミナル1
node worker.js "Worker-1"

# ターミナル2
node worker.js "Worker-2"

# ターミナル3
node worker.js "Worker-3"
```

**100個のタスクを送信:**
```bash
# ターミナル4
node producer.js many
```

---

## 実行結果例

### 2ワーカーでのラウンドロビン

**Producer:**
```
📤 タスクを送信中...

✓ 送信: Task 1 (処理時間: 1000ms)
✓ 送信: Task 2 (処理時間: 5000ms)
✓ 送信: Task 3 (処理時間: 1000ms)
✓ 送信: Task 4 (処理時間: 5000ms)
✓ 送信: Task 5 (処理時間: 1000ms)
✓ 送信: Task 6 (処理時間: 5000ms)
✓ 送信: Task 7 (処理時間: 1000ms)
✓ 送信: Task 8 (処理時間: 5000ms)

✅ 8個のタスクを送信しました
```

**Worker 1（奇数タスク）:**
```
🔧 [Worker-1] 起動しました
⏳ [Worker-1] タスクを待機中...

📋 [Worker-1] タスク受信: Task 1
   処理時間: 1000ms
✅ [Worker-1] Task 1 完了 (実際の処理時間: 1001ms)

📋 [Worker-1] タスク受信: Task 3
   処理時間: 1000ms
✅ [Worker-1] Task 3 完了 (実際の処理時間: 1002ms)

📋 [Worker-1] タスク受信: Task 5
   処理時間: 1000ms
✅ [Worker-1] Task 5 完了 (実際の処理時間: 1001ms)

📋 [Worker-1] タスク受信: Task 7
   処理時間: 1000ms
✅ [Worker-1] Task 7 完了 (実際の処理時間: 1003ms)

→ 合計: 約4秒で完了
```

**Worker 2（偶数タスク）:**
```
🔧 [Worker-2] 起動しました
⏳ [Worker-2] タスクを待機中...

📋 [Worker-2] タスク受信: Task 2
   処理時間: 5000ms
✅ [Worker-2] Task 2 完了 (実際の処理時間: 5002ms)

📋 [Worker-2] タスク受信: Task 4
   処理時間: 5000ms
✅ [Worker-2] Task 4 完了 (実際の処理時間: 5001ms)

📋 [Worker-2] タスク受信: Task 6
   処理時間: 5000ms
✅ [Worker-2] Task 6 完了 (実際の処理時間: 5003ms)

📋 [Worker-2] タスク受信: Task 8
   処理時間: 5000ms
✅ [Worker-2] Task 8 完了 (実際の処理時間: 5002ms)

→ 合計: 約20秒で完了
```

**問題点**: Worker 1は4秒で完了したが、Worker 2は20秒かかった！
Worker 1は16秒間アイドル状態で無駄になっている。

---

## 理解度チェック

### 質問

1. Work Queuesのデフォルト配信方式は何ですか？
2. なぜWorker間で負荷が偏ることがあるのですか？
3. Work Queuesに適したユースケースは？
4. ワーカー数を増やせば必ず高速化しますか？

### 回答

1. **ラウンドロビン方式**：メッセージを順番に各Workerに配信
2. **処理時間を考慮しないため**：タスクの重さが異なると、軽いタスクを担当したWorkerが先に終わる
3. **時間のかかる処理の並列化**：画像処理、メール配信、データ分析など
4. **いいえ**：タスクの性質、ネットワーク、メモリなどの制約があります。適切な数を見極める必要があります

---

## 次のステップ

Work Queuesの基本を理解できたら、次は処理の信頼性を高める [Lesson 3: Message Acknowledgment](03-message-ack.md) に進みましょう。

### 現在の問題点

1. ❌ Workerがクラッシュするとタスクが失われる → **Lesson 3で解決**
2. ❌ 負荷分散が最適化されていない → **Lesson 8で解決**
3. ❌ サーバー再起動でタスクが消える → **Lesson 4で解決**
4. ❌ タスクの優先度を付けられない → 応用編で解決

### 学んだこと

✅ 複数Workerでタスクを分散できる
✅ ラウンドロビン配信の仕組み
✅ Work Queuesの利点と制約
✅ 実際の運用での課題
