# RabbitMQ カリキュラム

## 目次

1. [RabbitMQ 全体像](#rabbitmq-全体像)
2. [技術的原理](#技術的原理)
3. [カリキュラム構成](#カリキュラム構成)

---

## RabbitMQ 全体像

### RabbitMQとは

RabbitMQは、AMQP（Advanced Message Queuing Protocol）を実装したオープンソースのメッセージブローカーです。異なるアプリケーション間で非同期的にメッセージを送受信するための仲介役として機能します。

### 主要コンポーネント

```
Producer → Exchange → Queue → Consumer
```

- **Producer（プロデューサー）**: メッセージを送信するアプリケーション
- **Exchange（交換機）**: メッセージをルーティングする仕組み
- **Queue（キュー）**: メッセージを保存するバッファ
- **Consumer（コンシューマー）**: メッセージを受信・処理するアプリケーション
- **Binding（バインディング）**: ExchangeとQueueを結びつけるルール

### RabbitMQが解決する課題

1. **疎結合なシステム設計**: サービス間の直接的な依存関係を排除
2. **非同期処理**: 時間のかかる処理を後回しにして応答性を向上
3. **負荷分散**: 複数のワーカーにタスクを分散
4. **信頼性の向上**: メッセージの永続化と再送機能
5. **スケーラビリティ**: 処理能力の柔軟な拡張

---

## 技術的原理

### 1. メッセージングモデル

RabbitMQは「ストア・アンド・フォワード」モデルを採用しています：

1. **メッセージの受信**: Producerからメッセージを受け取る
2. **メッセージの保存**: Queueにメッセージを一時保存
3. **メッセージの転送**: Consumerが準備できた時に配信

### 2. Exchange（交換機）の種類

RabbitMQには4種類のExchangeがあります：

| Exchange Type | 動作 | 用途 |
|--------------|------|------|
| **Direct** | ルーティングキーの完全一致でルーティング | 特定の処理への直接配信 |
| **Fanout** | すべての結びついたQueueに配信 | ブロードキャスト、イベント通知 |
| **Topic** | パターンマッチングでルーティング | 柔軟な条件分岐 |
| **Headers** | ヘッダー属性でルーティング | 複雑な条件指定 |

### 3. AMQP プロトコル

AMQP（Advanced Message Queuing Protocol）は以下の特徴を持つバイナリプロトコルです：

- **信頼性**: メッセージの確実な配信を保証
- **相互運用性**: 異なる言語・プラットフォーム間での通信
- **セキュリティ**: 認証・暗号化のサポート
- **トランザクション**: メッセージのアトミックな処理

### 4. メッセージフロー

```
┌──────────┐    ┌──────────┐    ┌───────┐    ┌──────────┐
│ Producer │───→│ Exchange │───→│ Queue │───→│ Consumer │
└──────────┘    └──────────┘    └───────┘    └──────────┘
                      │                            │
                      │         ┌──────────────────┘
                      │         │
                      │         ▼
                      │    ┌─────────┐
                      └───→│ Binding │
                           └─────────┘
```

### 5. 信頼性メカニズム

#### Message Acknowledgment（確認応答）
- **Manual Ack**: Consumerが明示的に処理完了を通知
- **Auto Ack**: メッセージ受信時に自動的に確認
- **Reject/Nack**: メッセージの拒否と再キューイング

#### Persistence（永続化）
- **Durable Queue**: Queueの永続化（再起動後も維持）
- **Persistent Message**: メッセージのディスク保存
- **Lazy Queue**: メモリ使用量を抑えた永続化

#### Delivery Guarantees（配信保証）
- **At-most-once**: 最大1回配信（確認なし）
- **At-least-once**: 最低1回配信（再送あり）
- **Exactly-once**: 正確に1回配信（重複排除）

### 6. パフォーマンス最適化

#### Prefetch Count
Consumerが同時に処理できるメッセージ数を制限することで：
- メモリ使用量を制御
- 負荷の均等分散
- 処理速度の最適化

#### Connection Pooling
接続の再利用により：
- オーバーヘッドの削減
- スループットの向上
- リソース効率化

---

## カリキュラム構成

このカリキュラムは、RabbitMQの各機能を1つずつ段階的に学習する構成になっています。

### 学習の進め方

各レッスンは以下の構成で提供されます：

1. **機能概要**: 機能の説明と目的
2. **メリット**: その機能を使う利点
3. **デメリット**: 注意点や制約
4. **技術的原理**: 内部の動作メカニズム
5. **ユースケース**: 実際の適用例
6. **実装例**: 動作するコードサンプル

### レッスン一覧

#### 基礎編
1. [Lesson 1: 基本的なメッセージキュー](curriculum/lessons/01-basic-queue.md)
   - Producer/Consumer の基本概念
   - シンプルなメッセージ送受信

2. [Lesson 2: Work Queues（タスク分散）](curriculum/lessons/02-work-queues.md)
   - 複数ワーカーによる負荷分散
   - ラウンドロビン配信

3. [Lesson 3: Message Acknowledgment](curriculum/lessons/03-message-ack.md)
   - メッセージ処理の確認応答
   - 信頼性の向上

#### 中級編
4. [Lesson 4: Message Durability（永続化）](curriculum/lessons/04-durability.md)
   - Queueとメッセージの永続化
   - システム障害対策

5. [Lesson 5: Publish/Subscribe（Fanout Exchange）](curriculum/lessons/05-pubsub.md)
   - ブロードキャスト配信
   - イベント駆動アーキテクチャ

6. [Lesson 6: Routing（Direct Exchange）](curriculum/lessons/06-routing.md)
   - ルーティングキーによる配信制御
   - 選択的なメッセージ配信

7. [Lesson 7: Topics（Topic Exchange）](curriculum/lessons/07-topics.md)
   - パターンマッチングルーティング
   - 柔軟なメッセージ配信

#### 上級編
8. [Lesson 8: Prefetch Count](curriculum/lessons/08-prefetch.md)
   - QoS（Quality of Service）設定
   - 負荷の均等分散

9. [Lesson 9: Dead Letter Exchange](curriculum/lessons/09-dead-letter.md)
   - 失敗メッセージの処理
   - エラーハンドリング

10. [Lesson 10: RPC Pattern](curriculum/lessons/10-rpc.md)
    - リクエスト/レスポンスパターン
    - 同期的な通信の実現

---

## 環境セットアップ

### 前提条件
- Node.js 14以上
- Docker（RabbitMQをコンテナで実行する場合）

### RabbitMQのインストール

#### Dockerを使用する場合（推奨）
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

#### 管理画面へのアクセス
- URL: http://localhost:15672
- ユーザー名: guest
- パスワード: guest

### プロジェクトセットアップ
```bash
npm init -y
npm install amqplib
```

---

## 学習リソース

- [RabbitMQ 公式ドキュメント](https://www.rabbitmq.com/documentation.html)
- [AMQP 0-9-1 仕様](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf)
- [RabbitMQ Tutorials](https://www.rabbitmq.com/getstarted.html)

---

## 次のステップ

まずは [Lesson 1: 基本的なメッセージキュー](curriculum/lessons/01-basic-queue.md) から始めましょう！
