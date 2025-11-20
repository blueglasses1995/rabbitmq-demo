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
