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
