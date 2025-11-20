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
