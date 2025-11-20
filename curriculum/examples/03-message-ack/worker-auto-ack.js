const amqp = require('amqplib');

const workerId = 'Worker-AUTO-ACK';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（Auto ACK モード）`);
    console.log(`⚠️  警告: メッセージは受信時に即座に削除されます`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    // Auto Ack モード（危険）
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`⚠️  メッセージは既にQueueから削除されています`);

        try {
          if (task.type === 'normal') {
            await new Promise(resolve => setTimeout(resolve, 1000));
            console.log(`✅ [${workerId}] Task ${task.id} 完了\n`);

          } else if (task.type === 'crash') {
            console.log(`💥 [${workerId}] クラッシュ！`);
            console.log(`❌ メッセージは既に削除されているため、損失します！\n`);
            setTimeout(() => process.exit(1), 500);
          }

        } catch (error) {
          console.log(`❌ [${workerId}] エラー: ${error.message}`);
          console.log(`❌ メッセージは既に削除されているため、再試行できません！\n`);
        }
      }
    }, {
      noAck: true  // Auto ACK モード（危険）
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
