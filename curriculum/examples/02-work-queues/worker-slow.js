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
