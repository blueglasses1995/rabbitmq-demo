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
