const amqp = require('amqplib');

const workerId = process.argv[2] || 'Worker-ACK';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（Manual ACK モード）`);
    console.log(`⏳ [${workerId}] タスクを待機中...\n`);

    // Manual Ack モード
    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`   Type: ${task.type}`);
        console.log(`   Redelivered: ${msg.fields.redelivered}`);

        try {
          // タスクの種類に応じて処理
          if (task.type === 'normal') {
            // 通常処理
            await new Promise(resolve => setTimeout(resolve, 1000));

            // 処理成功 → ACK
            channel.ack(msg);
            console.log(`✅ [${workerId}] Task ${task.id} 完了（ACK送信）\n`);

          } else if (task.type === 'error') {
            // エラーをシミュレート
            throw new Error('処理エラー');

          } else if (task.type === 'crash') {
            // クラッシュをシミュレート
            console.log(`💥 [${workerId}] クラッシュをシミュレート！`);
            console.log(`⚠️  ACKを送信せずにプロセス終了...\n`);

            // ACKを送信せずに終了
            // → RabbitMQは自動的に再キューイング
            setTimeout(() => process.exit(1), 500);
          }

        } catch (error) {
          console.log(`❌ [${workerId}] Task ${task.id} エラー: ${error.message}`);
          console.log(`🔄 [${workerId}] 再キューイング（Nack送信）\n`);

          // エラー時は再キュー
          channel.nack(msg, false, true);
          // パラメータ: (msg, multiple=false, requeue=true)
        }
      }
    }, {
      noAck: false  // Manual ACK モード
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
