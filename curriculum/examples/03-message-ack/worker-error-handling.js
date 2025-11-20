const amqp = require('amqplib');

const workerId = 'Worker-ERROR-HANDLER';

async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log(`🔧 [${workerId}] 起動（高度なエラーハンドリング）\n`);

    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const task = JSON.parse(msg.content.toString());

        // 再配信回数をチェック
        const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;
        const MAX_RETRIES = 3;

        console.log(`📋 [${workerId}] タスク受信: Task ${task.id}`);
        console.log(`   Redelivered: ${msg.fields.redelivered}`);
        console.log(`   Retry Count: ${retryCount}/${MAX_RETRIES}`);

        try {
          // タスク処理
          if (task.type === 'error') {
            throw new Error('一時的なエラー');
          }

          await new Promise(resolve => setTimeout(resolve, 1000));
          channel.ack(msg);
          console.log(`✅ [${workerId}] Task ${task.id} 完了\n`);

        } catch (error) {
          console.log(`❌ [${workerId}] エラー: ${error.message}`);

          if (retryCount < MAX_RETRIES) {
            // リトライ
            console.log(`🔄 [${workerId}] リトライします (${retryCount + 1}/${MAX_RETRIES})\n`);

            // 現在のメッセージを拒否
            channel.nack(msg, false, false);

            // リトライカウントを増やして再送信
            const headers = {
              'x-retry-count': retryCount + 1
            };

            channel.sendToQueue(queueName, msg.content, {
              headers: headers
            });

          } else {
            // 最大リトライ回数超過
            console.log(`⛔ [${workerId}] 最大リトライ回数超過 - メッセージを破棄\n`);
            channel.nack(msg, false, false);

            // Dead Letter Queue に送信するのが理想的（Lesson 9で実装）
          }
        }
      }
    }, {
      noAck: false
    });

  } catch (error) {
    console.error(`✗ [${workerId}] エラー:`, error.message);
    process.exit(1);
  }
}

startWorker();
