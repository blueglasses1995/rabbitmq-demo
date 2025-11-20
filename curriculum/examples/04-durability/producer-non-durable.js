const amqp = require('amqplib');

async function sendNonDurableMessages() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'non_durable_queue';

    // 非永続Queue を宣言
    await channel.assertQueue(queueName, {
      durable: false  // ❌ Queue は非永続化
    });

    console.log(`⚠️  非永続Queue "${queueName}" を作成しました\n`);

    // 10個のメッセージを送信
    for (let i = 1; i <= 10; i++) {
      const message = JSON.stringify({
        id: i,
        type: 'temporary_task',
        data: `Non-Durable Message ${i}`,
        timestamp: new Date().toISOString()
      });

      channel.sendToQueue(queueName, Buffer.from(message), {
        persistent: false  // ❌ メッセージは非永続化
      });

      console.log(`📤 送信: Message ${i} (非永続化)`);
    }

    console.log('\n⚠️  これらのメッセージはRabbitMQ再起動で失われます');
    console.log('   テスト: docker restart rabbitmq\n');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

sendNonDurableMessages();
