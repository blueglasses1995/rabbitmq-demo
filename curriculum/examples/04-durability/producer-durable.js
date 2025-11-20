const amqp = require('amqplib');

async function sendDurableMessages() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'durable_queue';

    // 永続Queue を宣言
    await channel.assertQueue(queueName, {
      durable: true  // ✅ Queue を永続化
    });

    console.log(`✅ 永続Queue "${queueName}" を作成しました\n`);

    // 10個のメッセージを送信
    for (let i = 1; i <= 10; i++) {
      const message = JSON.stringify({
        id: i,
        type: 'important_task',
        data: `Durable Message ${i}`,
        timestamp: new Date().toISOString()
      });

      channel.sendToQueue(queueName, Buffer.from(message), {
        persistent: true  // ✅ メッセージを永続化
      });

      console.log(`📤 送信: Message ${i} (永続化)`);
    }

    // Publisher Confirms を使った確実な送信
    await channel.waitForConfirms();
    console.log('\n✅ すべてのメッセージがディスクに書き込まれました');

    console.log('\n💡 RabbitMQを再起動しても、これらのメッセージは保持されます');
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

sendDurableMessages();
