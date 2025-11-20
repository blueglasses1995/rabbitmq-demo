const amqp = require('amqplib');

async function subscribeToLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    await channel.assertExchange(exchangeName, 'fanout', { durable: false });
    const q = await channel.assertQueue('', { exclusive: true });

    console.log('🖥️  [Console Subscriber] 起動しました');
    console.log(`   Queue: ${q.queue}\n`);

    await channel.bindQueue(q.queue, exchangeName, '');

    channel.consume(q.queue, (msg) => {
      if (msg !== null) {
        const log = JSON.parse(msg.content.toString());
        const colors = {
          'INFO': '\x1b[32m', 'DEBUG': '\x1b[36m',
          'WARN': '\x1b[33m', 'ERROR': '\x1b[31m'
        };
        const color = colors[log.level] || '';
        const reset = '\x1b[0m';

        console.log(`${color}🖥️  [${log.level}] [${log.service}] ${log.message}${reset}`);
        channel.ack(msg);
      }
    }, { noAck: false });

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

subscribeToLogs();
