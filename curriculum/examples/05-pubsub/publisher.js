const amqp = require('amqplib');

async function publishLogs() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const exchangeName = 'logs';

    await channel.assertExchange(exchangeName, 'fanout', { durable: false });

    const logs = [
      { level: 'INFO', message: 'Application started', service: 'api-server' },
      { level: 'DEBUG', message: 'Database connection established', service: 'api-server' },
      { level: 'WARN', message: 'High memory usage detected', service: 'worker' },
      { level: 'ERROR', message: 'Failed to process payment', service: 'payment-service' },
      { level: 'INFO', message: 'User logged in', service: 'auth-service' },
    ];

    console.log(`📡 Exchange "${exchangeName}" にログを発行中...\n`);

    for (const log of logs) {
      const message = JSON.stringify({ ...log, timestamp: new Date().toISOString() });
      channel.publish(exchangeName, '', Buffer.from(message));
      console.log(`📤 発行: [${log.level}] ${log.message}`);
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log('\n✅ すべてのログを発行しました\n');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

publishLogs();
