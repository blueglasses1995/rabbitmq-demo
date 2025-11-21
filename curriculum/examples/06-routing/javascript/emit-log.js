import amqp from 'amqplib';

async function emitLog(severity, message) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  channel.publish(exchange, severity, Buffer.from(message));
  console.log(`📤 [${severity}] ${message}`);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

const severity = process.argv[2] || 'info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitLog(severity, message);
