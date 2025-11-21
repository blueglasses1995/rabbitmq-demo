import amqp from 'amqplib';

async function emitTopic(routingKey, message) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const exchange = 'topic_logs';

  await channel.assertExchange(exchange, 'topic', { durable: false });
  channel.publish(exchange, routingKey, Buffer.from(message));
  console.log(`📤 [${routingKey}] ${message}`);

  setTimeout(() => { connection.close(); process.exit(0); }, 500);
}

const routingKey = process.argv[2] || 'anonymous.info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitTopic(routingKey, message);
