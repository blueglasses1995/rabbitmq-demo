import amqp, { Connection, Channel } from 'amqplib';

type Severity = 'error' | 'warning' | 'info' | 'debug';

async function emitLog(severity: Severity, message: string): Promise<void> {
  const connection: Connection = await amqp.connect('amqp://localhost');
  const channel: Channel = await connection.createChannel();
  const exchange = 'direct_logs';

  await channel.assertExchange(exchange, 'direct', { durable: false });

  channel.publish(exchange, severity, Buffer.from(message));
  console.log(`📤 [${severity}] ${message}`);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

const severity = (process.argv[2] as Severity) || 'info';
const message = process.argv.slice(3).join(' ') || 'Hello World!';

emitLog(severity, message).catch(console.error);
