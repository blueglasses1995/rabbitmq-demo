import amqp from 'amqplib';
import { randomUUID } from 'crypto';

async function callRPC(n) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  const q = await channel.assertQueue('', { exclusive: true });
  const correlationId = randomUUID();

  console.log(`📤 リクエスト送信: fib(${n})`);

  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      connection.close();
      reject(new Error('RPC timeout'));
    }, 5000);

    channel.consume(q.queue, (msg) => {
      if (msg.properties.correlationId === correlationId) {
        clearTimeout(timeout);
        const result = parseInt(msg.content.toString());

        console.log(`📨 レスポンス受信: ${result}\n`);

        resolve(result);
        setTimeout(() => {
          connection.close();
          process.exit(0);
        }, 500);
      }
    }, { noAck: true });

    channel.sendToQueue('rpc_queue', Buffer.from(n.toString()), {
      correlationId: correlationId,
      replyTo: q.queue
    });
  });
}

const n = parseInt(process.argv[2]) || 10;
callRPC(n).catch(console.error);
