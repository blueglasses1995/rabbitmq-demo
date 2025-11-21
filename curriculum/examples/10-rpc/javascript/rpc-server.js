import amqp from 'amqplib';

async function startRPCServer() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queue = 'rpc_queue';

  await channel.assertQueue(queue, { durable: false });
  channel.prefetch(1);

  console.log('🔧 RPC Server起動\n');

  channel.consume(queue, async (msg) => {
    const n = parseInt(msg.content.toString());
    console.log(`📋 リクエスト受信: fib(${n})`);

    const result = fibonacci(n);
    console.log(`✅ レスポンス送信: ${result}\n`);

    channel.sendToQueue(
      msg.properties.replyTo,
      Buffer.from(result.toString()),
      { correlationId: msg.properties.correlationId }
    );

    channel.ack(msg);
  }, { noAck: false });
}

function fibonacci(n) {
  if (n === 0) return 0;
  if (n === 1) return 1;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

startRPCServer();
