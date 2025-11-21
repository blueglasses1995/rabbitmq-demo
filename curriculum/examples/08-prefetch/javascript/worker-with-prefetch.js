import amqp from 'amqplib';

async function startWorker(workerId, prefetchCount) {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queue = 'task_queue';

  await channel.assertQueue(queue, { durable: true });
  channel.prefetch(prefetchCount);

  console.log(`🔧 [${workerId}] 起動 (prefetch: ${prefetchCount})\n`);

  channel.consume(queue, async (msg) => {
    const task = JSON.parse(msg.content.toString());
    console.log(`📋 [${workerId}] 受信: Task ${task.id} (${task.weight}秒)`);

    await new Promise(resolve => setTimeout(resolve, task.weight * 1000));

    console.log(`✅ [${workerId}] 完了: Task ${task.id}\n`);
    channel.ack(msg);
  }, { noAck: false });
}

const workerId = process.argv[2] || 'Worker-1';
const prefetchCount = parseInt(process.argv[3]) || 1;

startWorker(workerId, prefetchCount);
