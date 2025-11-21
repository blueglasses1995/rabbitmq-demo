import amqp from 'amqplib';

async function setupDLX() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  await channel.assertExchange('dlx', 'direct', { durable: true });
  await channel.assertQueue('dead_letter_queue', { durable: true });
  await channel.bindQueue('dead_letter_queue', 'dlx', 'dead_letter');

  await channel.assertQueue('main_queue', {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'dlx',
      'x-dead-letter-routing-key': 'dead_letter'
    }
  });

  console.log('✅ DLX設定完了');
  await connection.close();
}

setupDLX();
