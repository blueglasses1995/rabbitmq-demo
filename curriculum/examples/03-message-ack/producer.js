const amqp = require('amqplib');

async function sendTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'ack_queue';

    await channel.assertQueue(queueName, { durable: false });

    // いくつかのタスクを送信
    const tasks = [
      { id: 1, type: 'normal', data: 'Task 1' },
      { id: 2, type: 'normal', data: 'Task 2' },
      { id: 3, type: 'error', data: 'Task 3 - This will fail' },
      { id: 4, type: 'normal', data: 'Task 4' },
      { id: 5, type: 'crash', data: 'Task 5 - This will crash worker' },
      { id: 6, type: 'normal', data: 'Task 6' },
    ];

    console.log('📤 タスクを送信中...\n');

    for (const task of tasks) {
      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));
      console.log(`✓ 送信: Task ${task.id} (${task.type})`);
    }

    console.log(`\n✅ ${tasks.length}個のタスクを送信しました`);

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

sendTasks();
