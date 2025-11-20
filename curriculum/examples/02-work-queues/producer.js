const amqp = require('amqplib');

async function sendTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, {
      durable: false
    });

    // タスクの重さを変えて送信（デモ用）
    const tasks = [
      { id: 1, name: 'Task 1', processingTime: 1000 },   // 1秒
      { id: 2, name: 'Task 2', processingTime: 5000 },   // 5秒
      { id: 3, name: 'Task 3', processingTime: 1000 },   // 1秒
      { id: 4, name: 'Task 4', processingTime: 5000 },   // 5秒
      { id: 5, name: 'Task 5', processingTime: 1000 },   // 1秒
      { id: 6, name: 'Task 6', processingTime: 5000 },   // 5秒
      { id: 7, name: 'Task 7', processingTime: 1000 },   // 1秒
      { id: 8, name: 'Task 8', processingTime: 5000 },   // 5秒
    ];

    console.log('📤 タスクを送信中...\n');

    for (const task of tasks) {
      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));

      console.log(`✓ 送信: ${task.name} (処理時間: ${task.processingTime}ms)`);
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

// 大量タスク送信のデモ
async function sendManyTasks() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    const queueName = 'task_queue';

    await channel.assertQueue(queueName, { durable: false });

    console.log('📤 100個のタスクを送信中...\n');

    for (let i = 1; i <= 100; i++) {
      const task = {
        id: i,
        name: `Task ${i}`,
        processingTime: Math.floor(Math.random() * 3000) + 1000, // 1-4秒
        data: {
          timestamp: new Date().toISOString(),
          payload: `Data for task ${i}`
        }
      };

      const message = JSON.stringify(task);
      channel.sendToQueue(queueName, Buffer.from(message));

      if (i % 10 === 0) {
        console.log(`✓ ${i}個のタスクを送信完了`);
      }
    }

    console.log('\n✅ すべてのタスクを送信しました');

    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);

  } catch (error) {
    console.error('✗ エラー:', error.message);
    process.exit(1);
  }
}

// コマンドライン引数で実行モードを選択
const mode = process.argv[2] || 'default';

if (mode === 'many') {
  sendManyTasks();
} else {
  sendTasks();
}
