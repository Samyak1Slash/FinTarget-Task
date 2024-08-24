const cluster = require('cluster');
const os = require('os');
const express = require('express');
const { RateLimiterMemory } = require('rate-limiter-flexible');
const Bull = require('bull');
const fs = require('fs');
const path = require('path');
const winston = require('winston');

const logDir = 'logs';
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.File({ filename: path.join(logDir, 'task.log') }),
  ],
});

const numCPUs = os.cpus().length;

if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  for (let i = 0; i < 2; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());

  const rateLimiter = new RateLimiterMemory({
    points: 20, 
    duration: 60,
  });

  const taskQueue = new Bull('task-queue', {
    redis: { port: 6379, host: '127.0.0.1' },
  });

  app.post('/api/v1/task', async (req, res) => {
    const userId = req.body.user_id;

    try {
      await rateLimiter.consume(userId, 1);
      taskQueue.add({ userId });
      res.send('Task added to queue');
    } catch (rejRes) {
      res.status(429).send('Rate limit exceeded');
    }
  });

  taskQueue.process(async (job) => {
    const { userId } = job.data;
    await task(userId);
  });

  const task = async (userId) => {
    const logMessage = `${userId}-task completed at-${Date.now()}`;
    console.log(logMessage);
    logger.info(logMessage);
  };

  app.listen(3000, () => {
    console.log(`Worker ${process.pid} is running on port 3000`);
  });
}
