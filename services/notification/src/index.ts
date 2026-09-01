import { Kafka } from 'kafkajs';
import pg from 'pg';
import { createServer } from 'node:http';

const { Pool } = pg;
const db = new Pool({ connectionString: process.env.POSTGRES_URL });
await db.query('CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now())');

const kafka = new Kafka({ clientId: 'notification-service', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const consumer = kafka.consumer({ groupId: 'notification' });
const producer = kafka.producer();
await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: true });

createServer(async (req, res) => {
  if (req.url === '/health/live') { res.writeHead(200); return res.end(JSON.stringify({ status: 'ok' })); }
  if (req.url === '/health/ready') {
    try { await db.query('SELECT 1'); res.writeHead(200); return res.end(JSON.stringify({ status: 'ready' })); }
    catch { res.writeHead(503); return res.end(JSON.stringify({ status: 'not-ready' })); }
  }
  res.writeHead(404); res.end();
}).listen(Number(process.env.HEALTH_PORT || 8080));

const dlqTopic = process.env.KAFKA_DLQ_TOPIC || 'orders.DLQ';
const handled = new Set(['PaymentCompleted', 'PaymentFailed', 'InventoryReleased', 'OrderCancelled']);

await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
  if (!message.value) return;
  try {
    const event = JSON.parse(message.value.toString());
    const type = event.eventType || event.type;
    if (!handled.has(type)) return;
    const eventId = String(event.eventId || `${type}:${event.payload?.orderId || event.orderId}`);
    const inserted = await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING', [eventId]);
    if (inserted.rowCount === 0) return;
    const orderId = event.payload?.orderId || event.orderId;
    console.log(JSON.stringify({ event: type, orderId, notification: `order ${orderId} -> ${type}` }));
  } catch (error) {
    await producer.send({ topic: dlqTopic, messages: [{ key: message.key?.toString(), value: JSON.stringify({ failedAt: new Date().toISOString(), sourceTopic: topic, partition, offset: message.offset, error: error instanceof Error ? error.message : String(error), payload: message.value.toString() }) }] });
  }
});
