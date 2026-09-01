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
await consumer.subscribe({ topic: 'orders.retry', fromBeginning: true });

createServer(async (req, res) => {
  if (req.url === '/health/live') { res.writeHead(200); return res.end(JSON.stringify({ status: 'ok' })); }
  if (req.url === '/health/ready') {
    try { await db.query('SELECT 1'); res.writeHead(200); return res.end(JSON.stringify({ status: 'ready' })); }
    catch { res.writeHead(503); return res.end(JSON.stringify({ status: 'not-ready' })); }
  }
  res.writeHead(404); res.end();
}).listen(Number(process.env.HEALTH_PORT || 8080));

const retryTopic = process.env.KAFKA_RETRY_TOPIC || 'orders.retry';
const dlqTopic = process.env.KAFKA_DLQ_TOPIC || 'orders.DLQ';
const maxRetries = Number(process.env.KAFKA_MAX_RETRIES || 3);
const handled = new Set(['PaymentCompleted', 'PaymentFailed', 'InventoryReleased', 'OrderCancelled']);

await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
  if (!message.value) return;
  let eventId: string | undefined;
  try {
    const event = JSON.parse(message.value.toString());
    const type = event.eventType || event.type;
    if (!handled.has(type)) return;
    eventId = String(event.eventId || `${type}:${event.payload?.orderId || event.orderId}`);
    const inserted = await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING', [eventId]);
    if (inserted.rowCount === 0) return;
    const orderId = event.payload?.orderId || event.orderId;
    console.log(JSON.stringify({ event: type, orderId, notification: `order ${orderId} -> ${type}` }));
  } catch (error) {
    if (eventId) await db.query('DELETE FROM processed_events WHERE event_id=$1', [eventId]);
    const retries = Number(message.headers?.['x-retry-count']?.toString() || 0) + 1;
    const destination = retries <= maxRetries ? retryTopic : dlqTopic;
    const value = destination === retryTopic ? message.value.toString() : JSON.stringify({ failedAt: new Date().toISOString(), sourceTopic: topic, partition, offset: message.offset, retryCount: retries, error: error instanceof Error ? error.message : String(error), payload: message.value.toString() });
    await producer.send({ topic: destination, messages: [{ key: message.key?.toString(), headers: { 'x-retry-count': String(retries) }, value }] });
  }
});
