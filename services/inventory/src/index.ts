import pg from 'pg';
import { Kafka } from 'kafkajs';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'node:path';
import { createServer } from 'node:http';

const { Pool } = pg;
const db = new Pool({ connectionString: process.env.POSTGRES_URL });
await db.query(`CREATE TABLE IF NOT EXISTS inventory(product_id INTEGER PRIMARY KEY, stock INTEGER NOT NULL CHECK(stock>=0))`);
const seed = await db.query('SELECT count(*)::int c FROM inventory');
if (seed.rows[0].c === 0) await db.query('INSERT INTO inventory(product_id,stock) VALUES(1,20),(2,50)');
await db.query(`CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now())`);

const kafka = new Kafka({ clientId: 'inventory-service', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'inventory' });
await producer.connect();
await consumer.connect();
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

const def = protoLoader.loadSync(path.resolve(process.cwd(), 'proto/inventory.proto'));
const service = (grpc.loadPackageDefinition(def) as any).inventory.Inventory.service;
const server = new grpc.Server();
server.addService(service, { GetStock: async (call: any, cb: any) => {
  try { const q = await db.query('SELECT stock FROM inventory WHERE product_id=$1', [call.request.productId]); cb(null, { stock: q.rows[0]?.stock ?? 0 }); }
  catch (error) { cb(error); }
}});
server.bindAsync(`0.0.0.0:${process.env.GRPC_PORT || 50051}`, grpc.ServerCredentials.createInsecure(), () => server.start());

const publish = async (event: any, type: string) => producer.send({ topic: 'orders', messages: [{ key: event.payload?.orderId || event.orderId, value: JSON.stringify({ ...event, eventType: type, type }) }] });
const retryTopic = process.env.KAFKA_RETRY_TOPIC || 'orders.retry';
const dlqTopic = process.env.KAFKA_DLQ_TOPIC || 'orders.DLQ';
const maxRetries = Number(process.env.KAFKA_MAX_RETRIES || 3);

await consumer.run({ eachMessage: async ({ topic, partition, message }) => {
  if (!message.value) return;
  let event: any;
  let eventId: string | undefined;
  try {
    event = JSON.parse(message.value.toString());
    const type = event.eventType || event.type;
    if (!['OrderCreated', 'PaymentFailed'].includes(type)) return;
    eventId = String(event.eventId || `${type}:${event.payload?.orderId || event.orderId}`);
    const processed = await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING', [eventId]);
    if (processed.rowCount === 0) return;
    const payload = event.payload || event;
    const items = payload.items || [];
    const c = await db.connect();
    try {
      await c.query('BEGIN');
      for (const item of items) {
        const delta = type === 'OrderCreated' ? -item.quantity : item.quantity;
        const condition = type === 'OrderCreated' ? ' AND stock >= $1' : '';
        const values = type === 'OrderCreated' ? [item.quantity, delta, item.productId] : [0, delta, item.productId];
        const q = await c.query(`UPDATE inventory SET stock=stock+$2 WHERE product_id=$3${condition} RETURNING product_id`, values);
        if (!q.rowCount) throw new Error(type === 'OrderCreated' ? 'insufficient stock or unknown product' : 'unknown product');
      }
      await c.query('COMMIT');
      await publish(event, type === 'OrderCreated' ? 'InventoryReserved' : 'InventoryReleased');
    } catch (error) {
      await c.query('ROLLBACK');
      const reason = error instanceof Error ? error.message : String(error);
      if (type === 'OrderCreated' && (reason.includes('insufficient stock') || reason.includes('unknown product'))) await publish(event, 'InventoryFailed');
      else throw error;
    } finally { c.release(); }
  } catch (error) {
    if (eventId) await db.query('DELETE FROM processed_events WHERE event_id=$1', [eventId]);
    const retries = Number(message.headers?.['x-retry-count']?.toString() || 0) + 1;
    const destination = retries <= maxRetries ? retryTopic : dlqTopic;
    const value = destination === retryTopic ? message.value.toString() : JSON.stringify({ failedAt: new Date().toISOString(), sourceTopic: topic, partition, offset: message.offset, retryCount: retries, error: error instanceof Error ? error.message : String(error), payload: message.value.toString() });
    await producer.send({ topic: destination, messages: [{ key: message.key?.toString(), headers: { 'x-retry-count': String(retries) }, value }] });
  }
});

console.log('inventory gRPC listening');
