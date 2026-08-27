import express from 'express';
import pg from 'pg';
import { Kafka } from 'kafkajs';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { validateIdempotencyKey } from './idempotency.js';
import { createEvent, parseEvent, type DomainEvent } from './event-utils.js';

const { Pool } = pg;
const db = new Pool({ connectionString: process.env.POSTGRES_URL });
await db.query(`CREATE TABLE IF NOT EXISTS orders(id UUID PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,created_at TIMESTAMPTZ DEFAULT now())`);
await db.query(`CREATE TABLE IF NOT EXISTS order_items(order_id UUID,product_id INTEGER,quantity INTEGER,PRIMARY KEY(order_id,product_id))`);
await db.query(`CREATE TABLE IF NOT EXISTS idempotency(key TEXT PRIMARY KEY,order_id UUID NOT NULL)`);
await db.query(`CREATE TABLE IF NOT EXISTS processed_events(event_id TEXT PRIMARY KEY,processed_at TIMESTAMPTZ DEFAULT now())`);

const kafka = new Kafka({ clientId: 'order-service', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'order-status' });
await producer.connect();
await consumer.connect();

const pkg = protoLoader.loadSync(path.resolve(process.cwd(), 'proto/inventory.proto'));
const inv = (grpc.loadPackageDefinition(pkg) as any).inventory;
const client = new inv.Inventory(process.env.INVENTORY_GRPC || 'localhost:50051', grpc.credentials.createInsecure());

const app = express();
app.use(express.json());
app.get('/health', (_, r) => r.json({ status: 'ok', service: 'order' }));

app.post('/orders', async (req, r) => {
  let key: string;
  try { key = validateIdempotencyKey(req.header('idempotency-key')); }
  catch (e: any) { return r.status(400).json({ error: e.message }); }
  const { userId, items } = req.body;
  if (!userId || !Array.isArray(items) || !items.length) return r.status(400).json({ error: 'userId and items required' });
  const existing = await db.query('SELECT o.* FROM orders o JOIN idempotency i ON i.order_id=o.id WHERE i.key=$1', [key]);
  if (existing.rowCount) return r.json(existing.rows[0]);
  const id = randomUUID();
  const correlationId = randomUUID();
  const c = await db.connect();
  try {
    await c.query('BEGIN');
    await c.query('INSERT INTO orders(id,user_id,status) VALUES($1,$2,$3)', [id, userId, 'PENDING']);
    for (const item of items) {
      if (!Number.isInteger(item.productId) || !Number.isInteger(item.quantity) || item.quantity <= 0) throw new Error('invalid order item');
      const stock: any = await new Promise((resolve, reject) => client.GetStock({ productId: item.productId }, (e: any, v: any) => e ? reject(e) : resolve(v)));
      if (stock.stock < item.quantity) throw new Error(`insufficient stock for product ${item.productId}`);
      await c.query('INSERT INTO order_items(order_id,product_id,quantity) VALUES($1,$2,$3)', [id, item.productId, item.quantity]);
    }
    await c.query('INSERT INTO idempotency(key,order_id) VALUES($1,$2)', [key, id]);
    await c.query('COMMIT');
    const event = createEvent('OrderCreated', { orderId: id, userId, items }, correlationId);
    await producer.send({ topic: 'orders', messages: [{ key: id, value: JSON.stringify(event) }] });
    r.status(201).json({ id, status: 'PENDING' });
  } catch (e: any) {
    await c.query('ROLLBACK');
    r.status(e.code === '23505' ? 200 : 409).json({ error: e.message });
  } finally { c.release(); }
});

await consumer.subscribe({ topic: 'orders', fromBeginning: true });
await consumer.run({ eachMessage: async ({ message }) => {
  if (!message.value) return;
  let e: DomainEvent;
  try { e = parseEvent(message.value.toString()); } catch { return; }
  if (!['PaymentCompleted', 'PaymentFailed', 'InventoryReleased', 'InventoryFailed'].includes(e.eventType)) return;
  const inserted = await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING', [e.eventId]);
  if (inserted.rowCount === 0) return;
  const status = e.eventType === 'PaymentCompleted' ? 'CONFIRMED' : e.eventType === 'InventoryReleased' ? 'CANCELLED' : e.eventType === 'InventoryFailed' ? 'CANCELLED' : 'PAYMENT_FAILED';
  await db.query('UPDATE orders SET status=$1 WHERE id=$2', [status, (e.payload as any).orderId]);
}});

const shutdown = async () => {
  await consumer.disconnect().catch(() => undefined);
  await producer.disconnect().catch(() => undefined);
  await db.end().catch(() => undefined);
  process.exit(0);
};
process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);

app.listen(Number(process.env.PORT || 3002), () => console.log('order listening'));
