import pg from 'pg';
import { Kafka } from 'kafkajs';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import path from 'node:path';

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

const def = protoLoader.loadSync(path.resolve(process.cwd(), 'proto/inventory.proto'));
const service = (grpc.loadPackageDefinition(def) as any).inventory.Inventory.service;
const server = new grpc.Server();
server.addService(service, {
  GetStock: async (call: any, cb: any) => {
    try {
      const q = await db.query('SELECT stock FROM inventory WHERE product_id=$1', [call.request.productId]);
      cb(null, { stock: q.rows[0]?.stock ?? 0 });
    } catch (error) {
      cb(error);
    }
  }
});
server.bindAsync(`0.0.0.0:${process.env.GRPC_PORT || 50051}`, grpc.ServerCredentials.createInsecure(), () => server.start());

const publish = async (event: any, type: string) => producer.send({
  topic: 'orders',
  messages: [{ key: event.payload?.orderId || event.orderId, value: JSON.stringify({ ...event, eventType: type, type }) }]
});

await consumer.run({ eachMessage: async ({ message }) => {
  if (!message.value) return;
  const event = JSON.parse(message.value.toString());
  const type = event.eventType || event.type;
  if (!['OrderCreated', 'PaymentFailed'].includes(type)) return;
  const eventId = String(event.eventId || `${type}:${event.payload?.orderId || event.orderId}`);
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
      const q = await c.query(`UPDATE inventory SET stock=stock+$2 WHERE product_id=$3${condition} RETURNING product_id`, type === 'OrderCreated' ? [item.quantity, delta, item.productId] : [0, delta, item.productId]);
      if (!q.rowCount) throw new Error(type === 'OrderCreated' ? 'insufficient stock or unknown product' : 'unknown product');
    }
    await c.query('COMMIT');
    await publish(event, type === 'OrderCreated' ? 'InventoryReserved' : 'InventoryReleased');
  } catch (error) {
    await c.query('ROLLBACK');
    if (type === 'OrderCreated') await publish(event, 'InventoryFailed');
    else throw error;
  } finally {
    c.release();
  }
});

console.log('inventory gRPC listening');
