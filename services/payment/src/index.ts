import { Kafka } from 'kafkajs';
import pg from 'pg';
import { randomUUID } from 'node:crypto';

const { Pool } = pg;
const db = new Pool({ connectionString: process.env.POSTGRES_URL });
await db.query('CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now())');

const kafka = new Kafka({ clientId: 'payment-service', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const consumer = kafka.consumer({ groupId: 'payment' });
const producer = kafka.producer();
await consumer.connect();
await producer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: true });

const failureRate = Number(process.env.PAYMENT_FAILURE_RATE || 0.1);

await consumer.run({
  eachMessage: async ({ message }) => {
    if (!message.value) return;
    const event = JSON.parse(message.value.toString());
    const type = event.eventType || event.type;
    if (type !== 'InventoryReserved') return;

    const eventId = String(event.eventId || `${type}:${event.payload?.orderId || event.orderId}`);
    const inserted = await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING', [eventId]);
    if (inserted.rowCount === 0) return;

    const payload = event.payload || event;
    const failed = Math.random() < failureRate;
    const nextType = failed ? 'PaymentFailed' : 'PaymentCompleted';

    await producer.send({
      topic: 'orders',
      messages: [{
        key: payload.orderId,
        value: JSON.stringify({
          eventId: randomUUID(),
          eventType: nextType,
          version: 1,
          occurredAt: new Date().toISOString(),
          correlationId: event.correlationId || payload.orderId,
          payload
        })
      }]
    });
  }
});
