import express from 'express';
import pg from 'pg';
import { createClient } from 'redis';
import { Kafka } from 'kafkajs';
import { createEvent } from '../../../contracts/events.js';

const { Pool } = pg;
const db = new Pool({ connectionString: process.env.POSTGRES_URL });
const cache = createClient({ url: process.env.REDIS_URL });
const kafka = new Kafka({ clientId: 'product-service', brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',') });
const producer = kafka.producer();
await cache.connect();
await producer.connect();
await db.query(`CREATE TABLE IF NOT EXISTS products(id SERIAL PRIMARY KEY,name TEXT NOT NULL,price NUMERIC(12,2) NOT NULL,stock INTEGER NOT NULL DEFAULT 0,created_at TIMESTAMPTZ DEFAULT now())`);

const app = express();
app.use(express.json());
app.get('/health', (_, r) => r.json({ status: 'ok', service: 'product' }));
app.get('/products', async (_, r) => { const { rows } = await db.query('SELECT * FROM products ORDER BY id DESC'); r.json(rows); });
app.get('/products/:id', async (req, r) => {
  const key = `product:${req.params.id}`;
  const hit = await cache.get(key);
  if (hit) return r.json(JSON.parse(hit));
  const q = await db.query('SELECT * FROM products WHERE id=$1', [req.params.id]);
  if (!q.rowCount) return r.status(404).json({ error: 'product not found' });
  await cache.set(key, JSON.stringify(q.rows[0]), { EX: 60 });
  r.json(q.rows[0]);
});
app.post('/products', async (req, r) => {
  const { name, price, stock } = req.body;
  if (typeof name !== 'string' || !name.trim() || !Number.isFinite(Number(price)) || !Number.isInteger(stock) || stock < 0) return r.status(400).json({ error: 'name, numeric price and non-negative integer stock required' });
  const q = await db.query('INSERT INTO products(name,price,stock) VALUES($1,$2,$3) RETURNING *', [name.trim(), price, stock]);
  const product = q.rows[0];
  await producer.send({ topic: 'products', messages: [{ key: String(product.id), value: JSON.stringify(createEvent('ProductCreated', { productId: product.id, stock: product.stock }, String(product.id))) }] });
  r.status(201).json(product);
});

const server = app.listen(Number(process.env.PORT || 3001), () => console.log('product listening'));
const shutdown = async () => { server.close(); await producer.disconnect().catch(() => undefined); await cache.quit().catch(() => undefined); await db.end().catch(() => undefined); process.exit(0); };
process.once('SIGTERM', shutdown); process.once('SIGINT', shutdown);
