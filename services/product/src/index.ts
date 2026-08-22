import express from 'express'; import pg from 'pg'; import { createClient } from 'redis';
const {Pool}=pg; const db=new Pool({connectionString:process.env.POSTGRES_URL}); const cache=createClient({url:process.env.REDIS_URL}); await cache.connect();
await db.query(`CREATE TABLE IF NOT EXISTS products(id SERIAL PRIMARY KEY,name TEXT NOT NULL,price NUMERIC(12,2) NOT NULL,stock INTEGER NOT NULL DEFAULT 0,created_at TIMESTAMPTZ DEFAULT now())`);
const app=express();app.use(express.json());app.get('/health',(_,r)=>r.json({status:'ok',service:'product'}));
app.get('/products',async(_,r)=>{const {rows}=await db.query('SELECT * FROM products ORDER BY id DESC');r.json(rows)});
app.get('/products/:id',async(req,r)=>{const key=`product:${req.params.id}`;const hit=await cache.get(key);if(hit)return r.json(JSON.parse(hit));const q=await db.query('SELECT * FROM products WHERE id=$1',[req.params.id]);if(!q.rowCount)return r.status(404).json({error:'product not found'});await cache.set(key,JSON.stringify(q.rows[0]),{EX:60});r.json(q.rows[0])});
app.post('/products',async(req,r)=>{const {name,price,stock}=req.body;if(typeof name!=='string'||!Number.isFinite(Number(price))||!Number.isInteger(stock)||stock<0)return r.status(400).json({error:'name, numeric price and non-negative integer stock required'});const q=await db.query('INSERT INTO products(name,price,stock) VALUES($1,$2,$3) RETURNING *',[name,price,stock]);r.status(201).json(q.rows[0])});
app.listen(Number(process.env.PORT||3001),()=>console.log('product listening'));
