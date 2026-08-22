import {Kafka} from 'kafkajs';
import pg from 'pg';
const {Pool}=pg;
const db=new Pool({connectionString:process.env.POSTGRES_URL});
await db.query('CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now())');
const k=new Kafka({clientId:'notification-service',brokers:(process.env.KAFKA_BROKERS||'localhost:9092').split(',')});
const c=k.consumer({groupId:'notification'});await c.connect();await c.subscribe({topic:'orders',fromBeginning:true});
await c.run({eachMessage:async({message})=>{if(!message.value)return;const e=JSON.parse(message.value.toString());const type=e.eventType||e.type;if(!['PaymentCompleted','PaymentFailed','InventoryReleased','OrderCancelled'].includes(type))return;const eventId=String(e.eventId||`${type}:${e.payload?.orderId||e.orderId}`);const inserted=await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING',[eventId]);if(inserted.rowCount===0)return;const orderId=e.payload?.orderId||e.orderId;console.log(JSON.stringify({event:type,orderId,notification:`order ${orderId} -> ${type}`}));}});
