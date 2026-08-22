import {Kafka} from 'kafkajs';
import pg from 'pg';
const {Pool}=pg;
const db=new Pool({connectionString:process.env.POSTGRES_URL});
await db.query('CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, processed_at TIMESTAMPTZ DEFAULT now())');
const kafka=new Kafka({clientId:'payment-service',brokers:(process.env.KAFKA_BROKERS||'localhost:9092').split(',')});
const c=kafka.consumer({groupId:'payment'}),p=kafka.producer();await c.connect();await p.connect();await c.subscribe({topic:'orders',fromBeginning:true});
const failureRate=Number(process.env.PAYMENT_FAILURE_RATE||0.1);
await c.run({eachMessage:async({message})=>{if(!message.value)return;const e=JSON.parse(message.value.toString());const type=e.eventType||e.type;if(type!=='InventoryReserved')return;const eventId=String(e.eventId||`${type}:${e.payload?.orderId||e.orderId}`);const inserted=await db.query('INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING',[eventId]);if(inserted.rowCount===0)return;const payload=e.payload||e;const failed=Math.random()<failureRate;const nextType=failed?'PaymentFailed':'PaymentCompleted';await p.send({topic:'orders',messages:[{key:payload.orderId,value:JSON.stringify({eventId:crypto.randomUUID(),eventType:nextType,version:1,occurredAt:new Date().toISOString(),correlationId:e.correlationId||payload.orderId,payload})}]});console.log(`payment ${nextType} for ${payload.orderId}`)}});
