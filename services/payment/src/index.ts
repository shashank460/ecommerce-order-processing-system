import {Kafka} from 'kafkajs';
const kafka=new Kafka({clientId:'payment-service',brokers:(process.env.KAFKA_BROKERS||'localhost:9092').split(',')});
const c=kafka.consumer({groupId:'payment'}),p=kafka.producer();await c.connect();await p.connect();await c.subscribe({topic:'orders',fromBeginning:true});
const failureRate=Number(process.env.PAYMENT_FAILURE_RATE||0.1);
await c.run({eachMessage:async({message})=>{if(!message.value)return;const e=JSON.parse(message.value.toString());if(e.type!=='InventoryReserved')return;const failed=Math.random()<failureRate;const type=failed?'PaymentFailed':'PaymentCompleted';await p.send({topic:'orders',messages:[{key:e.orderId,value:JSON.stringify({...e,type})}]});console.log(`payment ${type} for ${e.orderId}`)}});
