const { Kafka } = require('kafkajs');
const axios = require('axios');
const redis = require('redis');

const kafka = new Kafka({
    clientId: 'worker-geoespacial',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'], 
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});

const consumer = kafka.consumer({ groupId: 'grupo-geoespacial' });
const producer = kafka.producer();

const redisClient = redis.createClient({ 
    url: process.env.REDIS_URL || 'redis://localhost:6379' 
});
redisClient.on('error', (err) => console.log('[Redis] Error:', err));

const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://localhost:3001';
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || 3);

const TOPIC_MAIN = 'consultas-geoespaciales';
const TOPIC_RETRY = 'consultas-reintento';
const TOPIC_DLQ = 'consultas-dlq';

async function procesarMensaje(mensajeJSON) {
    const { id, tipo_consulta, datos_consulta, retry_count } = mensajeJSON;
    
    try {
        console.log(`\n[Worker] 🔄 Intentando procesar ${tipo_consulta} (ID: ${id}) | Intento actual: ${retry_count}`);
        
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${tipo_consulta}`, {
            params: datos_consulta,
            timeout: 5000 
        });

        console.log(`[Worker] ÉXITO. Respuesta de ${tipo_consulta} (ID: ${id}):`, response.data);
        
    } catch (error) {
        console.error(`[Worker] ERROR en la consulta ${id}. Detalle: ${error.message}`);
        
        const nuevoRetryCount = retry_count + 1;
        mensajeJSON.retry_count = nuevoRetryCount;

        if (nuevoRetryCount >= MAX_RETRIES) {
            console.warn(`[Worker] MAX RETRIES ALCANZADO (${id}). Enviando a DLQ.`);
            await producer.send({
                topic: TOPIC_DLQ,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
        } else {
            console.info(`[Worker] Reencolando mensaje (${id}) al Tópico de Reintento (Intento ${nuevoRetryCount}).`);
            await producer.send({
                topic: TOPIC_RETRY,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
        }
    }
}

async function run() {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: TOPIC_MAIN, fromBeginning: true });
    await consumer.subscribe({ topic: TOPIC_RETRY, fromBeginning: true });

    console.log('========================================');
    console.log('[Worker]Conectado a Kafka y escuchando mensajes');
    console.log('========================================');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMsg = JSON.parse(message.value.toString());
            
            if (topic === TOPIC_RETRY) {
                console.log(`[Worker]Esperando 2 segundos antes de reintentar`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }

            await procesarMensaje(jsonMsg);
        },
    });
}

run().catch(error => {
    console.error('[Worker] Error fatal al arrancar Kafka:', error);
    process.exit(1);
});