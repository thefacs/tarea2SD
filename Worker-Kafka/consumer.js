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

const consumer = kafka.consumer({ groupId: process.env.GROUP_ID || 'grupo-geoespacial' });
const producer = kafka.producer();

const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379'
});
redisClient.on('error', (err) => console.log('[Redis] Error:', err));

const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://localhost:3001';
const METRICS_URL = process.env.METRICS_URL || 'http://localhost:4000';
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || 3);

const TOPIC_MAIN = process.env.TOPIC_PRINCIPAL || 'consultas-geoespaciales';
const TOPIC_RETRY = process.env.TOPIC_RETRY || 'consultas-geoespaciales-retry';
const TOPIC_DLQ = process.env.TOPIC_DLQ || 'consultas-geoespaciales-dlq';

async function procesarMensaje(mensajeJSON) {
    const { id, tipo_consulta, datos_consulta, retry_count } = mensajeJSON;
    const start = Date.now();

    // Generar Cache Key normalizada (Tarea 1)
    let cacheKey = '';
    const conf = datos_consulta.conf_min || 0.0;
    if (tipo_consulta === 'q4') {
        cacheKey = `compare:density:${datos_consulta.z1}:${datos_consulta.z2}:conf=${conf}`;
    } else if (tipo_consulta === 'q5') {
        cacheKey = `confidence_dist:${datos_consulta.zone_id}:bins=${datos_consulta.bins || 5}`;
    } else {
        const prefix = tipo_consulta === 'q1' ? 'count' : (tipo_consulta === 'q2' ? 'area' : 'density');
        cacheKey = `${prefix}:${datos_consulta.zone_id}:conf=${conf}`;
    }

    const zoneId = datos_consulta.zone_id || datos_consulta.z1;

    try {
        // 1. Verificar en Caché
        const cached = await redisClient.get(cacheKey);
        if (cached) {
            console.log(`[Worker] CACHE HIT (ID: ${id})`);
            await axios.post(`${METRICS_URL}/metrics/record`, {
                query_type: tipo_consulta,
                hit: true,
                latency_ms: Date.now() - start,
                zone_id: zoneId,
                cache_key: cacheKey
            }).catch(() => { });

            if (retry_count > 0) {
                await axios.post(`${METRICS_URL}/metrics/record`, { event_type: 'recovery' }).catch(() => { });
            }
            return;
        }

        // 2. Cache Miss -> Backend
        console.log(`[Worker] CACHE MISS (ID: ${id})`);
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${tipo_consulta}`, {
            params: datos_consulta,
            timeout: 5000
        });

        const latency = Date.now() - start;

        // Guardar en Redis con TTL
        await redisClient.setEx(cacheKey, parseInt(datos_consulta.ttl || 3600), JSON.stringify(response.data));

        // Reportar métrica de procesamiento
        await axios.post(`${METRICS_URL}/metrics/record`, {
            query_type: tipo_consulta,
            hit: false,
            latency_ms: latency,
            zone_id: zoneId,
            cache_key: cacheKey
        }).catch(() => { });

        if (retry_count > 0) {
            await axios.post(`${METRICS_URL}/metrics/record`, { event_type: 'recovery' }).catch(() => { });
        }

        console.log(`[Worker] ÉXITO. Respuesta de ${tipo_consulta} (ID: ${id})`);

    } catch (error) {
        console.error(`[Worker] ERROR en la consulta ${id}. Detalle: ${error.message}`);

        const nuevoRetryCount = retry_count + 1;
        mensajeJSON.retry_count = nuevoRetryCount;

        if (nuevoRetryCount > MAX_RETRIES) {
            console.warn(`[Worker] MAX RETRIES ALCANZADO (${id}). Enviando a DLQ.`);
            await producer.send({
                topic: TOPIC_DLQ,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
            await axios.post(`${METRICS_URL}/metrics/record`, { event_type: 'dlq' }).catch(() => { });
        } else {
            console.info(`[Worker] Reencolando mensaje (${id}) al Tópico de Reintento (Intento ${nuevoRetryCount}).`);
            await producer.send({
                topic: TOPIC_RETRY,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
            await axios.post(`${METRICS_URL}/metrics/record`, { event_type: 'retry' }).catch(() => { });
        }
    }
}

async function run() {
    await redisClient.connect();
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: TOPIC_MAIN, fromBeginning: true });
    await consumer.subscribe({ topic: TOPIC_RETRY, fromBeginning: true });

    console.log('========================================');
    console.log('[Worker] Conectado a Kafka y escuchando mensajes');
    console.log('========================================');

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMsg = JSON.parse(message.value.toString());

            if (topic === TOPIC_RETRY) {
                console.log(`[Worker] Esperando antes de reintentar (ID: ${jsonMsg.id})`);
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