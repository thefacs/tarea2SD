const redis = require('redis');
const axios = require('axios');
const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC_PRINCIPAL = process.env.TOPIC_PRINCIPAL || 'consultas-geoespaciales';
const TOPIC_RETRY = process.env.TOPIC_RETRY || 'consultas-geoespaciales-retry';
const TOPIC_DLQ = process.env.TOPIC_DLQ || 'consultas-geoespaciales-dlq';
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 2;
const GROUP_ID = process.env.GROUP_ID || 'grupo-consumidores-sd';
const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://response-generator:3001';
const METRICS_URL = process.env.METRICS_URL || 'http://metric-service:4000';

const queryConfig = {
    'q1': { buildKey: (q) => `count:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q2': { buildKey: (q) => `area:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q3': { buildKey: (q) => `density:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q4': { buildKey: (q) => `compare:density:${q.z1}:${q.z2}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.z1 },
    'q5': { buildKey: (q) => `confidence_dist:${q.zone_id}:bins=${q.bins || 5}`, getZoneId: (q) => q.zone_id }
};

function log(level, message, meta = {}) {
    const timestamp = new Date().toISOString();
    let logMsg = `[${timestamp}] [SERVIDOR] [${level}] ${message}`;
    if (Object.keys(meta).length > 0) {
        logMsg += ` | ${JSON.stringify(meta)}`;
    }
    console.log(logMsg);
}

const redisClient = redis.createClient({ url: process.env.REDIS_URL || 'redis://redis-cache:6379' });
redisClient.on('connect', () => log('INFO', 'Conexión a Redis establecida correctamente.'));
redisClient.on('error', (err) => log('ERROR', 'Fallo en la conexión con Redis.', { error: err.message }));

const kafka = new Kafka({
    clientId: 'consumidor_base',
    brokers: [KAFKA_BROKER],
    retry: {
        initialRetryTime: 100,
        retries: MAX_RETRIES
    }
});

const consumer = kafka.consumer({
    groupId: GROUP_ID,
    retry: {
        retries: MAX_RETRIES,
        factor: 1
    }
});
const producer = kafka.producer();

async function recordMetric(queryType, hit, latencyMs, zoneId) {
    try {
        await axios.post(`${METRICS_URL}/metrics/record`, {
            query_type: queryType,
            hit: hit,
            latency_ms: latencyMs,
            zone_id: zoneId
        }, { timeout: 1000 });
    } catch (err) {
        log('WARNING', 'No se pudo registrar la métrica.', { query: queryType, error: err.message });
    }
}

async function recordResilienceMetric(eventType) {
    try {
        await axios.post(`${METRICS_URL}/metrics/record`, {
            event_type: eventType
        }, { timeout: 1000 });
    } catch (err) {
        log('WARNING', `No se pudo registrar métrica de resiliencia: ${eventType}`, { error: err.message });
    }
}

async function handleKafkaMessage(messageValue) {
    const start = Date.now();
    let consulta;

    try {
        consulta = JSON.parse(messageValue);
    } catch (e) {
        log('ERROR', 'Error parseando el mensaje recibido de Kafka', { raw: messageValue });
        return;
    }

    const config = queryConfig[consulta.tipo_consulta];
    if (!config) {
        log('ERROR', `Tipo de consulta no soportado: ${consulta.tipo_consulta}`);
        return;
    }

    const queryParams = consulta.datos_consulta;
    const cacheKey = config.buildKey(queryParams);
    const zoneId = config.getZoneId(queryParams);

    // 1. Verificar en caché
    const cached = await redisClient.get(cacheKey);
    if (cached !== null) {
        const latency = Date.now() - start;
        await recordMetric(consulta.tipo_consulta, true, latency, zoneId);
        log('INFO', `CACHE HIT para ${consulta.tipo_consulta}`, { cacheKey, latency_ms: latency });
        if (consulta.retry_count > 0) {
            await recordResilienceMetric('recovery');
        }
        return;
    }

    log('INFO', `CACHE MISS para ${consulta.tipo_consulta}`, { cacheKey });

    // 2. Llamar al backend con reintento nativo (propagando el error)
    try {
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${consulta.tipo_consulta}`, {
            params: queryParams,
            timeout: 4000
        });
        const result = response.data;

        const ttl = parseInt(queryParams.ttl) || 3600;
        await redisClient.set(cacheKey, JSON.stringify(result), { EX: ttl });

        const latency = Date.now() - start;
        await recordMetric(consulta.tipo_consulta, false, latency, zoneId);

        if (consulta.retry_count > 0) {
            await recordResilienceMetric('recovery');
        }
    } catch (err) {
        // Reportar el intento de fallo (retry) antes de propagar
        await recordResilienceMetric('retry');
        throw err; // Propagar para que KafkaJS reintente 
    }
}

async function startServer() {
    await redisClient.connect();
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topics: [TOPIC_PRINCIPAL, TOPIC_RETRY], fromBeginning: true });

    log('INFO', 'Iniciando consumidor con retries nativos y DLQ manual.', {
        max_retries: MAX_RETRIES,
        group_id: GROUP_ID
    });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                await handleKafkaMessage(message.value.toString());
            } catch (err) {
                // Si llegamos aquí, KafkaJS agotó sus reintentos nativos para este mensaje
                const rawValue = message.value.toString();
                let consulta;
                try { consulta = JSON.parse(rawValue); } catch (e) { consulta = { id: 'unknown' }; }

                log('CRITICAL', `Agotados reintentos nativos. Desviando a DLQ.`, { id: consulta.id, error: err.message });

                await producer.send({
                    topic: TOPIC_DLQ,
                    messages: [{ key: message.key, value: rawValue }]
                });

                // Registro obligatorio en Metrics Storage
                await recordResilienceMetric('dlq');
            }
        },
    });
}

process.on('SIGINT', async () => {
    await consumer.disconnect();
    await producer.disconnect();
    await redisClient.quit();
    process.exit(0);
});

startServer().catch((err) => {
    log('ERROR', 'Fallo catastrófico.', { error: err.message });
    process.exit(1);
});