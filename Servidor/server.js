const redis = require('redis');
const axios = require('axios');
const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC_PRINCIPAL = process.env.TOPIC_PRINCIPAL || 'consultas-geoespaciales';
const TOPIC_RETRY = process.env.TOPIC_RETRY || 'consultas-geoespaciales-retry';
const TOPIC_DLQ = process.env.TOPIC_DLQ || 'consultas-geoespaciales-dlq';
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
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

const kafka = new Kafka({ clientId: 'consumidor_base', brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: GROUP_ID });
const producer = kafka.producer();

async function recordMetric(queryType, hit, latencyMs, zoneId) {
    try {
        await axios.post(`${METRICS_URL}/metrics/record`, {
            query_type: queryType,
            hit: hit,
            latency_ms: latencyMs,
            zone_id: zoneId
        }, { timeout: 500 });
    } catch (err) {
        log('WARNING', 'No se pudo registrar la métrica.', { query: queryType, error: err.message });
    }
}

async function recordResilienceMetric(eventType) {
    try {
        await axios.post(`${METRICS_URL}/metrics/record`, {
            event_type: eventType
        }, { timeout: 500 });
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

    try {
        const cached = await redisClient.get(cacheKey);
        if (cached !== null) {
            const latency = Date.now() - start;
            await recordMetric(consulta.tipo_consulta, true, latency, zoneId);
            log('INFO', `CACHE HIT para ${consulta.tipo_consulta}`, { cacheKey, latency_ms: latency });
            if (consulta.retry_count > 0) {
                log('INFO', `RECOVERY detectada para consulta ${consulta.id}`, { attempts: consulta.retry_count });
                await recordResilienceMetric('recovery');
            }
            return;
        }

        log('INFO', `CACHE MISS para ${consulta.tipo_consulta}`, { cacheKey });

        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${consulta.tipo_consulta}`, {
            params: queryParams,
            timeout: 5000
        });
        const result = response.data;

        const ttl = parseInt(queryParams.ttl) || 3600;
        await redisClient.set(cacheKey, JSON.stringify(result), { EX: ttl });

        const latency = Date.now() - start;
        await recordMetric(consulta.tipo_consulta, false, latency, zoneId);

        if (consulta.retry_count > 0) {
            log('INFO', `RECOVERY detectada para consulta ${consulta.id}`, { attempts: consulta.retry_count });
            await recordResilienceMetric('recovery');
        }

    } catch (err) {
        log('ERROR', `Fallo al procesar consulta ${consulta.tipo_consulta} en consumidor. Evaluando reintento...`, { error: err.message });

        consulta.retry_count = (consulta.retry_count || 0) + 1;
        consulta.last_error = err.message;

        try {
            if (consulta.retry_count <= MAX_RETRIES) {
                log('WARNING', `Enviando consulta a tópico de REINTENTO (#${consulta.retry_count})`, { id: consulta.id });
                await producer.send({
                    topic: TOPIC_RETRY,
                    messages: [{ key: consulta.id, value: JSON.stringify(consulta) }]
                });
                await recordResilienceMetric('retry');
            } else {
                log('CRITICAL', `Consulta superó el máximo de reintentos (${MAX_RETRIES}). Enviando a DLQ.`, { id: consulta.id });
                await producer.send({
                    topic: TOPIC_DLQ,
                    messages: [{ key: consulta.id, value: JSON.stringify(consulta) }]
                });
                await recordResilienceMetric('dlq');
            }
        } catch (kafkaErr) {
            log('ERROR', 'Fallo crítico al despachar métrica de resiliencia a Kafka', { error: kafkaErr.message });
        }
    }
}

async function startServer() {
    await redisClient.connect();
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topics: [TOPIC_PRINCIPAL, TOPIC_RETRY], fromBeginning: true });

    log('INFO', 'Configuración de servicios asíncronos cargada.', {
        kafka_broker: KAFKA_BROKER,
        topic_principal: TOPIC_PRINCIPAL,
        topic_retry: TOPIC_RETRY
    });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            // Refinamiento Audit: Si el mensaje viene de reintento, esperar un breve momento 
            // para permitir que el backend (Response Generator) se recupere.
            if (topic === TOPIC_RETRY) {
                await new Promise(resolve => setTimeout(resolve, 1500));
            }
            await handleKafkaMessage(message.value.toString());
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
    log('ERROR', 'Fallo catastrófico en el inicio del consumidor.', { error: err.message });
    process.exit(1);
});