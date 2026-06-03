const redis = require('redis');
const axios = require('axios');
const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC_PRINCIPAL = process.env.TOPIC_PRINCIPAL || 'consultas-geoespaciales';
const GROUP_ID = process.env.GROUP_ID || 'grupo-consumidores-sd';
const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://response-generator:3001';
const METRICS_URL = process.env.METRICS_URL || 'http://metric-service:4000';

// --- Mapeos de Claves de Caché heredados de la T1 ---
const queryConfig = {
    'q1': { buildKey: (q) => `count:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q2': { buildKey: (q) => `area:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q3': { buildKey: (q) => `density:${q.zone_id}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.zone_id },
    'q4': { buildKey: (q) => `compare:density:${q.z1}:${q.z2}:conf=${q.conf_min || 0.0}`, getZoneId: (q) => q.z1 },
    'q5': { buildKey: (q) => `confidence_dist:${q.zone_id}:bins=${q.bins || 5}`, getZoneId: (q) => q.zone_id }
};

function log(level, message, meta = {}) {
    if ((level === 'DEBUG' || level === 'INFO') &&
        !(message.includes('corriendo') || message.includes('cargada') || message.includes('establecida') || message.includes('Kafka'))) {
        return;
    }
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [SERVIDOR] [${level}] ${message} ${Object.keys(meta).length > 0 ? '| ' + JSON.stringify(meta) : ''}`);
}

// --- Clientes de Infraestructura ---
const redisClient = redis.createClient({ url: process.env.REDIS_URL || 'redis://redis-cache:6379' });
redisClient.on('connect', () => log('INFO', 'Conexión a Redis establecida correctamente.'));
redisClient.on('error', (err) => log('ERROR', 'Fallo en la conexión con Redis.', { error: err.message }));

const kafka = new Kafka({ clientId: 'consumidor_base', brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: GROUP_ID });

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

// --- Procesador del Mensaje de Kafka ---
async function handleKafkaMessage(messageValue) {
    const start = Date.now();
    const consulta = JSON.parse(messageValue); // Contrato acordado: id, tipo_consulta, datos_consulta
    const config = queryConfig[consulta.tipo_consulta];

    if (!config) {
        log('ERROR', `Tipo de consulta no soportado: ${consulta.tipo_consulta}`);
        return;
    }

    const queryParams = consulta.datos_consulta;
    const cacheKey = config.buildKey(queryParams);
    const zoneId = config.getZoneId(queryParams);

    try {
        // 1. Verificar en caché
        const cached = await redisClient.get(cacheKey);
        if (cached !== null) {
            const latency = Date.now() - start;
            await recordMetric(consulta.tipo_consulta, true, latency, zoneId);
            log('INFO', `CACHE HIT para ${consulta.tipo_consulta}`, { cacheKey, latency_ms: latency });
            return;
        }

        log('INFO', `CACHE MISS para ${consulta.tipo_consulta}`, { cacheKey });

        // 2. Llamar al generador de respuestas
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${consulta.tipo_consulta}`, {
            params: queryParams,
            timeout: 5000
        });
        const result = response.data;

        // 3. Almacenar en caché con TTL
        const ttl = parseInt(queryParams.ttl) || 3600;
        await redisClient.set(cacheKey, JSON.stringify(result), { EX: ttl });

        // 4. Registrar miss exitoso
        const latency = Date.now() - start;
        await recordMetric(consulta.tipo_consulta, false, latency, zoneId);
        
    } catch (err) {
        // El bloque catch queda disponible para la lógica de reintentos de Ariel
        log('ERROR', `Fallo al procesar consulta ${consulta.tipo_consulta} en consumidor`, { error: err.message, cacheKey });
    }
}

async function startServer() {
    await redisClient.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_PRINCIPAL, fromBeginning: true });
    
    log('INFO', 'Configuración de servicios asíncronos cargada.', { kafka_broker: KAFKA_BROKER, topic: TOPIC_PRINCIPAL });

    await consumer.run({
        eachMessage: async ({ message }) => {
            await handleKafkaMessage(message.value.toString());
        },
    });
}

process.on('SIGINT', async () => {
    await consumer.disconnect();
    await redisClient.quit();
    process.exit(0);
});

startServer().catch((err) => {
    log('ERROR', 'Fallo catastrófico en el inicio del consumidor.', { error: err.message });
    process.exit(1);
});