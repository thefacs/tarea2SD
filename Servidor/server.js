const express = require('express');
const redis = require('redis');
const axios = require('axios');

const app = express();
const PORT = 3000;

// --- Función de logging personalizada ---
function log(level, message, meta = {}) {
    if ((level === 'DEBUG' || level === 'INFO') &&
        !(message.includes('corriendo') || message.includes('cargada') || message.includes('establecida'))) {
        return;
    }

    const timestamp = new Date().toISOString();
    const prefix = `[${timestamp}] [SERVIDOR] [${level}]`;
    let logMsg = `${prefix} ${message}`;
    if (Object.keys(meta).length > 0) {
        logMsg += ` | ${JSON.stringify(meta)}`;
    }
    console.log(logMsg);
}

// --- Cliente Redis ---
const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://redis-cache:6379'
});

redisClient.on('connect', () => {
    log('INFO', 'Conexión a Redis establecida correctamente.', { host: process.env.REDIS_URL || 'redis://redis-cache:6379' });
});
redisClient.on('error', (err) => {
    log('ERROR', 'Fallo en la conexión con Redis.', { error: err.message });
});

redisClient.connect().catch((err) => {
    log('ERROR', 'No se pudo conectar a Redis. Abortando inicio.', { error: err.message });
    process.exit(1);
});

// --- URLs de servicios ---
const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://response-generator:3001';
const METRICS_URL = process.env.METRICS_URL || 'http://metric-service:4000';

log('INFO', 'Configuración de servicios cargada.', {
    response_generator: RESPONSE_GEN_URL,
    metrics_service: METRICS_URL
});

app.use(express.json());

// --- Registro de métricas ---
async function recordMetric(queryType, hit, latencyMs, zoneId) {
    try {
        await axios.post(`${METRICS_URL}/metrics/record`, {
            query_type: queryType,
            hit: hit,
            latency_ms: latencyMs,
            zone_id: zoneId
        }, { timeout: 500 });
        log('DEBUG', `Métrica registrada: ${queryType} ${hit ? 'hit' : 'miss'}`, { latency_ms: latencyMs, zone_id: zoneId });
    } catch (err) {
        log('WARNING', 'No se pudo registrar la métrica (timeout o error).', { query: queryType, error: err.message });
    }
}

// --- Manejador genérico de consultas ---
async function handleQuery(req, res, queryType, buildCacheKey, getZoneId) {
    const start = Date.now();
    const cacheKey = buildCacheKey(req.query);
    log('INFO', `Procesando consulta ${queryType}`, { cacheKey, params: req.query });

    try {
        // 1. Verificar en caché
        const cached = await redisClient.get(cacheKey);
        if (cached !== null) {
            const latency = Date.now() - start;
            const zoneId = getZoneId(req.query);
            await recordMetric(queryType, true, latency, zoneId);
            log('INFO', `CACHE HIT para ${queryType}`, { cacheKey, latency_ms: latency });

            // CORRECCIÓN: Se añade el campo source: 'hit' para que el generador lo reconozca
            return res.json({
                ...JSON.parse(cached),
                source: 'hit'
            });
        }

        log('INFO', `CACHE MISS para ${queryType}`, { cacheKey });

        // 2. Llamar al generador de respuestas
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${queryType}`, {
            params: req.query,
            timeout: 5000
        });
        const result = response.data;

        // 3. Almacenar en caché con TTL
        const ttl = parseInt(req.query.ttl) || 3600;
        await redisClient.set(cacheKey, JSON.stringify(result), { EX: ttl });
        log('INFO', `Resultado almacenado en caché`, { cacheKey, ttl_segundos: ttl });

        // 4. Registrar miss
        const latency = Date.now() - start;
        const zoneId = getZoneId(req.query);
        await recordMetric(queryType, false, latency, zoneId);

        // 5. Devolver resultado con source: 'miss'
        res.json({
            ...result,
            source: 'miss'
        });
    } catch (err) {
        log('ERROR', `Fallo al procesar consulta ${queryType}`, { error: err.message, cacheKey });
        res.status(500).json({ error: err.message });
    }
}

// --- Endpoints ---
app.get('/query/q1', (req, res) => {
    const buildKey = (q) => `count:${q.zone_id}:conf=${q.conf_min || 0.0}`;
    const getZoneId = (q) => q.zone_id;
    handleQuery(req, res, 'q1', buildKey, getZoneId);
});

app.get('/query/q2', (req, res) => {
    const buildKey = (q) => `area:${q.zone_id}:conf=${q.conf_min || 0.0}`;
    const getZoneId = (q) => q.zone_id;
    handleQuery(req, res, 'q2', buildKey, getZoneId);
});

app.get('/query/q3', (req, res) => {
    const buildKey = (q) => `density:${q.zone_id}:conf=${q.conf_min || 0.0}`;
    const getZoneId = (q) => q.zone_id;
    handleQuery(req, res, 'q3', buildKey, getZoneId);
});

app.get('/query/q4', (req, res) => {
    const buildKey = (q) => `compare:density:${q.z1}:${q.z2}:conf=${q.conf_min || 0.0}`;
    const getZoneId = (q) => q.z1;
    handleQuery(req, res, 'q4', buildKey, getZoneId);
});

app.get('/query/q5', (req, res) => {
    const buildKey = (q) => `confidence_dist:${q.zone_id}:bins=${q.bins || 5}`;
    const getZoneId = (q) => q.zone_id;
    handleQuery(req, res, 'q5', buildKey, getZoneId);
});

app.get('/health', (req, res) => {
    log('DEBUG', 'Health check solicitado');
    res.json({ status: 'ok', service: 'cache-proxy' });
});

app.listen(PORT, () => {
    log('INFO', `Cache Proxy corriendo en puerto ${PORT}`);
    log('INFO', `Redis: ${process.env.REDIS_URL || 'redis://redis-cache:6379'}`);
    log('INFO', `Response Generator: ${RESPONSE_GEN_URL}`);
    log('INFO', `Metrics Service: ${METRICS_URL}`);
});