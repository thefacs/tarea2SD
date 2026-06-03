const express = require('express');
const redis = require('redis');
const fs = require('fs');
const path = require('path');
const app = express();
const PORT = 4000;

app.use(express.json());

let redisReady = false;
const client = redis.createClient({
    url: process.env.REDIS_URL || 'redis://redis-cache:6379'
});
client.on('error', (err) => console.error('[METRICAS] Redis error:', err));
client.on('connect', () => {
    redisReady = true;
    console.log('[METRICAS] Conectado a Redis');
});
client.connect().catch(console.error);

let METRICS_FILE_NAME = process.env.METRICS_FILE_NAME || 'metricas.csv';
let FILE_PATH = `/data/${METRICS_FILE_NAME}`;
const csvHeaders = 'timestamp,query_type,hit,latency_ms,zone,cache_key\n';

if (!fs.existsSync('/data')) {
    fs.mkdirSync('/data', { recursive: true });
}

if (!fs.existsSync(FILE_PATH)) {
    fs.writeFileSync(FILE_PATH, csvHeaders);
}

const metrics = {
    hits: { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 },
    misses: { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 },
    hitLatencies: [],   // tiempos de aciertos
    missLatencies: [],  // tiempos de fallos
    startTime: Date.now(),
    totalRequests: 0,
    totalRetries: 0,
    totalRecoveries: 0,
    totalDLQ: 0,
    failureStart: null,
    recoveryTimeMs: 0,
    recoveryStarted: false,
    backlogSize: 0,
    totalSent: 0,
    by_zone: {
        Z1: { hits: 0, misses: 0 },
        Z2: { hits: 0, misses: 0 },
        Z3: { hits: 0, misses: 0 },
        Z4: { hits: 0, misses: 0 },
        Z5: { hits: 0, misses: 0 }
    },
    responseTimes: {
        q1: [],
        q2: [],
        q3: [],
        q4: [],
        q5: []
    }
};

function saveToCSV(timestamp, queryType, hit, latencyMs, zone, cacheKey) {
    try {
        const line = `${timestamp},${queryType},${hit ? 1 : 0},${latencyMs},${zone || 'N/A'},${cacheKey || 'N/A'}\n`;
        fs.appendFileSync(FILE_PATH, line);
    } catch (err) {
        console.error('[METRICAS] Error escribiendo CSV:', err.message);
    }
}

app.post('/metrics/record', (req, res) => {
    const { query_type, hit, latency_ms, zone_id, cache_key, event_type, backlog_size } = req.body;

    if (backlog_size !== undefined) {
        metrics.backlogSize = backlog_size;
    }

    if (event_type) {
        if (event_type === 'retry') {
            metrics.totalRetries++;
            if (!metrics.failureStart) metrics.failureStart = Date.now();
        } else if (event_type === 'dlq') {
            metrics.totalDLQ++;
        } else if (event_type === 'recovery') {
            metrics.totalRecoveries++;
        } else if (event_type === 'sent') {
            metrics.totalSent++;
        }
        return res.json({ ok: true });
    }

    if (hit) {
        metrics.hits[query_type]++;
        if (zone_id && metrics.by_zone[zone_id]) metrics.by_zone[zone_id].hits++;
        metrics.hitLatencies.push(latency_ms);
    } else {
        metrics.misses[query_type]++;
        if (zone_id && metrics.by_zone[zone_id]) metrics.by_zone[zone_id].misses++;
        metrics.missLatencies.push(latency_ms);
    }

    metrics.totalRequests++;

    if (metrics.responseTimes[query_type]) {
        metrics.responseTimes[query_type].push(latency_ms);
        if (metrics.responseTimes[query_type].length > 1000) metrics.responseTimes[query_type].shift();
    }

    if (metrics.hitLatencies.length > 5000) metrics.hitLatencies.shift();
    if (metrics.missLatencies.length > 5000) metrics.missLatencies.shift();

    // Refinamiento Audit: Si estábamos en recuperación y el backlog llegó a cero, calculamos recovery_time
    const currentBacklog = metrics.totalSent - (metrics.totalRequests + metrics.totalDLQ);
    if (metrics.failureStart && currentBacklog <= 0) {
        metrics.recoveryTimeMs = Date.now() - metrics.failureStart;
        metrics.failureStart = null; // Ciclo de recuperación completado
    }

    saveToCSV(Date.now(), query_type, hit, latency_ms, zone_id, cache_key);
    res.json({ ok: true });
});

app.get('/metrics', async (req, res) => {
    const totalHits = Object.values(metrics.hits).reduce((a, b) => a + b, 0);
    const totalMisses = Object.values(metrics.misses).reduce((a, b) => a + b, 0);
    const total = totalHits + totalMisses;

    const t_cache = metrics.hitLatencies.length > 0
        ? metrics.hitLatencies.reduce((a, b) => a + b, 0) / metrics.hitLatencies.length
        : 0;

    const t_dh = metrics.missLatencies.length > 0
        ? metrics.missLatencies.reduce((a, b) => a + b, 0) / metrics.missLatencies.length
        : 0;

    const efficiency = total > 0
        ? ((totalHits * t_cache) - (totalMisses * t_dh)) / total
        : 0;

    const allLatencies = [...metrics.hitLatencies, ...metrics.missLatencies];
    const sorted = [...allLatencies].sort((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length * 0.5)] || 0;
    const p95 = sorted[Math.floor(sorted.length * 0.95)] || 0;
    const p99 = sorted[Math.floor(sorted.length * 0.99)] || 0;
    const avgLatency = allLatencies.length > 0
        ? allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length
        : 0;

    const latencyByQuery = {};
    for (const [q, times] of Object.entries(metrics.responseTimes)) {
        if (times.length > 0) {
            const sortedTimes = [...times].sort((a, b) => a - b);
            latencyByQuery[q] = {
                avg: (times.reduce((a, b) => a + b, 0) / times.length).toFixed(2),
                p50: sortedTimes[Math.floor(sortedTimes.length * 0.5)],
                p95: sortedTimes[Math.floor(sortedTimes.length * 0.95)],
                count: times.length
            };
        } else {
            latencyByQuery[q] = { avg: 0, p50: 0, p95: 0, count: 0 };
        }
    }

    let evictionRate = 0;
    let totalKeys = 0;
    let usedMemory = 0;
    try {
        if (redisReady) {
            const info = await client.info('stats');
            const match = info.match(/evicted_keys:(\d+)/);
            const memoryMatch = await client.info('memory');
            const usedMemMatch = memoryMatch.match(/used_memory_human:([^\r\n]+)/);
            const keysMatch = await client.dbSize();

            if (match) {
                const elapsedMinutes = (Date.now() - metrics.startTime) / 60000;
                evictionRate = parseInt(match[1]) / elapsedMinutes;
            }
            if (usedMemMatch) usedMemory = usedMemMatch[1];
            totalKeys = keysMatch;
        }
    } catch (e) {
        console.error('[METRICAS] Error obteniendo estadísticas de Redis:', e.message);
    }

    const elapsedSeconds = (Date.now() - metrics.startTime) / 1000;
    const throughput = total / elapsedSeconds;

    const hitRateByZone = {};
    for (const [zone, data] of Object.entries(metrics.by_zone)) {
        const totalZone = data.hits + data.misses;
        hitRateByZone[zone] = totalZone > 0 ? (data.hits / totalZone).toFixed(3) : 0;
    }

    res.json({
        hit_rate: total > 0 ? totalHits / total : 0,
        cache_efficiency: efficiency.toFixed(2),
        t_cache_avg_ms: t_cache.toFixed(2),
        t_dh_avg_ms: t_dh.toFixed(2),
        throughput: throughput.toFixed(2),
        total_requests: total,
        total_hits: totalHits,
        total_misses: totalMisses,
        retry_rate: metrics.totalRetries,
        recovery_rate: metrics.totalRecoveries,
        dlq_rate: metrics.totalDLQ,
        backlog_size: Math.max(0, metrics.totalSent - (metrics.totalRequests + metrics.totalDLQ)),
        recovery_time: metrics.recoveryTimeMs,
        elapsed_seconds: elapsedSeconds.toFixed(2),
        latency: {
            avg: avgLatency.toFixed(2),
            p50: p50,
            p95: p95,
            p99: p99
        },
        latency_by_query: latencyByQuery,
        cache: {
            eviction_rate_per_min: evictionRate.toFixed(2),
            redis_total_keys: totalKeys,
            redis_used_memory: usedMemory
        },
        by_query: {
            q1: { hits: metrics.hits.q1, misses: metrics.misses.q1, hit_rate: metrics.hits.q1 + metrics.misses.q1 > 0 ? (metrics.hits.q1 / (metrics.hits.q1 + metrics.misses.q1)).toFixed(3) : 0 },
            q2: { hits: metrics.hits.q2, misses: metrics.misses.q2, hit_rate: metrics.hits.q2 + metrics.misses.q2 > 0 ? (metrics.hits.q2 / (metrics.hits.q2 + metrics.misses.q2)).toFixed(3) : 0 },
            q3: { hits: metrics.hits.q3, misses: metrics.misses.q3, hit_rate: metrics.hits.q3 + metrics.misses.q3 > 0 ? (metrics.hits.q3 / (metrics.hits.q3 + metrics.misses.q3)).toFixed(3) : 0 },
            q4: { hits: metrics.hits.q4, misses: metrics.misses.q4, hit_rate: metrics.hits.q4 + metrics.misses.q4 > 0 ? (metrics.hits.q4 / (metrics.hits.q4 + metrics.misses.q4)).toFixed(3) : 0 },
            q5: { hits: metrics.hits.q5, misses: metrics.misses.q5, hit_rate: metrics.hits.q5 + metrics.misses.q5 > 0 ? (metrics.hits.q5 / (metrics.hits.q5 + metrics.misses.q5)).toFixed(3) : 0 }
        },
        hit_rate_by_zone: hitRateByZone
    });
});

app.post('/metrics/setup', (req, res) => {
    const { file_name } = req.body;
    if (file_name) {
        METRICS_FILE_NAME = file_name.endsWith('.csv') ? file_name : `${file_name}.csv`;
        FILE_PATH = `/data/${METRICS_FILE_NAME}`;

        if (!fs.existsSync(FILE_PATH)) {
            fs.writeFileSync(FILE_PATH, csvHeaders);
        }

        metrics.hits = { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 };
        metrics.misses = { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 };
        metrics.hitLatencies = [];
        metrics.missLatencies = [];
        metrics.startTime = Date.now();
        metrics.totalRequests = 0;
        metrics.totalRetries = 0;
        metrics.totalRecoveries = 0;
        metrics.totalDLQ = 0;
        metrics.failureStart = null;
        metrics.recoveryTimeMs = 0;
        metrics.backlogSize = 0;
        metrics.totalSent = 0;

        for (const zone of Object.keys(metrics.by_zone)) {
            metrics.by_zone[zone] = { hits: 0, misses: 0 };
        }
        for (const q of Object.keys(metrics.responseTimes)) {
            metrics.responseTimes[q] = [];
        }

        console.log(`[METRICAS] Sistema configurado para nuevo experimento: ${FILE_PATH}`);
        res.json({ ok: true, file: FILE_PATH });
    } else {
        res.status(400).json({ error: 'file_name es requerido' });
    }
});

app.get('/metrics/csv', (req, res) => {
    res.download(FILE_PATH, METRICS_FILE_NAME);
});

app.post('/metrics/reset', (req, res) => {
    metrics.hits = { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 };
    metrics.misses = { q1: 0, q2: 0, q3: 0, q4: 0, q5: 0 };
    metrics.hitLatencies = [];
    metrics.missLatencies = [];
    metrics.startTime = Date.now();
    metrics.totalRequests = 0;
    metrics.totalRetries = 0;
    metrics.totalRecoveries = 0;
    metrics.totalDLQ = 0;
    metrics.failureStart = null;
    metrics.recoveryTimeMs = 0;
    metrics.backlogSize = 0;

    for (const zone of Object.keys(metrics.by_zone)) {
        metrics.by_zone[zone] = { hits: 0, misses: 0 };
    }
    for (const q of Object.keys(metrics.responseTimes)) {
        metrics.responseTimes[q] = [];
    }
    fs.writeFileSync(FILE_PATH, csvHeaders);
    console.log('[METRICAS] Métricas reiniciadas y CSV limpiado');
    res.json({ ok: true });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        service: 'metricas',
        total_requests: metrics.totalRequests,
        uptime_seconds: (Date.now() - metrics.startTime) / 1000
    });
});

function startServer() {
    app.listen(PORT, () => {
        console.log(`[METRICAS] Servicio de metricas corriendo en puerto ${PORT}`);
        console.log(`[METRICAS] Archivo CSV: ${FILE_PATH}`);
        console.log(`[METRICAS] Cache efficiency habilitada según fórmula del PDF.`);
    });
}

if (redisReady) {
    startServer();
} else {
    const timeout = setTimeout(() => {
        console.log('[METRICAS] Iniciando servidor sin esperar Redis (timeout)');
        startServer();
    }, 10000);
    client.once('connect', () => {
        clearTimeout(timeout);
        startServer();
    });
}