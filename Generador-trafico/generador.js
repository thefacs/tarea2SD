const axios = require('axios');

const CACHE_URL = process.env.CACHE_URL || 'http://cache-proxy:3000';
const METRICS_URL = process.env.METRICS_URL || 'http://metric-service:4000';
const ZONAS = ['Z1', 'Z2', 'Z3', 'Z4', 'Z5'];
const QUERIES = ['q1', 'q2', 'q3', 'q4', 'q5'];

const DURATION = parseInt(process.env.TEST_DURATION) || 60;
const RPS = parseInt(process.env.RPS) || 10;
const DIST = process.env.DISTRIBUTION || 'zipf';
const TTL = parseInt(process.env.TTL) || 3600;

// --- Nueva función: genera un valor de confianza aleatorio (0.0 a 1.0 con 1 decimal) ---
function getRandomConfidence() {
    // Valores posibles: 0.0, 0.1, 0.2, ..., 1.0
    return (Math.floor(Math.random() * 11) / 10).toFixed(1);
}

function getZipfZone() {
    const r = Math.random();
    if (r < 0.33) return 'Z1';
    if (r < 0.60) return 'Z2';
    if (r < 0.80) return 'Z3';
    if (r < 0.93) return 'Z4';
    return 'Z5';
}

function getUniformZone() {
    return ZONAS[Math.floor(Math.random() * ZONAS.length)];
}

function getRandomQuery() {
    return QUERIES[Math.floor(Math.random() * QUERIES.length)];
}

function getTwoDifferentZones() {
    let z1 = DIST === 'uniform' ? getUniformZone() : getZipfZone();
    let z2 = DIST === 'uniform' ? getUniformZone() : getZipfZone();
    while (z2 === z1) {
        z2 = DIST === 'uniform' ? getUniformZone() : getZipfZone();
    }
    return { z1, z2 };
}

async function sendRequest() {
    const query = getRandomQuery();
    // Usar confianza aleatoria (la clave variará mucho)
    const conf_min = getRandomConfidence();
    let params = { conf_min: conf_min, ttl: TTL };
    let zoneId = null;

    if (query === 'q4') {
        const { z1, z2 } = getTwoDifferentZones();
        params.z1 = z1;
        params.z2 = z2;
        zoneId = z1;
    } else {
        const zone = DIST === 'uniform' ? getUniformZone() : getZipfZone();
        params.zone_id = zone;
        zoneId = zone;
    }

    try {
        const start = Date.now();
        const response = await axios.get(`${CACHE_URL}/query/${query}`, {
            params: params,
            timeout: 5000
        });
        const latency = Date.now() - start;
        const hit = (response.data.source === 'hit');

        await axios.post(`${METRICS_URL}/metrics/record`, {
            query_type: query,
            hit: hit,
            latency_ms: latency,
            zone_id: zoneId,
            cache_key: response.data.cache_key || ''
        }).catch(e => console.error('[GENERADOR] Error registrando métrica:', e.message));

        return { success: true, hit: hit, latency: latency };
    } catch (error) {
        console.error(`[GENERADOR] Solicitud fallida para ${query}: ${error.message}`);
        return { success: false };
    }
}

async function runLoadTest() {
    console.log(`\n[GENERADOR] Iniciando prueba de carga:`);
    console.log(`   Distribucion: ${DIST}`);
    console.log(`   Duracion: ${DURATION} segundos`);
    console.log(`   Solicitudes por segundo: ${RPS}`);
    console.log(`   TTL: ${TTL} segundos`);
    console.log(`   Zonas: ${ZONAS.join(', ')}`);
    console.log(`   Consultas: ${QUERIES.join(', ')}`);
    console.log(`   Nota: conf_min variará aleatoriamente (0.0 a 1.0) para aumentar la variedad de claves.\n`);

    const endTime = Date.now() + DURATION * 1000;
    let requestCount = 0;
    let hitCount = 0;

    while (Date.now() < endTime) {
        const batchStart = Date.now();
        const promises = [];
        for (let i = 0; i < RPS; i++) {
            promises.push(sendRequest().then(res => {
                if (res.success) {
                    requestCount++;
                    if (res.hit) hitCount++;
                }
            }));
        }
        await Promise.all(promises);

        const elapsedTotal = (Date.now() - (endTime - DURATION * 1000)) / 1000;
        if (Math.floor(elapsedTotal / 5) > Math.floor((elapsedTotal - 1) / 5)) {
            console.log(`[GENERADOR] Progreso: ${elapsedTotal.toFixed(0)}s/${DURATION}s - Solicitudes: ${requestCount}`);
        }

        const batchElapsed = Date.now() - batchStart;
        if (batchElapsed < 1000) {
            await new Promise(r => setTimeout(r, 1000 - batchElapsed));
        }
    }

    await new Promise(r => setTimeout(r, 2000));

    try {
        const metrics = await axios.get(`${METRICS_URL}/metrics`);
        console.log(`\n${'='.repeat(60)}`);
        console.log(`[GENERADOR] RESULTADOS FINALES - ${DIST.toUpperCase()}`);
        console.log(`${'='.repeat(60)}`);
        console.log(`Metricas Globales:`);
        console.log(`   Total Solicitudes: ${metrics.data.total_requests}`);
        console.log(`   Hit Rate: ${(metrics.data.hit_rate * 100).toFixed(2)}%`);
        console.log(`   Miss Rate: ${((1 - metrics.data.hit_rate) * 100).toFixed(2)}%`);
        console.log(`   Throughput: ${metrics.data.throughput} solicitudes/segundo`);

        console.log(`\nEficiencia de Caché (según fórmula del PDF):`);
        console.log(`   Cache Efficiency: ${metrics.data.cache_efficiency} ms`);
        console.log(`   t_cache (latencia promedio de aciertos): ${metrics.data.t_cache_avg_ms} ms`);
        console.log(`   t_dh (latencia promedio de fallos): ${metrics.data.t_dh_avg_ms} ms`);

        console.log(`\nLatencia:`);
        console.log(`   Promedio: ${metrics.data.latency.avg}ms`);
        console.log(`   p50: ${metrics.data.latency.p50}ms`);
        console.log(`   p95: ${metrics.data.latency.p95}ms`);
        console.log(`\nCache:`);
        console.log(`   Tasa de expulsión: ${metrics.data.cache.eviction_rate_per_min} expulsiones/minuto`);
        console.log(`\nPor Tipo de Consulta:`);
        for (const [q, data] of Object.entries(metrics.data.by_query)) {
            console.log(`   ${q.toUpperCase()}: ${(data.hit_rate * 100).toFixed(1)}% hit (${data.hits} hits / ${data.misses} misses)`);
        }
        console.log(`${'='.repeat(60)}\n`);
    } catch (error) {
        console.error('[GENERADOR] Error obteniendo metricas finales:', error.message);
    }
}

runLoadTest().catch(console.error);