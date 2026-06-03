const axios = require('axios');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const METRICS_URL = process.env.METRICS_URL || 'http://metric-service:4000';
const ZONAS = ['Z1', 'Z2', 'Z3', 'Z4', 'Z5'];
const QUERIES = ['q1', 'q2', 'q3', 'q4', 'q5'];

const DURATION = parseInt(process.env.TEST_DURATION) || 60;
const RPS = parseInt(process.env.RPS) || 10;
const DIST = process.env.DISTRIBUTION || 'zipf';
const TTL = parseInt(process.env.TTL) || 3600;
const TOPIC_PRINCIPAL = process.env.TOPIC_PRINCIPAL || 'consultas-geoespaciales';

const kafka = new Kafka({
    clientId: 'traffic_generator',
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();

function getRandomConfidence() {
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
    const conf_min = getRandomConfidence();
    let params = { conf_min: conf_min, ttl: TTL };

    if (query === 'q4') {
        const { z1, z2 } = getTwoDifferentZones();
        params.z1 = z1;
        params.z2 = z2;
    } else {
        const zone = DIST === 'uniform' ? getUniformZone() : getZipfZone();
        params.zone_id = zone;
    }

    const jsonMensaje = {
        id: uuidv4(),
        timestamp_creacion: Date.now(),
        tipo_consulta: query,
        datos_consulta: params,
        retry_count: 0
    };

    try {
        await producer.send({
            topic: TOPIC_PRINCIPAL,
            messages: [
                { value: JSON.stringify(jsonMensaje) }
            ],
        });

        try {
            axios.post(`${METRICS_URL}/metrics/record`, { event_type: 'sent' }, { timeout: 200 }).catch(() => { });
        } catch (e) { }

        return { success: true };
    } catch (error) {
        console.error(`[GENERADOR] Error Kafka para ${query}: ${error.message}`);
        return { success: false };
    }
}

async function runLoadTest() {
    // Configuración dinámica del experimento en el servicio de métricas
    try {
        const fileName = `experimento_${process.env.DISTRIBUTION || 'default'}.csv`;
        await axios.post(`${METRICS_URL}/metrics/setup`, { file_name: fileName });
        console.log(`[GENERADOR] Configurado experimento en métricas: ${fileName}`);
    } catch (error) {
        console.error('[GENERADOR] Error configurando experimento en métricas:', error.message);
    }

    await producer.connect();

    console.log(`\n[GENERADOR] Iniciando prueba de carga ASÍNCRONA:`);
    console.log(`   Distribucion: ${DIST}`);
    console.log(`   Duracion: ${DURATION} segundos`);
    console.log(`   RPS (Mensajes/s): ${RPS}`);
    console.log(`   Target Topic: ${TOPIC_PRINCIPAL}\n`);

    const endTime = Date.now() + DURATION * 1000;
    let requestCount = 0;

    while (Date.now() < endTime) {
        const batchStart = Date.now();
        const promises = [];

        for (let i = 0; i < RPS; i++) {
            promises.push(sendRequest().then(res => {
                if (res.success) requestCount++;
            }));
        }
        await Promise.all(promises);

        const elapsedTotal = (Date.now() - (endTime - DURATION * 1000)) / 1000;
        if (Math.floor(elapsedTotal / 5) > Math.floor((elapsedTotal - 1) / 5)) {
            console.log(`[GENERADOR] Progreso: ${elapsedTotal.toFixed(0)}s/${DURATION}s - Enviados a Kafka: ${requestCount}`);
        }

        const batchElapsed = Date.now() - batchStart;
        if (batchElapsed < 1000) {
            await new Promise(r => setTimeout(r, 1000 - batchElapsed));
        }
    }

    await producer.disconnect();

    console.log(`\n[GENERADOR] Envío completado. Esperando un momento para la consolidación de métricas distribuidas...`);
    await new Promise(r => setTimeout(r, 5000));

    try {
        const metrics = await axios.get(`${METRICS_URL}/metrics`);
        console.log(`\n${'='.repeat(60)}`);
        console.log(`[GENERADOR] RESULTADOS FINALES - ${DIST.toUpperCase()}`);
        console.log(`${'='.repeat(60)}`);
        console.log(`Metricas Globales:`);
        console.log(`   Total Solicitudes: ${metrics.data.total_requests}`);
        console.log(`   Throughput: ${metrics.data.throughput} consultas/s`);
        console.log(`   Hit Rate: ${(metrics.data.hit_rate * 100).toFixed(2)}%`);

        console.log(`\nResiliencia & Tolerancia a Fallos (Kafka):`);
        console.log(`   Retry Rate: ${metrics.data.retry_rate || 0}`);
        console.log(`   Recovery Rate: ${metrics.data.recovery_rate || 0}`);
        console.log(`   DLQ Rate: ${metrics.data.dlq_rate || 0}`);
        console.log(`   Backlog Size: ${metrics.data.backlog_size || 0}`);
        console.log(`   Recovery Time: ${metrics.data.recovery_time || 0} ms`);

        console.log(`\nLatencia de Extremo a Extremo:`);
        console.log(`   Promedio: ${metrics.data.latency.avg}ms`);
        console.log(`   p50: ${metrics.data.latency.p50}ms`);
        console.log(`   p95: ${metrics.data.latency.p95}ms`);
        console.log(`${'='.repeat(60)}\n`);
    } catch (error) {
        console.error('[GENERADOR] Error obteniendo reporte acumulado de métricas:', error.message);
    }
}

process.on('SIGINT', async () => {
    await producer.disconnect();
    process.exit(0);
});

runLoadTest().catch(console.error);