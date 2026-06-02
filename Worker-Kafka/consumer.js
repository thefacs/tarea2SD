const { Kafka } = require('kafkajs');
const axios = require('axios');
const redis = require('redis');

// --- Configuración de Kafka ---
const kafka = new Kafka({
    clientId: 'worker-geoespacial',
    // Usamos variables de entorno para que Felipe pueda inyectar el broker en Docker
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'], 
    retry: {
        initialRetryTime: 100,
        retries: 8
    }
});

// El Consumer y el Producer (para reenviar a reintento/DLQ)
// IMPORTANTE: El groupId debe ser igual para todas las réplicas para el paralelismo
const consumer = kafka.consumer({ groupId: 'grupo-geoespacial' });
const producer = kafka.producer();

// --- Cliente Redis (Listo por si lo necesitas) ---
const redisClient = redis.createClient({ 
    url: process.env.REDIS_URL || 'redis://localhost:6379' 
});
redisClient.on('error', (err) => console.log('[Redis] Error:', err));
// Comentado temporalmente para que no arroje error si no tienes Redis local encendido
// redisClient.connect().catch(console.error);

// --- URLs y Variables de Entorno ---
const RESPONSE_GEN_URL = process.env.RESPONSE_GENERATOR_URL || 'http://localhost:3001';
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || 3);

// --- Nombres de Tópicos (Según el Acuerdo con Felipe) ---
const TOPIC_MAIN = 'consultas-geoespaciales';
const TOPIC_RETRY = 'consultas-reintento';
const TOPIC_DLQ = 'consultas-dlq';

// --- Lógica Principal de Resiliencia ---
async function procesarMensaje(mensajeJSON) {
    const { id, tipo_consulta, datos_consulta, retry_count } = mensajeJSON;
    
    try {
        console.log(`\n[Worker] 🔄 Intentando procesar ${tipo_consulta} (ID: ${id}) | Intento actual: ${retry_count}`);
        
        // Llamada HTTP al Generador de Respuestas
        const response = await axios.get(`${RESPONSE_GEN_URL}/query/${tipo_consulta}`, {
            params: datos_consulta,
            timeout: 5000 // 5 segundos máximo, si no responde, cae al catch
        });

        console.log(`[Worker] ÉXITO. Respuesta de ${tipo_consulta} (ID: ${id}):`, response.data);
        
    } catch (error) {
        console.error(`[Worker] ERROR en la consulta ${id}. Detalle: ${error.message}`);
        
        // --- LÓGICA DE REINTENTOS Y DLQ ---
        const nuevoRetryCount = retry_count + 1;
        mensajeJSON.retry_count = nuevoRetryCount;

        if (nuevoRetryCount >= MAX_RETRIES) {
            // Se superó el límite: A la Dead Letter Queue (DLQ)
            console.warn(`[Worker] MAX RETRIES ALCANZADO (${id}). Enviando a DLQ.`);
            await producer.send({
                topic: TOPIC_DLQ,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
            // Aquí en el futuro puedes sumar la métrica a Redis: DLQ Rate
        } else {
            // Aún hay intentos: Al Tópico de Reintento
            console.info(`[Worker] Reencolando mensaje (${id}) al Tópico de Reintento (Intento ${nuevoRetryCount}).`);
            await producer.send({
                topic: TOPIC_RETRY,
                messages: [{ value: JSON.stringify(mensajeJSON) }]
            });
            // Aquí en el futuro puedes sumar la métrica a Redis: Retry Rate
        }
    }
}

// --- Arranque del Sistema ---
async function run() {
    // 1. Conectar a Kafka
    await producer.connect();
    await consumer.connect();

    // 2. Suscribirse a los tópicos de trabajo
    await consumer.subscribe({ topic: TOPIC_MAIN, fromBeginning: true });
    await consumer.subscribe({ topic: TOPIC_RETRY, fromBeginning: true });

    console.log('========================================');
    console.log('[Worker]Conectado a Kafka y escuchando mensajes');
    console.log('========================================');

    // 3. Bucle infinito de procesamiento
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const jsonMsg = JSON.parse(message.value.toString());
            
            // Si viene de reintento, aplicamos un pequeño delay (Backoff)
            if (topic === TOPIC_RETRY) {
                console.log(`[Worker]Esperando 2 segundos antes de reintentar`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }

            // Pasamos el JSON a la función de procesamiento
            await procesarMensaje(jsonMsg);
        },
    });
}

// Ejecutar e imprimir cualquier error fatal
run().catch(error => {
    console.error('[Worker] Error fatal al arrancar Kafka:', error);
    process.exit(1);
});