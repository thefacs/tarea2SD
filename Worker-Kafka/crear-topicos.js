const { Kafka } = require('kafkajs');

const kafka = new Kafka({ 
    clientId: 'admin-setup', 
    brokers: ['localhost:9092'] 
});
const admin = kafka.admin();

async function run() {
    console.log("Conectando al administrador de Kafka...");
    await admin.connect();
    
    console.log("Creando tópicos del sistema...");
    await admin.createTopics({
        topics: [
            { topic: 'consultas-geoespaciales' },
            { topic: 'consultas-reintento' },
            { topic: 'consultas-dlq' }
        ],
        waitForLeaders: true
    });

    console.log("Topicos creados con exito, el Kafka ya está preparado");
    await admin.disconnect();
}

run().catch(console.error);