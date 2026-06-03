const { Kafka } = require('kafkajs');

// Nos conectamos al Kafka que acabas de levantar en Docker
const kafka = new Kafka({ 
    clientId: 'tester', 
    brokers: ['localhost:9092'] 
});
const producer = kafka.producer();

async function run() {
    await producer.connect();
    console.log("=========================================");
    console.log("Productor de Prueba Conectado.");
    
    // Este es el JSON con el formato exacto que acordaste con Felipe
    const mensajeFalso = {
        id: "msg-prueba-123",
        timestamp_creacion: Date.now(),
        tipo_consulta: "Q3",
        datos_consulta: { 
            zone_id: "Z1", 
            conf_min: 0.5 
        },
        retry_count: 0
    };

    console.log(`Enviando mensaje con ID: ${mensajeFalso.id}...`);

    // Inyectamos el mensaje en el tópico principal
    await producer.send({
        topic: 'consultas-geoespaciales',
        messages: [{ value: JSON.stringify(mensajeFalso) }]
    });

    console.log("¡Mensaje inyectado con éxito en Kafka!");
    console.log("=========================================");
    
    await producer.disconnect();
}

run().catch(console.error);