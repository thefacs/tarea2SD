# Sistema Distribuido Orientado a Eventos y Caché Resiliente para Consultas Geoespaciales

Este proyecto implementa un sistema distribuido orientado a eventos sobre el dataset **Google Open Buildings** (Región Metropolitana de Santiago), utilizando una arquitectura basada en microservicios con **Apache Kafka (KRaft)** para mensajería, **Redis** como caché en memoria y múltiples workers para procesamiento concurrente.  

El sistema evoluciona desde un modelo síncrono hacia un pipeline asíncrono, escalable y tolerante a fallos.

---

## Arquitectura del sistema

El sistema está compuesto por los siguientes microservicios:

| Servicio                  | Puerto | Función                                   |
|--------------------------|--------|-------------------------------------------|
| Generador de tráfico     | —      | Produce carga sintética de consultas      |
| Kafka Broker (KRaft)     | 9092   | Sistema de mensajería distribuida         |
| Consumidor base          | —      | Workers Kafka Consumer                    |
| Generador de respuestas  | 3001   | Procesamiento geoespacial en memoria      |
| Servicio de métricas     | 4000   | Registro y análisis de rendimiento         |
| Redis                    | 6379   | Caché en memoria                          |

---

## A tener en cuenta

Debido al tamaño del dataset, no se incluye en el repositorio de GitHub. Por esta razón, debe descargarse manualmente y ubicarse en la carpeta `data`.

Dentro de esta carpeta debe incluirse el dataset con el siguiente nombre:

`open_buildings_v3_points_your_own_wkt_polygon.csv`

### Estructura esperada del proyecto

```
.
├── data/
│   └── open_buildings_v3_points_your_own_wkt_polygon.csv
├── docker-compose.yml
├── Generador-respuestas/
├── Generador-trafico/
├── Metricas/
├── metrics_data/
├── qa_test.sh
├── Servidor/
└── Worker-Kafka/
```

---

## Ejecución de escenarios

Antes de ejecutar cualquier escenario, se recomienda limpiar el entorno:

```bash
docker-compose down -v --remove-orphans
docker volume prune -f
```

### Escenario 2 (1 Worker)

```bash
export METRICS_FILE_NAME=escenario2.csv
docker-compose up -d
docker-compose --profile manual run --rm -e DISTRIBUTION=uniform generador-trafico
```

---

### Escenario 3 (3 Workers)

```bash
export METRICS_FILE_NAME=escenario3.csv
docker-compose up -d --scale consumidor-base=3
sleep 20
docker-compose --profile manual run --rm -e DISTRIBUTION=uniform generador-trafico
```

---

### Escenario 4 (Falla temporal en backend)

```bash
export METRICS_FILE_NAME=escenario4.csv
docker-compose up -d --scale consumidor-base=3
docker-compose --profile manual run --rm -e DISTRIBUTION=uniform generador-trafico
```

---

### Escenario 5 (Falla permanente en backend)

```bash
export METRICS_FILE_NAME=escenario5.csv
docker-compose down -v --remove-orphans
docker-compose up -d --scale consumidor-base=3
docker-compose --profile manual run --rm -e DISTRIBUTION=uniform generador-trafico
```

---

## Resultados principales

- El escalamiento horizontal de workers reduce la latencia del sistema.
- Redis mejora el rendimiento gracias al caching de consultas repetidas.
- Kafka asegura tolerancia a fallos mediante persistencia de eventos.
- El sistema mantiene estabilidad incluso bajo fallas del backend.
- DLQ (Dead Letter Queue) evita pérdida de mensajes críticos.

---

## Conclusiones

- La arquitectura orientada a eventos mejora la escalabilidad frente al modelo síncrono.
- El uso de Kafka desacopla productores y consumidores.
- Redis reduce significativamente la carga del backend.
- El sistema es resiliente frente a fallas parciales del sistema.
- El rendimiento depende fuertemente del nivel de concurrencia de workers.

---

## Mejoras futuras

- Implementar cache warming para mejorar hit rate inicial.
- Ajustar estrategias de particionado en Kafka.
- Incorporar TTL dinámico en Redis.
- Optimizar balanceo de carga entre consumidores.
- Agregar observabilidad más granular (tracing distribuido).

---

## Integrantes

- Felipe Cuevas  
- Ariel Oyarce