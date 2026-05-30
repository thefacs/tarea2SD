const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');

const app = express();
const PORT = 3001;

// Zonas (BBox)
const ZONES = {
    'Z1': { id: 'Z1', lat_min: -33.445, lat_max: -33.420, lon_min: -70.640, lon_max: -70.600 },
    'Z2': { id: 'Z2', lat_min: -33.420, lat_max: -33.390, lon_min: -70.600, lon_max: -70.550 },
    'Z3': { id: 'Z3', lat_min: -33.530, lat_max: -33.490, lon_min: -70.790, lon_max: -70.740 },
    'Z4': { id: 'Z4', lat_min: -33.460, lat_max: -33.430, lon_min: -70.670, lon_max: -70.630 },
    'Z5': { id: 'Z5', lat_min: -33.470, lat_max: -33.430, lon_min: -70.810, lon_max: -70.760 }
};

// Área de cada zona en km² aproximada
const ZONE_AREA_KM2 = {};
for (const [id, z] of Object.entries(ZONES)) {
    const dLat = (z.lat_max - z.lat_min) * 111.0;
    const dLon = (z.lon_max - z.lon_min) * 92.67; // approx cos(33.4) * 111
    ZONE_AREA_KM2[id] = dLat * dLon;
}

// Estructura en memoria
// data["Z1"] = [{ lat, lon, area, confidence }, ...]
const data = {
    'Z1': [],
    'Z2': [],
    'Z3': [],
    'Z4': [],
    'Z5': []
};

// Identifica si un punto está dentro de un bbox
function getZoneId(lat, lon) {
    for (const z of Object.values(ZONES)) {
        if (lat >= z.lat_min && lat <= z.lat_max && lon >= z.lon_min && lon <= z.lon_max) {
            return z.id;
        }
    }
    return null;
}

// Cargar dataset en memoria
function loadData() {
    console.log("Iniciando carga de datos en memoria...");
    return new Promise((resolve, reject) => {
        fs.createReadStream('/app/data/open_buildings_v3_points_your_own_wkt_polygon.csv')
            .pipe(csv())
            .on('data', (row) => {
                const lat = parseFloat(row.latitude);
                const lon = parseFloat(row.longitude);
                const area = parseFloat(row.area_in_meters);
                const conf = parseFloat(row.confidence);

                if (!isNaN(lat) && !isNaN(lon)) {
                    const zoneId = getZoneId(lat, lon);
                    if (zoneId) {
                        data[zoneId].push({ lat, lon, area, confidence: conf });
                    }
                }
            })
            .on('end', () => {
                console.log("Carga de datos finalizada.");
                for (const z in data) {
                    console.log(`Zona ${z}: ${data[z].length} registros calculados. Área: ${ZONE_AREA_KM2[z].toFixed(2)} km2`);
                }
                resolve();
            })
            .on('error', reject);
    });
}

// Funciones Q1-Q5

function q1_count(zone_id, confidence_min = 0.0) {
    const records = data[zone_id] || [];
    return records.reduce((count, r) => count + (r.confidence >= confidence_min ? 1 : 0), 0);
}

function q2_area(zone_id, confidence_min = 0.0) {
    const records = data[zone_id] || [];
    let sum = 0;
    let n = 0;
    for (const r of records) {
        if (r.confidence >= confidence_min) {
            sum += r.area;
            n++;
        }
    }
    return {
        avg_area: n > 0 ? (sum / n) : 0,
        total_area: sum,
        n: n
    };
}

function q3_density(zone_id, confidence_min = 0.0) {
    const count = q1_count(zone_id, confidence_min);
    const area = ZONE_AREA_KM2[zone_id] || 1;
    return count / area; // density per km2
}

function q4_compare(zone_a, zone_b, confidence_min = 0.0) {
    const da = q3_density(zone_a, confidence_min);
    const db = q3_density(zone_b, confidence_min);
    return {
        zone_a: da,
        zone_b: db,
        winner: da > db ? zone_a : zone_b
    };
}

function q5_confidence_dist(zone_id, bins = 5) {
    const records = data[zone_id] || [];
    const validScores = records.map(r => r.confidence);
    const bucketSize = 1.0 / bins;

    // Inicializar buckets (0 a 1)
    const distribution = Array.from({ length: bins }, (_, i) => ({
        bucket: i,
        min: i * bucketSize,
        max: (i === bins - 1) ? 1.0 : (i + 1) * bucketSize,
        count: 0
    }));

    for (const conf of validScores) {
        let b = Math.floor(conf / bucketSize);
        if (b >= bins) b = bins - 1; // Para aquellos con conf = 1.0
        distribution[b].count++;
    }
    return distribution;
}

// Rutas Express (app.use y demás)
app.use(express.json());

app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'response-generator' });
});

// Q1: Conteo
app.get('/query/q1', (req, res) => {
    const zone_id = req.query.zone_id;
    const conf_min = parseFloat(req.query.conf_min || 0.0);
    const result = q1_count(zone_id, conf_min);
    res.json({ count: result });
});

// Q2: Área
app.get('/query/q2', (req, res) => {
    const zone_id = req.query.zone_id;
    const conf_min = parseFloat(req.query.conf_min || 0.0);
    const result = q2_area(zone_id, conf_min);
    res.json(result);
});

// Q3: Densidad
app.get('/query/q3', (req, res) => {
    const zone_id = req.query.zone_id;
    const conf_min = parseFloat(req.query.conf_min || 0.0);
    const result = q3_density(zone_id, conf_min);
    res.json({ density: result });
});

// Q4: Comparación
app.get('/query/q4', (req, res) => {
    const z1 = req.query.z1;
    const z2 = req.query.z2;
    const conf_min = parseFloat(req.query.conf_min || 0.0);
    const result = q4_compare(z1, z2, conf_min);
    res.json(result);
});

// Q5: Histograma
app.get('/query/q5', (req, res) => {
    const zone_id = req.query.zone_id;
    const bins = parseInt(req.query.bins || 5);
    const result = q5_confidence_dist(zone_id, bins);
    res.json(result);
});


loadData().then(() => {
    app.listen(PORT, () => {
        console.log(`Response Generator corriendo en el puerto ${PORT}`);
    });
}).catch(err => {
    console.error("Error al cargar datos:", err);
    process.exit(1);
});