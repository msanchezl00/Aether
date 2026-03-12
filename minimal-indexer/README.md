# 🧠 Módulo `minimal-indexer`

El **indexer** es el módulo que transforma la información cruda del crawler en un formato enriquecido y estandarizado para su almacenamiento y consumo analítico. En términos generales, actúa como la **capa de normalización y etiquetado semántico** entre crawling y almacenamiento distribuido.

## Papel del indexer dentro de Aether

Dentro del pipeline global:

1. `minimal-crawler` publica documentos crudos en Kafka (`consumer-topic`).
2. `minimal-indexer` consume esos eventos, extrae estructura útil (dominio, ruta, tags, contenido), y serializa a Avro.
3. Publica el resultado en otro tópico (`producer-topic`) para que Kafka Connect/HDFS lo persista.

Con esto, el sistema pasa de “contenido web bruto” a “documentos indexables y consistentes”.

## Estructura general del módulo

```text
minimal-indexer/
├── main.go                        # bootstrap: carga config/dataset, crea servicios y arranca el indexer
├── config.json                    # configuración operativa (Kafka, workers, retries, topics)
├── dataset.json                   # diccionario de keywords y patrones regex para etiquetado
├── Models/                        # contratos de datos (crawler payload, index payload, schema Avro)
├── modules/
│   ├── consumer/                  # lectura continua desde Kafka
│   ├── transformer/               # transformación y generación de tags
│   ├── storage/                   # publicación en Kafka con reintentos
│   ├── indexer/                   # orquestación del flujo consume -> transforma -> almacena
│   └── config/                    # tipos/config compartida y logger
└── utils/                         # helpers de URL y serialización Avro
```

## Cómo funciona (flujo interno)

### 1) Inicialización (`main.go`)

- Inicializa logger.
- Carga `config.json` y `dataset.json`.
- Permite sobreescribir brokers/topics con variables de entorno (`CONF_BROKERS`, `CONF_PRODUCER_TOPIC`, `CONF_CONSUMER_TOPIC`).
- Construye los servicios (`consumer`, `transformer`, `storage`) y el `indexer.Handler`.
- Arranca `InitIndexer(ctx)`.

### 2) Consumo concurrente (`modules/indexer` + `modules/consumer`)

- Se abre un consumidor Kafka por `group-id`.
- Por cada mensaje:
  - Se deserializa `KafkaCrawlerPayload`.
  - Se procesa en goroutine.
  - Se limita concurrencia mediante un pool (`workers`) para no saturar recursos.

### 3) Transformación semántica (`modules/transformer`)

Para cada documento:

- Extrae `domain` y `path` desde la URL.
- Genera tags por:
  - coincidencia de palabras clave (`dataset.json > keywords`),
  - extracción dinámica por regex (`dataset.json > patterns`),
  - señales técnicas (scripts, styles, https, archivos),
  - heurísticas por dominio/path (`github`, `shop`, `login`, `news`, etc.).
- Genera `KafkaIndexerPayload` con fecha, ruta sanitizada (`path`) y ruta real (`real_path`).

### 4) Serialización y publicación (`utils/indexerUtils` + `modules/storage`)

- Convierte el payload a formato nativo Avro.
- Obtiene/crea schema en Schema Registry (`parquet_data-value`).
- Publica el mensaje en Kafka con wire format de Confluent (magic byte + schema ID + binario Avro).
- Si falla la publicación, ejecuta reintentos según `retry-delays`.

## Cosas importantes a tener en cuenta

### 1. Configuración clave

- `workers`: controla paralelismo de indexación.
- `retry-delays`: define estrategia de reintento progresivo para escritura en Kafka.
- `min-bytes` / `max-bytes`: afectan throughput/latencia de lectura en Kafka.
- `consumer-topic` y `producer-topic`: deben estar alineados con crawler y sink posterior.

### 2. Dependencias externas críticas

El módulo depende de que estén disponibles:

- **Kafka broker**.
- **Schema Registry** en `http://schema-registry:8081`.
- Tópicos Kafka esperados.

Si Schema Registry o Kafka no están accesibles, la serialización/publicación fallará.

### 3. Semántica de tags

- El etiquetado mezcla reglas estáticas y dinámicas.
- `dataset.json` es la fuente principal para ajustar comportamiento sin tocar código.
- Cambios en regex pueden aumentar precisión o introducir ruido: conviene validarlos con muestras reales.

### 4. Concurrencia y ciclo de vida

- La indexación usa goroutines con límite por pool.
- El `consumer` corre en loop continuo hasta cancelación del contexto.
- Es recomendable cerrar el writer de Kafka en apagados controlados para un shutdown más limpio.

### 5. Compatibilidad de esquema

- El contrato Avro (`Models/AvroSchemas.go`) define qué consume downstream.
- Cambios de schema deben gestionarse con cuidado para evitar romper consumidores/sinks.

## Ejecución local

Desde `minimal-indexer/`:

```bash
go run main.go
```

O con Docker (según el `docker-compose` del proyecto).

## Resumen funcional

El indexer no “busca” ni “rankea”; su misión es **transformar y enriquecer** documentos para que el resto del sistema trabaje sobre datos limpios, etiquetados y compatibles con almacenamiento distribuido. Es, en la práctica, el puente entre el dato crudo del crawler y el dato indexable para analítica/búsqueda.
