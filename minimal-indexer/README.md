# Minimal Indexer

El módulo `minimal-indexer` orquesta el flujo completo de indexación dentro de Aether.

## Responsabilidad

Recibe documentos desde Kafka (salida del crawler), ejecuta la transformación de contenido para generar tags y metadatos normalizados, y envía el resultado serializado en Avro al tópico de salida para su persistencia posterior.

## Componentes

- **`Handler`**: estructura principal del indexador.
  - `IndexerConfig`: configuración de concurrencia (workers).
  - `ConsumerService`: servicio para consumir mensajes Kafka de entrada.
  - `TransformerService`: servicio para transformar payloads de crawler en payloads de indexación.
  - `StorageService`: servicio para publicar payloads transformados en Kafka.

## Flujo de ejecución

1. `InitIndexer(ctx)` crea un pool de workers (`chan struct{}`) según `IndexerConfig.Workers`.
2. Inicia el loop de consumo con `ConsumerService.Consumer(...)`.
3. Por cada mensaje Kafka:
   - deserializa a `KafkaCrawlerPayload`.
   - adquiere un slot del pool para limitar concurrencia.
   - lanza una goroutine que invoca `Indexer(payload)`.
4. `Indexer(payload)`:
   - transforma el contenido mediante `TransformerService.Transform`.
   - serializa/publica el resultado con `StorageService.KafkaStorage(...)`.

## Concurrencia

- El número de tareas simultáneas está acotado por `workers`.
- Se usa `sync.WaitGroup` para esperar goroutines en el cierre del proceso.
- Cada tarea libera su slot del pool al finalizar.

## Dependencias

- `modules/consumer`: lectura de eventos desde Kafka.
- `modules/transformer`: extracción/normalización de tags y estructura final.
- `modules/storage`: escritura en tópico Kafka de salida.
- `utils/BuildPayloadAvro`: codificación del payload para schema registry + Avro.

## Configuración relacionada

Parámetros consumidos de `config.json` que impactan este módulo:

- `workers`
- `brokers`
- `consumer-topic`
- `producer-topic`
- `group-id`
- `retry-delays`

## Notas operativas

- Si falla la deserialización del mensaje de entrada, el mensaje se descarta y se registra error.
- Si falla la transformación o el almacenamiento, se registra error en logs.
- La capa de storage aplica reintentos exponiendo el número de intento vía `retry-delays`.
