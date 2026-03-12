# Minimal Indexer

The `minimal-indexer` module orchestrates the complete indexing flow inside Aether.

## Responsibility

It consumes documents from Kafka (crawler output), transforms content to generate tags and normalized metadata, and publishes serialized Avro output to the destination topic for downstream persistence.

## Components

- **`Handler`**: main indexer orchestrator.
  - `IndexerConfig`: concurrency configuration (`workers`).
  - `ConsumerService`: consumes input Kafka messages.
  - `TransformerService`: converts crawler payloads into indexer payloads.
  - `StorageService`: publishes transformed payloads to Kafka.

## Execution Flow

1. `InitIndexer(ctx)` creates a worker pool (`chan struct{}`) using `IndexerConfig.Workers`.
2. It starts the consumer loop via `ConsumerService.Consumer(...)`.
3. For each Kafka message:
   - Deserialize into `KafkaCrawlerPayload`.
   - Acquire a pool slot to enforce concurrency limits.
   - Launch a goroutine that calls `Indexer(payload)`.
4. `Indexer(payload)`:
   - Transforms content using `TransformerService.Transform`.
   - Serializes and publishes output with `StorageService.KafkaStorage(...)`.

## Concurrency

- Maximum parallel tasks are bounded by `workers`.
- `sync.WaitGroup` is used to wait for goroutines during shutdown.
- Each task releases its pool slot when it finishes.

## Dependencies

- `modules/consumer`: reads events from Kafka.
- `modules/transformer`: extracts/normalizes tags and final document structure.
- `modules/storage`: writes to the output Kafka topic.
- `utils/BuildPayloadAvro`: encodes payloads for Schema Registry + Avro.

## Related Configuration

`config.json` keys that affect this module:

- `workers`
- `brokers`
- `consumer-topic`
- `producer-topic`
- `group-id`
- `retry-delays`

## Operational Notes

- If input message deserialization fails, the message is discarded and an error is logged.
- If transformation or storage fails, errors are logged.
- The storage layer retries according to `retry-delays`.
