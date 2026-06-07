# mqtt-to-kafka-bridge

Spring Boot service that receives MQTT sensor payloads, validates them, enriches them, and publishes Avro events to Kafka.

## Role in the Platform

This module is the ingestion edge of the system.
It reads MQTT messages, writes them into an internal queue, and hands them to the ingestion service that performs validation, enrichment, retry handling, and Kafka publishing.

## Inputs

- MQTT sensor messages from the local Mosquitto broker

## Outputs

- `sensor.raw.bmp180`
- `sensor.raw.invalid`
- `sensor.raw.retry`

## Responsibilities

- Subscribe to the configured MQTT topic
- Buffer incoming messages in an internal queue
- Validate and enrich sensor payloads
- Add ingestion metadata such as event id and ingestion timestamp
- Publish Avro payloads to Kafka with retry handling

## Important Configuration

Configuration is driven by `src/main/resources/application.yaml` and environment variables.

Relevant settings:

- `MQTT_BROKER`
- `MQTT_TOPIC`
- `KAFKA_BOOTSTRAP_SERVERS`
- `SCHEMA_REGISTRY_URL`
- `INGESTION_QUEUE_CAPACITY`
- `INGESTION_MAX_PUBLISH_ATTEMPTS`

## Main Packages

- `org.kergru.mqtt2kafkabridge` - Spring Boot application and ingestion components
- `MqttSubscriber` - MQTT consumer entry point
- `IngestionService` - queue consumer, validation, enrichment, Kafka publishing
- `KafkaEventPublisher` - Avro producer wrapper

## Build

```bash
./gradlew :mqtt-to-kafka-bridge:build
```

## Run Locally

The bridge expects Kafka, Schema Registry, and Mosquitto to be available locally.
The full stack is started from the repository root:

```bash
docker compose up -d --build
```

## Notes

- Keep MQTT subscription logic thin.
- All domain event types should come from `common-events`.
- Prefer adding new ingestion rules in `IngestionService` rather than in the subscriber callback.
