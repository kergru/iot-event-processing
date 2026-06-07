# event-processor-spring-cloud-function

Spring Cloud Function module that splits the enriched sensor stream into processing and measurement-specific topics.

## Role in the Platform

This module performs the fan-out step after ingestion.
It consumes the raw enriched sensor topic and publishes three downstream event streams:

- processed events for MongoDB persistence
- temperature events for Kafka Streams
- pressure events for Flink

## Inputs

- `sensor.raw.bmp180`

## Outputs

- `sensor.processed`
- `sensor.temperature`
- `sensor.pressure`

## Responsibilities

- Deserialize Avro payloads from Kafka
- Split enriched sensor events into specialized event types
- Publish the split events with native Kafka serialization
- Keep the transformation logic centralized and reusable

## Main Packages

- `org.kergru.iotplatform.processor.function` - Spring Boot app, function wiring, and processor configuration
- `SplitSensorEvents` - domain split logic
- `AvroMessageCodec` - Avro serialization helper
- `SensorSplitFunctionConfiguration` - Spring Cloud Function bindings

## Function Binding Model

The function is exposed as `splitSensorEvent` and is wired to the Kafka input and output bindings in `src/main/resources/application.yaml`.

## Build

```bash
./gradlew :event-processor-spring-cloud-function:build
```

## Run Locally

The processor is part of the Docker Compose stack and is started automatically with the rest of the system.

```bash
docker compose up -d --build
```

## Notes

- This module does not aggregate data.
- It is responsible for event routing and normalization only.
- Downstream processors should consume the specialized topics, not the processed topic.
