# common-events

Shared domain model for the IoT platform.

This module is a library, not an application. It provides the event types and Avro helpers that are reused by the bridge and all processors.

## Purpose

- Define the shared sensor and ingestion event model
- Keep Avro schema definitions in one place
- Provide conversion helpers between Java records and Avro `GenericRecord`
- Avoid duplicating event contracts across modules

## Event Types

### Sensor events

- `SensorEvent`
- `TemperatureEvent`
- `PressureEvent`

### Ingestion events

- `EnrichedSensorEvent`
- `InvalidIngestionEvent`
- `RetryIngestionEvent`

### Processing events

- `ProcessedSensorEvent`
- `TemperatureAggregateEvent`
- `PressureAggregateEvent`

## Avro Schemas

Schemas are stored under `src/main/resources/avro` and are used by all publisher and consumer modules.

## When to Depend on This Module

Use `common-events` whenever a module needs:

- a shared event type
- a Kafka or Schema Registry payload contract
- a conversion helper for Avro serialization or deserialization

## Build

```bash
./gradlew :common-events:build
```

## Notes

- Keep the module free of application wiring.
- Add new event types here first, then wire the downstream processors to them.
