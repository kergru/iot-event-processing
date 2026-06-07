# event-processor-flink

Apache Flink processor that computes windowed aggregates for pressure events.

## Role in the Platform

This module consumes the pressure stream produced by the Spring Cloud Function processor and publishes pressure aggregates to Kafka.

## Inputs

- `sensor.pressure`

## Outputs

- `sensor.pressure.aggregates.flink`

## Aggregation Model

The current implementation uses a 5-minute tumbling processing-time window and computes:

- event count
- minimum pressure
- maximum pressure
- average pressure
- pressure delta
- trend classification

The aggregate key is derived from device id and sensor type.

## Main Packages

- `org.kergru.iotplatform.processor.flink` - Flink job, source/sink setup, and aggregation functions
- `PressureAggregateFunction` - aggregation state reducer
- `PressureAggregateWindowFunction` - window finalization
- `PressureEventDeserializer` - Kafka to domain conversion
- `PressureAggregateSerializer` - domain to Kafka conversion

## Build

```bash
./gradlew :event-processor-flink:build
```

## Run Locally

The processor is part of the Docker Compose stack and starts automatically.

```bash
docker compose up -d --build
```

## Notes

- This module is a good place for event-time, windowing, and time-series style stream logic.
- Keep Flink-specific code here and keep event contracts in `common-events`.
