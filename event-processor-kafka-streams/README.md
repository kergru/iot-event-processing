# event-processor-kafka-streams

Kafka Streams processor that computes windowed aggregates for temperature events.

## Role in the Platform

This module consumes the temperature stream produced by the Spring Cloud Function processor and turns it into rolling temperature aggregates.

## Inputs

- `sensor.temperature`

## Outputs

- `sensor.temperature.aggregates.kafka-streams`

## Aggregation Model

The current implementation uses a 5-minute hopping window with a 1-minute advance and computes:

- event count
- minimum temperature
- maximum temperature
- average temperature

The aggregate key is derived from device id and sensor type.

## Main Packages

- `org.kergru.iotplatform.processor.kafkastreams` - application entry point and Kafka Streams wiring
- `TemperatureStats` - in-memory aggregation state
- `TemperatureEventSerde` - custom serde for temperature events
- `TemperatureStatsSerde` - serde for aggregation state

## Build

```bash
./gradlew :event-processor-kafka-streams:build
```

## Run Locally

The processor is part of the Docker Compose stack and starts automatically.

```bash
docker compose up -d --build
```

## Notes

- This module is focused on Kafka-native aggregation.
- Keep the input topic limited to temperature events.
- If you add more advanced windows or alerts, create a dedicated aggregate event type in `common-events`.
