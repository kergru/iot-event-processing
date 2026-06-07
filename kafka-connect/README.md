# Kafka Connect

This folder contains the custom Kafka Connect image used by the IoT Event Processing Docker Compose stack.

The base image does not include the MongoDB sink connector, so the local `Dockerfile` extends `confluentinc/cp-kafka-connect-base:8.1.0` and installs the connector with Confluent Hub:

```dockerfile
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
```

## Role In The Pipeline

Kafka Connect persists selected Kafka topics into MongoDB:

- `sensor.processed` -> `iot.sensor_measurements`
- `sensor.temperature.aggregates.kafka-streams` -> `iot.temperature_aggregates`
- `sensor.pressure.aggregates.flink` -> `iot.pressure_aggregates`

The connector definitions are created by the `kafka-connect-init` service in the root `docker-compose.yml` after the Kafka Connect REST API is reachable.

## Local Usage

Build and start Kafka Connect through the root Compose stack:

```bash
docker compose up -d --build kafka-connect kafka-connect-init
```

Useful checks:

```bash
curl -fsS http://localhost:8083/connectors
curl -fsS http://localhost:8083/connectors/mongodb-sink-sensor-processed/status
curl -fsS http://localhost:8083/connectors/mongodb-sink-temperature-aggregates/status
curl -fsS http://localhost:8083/connectors/mongodb-sink-pressure-aggregates/status
```

This folder is still needed as long as the stack uses Kafka Connect to persist Avro Kafka records into MongoDB.
