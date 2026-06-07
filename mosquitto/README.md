# Mosquitto

This folder contains the Mosquitto MQTT broker configuration used by the IoT Event Processing Docker Compose stack.

## Role In The Pipeline

Mosquitto is the local MQTT ingestion point. Sensor publishers send BMP180 measurements to MQTT topics such as:

```text
sensors/raspberrypi/bmp180
```

The `mqtt-to-kafka-bridge` service subscribes to `sensors/#`, validates the JSON payload, enriches it, and publishes Avro events to Kafka.

## Configuration

The Compose service mounts `mosquitto/config` into the container as read-only configuration. The current config enables:

- TCP MQTT listener on port `1883`
- WebSocket listener on port `9001`
- anonymous access for local development

## Local Usage

Start the broker through the root Compose stack:

```bash
docker compose up -d mosquitto
```

Check that it is running:

```bash
docker compose ps mosquitto
```

From a Raspberry Pi or another machine on the same LAN, use the Docker host IP as `MQTT_BROKER` and port `1883`.

This configuration is intended for local/demo use. Do not expose it to an untrusted network without adding authentication and transport security.
