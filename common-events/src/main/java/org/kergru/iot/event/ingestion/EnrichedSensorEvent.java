package org.kergru.iot.event.ingestion;

import java.time.Instant;
import java.util.UUID;
import org.kergru.iot.event.sensor.SensorEvent;

public record EnrichedSensorEvent(
    UUID eventId,
    Instant ingestionTimestamp,
    SensorEvent payload
) {}
