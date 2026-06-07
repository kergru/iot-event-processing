package org.kergru.iot.event.sensor;

import java.time.Instant;
import java.util.UUID;

public record TemperatureEvent(
    UUID eventId,
    UUID sourceEventId,
    Instant ingestionTimestamp,
    Instant measurementTimestamp,
    String deviceId,
    String sensor,
    Double temperatureC
) {}
