package org.kergru.iot.event.processing;

import java.time.Instant;
import java.util.UUID;

public record ProcessedSensorEvent(
    UUID eventId,
    UUID sourceEventId,
    Instant ingestionTimestamp,
    Instant processedAt,
    String processor,
    String deviceId,
    String sensor,
    Double temperatureC,
    Double pressureHpa
) {}
