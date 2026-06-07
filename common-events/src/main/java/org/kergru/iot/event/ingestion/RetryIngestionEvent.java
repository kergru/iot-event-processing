package org.kergru.iot.event.ingestion;

import java.time.Instant;
import java.util.UUID;

public record RetryIngestionEvent(
    UUID eventId,
    Instant ingestionTimestamp,
    String mqttTopic,
    String reason,
    int attempts,
    String rawPayload
) {}
