package org.kergru.iot.event.ingestion;

import java.time.Instant;
import java.util.UUID;

public record InvalidIngestionEvent(
    UUID eventId,
    Instant ingestionTimestamp,
    String mqttTopic,
    String reason,
    String rawPayload
) {}
