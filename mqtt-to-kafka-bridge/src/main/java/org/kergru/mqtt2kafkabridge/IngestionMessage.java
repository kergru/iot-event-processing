package org.kergru.mqtt2kafkabridge;

import java.time.Instant;

public record IngestionMessage(
    String mqttTopic,
    byte[] payload,
    Instant receivedAt
) {}
