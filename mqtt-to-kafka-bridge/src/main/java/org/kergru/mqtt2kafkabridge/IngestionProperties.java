package org.kergru.mqtt2kafkabridge;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ingestion")
public record IngestionProperties(
    int queueCapacity,
    Duration offerTimeout,
    int workers,
    int maxPublishAttempts,
    Duration retryBackoff
) {}
