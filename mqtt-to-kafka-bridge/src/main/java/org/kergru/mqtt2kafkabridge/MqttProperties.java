package org.kergru.mqtt2kafkabridge;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mqtt")
public record MqttProperties(
    String broker,
    String clientId,
    String topic,
    Duration reconnectBackoff
) {}
