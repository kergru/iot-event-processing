package org.kergru.mqtt2kafkabridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("iot.kafka.topics")
public record KafkaTopicProperties(
    String sensorRaw,
    String sensorInvalid,
    String sensorRetry
) {}
