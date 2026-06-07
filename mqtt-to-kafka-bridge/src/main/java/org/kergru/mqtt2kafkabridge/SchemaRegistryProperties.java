package org.kergru.mqtt2kafkabridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("iot.kafka.schema-registry")
public record SchemaRegistryProperties(String url) {}
