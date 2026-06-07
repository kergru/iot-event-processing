package org.kergru.mqtt2kafkabridge;

import io.github.resilience4j.retry.Retry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetryConfig {

  @Bean
  public Retry kafkaPublishRetry(IngestionProperties properties) {
    io.github.resilience4j.retry.RetryConfig config =
        io.github.resilience4j.retry.RetryConfig.custom()
            .maxAttempts(properties.maxPublishAttempts())
            .waitDuration(properties.retryBackoff())
            .retryExceptions(Exception.class)
            .build();

    return Retry.of("kafka-publish", config);
  }
}
