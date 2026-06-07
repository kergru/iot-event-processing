package org.kergru.mqtt2kafkabridge;

import io.github.resilience4j.retry.Retry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.ingestion.InvalidIngestionEvent;
import org.kergru.iot.event.ingestion.RetryIngestionEvent;
import org.kergru.iot.event.sensor.SensorEvent;
import org.springframework.kafka.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class IngestionService {

  private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

  private final IngestionQueue queue;
  private final ObjectMapper mapper;
  private final KafkaEventPublisher publisher;
  private final IngestionProperties properties;
  private final Retry kafkaPublishRetry;
  private final List<Thread> workers = new ArrayList<>();

  private volatile boolean running;

  public IngestionService(
      IngestionQueue queue,
      ObjectMapper mapper,
      KafkaEventPublisher publisher,
      IngestionProperties properties,
      Retry kafkaPublishRetry
  ) {
    this.queue = queue;
    this.mapper = mapper;
    this.publisher = publisher;
    this.properties = properties;
    this.kafkaPublishRetry = kafkaPublishRetry;
    this.kafkaPublishRetry.getEventPublisher()
        .onRetry(event -> log.warn(
            "Retrying Kafka publish. attempt={}, name={}, lastThrowable={}",
            event.getNumberOfRetryAttempts(),
            event.getName(),
            event.getLastThrowable().toString()
        ));
  }

  @PostConstruct
  public void start() {
    running = true;

    for (int workerIndex = 0; workerIndex < properties.workers(); workerIndex++) {
      Thread worker = new Thread(this::runWorker, "ingestion-worker-" + workerIndex);
      worker.start();
      workers.add(worker);
    }
  }

  @PreDestroy
  public void stop() throws InterruptedException {
    running = false;

    for (Thread worker : workers) {
      worker.interrupt();
    }

    for (Thread worker : workers) {
      worker.join(5_000);
    }
  }

  private void runWorker() {
    while (running) {
      try {
        process(queue.take());
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception exception) {
        log.error("Unexpected ingestion worker failure", exception);
      }
    }
  }

  private void process(IngestionMessage message) {
    SensorEvent event;

    try {
      event = mapper.readValue(message.payload(), SensorEvent.class);
      validate(event);
    } catch (Exception exception) {
      publishInvalid(message, exception.getMessage());
      return;
    }

    EnrichedSensorEvent enrichedEvent =
        new EnrichedSensorEvent(
            UUID.randomUUID(),
            Instant.now(),
            event
        );

    try {
      publishWithRetry(event.deviceId(), enrichedEvent);
      log.debug("Ingested MQTT event from topic {} for device {}", message.mqttTopic(), event.deviceId());
    } catch (Exception exception) {
      publishToRetryTopic(message, exception.getMessage(), properties.maxPublishAttempts());
    }
  }

  private void publishWithRetry(String key, EnrichedSensorEvent event) throws Exception {
    try {
      Retry.decorateCheckedRunnable(
          kafkaPublishRetry,
          () -> publisher.publishSensorEvent(key, event)
      ).run();
    } catch (Exception exception) {
      throw exception;
    } catch (Throwable throwable) {
      throw new KafkaException("Kafka publish retry failed", throwable);
    }
  }

  private void publishInvalid(IngestionMessage message, String reason) {
    InvalidIngestionEvent invalidEvent =
        new InvalidIngestionEvent(
            UUID.randomUUID(),
            Instant.now(),
            message.mqttTopic(),
            reason,
            rawPayload(message)
        );

    try {
      publisher.publishInvalidEvent(message.mqttTopic(), invalidEvent);
      log.warn("Published invalid MQTT event from topic {}: {}", message.mqttTopic(), reason);
    } catch (Exception exception) {
      log.error("Failed to publish invalid MQTT event from topic {}", message.mqttTopic(), exception);
    }
  }

  private void publishToRetryTopic(IngestionMessage message, String reason, int attempts) {
    RetryIngestionEvent retryEvent =
        new RetryIngestionEvent(
            UUID.randomUUID(),
            Instant.now(),
            message.mqttTopic(),
            reason,
            attempts,
            rawPayload(message)
        );

    try {
      publisher.publishRetryEvent(message.mqttTopic(), retryEvent);
      log.warn("Published retry MQTT event from topic {} after {} attempts", message.mqttTopic(), attempts);
    } catch (Exception exception) {
      log.error("Failed to publish retry MQTT event from topic {}", message.mqttTopic(), exception);
    }
  }

  private String rawPayload(IngestionMessage message) {
    return new String(message.payload(), StandardCharsets.UTF_8);
  }

  private void validate(SensorEvent event) {
    if (event.deviceId() == null || event.deviceId().isBlank()) {
      throw new IllegalArgumentException("deviceId must not be blank");
    }
    if (event.sensor() == null || event.sensor().isBlank()) {
      throw new IllegalArgumentException("sensor must not be blank");
    }
  }
}
