package org.kergru.mqtt2kafkabridge;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.kergru.iot.event.avro.AvroEvents;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.ingestion.InvalidIngestionEvent;
import org.kergru.iot.event.ingestion.RetryIngestionEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaEventPublisher {

  private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

  private final KafkaTemplate<String, GenericRecord> avroKafkaTemplate;
  private final KafkaTemplate<String, String> stringKafkaTemplate;
  private final KafkaTopicProperties topics;
  private final ObjectMapper mapper;

  public KafkaEventPublisher(
      @Qualifier("avroKafkaTemplate") KafkaTemplate<String, GenericRecord> avroKafkaTemplate,
      @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate,
      KafkaTopicProperties topics,
      ObjectMapper mapper
  ) {
    this.avroKafkaTemplate = avroKafkaTemplate;
    this.stringKafkaTemplate = stringKafkaTemplate;
    this.topics = topics;
    this.mapper = mapper;
  }

  public void publishSensorEvent(String key, EnrichedSensorEvent event) throws Exception {
    avroKafkaTemplate
        .send(topics.sensorRaw(), key, AvroEvents.toRecord(event))
        .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }

  public void publishInvalidEvent(String key, InvalidIngestionEvent event) throws Exception {
    sendJson(topics.sensorInvalid(), key, event);
  }

  public void publishRetryEvent(String key, RetryIngestionEvent event) throws Exception {
    sendJson(topics.sensorRetry(), key, event);
  }

  private void sendJson(String topic, String key, Object event) throws Exception {
    stringKafkaTemplate
        .send(topic, key, mapper.writeValueAsString(event))
        .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
  }
}
