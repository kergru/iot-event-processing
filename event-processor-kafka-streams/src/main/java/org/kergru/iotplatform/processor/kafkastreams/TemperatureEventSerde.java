package org.kergru.iotplatform.processor.kafkastreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kergru.iot.event.sensor.TemperatureEvent;

public class TemperatureEventSerde implements Serde<TemperatureEvent> {

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  @Override
  public Serializer<TemperatureEvent> serializer() {
    return (topic, data) -> {
      try {
        return data == null ? null : mapper.writeValueAsBytes(data);
      } catch (Exception exception) {
        throw new IllegalArgumentException("Failed to serialize temperature event", exception);
      }
    };
  }

  @Override
  public Deserializer<TemperatureEvent> deserializer() {
    return (topic, data) -> {
      try {
        return data == null ? null : mapper.readValue(data, TemperatureEvent.class);
      } catch (Exception exception) {
        throw new IllegalArgumentException("Failed to deserialize temperature event", exception);
      }
    };
  }
}
