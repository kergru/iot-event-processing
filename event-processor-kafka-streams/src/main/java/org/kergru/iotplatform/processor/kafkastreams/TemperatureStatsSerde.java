package org.kergru.iotplatform.processor.kafkastreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TemperatureStatsSerde implements Serde<TemperatureStats> {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Serializer<TemperatureStats> serializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }
      try {
        return mapper.writeValueAsBytes(data);
      } catch (IOException exception) {
        throw new IllegalArgumentException("Failed to serialize temperature stats", exception);
      }
    };
  }

  @Override
  public Deserializer<TemperatureStats> deserializer() {
    return (topic, data) -> {
      if (data == null) {
        return null;
      }
      try {
        return mapper.readValue(data, TemperatureStats.class);
      } catch (IOException exception) {
        throw new IllegalArgumentException("Failed to deserialize temperature stats", exception);
      }
    };
  }
}
