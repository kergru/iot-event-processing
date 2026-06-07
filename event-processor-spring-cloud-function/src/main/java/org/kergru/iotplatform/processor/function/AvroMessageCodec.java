package org.kergru.iotplatform.processor.function;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.kergru.iot.event.avro.AvroEvents;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.processing.ProcessedSensorEvent;
import org.kergru.iot.event.sensor.PressureEvent;
import org.kergru.iot.event.sensor.TemperatureEvent;
import org.springframework.stereotype.Component;

@Component
public class AvroMessageCodec {

  private final ProcessorProperties properties;
  private KafkaAvroDeserializer deserializer;
  private KafkaAvroSerializer serializer;

  public AvroMessageCodec(ProcessorProperties properties) {
    this.properties = properties;
  }

  public EnrichedSensorEvent deserializeEnrichedSensorEvent(byte[] payload) {
    GenericRecord record = (GenericRecord) deserializer().deserialize(properties.inputTopic(), payload);
    return AvroEvents.toEnrichedSensorEvent(record);
  }

  public byte[] serialize(ProcessedSensorEvent event) {
    return serializer().serialize(properties.processedOutputTopic(), AvroEvents.toRecord(event));
  }

  public byte[] serialize(TemperatureEvent event) {
    return serializer().serialize(properties.temperatureOutputTopic(), AvroEvents.toRecord(event));
  }

  public byte[] serialize(PressureEvent event) {
    return serializer().serialize(properties.pressureOutputTopic(), AvroEvents.toRecord(event));
  }

  private KafkaAvroDeserializer deserializer() {
    if (deserializer == null) {
      deserializer = new KafkaAvroDeserializer();
      deserializer.configure(Map.of("schema.registry.url", properties.schemaRegistryUrl()), false);
    }
    return deserializer;
  }

  private KafkaAvroSerializer serializer() {
    if (serializer == null) {
      serializer = new KafkaAvroSerializer();
      serializer.configure(Map.of("schema.registry.url", properties.schemaRegistryUrl()), false);
    }
    return serializer;
  }
}
