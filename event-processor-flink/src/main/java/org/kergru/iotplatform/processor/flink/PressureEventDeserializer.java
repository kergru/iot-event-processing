package org.kergru.iotplatform.processor.flink;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.kergru.iot.event.avro.AvroEvents;
import org.kergru.iot.event.sensor.PressureEvent;

public class PressureEventDeserializer implements MapFunction<byte[], PressureEvent> {

  private final String inputTopic;
  private final String schemaRegistryUrl;
  private transient KafkaAvroDeserializer deserializer;

  public PressureEventDeserializer(String inputTopic, String schemaRegistryUrl) {
    this.inputTopic = inputTopic;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public PressureEvent map(byte[] rawEvent) {
    GenericRecord record = (GenericRecord) deserializer().deserialize(inputTopic, rawEvent);
    return AvroEvents.toPressureEvent(record);
  }

  private KafkaAvroDeserializer deserializer() {
    if (deserializer == null) {
      deserializer = new KafkaAvroDeserializer();
      deserializer.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
    }
    return deserializer;
  }
}
