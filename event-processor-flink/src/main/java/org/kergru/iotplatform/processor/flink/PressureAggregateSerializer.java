package org.kergru.iotplatform.processor.flink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.kergru.iot.event.avro.AvroEvents;
import org.kergru.iot.event.processing.PressureAggregateEvent;

public class PressureAggregateSerializer implements MapFunction<PressureAggregateEvent, byte[]> {

  private final String outputTopic;
  private final String schemaRegistryUrl;
  private transient KafkaAvroSerializer serializer;

  public PressureAggregateSerializer(String outputTopic, String schemaRegistryUrl) {
    this.outputTopic = outputTopic;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public byte[] map(PressureAggregateEvent event) {
    return serializer().serialize(outputTopic, AvroEvents.toRecord(event));
  }

  private KafkaAvroSerializer serializer() {
    if (serializer == null) {
      serializer = new KafkaAvroSerializer();
      serializer.configure(Map.of("schema.registry.url", schemaRegistryUrl), false);
    }
    return serializer;
  }
}
