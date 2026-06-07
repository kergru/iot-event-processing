package org.kergru.iotplatform.processor.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.ByteArraySchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import java.time.Duration;
import org.kergru.iot.event.sensor.PressureEvent;

public class FlinkProcessorJob {

  public static void main(String[] args) throws Exception {
    ProcessorProperties properties = ProcessorProperties.fromEnvironment();

    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
        .setBootstrapServers(properties.bootstrapServers())
        .setTopics(properties.inputTopic())
        .setGroupId(properties.groupId())
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new ByteArraySchema())
        .build();

    KafkaSink<byte[]> sink = KafkaSink.<byte[]>builder()
        .setBootstrapServers(properties.bootstrapServers())
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(properties.outputTopic())
                .setValueSerializationSchema(new ByteArraySchema())
                .build()
        )
        .build();

    environment
        .fromSource(source, WatermarkStrategy.noWatermarks(), "sensor-pressure-kafka-source")
        .map(new PressureEventDeserializer(properties.inputTopic(), properties.schemaRegistryUrl()))
        .name("deserialize-pressure-events")
        .keyBy(FlinkProcessorJob::aggregateKey)
        .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(5)))
        .aggregate(new PressureAggregateFunction(), new PressureAggregateWindowFunction())
        .name("aggregate-pressure-events")
        .map(new PressureAggregateSerializer(properties.outputTopic(), properties.schemaRegistryUrl()))
        .name("serialize-pressure-aggregates")
        .sinkTo(sink)
        .name("pressure-aggregate-kafka-sink");

    environment.execute("iot-flink-pressure-aggregate-processor");
  }

  private static String aggregateKey(PressureEvent event) {
    return event.deviceId() + ":" + event.sensor();
  }
}
