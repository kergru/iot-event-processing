package org.kergru.iotplatform.processor.kafkastreams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.kergru.iot.event.avro.AvroEvents;
import org.kergru.iot.event.sensor.TemperatureEvent;

public class KafkaStreamsProcessorApplication {

  public static void main(String[] args) throws InterruptedException {
    ProcessorProperties properties = ProcessorProperties.fromEnvironment();

    StreamsBuilder builder = new StreamsBuilder();

    try (Serde<GenericRecord> avroSerde = avroSerde(properties);
        Serde<TemperatureStats> statsSerde = new TemperatureStatsSerde()) {

      builder.stream(properties.inputTopic(), Consumed.with(Serdes.String(), avroSerde))
          .mapValues(AvroEvents::toTemperatureEvent)
          .selectKey((key, event) -> aggregateKey(event))
          .groupByKey(Grouped.with(Serdes.String(), temperatureEventSerde()))
          .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
              .advanceBy(Duration.ofMinutes(1)))
          .aggregate(
              TemperatureStats::empty,
              (key, event, stats) -> stats.add(event),
              Materialized.with(Serdes.String(), statsSerde)
          )
          .toStream()
          .mapValues(KafkaStreamsProcessorApplication::toAggregateRecord)
          .selectKey((windowedKey, aggregate) -> windowedKey.key())
          .to(properties.outputTopic(), Produced.with(Serdes.String(), avroSerde));

      KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties(properties));
      CountDownLatch shutdown = new CountDownLatch(1);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        streams.close();
        shutdown.countDown();
      }));

      streams.start();
      shutdown.await();
    }
  }

  private static String aggregateKey(TemperatureEvent event) {
    return event.deviceId() + ":" + event.sensor();
  }

  private static GenericRecord toAggregateRecord(Windowed<String> key, TemperatureStats stats) {
    return AvroEvents.toRecord(stats.toEvent(
        Instant.ofEpochMilli(key.window().start()),
        Instant.ofEpochMilli(key.window().end())
    ));
  }

  private static Serde<GenericRecord> avroSerde(ProcessorProperties properties) {
    GenericAvroSerde serde = new GenericAvroSerde();
    serde.configure(Map.of("schema.registry.url", properties.schemaRegistryUrl()), false);
    return serde;
  }

  private static Serde<TemperatureEvent> temperatureEventSerde() {
    return new TemperatureEventSerde();
  }

  private static Properties streamsProperties(ProcessorProperties properties) {
    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.applicationId());
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
    streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    streamsProperties.put("schema.registry.url", properties.schemaRegistryUrl());
    return streamsProperties;
  }
}
