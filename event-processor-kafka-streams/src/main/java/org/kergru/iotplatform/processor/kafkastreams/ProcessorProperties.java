package org.kergru.iotplatform.processor.kafkastreams;

public record ProcessorProperties(
    String bootstrapServers,
    String schemaRegistryUrl,
    String applicationId,
    String inputTopic,
    String outputTopic
) {

  public static ProcessorProperties fromEnvironment() {
    return new ProcessorProperties(
        env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        env("KAFKA_STREAMS_APPLICATION_ID", "iot-kafka-streams-temperature-processor"),
        env("KAFKA_INPUT_TOPIC", "sensor.temperature"),
        env("KAFKA_STREAMS_OUTPUT_TOPIC", "sensor.temperature.aggregates.kafka-streams")
    );
  }

  private static String env(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
