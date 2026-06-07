package org.kergru.iotplatform.processor.flink;

public record ProcessorProperties(
    String bootstrapServers,
    String schemaRegistryUrl,
    String groupId,
    String inputTopic,
    String outputTopic
) {

  public static ProcessorProperties fromEnvironment() {
    return new ProcessorProperties(
        env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        env("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        env("FLINK_KAFKA_GROUP_ID", "iot-flink-pressure-aggregate-processor"),
        env("KAFKA_INPUT_TOPIC", "sensor.pressure"),
        env("FLINK_OUTPUT_TOPIC", "sensor.pressure.aggregates.flink")
    );
  }

  private static String env(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
