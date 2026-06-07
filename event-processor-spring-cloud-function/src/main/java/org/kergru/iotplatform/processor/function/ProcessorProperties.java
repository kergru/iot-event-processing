package org.kergru.iotplatform.processor.function;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("iot.processor.function")
public record ProcessorProperties(
    String inputTopic,
    String processedOutputTopic,
    String temperatureOutputTopic,
    String pressureOutputTopic,
    String processedOutputBinding,
    String temperatureOutputBinding,
    String pressureOutputBinding,
    String schemaRegistryUrl
) {

  public ProcessorProperties {
    inputTopic = defaultValue(inputTopic, "sensor.raw.bmp180");
    processedOutputTopic = defaultValue(processedOutputTopic, "sensor.processed");
    temperatureOutputTopic = defaultValue(temperatureOutputTopic, "sensor.temperature");
    pressureOutputTopic = defaultValue(pressureOutputTopic, "sensor.pressure");
    processedOutputBinding = defaultValue(processedOutputBinding, "processed-out-0");
    temperatureOutputBinding = defaultValue(temperatureOutputBinding, "temperature-out-0");
    pressureOutputBinding = defaultValue(pressureOutputBinding, "pressure-out-0");
    schemaRegistryUrl = defaultValue(schemaRegistryUrl, "http://localhost:8081");
  }

  private static String defaultValue(String value, String fallback) {
    return value == null || value.isBlank() ? fallback : value;
  }
}
