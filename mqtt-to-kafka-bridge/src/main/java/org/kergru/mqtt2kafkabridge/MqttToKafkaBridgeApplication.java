package org.kergru.mqtt2kafkabridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
    MqttProperties.class,
    IngestionProperties.class,
    KafkaTopicProperties.class,
    SchemaRegistryProperties.class
})
public class MqttToKafkaBridgeApplication {

  public static void main(String[] args) {
    SpringApplication.run(MqttToKafkaBridgeApplication.class, args);
  }

}
