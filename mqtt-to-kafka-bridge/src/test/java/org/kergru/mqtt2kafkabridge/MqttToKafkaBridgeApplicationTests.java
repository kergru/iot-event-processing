package org.kergru.mqtt2kafkabridge;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class MqttToKafkaBridgeApplicationTests {

  @MockitoBean
  private MqttSubscriber mqttSubscriber;

  @Test
  void contextLoads() {
  }

}
