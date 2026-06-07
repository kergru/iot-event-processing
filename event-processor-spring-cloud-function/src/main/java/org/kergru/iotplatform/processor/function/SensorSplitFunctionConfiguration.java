package org.kergru.iotplatform.processor.function;

import java.util.function.Consumer;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Configuration
public class SensorSplitFunctionConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SensorSplitFunctionConfiguration.class);

  @Bean
  public Consumer<Message<byte[]>> splitSensorEvent(
      AvroMessageCodec codec,
      ProcessorProperties properties,
      StreamBridge streamBridge
  ) {
    return message -> {
      EnrichedSensorEvent event = codec.deserializeEnrichedSensorEvent(message.getPayload());
      SplitSensorEvents splitEvents = SplitSensorEvents.from(event);

      streamBridge.send(
          properties.processedOutputBinding(),
          MessageBuilder.withPayload(codec.serialize(splitEvents.processedEvent())).build()
      );
      streamBridge.send(
          properties.temperatureOutputBinding(),
          MessageBuilder.withPayload(codec.serialize(splitEvents.temperatureEvent())).build()
      );
      streamBridge.send(
          properties.pressureOutputBinding(),
          MessageBuilder.withPayload(codec.serialize(splitEvents.pressureEvent())).build()
      );

      log.debug(
          "Processed and split sensor event {} from device {} into processed, temperature and pressure events",
          event.eventId(),
          event.payload().deviceId()
      );
    };
  }
}
