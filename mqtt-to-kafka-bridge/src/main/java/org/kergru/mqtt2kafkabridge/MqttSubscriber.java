package org.kergru.mqtt2kafkabridge;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MqttSubscriber {

  private static final Logger log = LoggerFactory.getLogger(MqttSubscriber.class);

  private final MqttProperties mqttProperties;
  private final IngestionQueue ingestionQueue;
  private final CountDownLatch stopSignal = new CountDownLatch(1);

  private volatile IMqttClient client;
  private Thread subscriberThread;

  public MqttSubscriber(
      MqttProperties mqttProperties,
      IngestionQueue ingestionQueue
  ) {
    this.mqttProperties = mqttProperties;
    this.ingestionQueue = ingestionQueue;
  }

  @PostConstruct
  public void start() {
    subscriberThread = new Thread(this::runSubscriber, "mqtt-subscriber");
    subscriberThread.start();
  }

  @PreDestroy
  public void stop() throws Exception {
    stopSignal.countDown();

    IMqttClient currentClient = client;
    if (currentClient != null && currentClient.isConnected()) {
      currentClient.disconnect();
    }

    if (subscriberThread != null) {
      subscriberThread.join(5_000);
    }
  }

  private void runSubscriber() {
    while (!isStopping()) {
      String clientId = mqttProperties.clientId() + "-" + UUID.randomUUID();

      try (IMqttClient mqttClient = new MqttClient(mqttProperties.broker(), clientId)) {
        client = mqttClient;
        mqttClient.connect();
        mqttClient.subscribe(mqttProperties.topic(), this::enqueueMessage);
        log.info("Subscribed MQTT client {} to {}", clientId, mqttProperties.topic());

        stopSignal.await();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        log.warn("MQTT subscriber interrupted", exception);
        return;
      } catch (Exception exception) {
        if (!isStopping()) {
          log.warn(
              "MQTT subscriber connection failed for broker {} and topic {}. Retrying in {}",
              mqttProperties.broker(),
              mqttProperties.topic(),
              reconnectBackoff(),
              exception
          );
          awaitReconnectBackoff();
        }
      } finally {
        client = null;
      }
    }
  }

  private boolean isStopping() {
    return stopSignal.getCount() == 0;
  }

  private void awaitReconnectBackoff() {
    try {
      stopSignal.await(reconnectBackoff().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private Duration reconnectBackoff() {
    Duration configuredBackoff = mqttProperties.reconnectBackoff();
    return configuredBackoff == null || configuredBackoff.isNegative() || configuredBackoff.isZero()
        ? Duration.ofSeconds(5)
        : configuredBackoff;
  }

  private void enqueueMessage(String topic, MqttMessage message) throws Exception {
    IngestionMessage ingestionMessage =
        new IngestionMessage(topic, message.getPayload().clone(), Instant.now());

    if (!ingestionQueue.offer(ingestionMessage)) {
      throw new IllegalStateException(
          "Ingestion queue is full after waiting for configured offer timeout"
      );
    }

    log.debug("Queued MQTT message from topic {}. Queue size: {}", topic, ingestionQueue.size());
  }
}
