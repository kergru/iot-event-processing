package org.kergru.mqtt2kafkabridge;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

@Component
public class IngestionQueue {

  private final BlockingQueue<IngestionMessage> messages;
  private final Duration offerTimeout;

  public IngestionQueue(IngestionProperties properties) {
    this.messages = new ArrayBlockingQueue<>(properties.queueCapacity());
    this.offerTimeout = properties.offerTimeout();
  }

  public boolean offer(IngestionMessage message) throws InterruptedException {
    return messages.offer(message, offerTimeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public IngestionMessage take() throws InterruptedException {
    return messages.take();
  }

  public int size() {
    return messages.size();
  }
}
