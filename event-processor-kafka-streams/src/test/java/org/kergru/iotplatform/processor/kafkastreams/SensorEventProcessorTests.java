package org.kergru.iotplatform.processor.kafkastreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.kergru.iot.event.processing.TemperatureAggregateEvent;
import org.kergru.iot.event.sensor.TemperatureEvent;

class SensorEventProcessorTests {

  @Test
  void aggregatesTemperatureStats() {
    TemperatureStats stats = TemperatureStats.empty()
        .add(new TemperatureEvent(null, null, Instant.parse("2026-06-05T12:00:00Z"), null, "device-1", "bmp180", 22.5))
        .add(new TemperatureEvent(null, null, Instant.parse("2026-06-05T12:01:00Z"), null, "device-1", "bmp180", 24.5))
        .add(new TemperatureEvent(null, null, Instant.parse("2026-06-05T12:02:00Z"), null, "device-1", "bmp180", 21.0));

    TemperatureAggregateEvent aggregate = stats.toEvent(
        Instant.parse("2026-06-05T12:00:00Z"),
        Instant.parse("2026-06-05T12:05:00Z")
    );

    assertEquals("device-1", aggregate.deviceId());
    assertEquals(3, aggregate.count());
    assertEquals(21.0, aggregate.minTemperatureC());
    assertEquals(24.5, aggregate.maxTemperatureC());
    assertEquals(22.666666666666668, aggregate.avgTemperatureC());
  }
}
