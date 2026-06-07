package org.kergru.iotplatform.processor.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.kergru.iot.event.processing.PressureAggregateEvent;
import org.kergru.iot.event.sensor.PressureEvent;

class SensorEventProcessFunctionTests {

  @Test
  void aggregatesPressureStats() {
    PressureAggregateAccumulator accumulator = new PressureAggregateAccumulator();
    accumulator.add(new PressureEvent(null, null, Instant.parse("2026-06-05T12:00:00Z"), null, "device-1", "bmp180", 1012.0));
    accumulator.add(new PressureEvent(null, null, Instant.parse("2026-06-05T12:01:00Z"), null, "device-1", "bmp180", 1009.0));
    accumulator.add(new PressureEvent(null, null, Instant.parse("2026-06-05T12:02:00Z"), null, "device-1", "bmp180", 1008.5));

    PressureAggregateEvent aggregate = new PressureAggregateEvent(
        accumulator.deviceId(),
        accumulator.sensor(),
        Instant.parse("2026-06-05T12:00:00Z"),
        Instant.parse("2026-06-05T12:05:00Z"),
        accumulator.count(),
        accumulator.minPressureHpa(),
        accumulator.maxPressureHpa(),
        accumulator.avgPressureHpa(),
        accumulator.pressureDeltaHpa(),
        "falling"
    );

    assertEquals("device-1", aggregate.deviceId());
    assertEquals(3, aggregate.count());
    assertEquals(1008.5, aggregate.minPressureHpa());
    assertEquals(1012.0, aggregate.maxPressureHpa());
    assertEquals(1009.8333333333334, aggregate.avgPressureHpa());
    assertEquals(-3.5, aggregate.pressureDeltaHpa());
    assertEquals("falling", aggregate.trend());
  }
}
