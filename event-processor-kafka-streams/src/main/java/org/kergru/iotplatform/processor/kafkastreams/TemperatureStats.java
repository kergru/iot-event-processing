package org.kergru.iotplatform.processor.kafkastreams;

import java.time.Instant;
import org.kergru.iot.event.processing.TemperatureAggregateEvent;
import org.kergru.iot.event.sensor.TemperatureEvent;

public record TemperatureStats(
    String deviceId,
    String sensor,
    long count,
    Double minTemperatureC,
    Double maxTemperatureC,
    Double avgTemperatureC
) {

  public static TemperatureStats empty() {
    return new TemperatureStats(null, null, 0, null, null, null);
  }

  public TemperatureStats add(TemperatureEvent event) {
    if (event.temperatureC() == null) {
      return this;
    }

    long nextCount = count + 1;
    double nextMin = minTemperatureC == null ? event.temperatureC() : Math.min(minTemperatureC, event.temperatureC());
    double nextMax = maxTemperatureC == null ? event.temperatureC() : Math.max(maxTemperatureC, event.temperatureC());
    double nextAvg = avgTemperatureC == null
        ? event.temperatureC()
        : ((avgTemperatureC * count) + event.temperatureC()) / nextCount;

    return new TemperatureStats(
        event.deviceId(),
        event.sensor(),
        nextCount,
        nextMin,
        nextMax,
        nextAvg
    );
  }

  public TemperatureAggregateEvent toEvent(Instant windowStart, Instant windowEnd) {
    return new TemperatureAggregateEvent(
        deviceId,
        sensor,
        windowStart,
        windowEnd,
        count,
        minTemperatureC,
        maxTemperatureC,
        avgTemperatureC
    );
  }
}
