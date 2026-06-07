package org.kergru.iot.event.processing;

import java.time.Instant;

public record TemperatureAggregateEvent(
    String deviceId,
    String sensor,
    Instant windowStart,
    Instant windowEnd,
    long count,
    Double minTemperatureC,
    Double maxTemperatureC,
    Double avgTemperatureC
) {}
