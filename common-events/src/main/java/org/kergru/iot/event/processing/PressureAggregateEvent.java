package org.kergru.iot.event.processing;

import java.time.Instant;

public record PressureAggregateEvent(
    String deviceId,
    String sensor,
    Instant windowStart,
    Instant windowEnd,
    long count,
    Double minPressureHpa,
    Double maxPressureHpa,
    Double avgPressureHpa,
    Double pressureDeltaHpa,
    String trend
) {}
