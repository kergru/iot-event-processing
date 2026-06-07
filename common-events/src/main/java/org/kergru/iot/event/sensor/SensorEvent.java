package org.kergru.iot.event.sensor;

import java.time.Instant;

public record SensorEvent(
    String deviceId,
    String sensor,
    Double temperatureC,
    Double pressureHpa,
    Instant timestamp
) {}
