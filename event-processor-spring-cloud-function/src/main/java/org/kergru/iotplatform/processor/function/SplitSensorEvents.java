package org.kergru.iotplatform.processor.function;

import java.time.Instant;
import java.util.UUID;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.processing.ProcessedSensorEvent;
import org.kergru.iot.event.sensor.PressureEvent;
import org.kergru.iot.event.sensor.SensorEvent;
import org.kergru.iot.event.sensor.TemperatureEvent;

public record SplitSensorEvents(
    ProcessedSensorEvent processedEvent,
    TemperatureEvent temperatureEvent,
    PressureEvent pressureEvent
) {

  public static SplitSensorEvents from(EnrichedSensorEvent event) {
    SensorEvent payload = event.payload();
    return new SplitSensorEvents(
        new ProcessedSensorEvent(
            UUID.randomUUID(),
            event.eventId(),
            event.ingestionTimestamp(),
            Instant.now(),
            "spring-cloud-function",
            payload.deviceId(),
            payload.sensor(),
            payload.temperatureC(),
            payload.pressureHpa()
        ),
        new TemperatureEvent(
            UUID.randomUUID(),
            event.eventId(),
            event.ingestionTimestamp(),
            payload.timestamp(),
            payload.deviceId(),
            payload.sensor(),
            payload.temperatureC()
        ),
        new PressureEvent(
            UUID.randomUUID(),
            event.eventId(),
            event.ingestionTimestamp(),
            payload.timestamp(),
            payload.deviceId(),
            payload.sensor(),
            payload.pressureHpa()
        )
    );
  }
}
