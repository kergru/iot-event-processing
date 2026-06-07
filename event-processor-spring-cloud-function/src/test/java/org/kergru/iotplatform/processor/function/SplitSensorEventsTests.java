package org.kergru.iotplatform.processor.function;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.sensor.SensorEvent;

class SplitSensorEventsTests {

  @Test
  void splitsEnrichedSensorEventIntoTemperatureAndPressureEvents() {
    UUID sourceEventId = UUID.randomUUID();
    Instant ingestionTimestamp = Instant.parse("2026-06-05T12:00:00Z");
    Instant measurementTimestamp = Instant.parse("2026-06-05T11:59:00Z");
    EnrichedSensorEvent event = new EnrichedSensorEvent(
        sourceEventId,
        ingestionTimestamp,
        new SensorEvent("device-1", "bmp180", 22.5, 1012.0, measurementTimestamp)
    );

    SplitSensorEvents result = SplitSensorEvents.from(event);

    assertThat(result.processedEvent().sourceEventId()).isEqualTo(sourceEventId);
    assertThat(result.processedEvent().ingestionTimestamp()).isEqualTo(ingestionTimestamp);
    assertThat(result.processedEvent().processor()).isEqualTo("spring-cloud-function");
    assertThat(result.processedEvent().deviceId()).isEqualTo("device-1");
    assertThat(result.processedEvent().temperatureC()).isEqualTo(22.5);
    assertThat(result.processedEvent().pressureHpa()).isEqualTo(1012.0);

    assertThat(result.temperatureEvent().sourceEventId()).isEqualTo(sourceEventId);
    assertThat(result.temperatureEvent().ingestionTimestamp()).isEqualTo(ingestionTimestamp);
    assertThat(result.temperatureEvent().measurementTimestamp()).isEqualTo(measurementTimestamp);
    assertThat(result.temperatureEvent().deviceId()).isEqualTo("device-1");
    assertThat(result.temperatureEvent().temperatureC()).isEqualTo(22.5);

    assertThat(result.pressureEvent().sourceEventId()).isEqualTo(sourceEventId);
    assertThat(result.pressureEvent().ingestionTimestamp()).isEqualTo(ingestionTimestamp);
    assertThat(result.pressureEvent().measurementTimestamp()).isEqualTo(measurementTimestamp);
    assertThat(result.pressureEvent().deviceId()).isEqualTo("device-1");
    assertThat(result.pressureEvent().pressureHpa()).isEqualTo(1012.0);
  }
}
