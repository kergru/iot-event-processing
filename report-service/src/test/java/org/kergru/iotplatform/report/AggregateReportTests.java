package org.kergru.iotplatform.report;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Date;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class AggregateReportTests {

  @Test
  void mapsTemperatureAggregateDocument() {
    Instant windowEnd = Instant.parse("2026-01-01T10:01:00Z");
    Document document = new Document("_id", "abc")
        .append("deviceId", "bmp180-1")
        .append("sensor", "temperature")
        .append("windowStart", "2026-01-01T10:00:00Z")
        .append("windowEnd", Date.from(windowEnd))
        .append("count", 3)
        .append("minTemperatureC", 20.1)
        .append("maxTemperatureC", 21.7)
        .append("avgTemperatureC", 20.9);

    AggregateReport report = AggregateReport.temperature(document);

    assertThat(report.id()).isEqualTo("abc");
    assertThat(report.deviceId()).isEqualTo("bmp180-1");
    assertThat(report.windowEnd()).isEqualTo(windowEnd);
    assertThat(report.count()).isEqualTo(3);
    assertThat(report.min()).isEqualTo(20.1);
    assertThat(report.max()).isEqualTo(21.7);
    assertThat(report.avg()).isEqualTo(20.9);
    assertThat(report.delta()).isNull();
    assertThat(report.trend()).isNull();
  }

  @Test
  void mapsPressureAggregateDocument() {
    Document document = new Document("deviceId", "bmp180-1")
        .append("sensor", "pressure")
        .append("count", 5L)
        .append("minPressureHpa", 1000.1)
        .append("maxPressureHpa", 1002.2)
        .append("avgPressureHpa", 1001.5)
        .append("pressureDeltaHpa", 2.1)
        .append("trend", "rising");

    AggregateReport report = AggregateReport.pressure(document);

    assertThat(report.count()).isEqualTo(5);
    assertThat(report.min()).isEqualTo(1000.1);
    assertThat(report.max()).isEqualTo(1002.2);
    assertThat(report.avg()).isEqualTo(1001.5);
    assertThat(report.delta()).isEqualTo(2.1);
    assertThat(report.trend()).isEqualTo("rising");
  }
}
