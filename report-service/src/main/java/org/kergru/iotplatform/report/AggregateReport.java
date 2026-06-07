package org.kergru.iotplatform.report;

import java.time.Instant;
import org.bson.Document;

public record AggregateReport(
    String id,
    String deviceId,
    String sensor,
    Instant windowStart,
    Instant windowEnd,
    long count,
    Double min,
    Double max,
    Double avg,
    Double delta,
    String trend
) {

  static AggregateReport temperature(Document document) {
    return from(document, "minTemperatureC", "maxTemperatureC", "avgTemperatureC", null, null);
  }

  static AggregateReport pressure(Document document) {
    return from(document, "minPressureHpa", "maxPressureHpa", "avgPressureHpa", "pressureDeltaHpa", "trend");
  }

  private static AggregateReport from(Document document, String minField, String maxField, String avgField, String deltaField, String trendField) {
    return new AggregateReport(
        stringValue(document, "_id"),
        stringValue(document, "deviceId"),
        stringValue(document, "sensor"),
        instantValue(document, "windowStart"),
        instantValue(document, "windowEnd"),
        longValue(document, "count"),
        doubleValue(document, minField),
        doubleValue(document, maxField),
        doubleValue(document, avgField),
        deltaField == null ? null : doubleValue(document, deltaField),
        trendField == null ? null : stringValue(document, trendField)
    );
  }

  private static String stringValue(Document document, String fieldName) {
    Object value = document.get(fieldName);
    return value == null ? null : value.toString();
  }

  private static Instant instantValue(Document document, String fieldName) {
    Object value = document.get(fieldName);
    if (value instanceof Instant instant) {
      return instant;
    }
    if (value instanceof java.util.Date date) {
      return date.toInstant();
    }
    if (value instanceof CharSequence text) {
      return Instant.parse(text);
    }
    return null;
  }

  private static long longValue(Document document, String fieldName) {
    Object value = document.get(fieldName);
    return value instanceof Number number ? number.longValue() : 0;
  }

  private static Double doubleValue(Document document, String fieldName) {
    Object value = document.get(fieldName);
    return value instanceof Number number ? number.doubleValue() : null;
  }
}
