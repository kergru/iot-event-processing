package org.kergru.iot.event.avro;

import java.time.Instant;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.kergru.iot.event.ingestion.EnrichedSensorEvent;
import org.kergru.iot.event.processing.PressureAggregateEvent;
import org.kergru.iot.event.processing.ProcessedSensorEvent;
import org.kergru.iot.event.processing.TemperatureAggregateEvent;
import org.kergru.iot.event.sensor.PressureEvent;
import org.kergru.iot.event.sensor.SensorEvent;
import org.kergru.iot.event.sensor.TemperatureEvent;

public final class AvroEvents {

  private AvroEvents() {
  }

  public static GenericRecord toRecord(EnrichedSensorEvent event) {
    GenericRecord payload = toSensorRecord(
        AvroSchemas.ENRICHED_SENSOR_EVENT.getField("payload").schema(),
        event.payload()
    );

    GenericRecord record = new GenericData.Record(AvroSchemas.ENRICHED_SENSOR_EVENT);
    record.put("eventId", event.eventId().toString());
    record.put("ingestionTimestamp", event.ingestionTimestamp().toString());
    record.put("payload", payload);
    return record;
  }

  public static GenericRecord toRecord(TemperatureEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.TEMPERATURE_EVENT);
    record.put("eventId", event.eventId().toString());
    record.put("sourceEventId", event.sourceEventId().toString());
    record.put("ingestionTimestamp", event.ingestionTimestamp().toString());
    record.put("measurementTimestamp", event.measurementTimestamp() == null ? null : event.measurementTimestamp().toString());
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("temperatureC", event.temperatureC());
    return record;
  }

  public static GenericRecord toRecord(PressureEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.PRESSURE_EVENT);
    record.put("eventId", event.eventId().toString());
    record.put("sourceEventId", event.sourceEventId().toString());
    record.put("ingestionTimestamp", event.ingestionTimestamp().toString());
    record.put("measurementTimestamp", event.measurementTimestamp() == null ? null : event.measurementTimestamp().toString());
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("pressureHpa", event.pressureHpa());
    return record;
  }

  public static GenericRecord toRecord(TemperatureAggregateEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.TEMPERATURE_AGGREGATE_EVENT);
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("windowStart", event.windowStart().toString());
    record.put("windowEnd", event.windowEnd().toString());
    record.put("count", event.count());
    record.put("minTemperatureC", event.minTemperatureC());
    record.put("maxTemperatureC", event.maxTemperatureC());
    record.put("avgTemperatureC", event.avgTemperatureC());
    return record;
  }

  public static GenericRecord toRecord(PressureAggregateEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.PRESSURE_AGGREGATE_EVENT);
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("windowStart", event.windowStart().toString());
    record.put("windowEnd", event.windowEnd().toString());
    record.put("count", event.count());
    record.put("minPressureHpa", event.minPressureHpa());
    record.put("maxPressureHpa", event.maxPressureHpa());
    record.put("avgPressureHpa", event.avgPressureHpa());
    record.put("pressureDeltaHpa", event.pressureDeltaHpa());
    record.put("trend", event.trend());
    return record;
  }

  public static GenericRecord toRecord(ProcessedSensorEvent event) {
    GenericRecord record = new GenericData.Record(AvroSchemas.PROCESSED_SENSOR_EVENT);
    record.put("eventId", event.eventId().toString());
    record.put("sourceEventId", event.sourceEventId().toString());
    record.put("ingestionTimestamp", event.ingestionTimestamp().toString());
    record.put("processedAt", event.processedAt().toString());
    record.put("processor", event.processor());
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("temperatureC", event.temperatureC());
    record.put("pressureHpa", event.pressureHpa());
    return record;
  }

  public static EnrichedSensorEvent toEnrichedSensorEvent(GenericRecord record) {
    GenericRecord payload = (GenericRecord) record.get("payload");
    return new EnrichedSensorEvent(
        UUID.fromString(string(record, "eventId")),
        Instant.parse(string(record, "ingestionTimestamp")),
        toSensorEvent(payload)
    );
  }

  public static TemperatureEvent toTemperatureEvent(GenericRecord record) {
    return new TemperatureEvent(
        UUID.fromString(string(record, "eventId")),
        UUID.fromString(string(record, "sourceEventId")),
        Instant.parse(string(record, "ingestionTimestamp")),
        instantOrNull(record, "measurementTimestamp"),
        string(record, "deviceId"),
        string(record, "sensor"),
        (Double) record.get("temperatureC")
    );
  }

  public static PressureEvent toPressureEvent(GenericRecord record) {
    return new PressureEvent(
        UUID.fromString(string(record, "eventId")),
        UUID.fromString(string(record, "sourceEventId")),
        Instant.parse(string(record, "ingestionTimestamp")),
        instantOrNull(record, "measurementTimestamp"),
        string(record, "deviceId"),
        string(record, "sensor"),
        (Double) record.get("pressureHpa")
    );
  }

  public static TemperatureAggregateEvent toTemperatureAggregateEvent(GenericRecord record) {
    return new TemperatureAggregateEvent(
        string(record, "deviceId"),
        string(record, "sensor"),
        Instant.parse(string(record, "windowStart")),
        Instant.parse(string(record, "windowEnd")),
        (Long) record.get("count"),
        (Double) record.get("minTemperatureC"),
        (Double) record.get("maxTemperatureC"),
        (Double) record.get("avgTemperatureC")
    );
  }

  public static PressureAggregateEvent toPressureAggregateEvent(GenericRecord record) {
    return new PressureAggregateEvent(
        string(record, "deviceId"),
        string(record, "sensor"),
        Instant.parse(string(record, "windowStart")),
        Instant.parse(string(record, "windowEnd")),
        (Long) record.get("count"),
        (Double) record.get("minPressureHpa"),
        (Double) record.get("maxPressureHpa"),
        (Double) record.get("avgPressureHpa"),
        (Double) record.get("pressureDeltaHpa"),
        string(record, "trend")
    );
  }

  public static ProcessedSensorEvent toProcessedSensorEvent(GenericRecord record) {
    return new ProcessedSensorEvent(
        UUID.fromString(string(record, "eventId")),
        UUID.fromString(string(record, "sourceEventId")),
        Instant.parse(string(record, "ingestionTimestamp")),
        Instant.parse(string(record, "processedAt")),
        string(record, "processor"),
        string(record, "deviceId"),
        string(record, "sensor"),
        (Double) record.get("temperatureC"),
        (Double) record.get("pressureHpa")
    );
  }

  private static GenericRecord toSensorRecord(Schema schema, SensorEvent event) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("deviceId", event.deviceId());
    record.put("sensor", event.sensor());
    record.put("temperatureC", event.temperatureC());
    record.put("pressureHpa", event.pressureHpa());
    record.put("timestamp", event.timestamp() == null ? null : event.timestamp().toString());
    return record;
  }

  private static SensorEvent toSensorEvent(GenericRecord record) {
    String timestamp = nullableString(record, "timestamp");
    return new SensorEvent(
        string(record, "deviceId"),
        string(record, "sensor"),
        (Double) record.get("temperatureC"),
        (Double) record.get("pressureHpa"),
        timestamp == null ? null : Instant.parse(timestamp)
    );
  }

  private static String string(GenericRecord record, String field) {
    return record.get(field).toString();
  }

  private static Instant instantOrNull(GenericRecord record, String field) {
    String value = nullableString(record, field);
    return value == null ? null : Instant.parse(value);
  }

  private static String nullableString(GenericRecord record, String field) {
    Object value = record.get(field);
    return value == null ? null : value.toString();
  }
}
