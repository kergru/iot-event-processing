package org.kergru.iot.event.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;

public final class AvroSchemas {

  public static final Schema SENSOR_EVENT = load("avro/sensor_event.avsc");
  public static final Schema ENRICHED_SENSOR_EVENT = load("avro/enriched_sensor_event.avsc");
  public static final Schema PROCESSED_SENSOR_EVENT = load("avro/processed_sensor_event.avsc");
  public static final Schema TEMPERATURE_EVENT = load("avro/temperature_event.avsc");
  public static final Schema PRESSURE_EVENT = load("avro/pressure_event.avsc");
  public static final Schema TEMPERATURE_AGGREGATE_EVENT = load("avro/temperature_aggregate_event.avsc");
  public static final Schema PRESSURE_AGGREGATE_EVENT = load("avro/pressure_aggregate_event.avsc");

  private AvroSchemas() {
  }

  private static Schema load(String path) {
    try (InputStream inputStream = AvroSchemas.class.getClassLoader().getResourceAsStream(path)) {
      if (inputStream == null) {
        throw new IllegalStateException("Avro schema not found: " + path);
      }
      return new Schema.Parser().parse(inputStream);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }
}
