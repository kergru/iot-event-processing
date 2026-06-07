package org.kergru.iotplatform.processor.flink;

import java.io.Serializable;
import org.kergru.iot.event.sensor.PressureEvent;

public class PressureAggregateAccumulator implements Serializable {

  private String deviceId;
  private String sensor;
  private long count;
  private Double minPressureHpa;
  private Double maxPressureHpa;
  private double sumPressureHpa;
  private Double firstPressureHpa;
  private Double lastPressureHpa;

  public void add(PressureEvent event) {
    if (event.pressureHpa() == null) {
      return;
    }
    deviceId = event.deviceId();
    sensor = event.sensor();
    count++;
    minPressureHpa = minPressureHpa == null ? event.pressureHpa() : Math.min(minPressureHpa, event.pressureHpa());
    maxPressureHpa = maxPressureHpa == null ? event.pressureHpa() : Math.max(maxPressureHpa, event.pressureHpa());
    sumPressureHpa += event.pressureHpa();
    if (firstPressureHpa == null) {
      firstPressureHpa = event.pressureHpa();
    }
    lastPressureHpa = event.pressureHpa();
  }

  public String deviceId() {
    return deviceId;
  }

  public String sensor() {
    return sensor;
  }

  public long count() {
    return count;
  }

  public Double minPressureHpa() {
    return minPressureHpa;
  }

  public Double maxPressureHpa() {
    return maxPressureHpa;
  }

  public Double avgPressureHpa() {
    return count == 0 ? null : sumPressureHpa / count;
  }

  public Double pressureDeltaHpa() {
    return firstPressureHpa == null || lastPressureHpa == null ? null : lastPressureHpa - firstPressureHpa;
  }
}
