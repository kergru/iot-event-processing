package org.kergru.iotplatform.processor.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.kergru.iot.event.sensor.PressureEvent;

public class PressureAggregateFunction
    implements AggregateFunction<PressureEvent, PressureAggregateAccumulator, PressureAggregateAccumulator> {

  @Override
  public PressureAggregateAccumulator createAccumulator() {
    return new PressureAggregateAccumulator();
  }

  @Override
  public PressureAggregateAccumulator add(PressureEvent value, PressureAggregateAccumulator accumulator) {
    accumulator.add(value);
    return accumulator;
  }

  @Override
  public PressureAggregateAccumulator getResult(PressureAggregateAccumulator accumulator) {
    return accumulator;
  }

  @Override
  public PressureAggregateAccumulator merge(PressureAggregateAccumulator first, PressureAggregateAccumulator second) {
    return first;
  }
}
