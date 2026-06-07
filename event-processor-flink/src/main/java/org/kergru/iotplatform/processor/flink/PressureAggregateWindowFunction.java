package org.kergru.iotplatform.processor.flink;

import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.kergru.iot.event.processing.PressureAggregateEvent;

public class PressureAggregateWindowFunction
    implements WindowFunction<PressureAggregateAccumulator, PressureAggregateEvent, String, TimeWindow> {

  @Override
  public void apply(
      String key,
      TimeWindow window,
      Iterable<PressureAggregateAccumulator> input,
      Collector<PressureAggregateEvent> output
  ) {
    PressureAggregateAccumulator accumulator = input.iterator().next();
    output.collect(new PressureAggregateEvent(
        accumulator.deviceId(),
        accumulator.sensor(),
        Instant.ofEpochMilli(window.getStart()),
        Instant.ofEpochMilli(window.getEnd()),
        accumulator.count(),
        accumulator.minPressureHpa(),
        accumulator.maxPressureHpa(),
        accumulator.avgPressureHpa(),
        accumulator.pressureDeltaHpa(),
        trend(accumulator.pressureDeltaHpa())
    ));
  }

  private static String trend(Double delta) {
    if (delta == null) {
      return "stable";
    }
    if (delta <= -1.0) {
      return "falling";
    }
    if (delta >= 1.0) {
      return "rising";
    }
    return "stable";
  }
}
