package org.kergru.iotplatform.report;

import java.time.Instant;

public record AggregateSummary(
    String type,
    long windows,
    long samples,
    Instant latestWindowEnd,
    Double latestAvg,
    String latestTrend
) {}
