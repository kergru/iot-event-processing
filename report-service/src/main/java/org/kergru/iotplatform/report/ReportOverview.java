package org.kergru.iotplatform.report;

public record ReportOverview(
    AggregateSummary temperature,
    AggregateSummary pressure
) {}
