package org.kergru.iotplatform.report;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "report")
public record ReportProperties(
    String temperatureCollection,
    String pressureCollection
) {}
