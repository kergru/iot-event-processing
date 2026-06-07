package org.kergru.iotplatform.report;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReportController {

  private final ReportService reportService;

  public ReportController(ReportService reportService) {
    this.reportService = reportService;
  }

  @GetMapping("/api/reports")
  public ReportOverview overview() {
    return reportService.overview();
  }

  @GetMapping("/api/reports/temperature")
  public List<AggregateReport> latestTemperatures(@RequestParam(defaultValue = "20") int limit) {
    return reportService.latestTemperatures(sanitizedLimit(limit));
  }

  @GetMapping("/api/reports/pressure")
  public List<AggregateReport> latestPressures(@RequestParam(defaultValue = "20") int limit) {
    return reportService.latestPressures(sanitizedLimit(limit));
  }

  private int sanitizedLimit(int limit) {
    return Math.min(Math.max(limit, 1), 200);
  }
}
