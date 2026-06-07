package org.kergru.iotplatform.report;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class ReportViewController {

  private final ReportService reportService;

  public ReportViewController(ReportService reportService) {
    this.reportService = reportService;
  }

  @GetMapping("/")
  public String index(@RequestParam(defaultValue = "20") int limit, Model model) {
    int sanitizedLimit = sanitizedLimit(limit);
    model.addAttribute("limit", sanitizedLimit);
    model.addAttribute("overview", reportService.overview());
    model.addAttribute("temperatures", reportService.latestTemperatures(sanitizedLimit));
    model.addAttribute("pressures", reportService.latestPressures(sanitizedLimit));
    return "reports";
  }

  private int sanitizedLimit(int limit) {
    return Math.min(Math.max(limit, 1), 200);
  }
}
