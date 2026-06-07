package org.kergru.iotplatform.report;

import java.util.List;
import java.util.function.Function;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
public class ReportService {

  private final MongoTemplate mongoTemplate;
  private final ReportProperties properties;

  public ReportService(MongoTemplate mongoTemplate, ReportProperties properties) {
    this.mongoTemplate = mongoTemplate;
    this.properties = properties;
  }

  public List<AggregateReport> latestTemperatures(int limit) {
    return latest(properties.temperatureCollection(), limit).stream().map(AggregateReport::temperature).toList();
  }

  public List<AggregateReport> latestPressures(int limit) {
    return latest(properties.pressureCollection(), limit).stream().map(AggregateReport::pressure).toList();
  }

  public ReportOverview overview() {
    return new ReportOverview(
        summary("temperature", properties.temperatureCollection(), AggregateReport::temperature),
        summary("pressure", properties.pressureCollection(), AggregateReport::pressure)
    );
  }

  private List<Document> latest(String collectionName, int limit) {
    Query query = new Query().with(Sort.by(Sort.Direction.DESC, "windowEnd")).limit(limit);
    return mongoTemplate.find(query, Document.class, collectionName);
  }

  private AggregateSummary summary(String type, String collectionName, Function<Document, AggregateReport> mapper) {
    long windows = mongoTemplate.getCollection(collectionName).countDocuments();
    AggregateReport latest = latest(collectionName, 1).stream().findFirst().map(mapper).orElse(null);
    return new AggregateSummary(
        type,
        windows,
        totalSamples(collectionName),
        latest == null ? null : latest.windowEnd(),
        latest == null ? null : latest.avg(),
        latest == null ? null : latest.trend()
    );
  }

  private long totalSamples(String collectionName) {
    Document result = mongoTemplate.getCollection(collectionName).aggregate(List.of(
        new Document("$group", new Document("_id", null).append("total", new Document("$sum", "$count")))
    )).first();
    if (result == null) {
      return 0;
    }
    Object total = result.get("total");
    return total instanceof Number number ? number.longValue() : 0;
  }
}
