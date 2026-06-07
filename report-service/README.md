# report-service

Spring MVC service that reads aggregate sensor data from MongoDB and renders a small report UI.

## Role in the Platform

This module is the reporting layer of the system.
It reads the aggregate collections written by Kafka Connect and exposes the data as both an HTML page and JSON endpoints.

## Inputs

- `iot.temperature_aggregates`
- `iot.pressure_aggregates`

The collection names are configurable for local experiments or alternate sink mappings.

## Outputs

- HTML report UI at `/`
- JSON overview at `/api/reports`
- JSON temperature aggregate list at `/api/reports/temperature`
- JSON pressure aggregate list at `/api/reports/pressure`

## Report UI

The UI shows compact summaries for temperature and pressure aggregates plus the latest aggregate windows.
Use the `limit` query parameter to control how many rows are shown:

```text
http://localhost:8080/?limit=50
```

The limit is clamped to `1..200`.

## Important Configuration

Configuration is driven by `src/main/resources/application.yaml` and environment variables.

Relevant settings:

- `SERVER_PORT`
- `MONGODB_HOST`
- `MONGODB_PORT`
- `MONGODB_DATABASE`
- `MONGODB_USERNAME`
- `MONGODB_PASSWORD`
- `MONGODB_AUTHENTICATION_DATABASE`
- `TEMPERATURE_AGGREGATES_COLLECTION`
- `PRESSURE_AGGREGATES_COLLECTION`

For local development outside Docker, the default MongoDB connection is:

```text
mongodb://admin:admin@localhost:27017/iot?authSource=admin
```

## Main Packages

- `org.kergru.iotplatform.report` - Spring Boot application, controllers, service, and DTOs
- `ReportViewController` - HTML report entry point
- `ReportController` - JSON API endpoints
- `ReportService` - MongoDB queries and report summary calculation
- `AggregateReport` - mapper from MongoDB aggregate documents to response records

## Build

```bash
./gradlew :report-service:build
```

## Run Locally

The service expects MongoDB to contain aggregate data written by the Kafka Connect sinks.
The full stack is started from the repository root:

```bash
docker compose up -d --build
```

Open the report UI:

```text
http://localhost:8080/
```

Useful JSON checks:

```bash
curl -fsS http://localhost:8080/api/reports
curl -fsS 'http://localhost:8080/api/reports/temperature?limit=20'
curl -fsS 'http://localhost:8080/api/reports/pressure?limit=20'
```

To run only this module against a local MongoDB instance:

```bash
./gradlew :report-service:bootRun
```

## Notes

- Keep this module read-only against MongoDB.
- Keep report-specific MongoDB document mapping in this module.
- Add new report views only after the corresponding aggregate stream and MongoDB sink exist.
