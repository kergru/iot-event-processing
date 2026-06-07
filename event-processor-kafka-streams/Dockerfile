FROM eclipse-temurin:21-jre
WORKDIR /app
COPY event-processor-kafka-streams/build/install/event-processor-kafka-streams/ ./
ENTRYPOINT ["/app/bin/event-processor-kafka-streams"]
