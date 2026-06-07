FROM eclipse-temurin:21-jre
WORKDIR /app
COPY mqtt-to-kafka-bridge/build/libs/mqtt-to-kafka-bridge-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
