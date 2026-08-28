# Service image. Builds only the service module and its parent, so Spark and
# the lakehouse dependencies never enter this image.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY cloudrelay-service/pom.xml cloudrelay-service/
# The lakehouse pom, and nothing else of it. Maven reads every module the
# parent declares before it works out which ones to build, so the file has to
# exist; without its src the reactor still skips it entirely.
COPY cloudrelay-lakehouse/pom.xml cloudrelay-lakehouse/
# -pl selects the module, -am builds the parent it needs. Without -pl the
# reactor would build the lakehouse too and drag ~300MB of Spark into a layer
# that gets thrown away.
RUN mvn -B -pl cloudrelay-service -am dependency:go-offline
COPY cloudrelay-service/src cloudrelay-service/src
RUN mvn -B -pl cloudrelay-service -am package -DskipTests

FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/cloudrelay-service/target/cloudrelay-service-*.jar app.jar
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar", "--spring.profiles.active=docker"]
