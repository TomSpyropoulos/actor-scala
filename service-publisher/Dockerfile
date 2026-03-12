FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.5_3.8.2 AS build
WORKDIR /app
COPY . .

RUN sbt assembly

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Copy the fat JAR from the build stage
# Note: Path depends on your sbt project structure and assembly settings
COPY --from=build /app/target/scala-*/service-publisher-assembly*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
