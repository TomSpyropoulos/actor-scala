FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.5_3.8.2 AS builder
WORKDIR /app

# cache project definition and deps
COPY build.sbt /app/
COPY project /app/project/
RUN sbt update
# then copy sources and assemble
COPY . /app
RUN sbt clean assembly

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
# Copy the fat JAR from the build stage
# Note: Path depends on your sbt project structure and assembly settings
COPY --from=builder /app/target/scala-*/app-assembly*.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
