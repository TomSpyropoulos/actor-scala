FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.12.5_3.8.2 AS builder
WORKDIR /app

# Phase 1: cache project definition and deps
COPY build.sbt /app/
COPY project /app/project/
RUN sbt update

# Phase 2: copy sources and compile the app
COPY . /app
RUN sbt compile

# Phase 3: assemble the fat JAR
RUN sbt assembly

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /app/target/scala-*/app-assembly*.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
