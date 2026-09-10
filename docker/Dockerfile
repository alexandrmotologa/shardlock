# Multi-stage Dockerfile for ShardLock daemon
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy root pom and module poms first for caching
COPY pom.xml .
COPY shardlock-core/pom.xml shardlock-core/
COPY shardlock-client/pom.xml shardlock-client/

# Copy sources
COPY shardlock-core/src shardlock-core/src
COPY shardlock-client/src shardlock-client/src

# Build production artifacts
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S shardlock && adduser -S shardlock -G shardlock
USER shardlock

COPY --from=builder /build/shardlock-core/target/shardlock-core-*.jar app.jar

EXPOSE 8001 9001

ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-jar", "app.jar"]
