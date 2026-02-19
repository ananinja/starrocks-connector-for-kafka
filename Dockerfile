# Multi-stage build: compile the connector, then package into a Strimzi Kafka Connect image.
#
# Usage:
#   docker build --platform linux/amd64 -t <ecr-repo>/debezium:<tag> .
#
# The final image contains:
#   - Strimzi Kafka Connect 3.9.0 (base)
#   - StarRocks Kafka Connector (patched, built from source)
#   - Debezium MySQL Connector 3.0.5 (for ExtractNewRecordState and other transforms)

# --- Stage 1: Build the connector JAR ---
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY pom.xml .
# Download dependencies first (cacheable layer)
RUN mvn dependency:go-offline -q

COPY src/ src/
RUN mvn package -q -DskipTests \
    && ls target/starrocks-connector-for-kafka-*-with-dependencies.jar

# --- Stage 2: Assemble the Kafka Connect image ---
FROM quay.io/strimzi/kafka:0.47.0-kafka-3.9.0

USER root

# StarRocks Kafka Connector (patched build from stage 1)
RUN mkdir -p /opt/kafka/plugins/starrocks-kafka-connector
COPY --from=builder /build/target/starrocks-connector-for-kafka-*-with-dependencies.jar \
     /opt/kafka/plugins/starrocks-kafka-connector/

# Debezium MySQL Connector (includes ExtractNewRecordState and other transform classes)
RUN mkdir -p /opt/kafka/plugins/debezium-mysql-connector && \
    cd /opt/kafka/plugins/debezium-mysql-connector && \
    curl -sL https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.0.5.Final/debezium-connector-mysql-3.0.5.Final-plugin.tar.gz | tar xz --strip-components=1

USER 1001
