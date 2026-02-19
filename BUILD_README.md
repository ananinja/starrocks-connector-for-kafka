# Building the StarRocks Kafka Connector Image

This repo contains a patched fork of the [StarRocks Kafka Connector](https://github.com/StarRocks/starrocks-connector-for-kafka) with the following fixes:

- **Debezium temporal type support** — `JsonConverter` now converts Debezium logical types (`io.debezium.time.Date`, `io.debezium.time.Timestamp`, etc.) to human-readable strings that StarRocks can parse directly into `DATE`/`DATETIME` columns.
- **Schemaless record support** — `AddOpFieldForDebeziumRecord` now handles `Map`-based records (when `schemas.enable=false`), in addition to the original `Struct`-based path.

## Prerequisites

- Docker (with BuildKit)
- AWS CLI configured with access to ECR (`336791371919.dkr.ecr.eu-west-1.amazonaws.com`)

Maven and Java are **not** required locally — the build runs inside a Docker multi-stage build.

## Build and push

```bash
# Login to ECR
aws ecr get-login-password --region eu-west-1 | \
  docker login --username AWS --password-stdin 336791371919.dkr.ecr.eu-west-1.amazonaws.com

# Build (use linux/amd64 when building on Apple Silicon)
docker build --platform linux/amd64 \
  -t 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.7-sr .

# Push
docker push 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.7-sr
```

Then update the image tag in the infra repo:

```yaml
# clusters/prod/debezium/orders-starrocks-sink-connect.yaml
spec:
  image: 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.7-sr
```

## What's in the image

| Layer | Version | Purpose |
|-------|---------|---------|
| Strimzi Kafka | 0.47.0 / Kafka 3.9.0 | Base Kafka Connect runtime |
| StarRocks Kafka Connector | 1.0.5 (patched) | Sink connector + patched JsonConverter |
| Debezium MySQL Connector | 3.0.5.Final | ExtractNewRecordState (unwrap) and other SMTs |

## Versioning

Bump the version in `pom.xml` (`<version>1.0.5</version>`) and the Docker tag together. Use the convention `<connector-version>-sr` for the ECR tag (e.g. `1.0.7-sr`).

## Local build (without Docker)

If you need to build locally for testing:

```bash
mvn clean package -DskipTests
ls target/starrocks-connector-for-kafka-*-with-dependencies.jar
```

Requires Java 8+ and Maven 3.6+.
