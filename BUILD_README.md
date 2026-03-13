# Building the StarRocks Kafka Connector Image

This repo contains a patched fork of the [StarRocks Kafka Connector](https://github.com/StarRocks/starrocks-connector-for-kafka) with the following additions:

- **`AddOpFieldForDebeziumRecord`** — unwraps Debezium envelopes for schemaless `Map`-based records (when `schemas.enable=false`), adding `__op` for StarRocks PRIMARY KEY merge semantics. The upstream connector only handled `Struct`-based (schema-enabled) records.
- **`MicroTimestampConverter`** — converts Debezium `MicroTimestamp` fields (microseconds since Unix epoch) to `"yyyy-MM-dd HH:mm:ss"` strings for StarRocks `DATETIME` columns. Used for PostgreSQL/MySQL `timestamp` columns.
- **`EpochDayConverter`** — converts Debezium `Date` fields (days since Unix epoch) to `"yyyy-MM-dd"` strings for StarRocks `DATE` columns. Used for MySQL `date` columns.
- **`MicroTimeConverter`** — converts Debezium `MicroTime` fields (microseconds since midnight) to `"HH:mm:ss"` strings for StarRocks `VARCHAR` time columns. Used for PostgreSQL `time without time zone` columns.

## Prerequisites

- Docker (with BuildKit)
- AWS CLI configured with access to ECR (`336791371919.dkr.ecr.eu-west-1.amazonaws.com`)

Maven and Java are **not** required locally — the build runs inside a Docker multi-stage build.

## Build and push

```bash
# Login to ECR
aws ecr get-login-password --region eu-west-1 | \
  docker login --username AWS --password-stdin 336791371919.dkr.ecr.eu-west-1.amazonaws.com

# Build (use --platform linux/amd64 when building on Apple Silicon)
docker build --platform linux/amd64 \
  -t 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.10-sr .

# Push
docker push 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.10-sr
```

Then update the image tag in all `*-starrocks-sink-connect.yaml` files in the infra repo:

```yaml
spec:
  image: 336791371919.dkr.ecr.eu-west-1.amazonaws.com/debezium:1.0.10-sr
```

## What's in the image

| Layer | Version | Purpose |
|-------|---------|---------|
| Strimzi Kafka | 0.47.0 / Kafka 3.9.0 | Base Kafka Connect runtime |
| StarRocks Kafka Connector | patched (this repo) | Sink connector + custom SMTs |
| Debezium MySQL Connector | 3.0.5.Final | Bundled for SMT classes (ExtractNewRecordState etc.) |

## Versioning

Use the convention `<connector-version>-sr` for the ECR tag (e.g. `1.0.10-sr`).
Bump the tag whenever new SMTs or fixes are added to this repo.

## Local build (without Docker)

If you need to build locally for testing:

```bash
mvn clean package -DskipTests
ls target/starrocks-connector-for-kafka-*-with-dependencies.jar
```

Requires Java 8+ and Maven 3.6+.
