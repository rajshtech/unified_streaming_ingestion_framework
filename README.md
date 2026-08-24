# Unified Streaming Ingestion Framework

A config-driven Spark Structured Streaming framework for moving data between systems without writing new Spark code for every pipeline. A pipeline is defined entirely by a HOCON config file: pick a **source**, a chain of **transforms**, and a **sink**, and the framework wires them together.

Currently implemented and tested end-to-end: **Kafka → Delta Lake**.

## How it works

```
Source (readStream)  ──▶  Transform chain  ──▶  Sink (writeStream)
     KafkaStreamSource        ChainTransform          DeltalakeStreamSink
                               ├─ SqlSelectTransform
                               └─ CustomTransform
```

Every pipeline runs through the same entry point, [`UnifiedStreamingManager`](src/main/scala/com/unified_streaming_ingestion_framework/spark/pipelines/UnifiedStreamingManager.scala):

1. Parse CLI args and load the HOCON config file
2. Resolve secrets (file / env / Vault / API) and merge them into the config
3. Build a `SparkSession`, register a shutdown hook and a Kafka-lag listener
4. `readStream()` → build the configured source and load a streaming `DataFrame`
5. `applyTransformations()` → run the configured transform chain
6. `writeStream()` → build the configured sink and start the streaming query

Source, transform, and sink types are each resolved through a small factory/builder (`StreamingSourceBuilder`, `StreamingTransformFactory`, `StreamingSinkBuilder`) based on a `"type"` / `"source_type"` / `"sink_type"` field in the config, so adding a new implementation doesn't require touching the pipeline driver.

## Status

| Component | Status |
|---|---|
| Kafka source (Avro via Schema Registry, or JSON — value-as-string / struct-with-explicit-schema) | ✅ Implemented |
| Delta Lake sink (insert and merge/upsert) | ✅ Implemented |
| `select` / `custom` / `chain` transforms | ✅ Implemented |
| File-based secrets | ✅ Implemented |
| HashiCorp Vault secrets | ⚠️ Implemented, untested here |
| Env-var secrets | ⚠️ Implemented, untested here |
| API/HTTPS-based secrets | ❌ Not implemented |
| S3 source, S3/Kafka/SQL/Greenplum sinks | ❌ Not implemented |
| `sql` / `filter` transforms | ❌ Not implemented |
| Auto-offset-toggle (detect newly-added topics on restart) | ⚠️ Implemented, lightly tested — only exercised once a checkpoint already exists |

Unimplemented sink/source types fail fast with a clear `UnsupportedOperationException` / `IllegalArgumentException` rather than doing something silently wrong.

## Prerequisites

- JDK 17 (the build targets Java 8 bytecode via `maven-compiler-plugin`, but compiles and runs fine under JDK 17)
- Maven 3.9+
- Docker Desktop (for the local Kafka broker via `docker-compose.yml`)
- A local [Apache Spark 3.5.2](https://spark.apache.org/downloads.html) distribution (matching `spark.version` in `pom.xml`) if you want to run the pipeline with `spark-submit` rather than in an existing cluster
- On Windows: [`winutils.exe`](https://github.com/cdarlint/winutils) for Hadoop 3.3.x, with `HADOOP_HOME` pointing at the folder containing `bin/winutils.exe`

## Project layout

```
src/main/scala/.../spark/
├── pipelines/            # Entry point: UnifiedStreamingManager, StreamingPipelineController
└── components/
    ├── sources/          # StreamingSource + StreamingSourceBuilder, KafkaStreamSource
    ├── sinks/             # StreamingSink + StreamingSinkBuilder, DeltalakeStreamSink
    ├── transformers/      # StreamingTransform + StreamingTransformFactory, Chain/SqlSelect/Custom
    ├── secret_manager/    # SecretsEngine + File/Env/Vault/Https IO clients
    ├── listeners/         # KafkaLagListener - offset tracking & commit-back-to-Kafka
    ├── shutdown_manager/  # Graceful shutdown hook
    ├── spark_utils/       # Filesystem helpers, email notifications
    └── utils/             # Shared config/Kafka-param helpers

src/main/resources/
├── kafka2delta.conf        # Sample Kafka → Delta pipeline config
└── config_secrets.example  # Template for local file-based secrets (copy to config_secrets)

docker-compose.yml           # Single-node Kafka (KRaft mode, SASL_PLAINTEXT) for local dev
docker/secrets/broker_jaas.conf.example  # Template broker credentials (copy to broker_jaas.conf)
```

## Running the sample pipeline locally

### 1. Set up local secrets (not committed to git)

```bash
cp src/main/resources/config_secrets.example src/main/resources/config_secrets
cp docker/secrets/broker_jaas.conf.example docker/secrets/broker_jaas.conf
```

Edit both files and replace `CHANGEME_USERNAME` / `CHANGEME_PASSWORD` with matching values — the username/password in `config_secrets` (what the pipeline authenticates with) must match a `user_<username>="<password>"` entry in `broker_jaas.conf` (what the broker accepts). Keep the `admin` / `admin-secret` entry in `broker_jaas.conf` as-is; the broker uses it for its own inter-broker auth.

### 2. Point the config at your checkout

`src/main/resources/kafka2delta.conf` currently uses absolute paths. Update these to match your machine:

- `envConfig.file.fetch_creds[].path` → path to your local `config_secrets`
- `target.checkpoint_path` / `target.target_path` → where you want the checkpoint and Delta table written locally

### 3. Start Kafka

```bash
docker compose up -d
```

This brings up a single-node Kafka broker (KRaft mode, no Zookeeper) on `localhost:9092`, secured with SASL_PLAINTEXT using the credentials from `broker_jaas.conf`.

### 4. Build

```bash
mvn clean package -DskipTests
```

This produces `target/unified_streaming_ingestion_framework-1.0.0-SNAPSHOT-uber.jar`. Spark, Hadoop, and Delta are deliberately excluded from the shaded jar (see the `maven-shade-plugin` filters in `pom.xml`) since a real cluster already provides them — for local runs, supply Delta via `--packages`.

### 5. Produce a test message

The sample config expects JSON messages on a topic named `user_activity` matching the schema in `source.infer_as_struct`. From inside the Kafka container:

```bash
docker exec -it kafka bash -c '
cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<CHANGEME_USERNAME>" password="<CHANGEME_PASSWORD>";
EOF
echo "{\"article_id\":\"a1\",\"article_version_id\":\"v1\",\"client_code\":\"c1\",\"eecode\":\"e1\",\"id\":\"1\",\"module_id\":\"m1\",\"origin\":\"web\",\"proc_city\":\"nyc\",\"user_device\":\"mobile\",\"view_timestamp\":1700000000}" | kafka-console-producer --bootstrap-server localhost:9092 --producer.config /tmp/client.properties --topic user_activity
'
```

### 6. Run the pipeline

```bash
spark-submit \
  --class com.unified_streaming_ingestion_framework.spark.pipelines.StreamingPipelineController \
  --master "local[*]" \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  target/unified_streaming_ingestion_framework-1.0.0-SNAPSHOT-uber.jar \
  config_path=/absolute/path/to/src/main/resources/kafka2delta.conf \
  pipeline_name=kafka2delta \
  app_name=local_test \
  spark_master=local[*]
```

`trigger_once` is `true` in the sample config, so this processes everything currently available on the topic and exits (`Trigger.AvailableNow`). Set it to `"false"` and adjust `trigger_seconds` for continuous processing.

Query the result:

```bash
spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  -e "SELECT * FROM delta.\`/absolute/path/to/target_lo/data\`"
```

## Configuration reference

Pipelines are configured via a single HOCON file, passed with `config_path=...`. Top-level sections:

| Section | Purpose |
|---|---|
| `appControlConfig` | Stop-flag file paths for external graceful shutdown |
| `email` | SMTP settings for failure notifications (optional — skipped if `email_to_address` is blank) |
| `envConfig` | Where to resolve secrets from (`file`, `env`, `vault`, `api`) and S3/schema-registry toggles |
| `source` | `source_type` (`kafka`), Kafka connection/topic settings, payload format |
| `target` | `sink_type` (`delta`), table path, checkpoint path, trigger, partitioning, write mode |
| `transform` | A `chain` of `select` / `custom` steps applied to the source `DataFrame` |

See the inline comments in [`kafka2delta.conf`](src/main/resources/kafka2delta.conf) for the full set of options — most of the interesting behavior (JSON schema inference mode, SASL toggles, merge vs. insert) is driven from there.

## License

No license file is currently included — until one is added, default copyright applies (all rights reserved). Add a `LICENSE` file before relying on this being open source in practice.
