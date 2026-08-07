SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'pipeline.name' = 'events-to-serving-and-lakehouse';
SET 'table.exec.source.idle-timeout' = '30 s';

CREATE TABLE app_events (
  event_id STRING NOT NULL,
  event_type STRING,
  event_version INT,
  producer STRING,
  occurred_at TIMESTAMP_LTZ(3),
  ingested_at TIMESTAMP_LTZ(3),
  correlation_id STRING,
  partition_key STRING,
  payload MAP<STRING, STRING>,
  source_topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  WATERMARK FOR occurred_at AS occurred_at - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic-pattern' = 'app[.](python|php)[.]events[.]v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-events-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE starrocks_events (
  event_id STRING,
  event_type STRING,
  event_version INT,
  producer STRING,
  occurred_at TIMESTAMP(3),
  ingested_at TIMESTAMP(3),
  correlation_id STRING,
  partition_key STRING,
  payload_json STRING,
  source_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'starrocks',
  'jdbc-url' = 'jdbc:mysql://starrocks:9030',
  'load-url' = 'starrocks:8080',
  'database-name' = 'analytics',
  'table-name' = 'events_realtime',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.interval-ms' = '5000',
  'sink.max-retries' = '5',
  'sink.properties.format' = 'json',
  'sink.properties.strip_outer_array' = 'true'
);

CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://seaweedfs:8181',
  'warehouse' = 's3://iceberg-warehouse/',
  'credential' = '__AWS_ACCESS_KEY_ID__:__AWS_SECRET_ACCESS_KEY__',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.access-key-id' = '__AWS_ACCESS_KEY_ID__',
  's3.secret-access-key' = '__AWS_SECRET_ACCESS_KEY__',
  's3.path-style-access' = 'true',
  's3.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.events (
  event_id STRING,
  event_type STRING,
  event_version INT,
  producer STRING,
  occurred_at TIMESTAMP_LTZ(3),
  ingested_at TIMESTAMP_LTZ(3),
  correlation_id STRING,
  partition_key STRING,
  payload MAP<STRING, STRING>,
  source_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  event_date DATE
)
PARTITIONED BY (event_date)
WITH (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.target-file-size-bytes' = '134217728'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO starrocks_events
  SELECT
    event_id,
    event_type,
    event_version,
    producer,
    CAST(occurred_at AS TIMESTAMP(3)),
    CAST(ingested_at AS TIMESTAMP(3)),
    correlation_id,
    partition_key,
    CAST(payload AS STRING),
    source_topic,
    kafka_partition,
    kafka_offset
  FROM app_events
  WHERE event_id IS NOT NULL AND occurred_at IS NOT NULL;

  INSERT INTO lakehouse.bronze.events
  SELECT
    event_id,
    event_type,
    event_version,
    producer,
    occurred_at,
    ingested_at,
    correlation_id,
    partition_key,
    payload,
    source_topic,
    kafka_partition,
    kafka_offset,
    CAST(occurred_at AS DATE)
  FROM app_events
  WHERE event_id IS NOT NULL AND occurred_at IS NOT NULL;
END;
