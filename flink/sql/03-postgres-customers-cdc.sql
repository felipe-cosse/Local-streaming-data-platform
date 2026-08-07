SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'pipeline.name' = 'postgres-customers-cdc-to-starrocks';

CREATE TABLE postgres_customers_cdc (
  id BIGINT,
  email STRING,
  full_name STRING,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.postgres.public.customers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-postgres-customers-cdc-v1',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'false',
  'value.debezium-json.ignore-parse-errors' = 'true'
);

CREATE TABLE starrocks_customers (
  id BIGINT,
  email STRING,
  full_name STRING,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'starrocks',
  'jdbc-url' = 'jdbc:mysql://starrocks:9030',
  'load-url' = 'starrocks:8080',
  'database-name' = 'analytics',
  'table-name' = 'customers_current',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.interval-ms' = '3000',
  'sink.max-retries' = '5',
  'sink.properties.format' = 'json',
  'sink.properties.strip_outer_array' = 'true'
);

INSERT INTO starrocks_customers SELECT id, email, full_name, status FROM postgres_customers_cdc;
