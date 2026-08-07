SET 'execution.runtime-mode' = 'streaming';
SET 'execution.attached' = 'false';
SET 'pipeline.name' = 'mysql-orders-cdc-to-starrocks';

CREATE TABLE mysql_orders_cdc (
  id BIGINT,
  customer_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.mysql.inventory.orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-mysql-orders-cdc-v1',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json',
  'value.debezium-json.schema-include' = 'false',
  'value.debezium-json.ignore-parse-errors' = 'true'
);

CREATE TABLE starrocks_orders (
  id BIGINT,
  customer_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'starrocks',
  'jdbc-url' = 'jdbc:mysql://starrocks:9030',
  'load-url' = 'starrocks:8080',
  'database-name' = 'analytics',
  'table-name' = 'orders_current',
  'username' = 'root',
  'password' = '',
  'sink.buffer-flush.interval-ms' = '3000',
  'sink.max-retries' = '5',
  'sink.properties.format' = 'json',
  'sink.properties.strip_outer_array' = 'true'
);

INSERT INTO starrocks_orders SELECT id, customer_id, status, amount FROM mysql_orders_cdc;
