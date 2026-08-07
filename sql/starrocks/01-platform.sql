CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS marts;
CREATE DATABASE IF NOT EXISTS quality;

CREATE TABLE IF NOT EXISTS analytics.events_realtime (
  event_id VARCHAR(36) NOT NULL,
  event_type VARCHAR(100) NULL,
  event_version INT NULL,
  producer VARCHAR(64) NULL,
  occurred_at DATETIME NULL,
  ingested_at DATETIME NULL,
  correlation_id VARCHAR(36) NULL,
  partition_key VARCHAR(100) NULL,
  payload_json STRING NULL,
  source_topic VARCHAR(255) NULL,
  kafka_partition INT NULL,
  kafka_offset BIGINT NULL
)
PRIMARY KEY (event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS analytics.orders_current (
  id BIGINT NOT NULL,
  customer_id BIGINT NULL,
  status VARCHAR(32) NULL,
  amount DECIMAL(12, 2) NULL
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS analytics.customers_current (
  id BIGINT NOT NULL,
  email VARCHAR(320) NULL,
  full_name VARCHAR(255) NULL,
  status VARCHAR(32) NULL
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS quality.scan_results (
  scan_id VARCHAR(36) NOT NULL,
  scanned_at DATETIME NOT NULL,
  check_name VARCHAR(255) NOT NULL,
  outcome VARCHAR(32) NOT NULL,
  observed_value DOUBLE NULL,
  details STRING NULL
)
DUPLICATE KEY (scan_id, scanned_at)
DISTRIBUTED BY HASH(scan_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE USER IF NOT EXISTS '{{STARROCKS_READER_USER}}' IDENTIFIED BY '{{STARROCKS_READER_PASSWORD}}';
GRANT SELECT ON ALL TABLES IN DATABASE analytics TO USER '{{STARROCKS_READER_USER}}';
GRANT SELECT ON ALL TABLES IN DATABASE marts TO USER '{{STARROCKS_READER_USER}}';
