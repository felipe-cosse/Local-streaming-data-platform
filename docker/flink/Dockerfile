FROM flink:1.20.5-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG ICEBERG_VERSION=1.11.0
ARG STARROCKS_CONNECTOR_VERSION=1.2.15_flink-1.20
ARG HADOOP_VERSION=3.3.6

USER root
RUN set -eux; \
    curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/flink-connector-starrocks-${STARROCKS_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/${STARROCKS_CONNECTOR_VERSION}/flink-connector-starrocks-${STARROCKS_CONNECTOR_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-api-${HADOOP_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar; \
    curl -fsSL -o /opt/flink/lib/hadoop-client-runtime-${HADOOP_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

USER flink
