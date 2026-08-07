from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractsTest(unittest.TestCase):
    def test_all_json_files_are_valid(self) -> None:
        files = sorted(ROOT.rglob("*.json"))
        self.assertTrue(files)
        for path in files:
            with self.subTest(path=path.relative_to(ROOT)):
                json.loads(path.read_text())

    def test_event_contract_has_stable_envelope(self) -> None:
        schema = json.loads((ROOT / "contracts/event-v1.schema.json").read_text())
        required = set(schema["required"])
        self.assertEqual(
            required,
            {
                "event_id",
                "event_type",
                "event_version",
                "producer",
                "occurred_at",
                "ingested_at",
                "correlation_id",
                "partition_key",
                "payload",
            },
        )
        self.assertEqual(schema["properties"]["event_version"]["const"], 1)

    def test_connector_topic_names_match_flink_sources(self) -> None:
        mysql = json.loads((ROOT / "config/debezium/mysql-orders.json").read_text())
        postgres = json.loads((ROOT / "config/debezium/postgres-customers.json").read_text())
        mysql_sql = (ROOT / "flink/sql/02-mysql-orders-cdc.sql").read_text()
        postgres_sql = (ROOT / "flink/sql/03-postgres-customers-cdc.sql").read_text()
        mysql_topic = f"{mysql['topic.prefix']}.{mysql['table.include.list']}"
        postgres_topic = f"{postgres['topic.prefix']}.{postgres['table.include.list']}"
        self.assertIn(mysql_topic, mysql_sql)
        self.assertIn(postgres_topic, postgres_sql)
        self.assertEqual(postgres["publication.autocreate.mode"], "disabled")

    def test_mysql_schema_history_topic_accepts_unkeyed_records(self) -> None:
        bootstrap = (ROOT / "services/python/bootstrap.py").read_text()
        self.assertRegex(
            bootstrap,
            r'"schema-history\.mysql\.inventory"[\s\S]+?"cleanup\.policy": "delete"',
        )
        self.assertIn('"retention.ms": "-1"', bootstrap)
        self.assertIn("incremental_alter_configs", bootstrap)
        self.assertIn('params={"includeTasks": "true", "onlyFailed": "true"}', bootstrap)

    def test_debezium_heartbeat_topics_are_bootstrapped(self) -> None:
        bootstrap = (ROOT / "services/python/bootstrap.py").read_text()
        smoke = (ROOT / "services/python/smoke_test.py").read_text()
        for topic_prefix in ("cdc.mysql", "cdc.postgres"):
            topic = f"__debezium-heartbeat.{topic_prefix}"
            self.assertIn(topic, bootstrap)
            self.assertIn(topic, smoke)

    def test_streaming_pipelines_have_unique_names(self) -> None:
        names = []
        for path in sorted((ROOT / "flink/sql").glob("*.sql")):
            match = re.search(r"SET 'pipeline\.name' = '([^']+)'", path.read_text())
            self.assertIsNotNone(match, path)
            names.append(match.group(1))
        self.assertEqual(len(names), len(set(names)))

    def test_flink_image_has_lakehouse_runtime_and_strict_submission(self) -> None:
        dockerfile = (ROOT / "docker/flink/Dockerfile").read_text()
        submit = (ROOT / "scripts/submit-flink.sh").read_text()
        compose = (ROOT / "compose.yaml").read_text()

        self.assertIn("hadoop-client-api-${HADOOP_VERSION}.jar", dockerfile)
        self.assertIn("hadoop-client-runtime-${HADOOP_VERSION}.jar", dockerfile)
        self.assertIn("grep -Fq '[ERROR]'", submit)
        self.assertIn("CREATED|INITIALIZING|RUNNING|RESTARTING", submit)
        self.assertRegex(
            compose,
            r"(?s)flink-submit:.*?profiles: \[lakehouse-tools\]",
        )
        self.assertRegex(
            compose,
            r"(?s)flink-submit:.*?command: \[/bin/bash, /opt/platform/scripts/submit-flink\.sh\].*?rest\.address: flink-jobmanager",
        )
        self.assertRegex(
            compose,
            r"(?s)flink-submit:.*?execution\.checkpointing\.interval: 30s.*?execution\.checkpointing\.dir: s3://flink-checkpoints/checkpoints",
        )
        self.assertGreaterEqual(compose.count("AWS_DEFAULT_REGION: ${AWS_REGION:-us-east-1}"), 3)
        self.assertIn(
            "ICEBERG_WAREHOUSE: ${ICEBERG_DATA_WAREHOUSE:-s3://platform-artifacts/iceberg}",
            compose,
        )

        for path in sorted((ROOT / "flink/sql").glob("*.sql")):
            self.assertIn("'load-url' = 'starrocks:8080'", path.read_text())

        events_sql = (ROOT / "flink/sql/01-events.sql").read_text()
        self.assertIn("'topic-pattern' = 'app[.](python|php)[.]events[.]v1'", events_sql)

    def test_compose_uses_pinned_images_and_selected_components(self) -> None:
        compose = (ROOT / "compose.yaml").read_text()
        image_lines = [line.strip() for line in compose.splitlines() if "image:" in line]
        self.assertTrue(image_lines)
        for line in image_lines:
            self.assertNotRegex(line, r":latest(?:\s|$)")
        removed_transport = "warp" + "stream"
        replaced_object_store = "min" + "io"
        self.assertNotIn(removed_transport, compose.lower())
        self.assertNotIn(replaced_object_store, compose.lower())
        self.assertIn("chrislusf/seaweedfs:4.26", compose)
        self.assertIn("apache/kafka:4.1.2", compose)
        self.assertIn("KAFKA_LOG_DIRS: /tmp/kraft-combined-logs", compose)
        self.assertIn("KAFKA_HEAP_OPTS: -Xms256M -Xmx768M", compose)
        self.assertIn("postgresql+psycopg://", compose)

    def test_removed_components_are_absent_from_repository(self) -> None:
        removed_transport = "warp" + "stream"
        replaced_object_store = "min" + "io"
        text_extensions = {".md", ".yaml", ".yml", ".json", ".py", ".sql", ".sh"}
        for path in ROOT.rglob("*"):
            if path.is_file() and path.suffix in text_extensions:
                content = path.read_text(encoding="utf-8", errors="replace").lower()
                self.assertNotIn(removed_transport, content, path)
                self.assertNotIn(replaced_object_store, content, path)

    def test_python_tool_runtimes_are_isolated(self) -> None:
        airflow_requirements = (ROOT / "docker/airflow/requirements.txt").read_text().lower()
        quality_requirements = (ROOT / "quality/requirements.txt").read_text().lower()
        compose = (ROOT / "compose.yaml").read_text()
        dag = (ROOT / "airflow/dags/platform_workflows.py").read_text()

        self.assertNotIn("soda", airflow_requirements)
        self.assertNotIn("weaviate", airflow_requirements)
        self.assertIn("soda-core-mysql==3.5.6", quality_requirements)
        self.assertIn("dockerfile: docker/quality/Dockerfile", compose)
        self.assertIn("http://quality-api:8000/scan", dag)
        self.assertIn("http://rag-api:8000/index", dag)

    def test_documented_make_targets_exist(self) -> None:
        makefile = (ROOT / "Makefile").read_text()
        for target in ("preflight", "up-core", "up-lakehouse", "bootstrap", "smoke", "rag-index"):
            self.assertRegex(makefile, rf"(?m)^{re.escape(target)}:")


if __name__ == "__main__":
    unittest.main()
