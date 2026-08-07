from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

import weaviate
from weaviate.classes.config import Configure, DataType, Property


COLLECTION = "PlatformContext"
ROOT = Path(os.getenv("WORKSPACE_ROOT", "/workspace"))
INCLUDE_PATTERNS = (
    "README.md",
    "docs/**/*.md",
    "contracts/*.json",
    "config/debezium/*.json",
    "flink/sql/*.sql",
    "dbt/**/*.sql",
    "dbt/**/*.yml",
    "quality/*.yml",
    "airflow/dags/*.py",
)


def connect():
    host = os.getenv("WEAVIATE_HOST", "weaviate")
    return weaviate.connect_to_custom(
        http_host=host,
        http_port=int(os.getenv("WEAVIATE_HTTP_PORT", "8080")),
        http_secure=False,
        grpc_host=host,
        grpc_port=int(os.getenv("WEAVIATE_GRPC_PORT", "50051")),
        grpc_secure=False,
    )


def source_kind(path: Path) -> str:
    relative = path.relative_to(ROOT).as_posix()
    if relative.startswith("contracts/"):
        return "data-contract"
    if relative.startswith("dbt/"):
        return "dbt-metadata"
    if relative.startswith("quality/"):
        return "quality-rule"
    if relative.startswith("airflow/"):
        return "orchestration"
    if relative.startswith("flink/"):
        return "streaming-sql"
    if relative.startswith("config/debezium/"):
        return "cdc-config"
    return "documentation"


def chunks(text: str, size: int = 1800, overlap: int = 200):
    compact = text.strip()
    start = 0
    index = 0
    while start < len(compact):
        end = min(start + size, len(compact))
        if end < len(compact):
            newline = compact.rfind("\n", start + size // 2, end)
            if newline > start:
                end = newline
        value = compact[start:end].strip()
        if value:
            yield index, value
            index += 1
        if end == len(compact):
            break
        start = max(end - overlap, start + 1)


def files_to_index() -> list[Path]:
    files: set[Path] = set()
    for pattern in INCLUDE_PATTERNS:
        files.update(path for path in ROOT.glob(pattern) if path.is_file())
    return sorted(files)


def main() -> None:
    client = connect()
    try:
        if not client.is_ready():
            raise RuntimeError("Weaviate is not ready")
        if client.collections.exists(COLLECTION):
            client.collections.delete(COLLECTION)

        collection = client.collections.create(
            name=COLLECTION,
            description="Versioned local context for the data platform and its operations.",
            properties=[
                Property(name="path", data_type=DataType.TEXT),
                Property(name="kind", data_type=DataType.TEXT),
                Property(name="content", data_type=DataType.TEXT),
                Property(name="content_hash", data_type=DataType.TEXT),
                Property(name="chunk_index", data_type=DataType.INT),
            ],
            vector_config=Configure.Vectors.text2vec_ollama(
                source_properties=["content"],
                api_endpoint="http://ollama:11434",
                model=os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text"),
            ),
        )

        object_count = 0
        with collection.batch.dynamic() as batch:
            for path in files_to_index():
                relative = path.relative_to(ROOT).as_posix()
                text = path.read_text(encoding="utf-8", errors="replace")
                for index, content in chunks(text):
                    digest = hashlib.sha256(content.encode()).hexdigest()
                    object_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{relative}:{index}:{digest}")
                    batch.add_object(
                        uuid=object_id,
                        properties={
                            "path": relative,
                            "kind": source_kind(path),
                            "content": content,
                            "content_hash": digest,
                            "chunk_index": index,
                        },
                    )
                    object_count += 1

        if collection.batch.failed_objects:
            first = collection.batch.failed_objects[0]
            raise RuntimeError(
                f"{len(collection.batch.failed_objects)} chunks failed; first={first}"
            )
        print(f"Indexed {object_count} chunks from {len(files_to_index())} files")
    finally:
        client.close()


if __name__ == "__main__":
    main()
