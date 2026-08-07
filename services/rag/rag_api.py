from __future__ import annotations

import json
import os
import re
import threading
from typing import Any

import pymysql
import requests
import weaviate
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from weaviate.classes.query import MetadataQuery

from indexer import main as index_platform_context


COLLECTION = "PlatformContext"
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434").rstrip("/")
CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL", "qwen3:1.7b")
FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|load|export|set|use|call|execute)\b",
    re.IGNORECASE,
)

app = FastAPI(
    title="Local Data Platform Context API",
    version="1.0.0",
    description="RAG over platform metadata with a read-only StarRocks query path.",
)
index_lock = threading.Lock()


class Question(BaseModel):
    question: str = Field(min_length=3, max_length=2000)
    max_sources: int = Field(default=6, ge=1, le=12)


class SqlRequest(BaseModel):
    sql: str = Field(min_length=6, max_length=10000)


def weaviate_client():
    host = os.getenv("WEAVIATE_HOST", "weaviate")
    return weaviate.connect_to_custom(
        http_host=host,
        http_port=int(os.getenv("WEAVIATE_HTTP_PORT", "8080")),
        http_secure=False,
        grpc_host=host,
        grpc_port=int(os.getenv("WEAVIATE_GRPC_PORT", "50051")),
        grpc_secure=False,
    )


def retrieve(question: str, limit: int) -> list[dict[str, Any]]:
    client = weaviate_client()
    try:
        if not client.collections.exists(COLLECTION):
            raise HTTPException(
                status_code=503,
                detail="Context index is missing. Run `make ai-models` and `make rag-index`.",
            )
        collection = client.collections.use(COLLECTION)
        response = collection.query.near_text(
            query=question,
            limit=limit,
            return_metadata=MetadataQuery(distance=True),
        )
        return [
            {
                "path": obj.properties.get("path"),
                "kind": obj.properties.get("kind"),
                "content": obj.properties.get("content"),
                "distance": obj.metadata.distance,
            }
            for obj in response.objects
        ]
    finally:
        client.close()


def ollama_chat(messages: list[dict[str, str]], json_mode: bool = False) -> str:
    body: dict[str, Any] = {
        "model": CHAT_MODEL,
        "messages": messages,
        "stream": False,
        "options": {"temperature": 0},
    }
    if json_mode:
        body["format"] = "json"
    try:
        response = requests.post(f"{OLLAMA_URL}/api/chat", json=body, timeout=180)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Ollama model {CHAT_MODEL!r} is unavailable: {exc}",
        ) from exc
    return response.json()["message"]["content"]


def validate_read_only_sql(sql: str) -> str:
    candidate = sql.strip()
    if candidate.endswith(";"):
        candidate = candidate[:-1].rstrip()
    if ";" in candidate or "--" in candidate or "/*" in candidate:
        raise ValueError("Multiple statements and SQL comments are not allowed")
    if not re.match(r"^(select|with)\b", candidate, re.IGNORECASE):
        raise ValueError("Only SELECT or WITH queries are allowed")
    if FORBIDDEN_SQL.search(candidate):
        raise ValueError("The query contains a forbidden write or administration keyword")
    if not re.search(r"\blimit\s+\d+\s*$", candidate, re.IGNORECASE):
        candidate = f"{candidate} LIMIT 100"
    return candidate


def run_sql(sql: str) -> dict[str, Any]:
    safe_sql = validate_read_only_sql(sql)
    connection = pymysql.connect(
        host=os.getenv("STARROCKS_HOST", "starrocks"),
        port=int(os.getenv("STARROCKS_PORT", "9030")),
        user=os.getenv("STARROCKS_READER_USER", "analytics_reader"),
        password=os.getenv("STARROCKS_READER_PASSWORD", "local_analytics_reader"),
        database="analytics",
        connect_timeout=5,
        read_timeout=15,
        cursorclass=pymysql.cursors.DictCursor,
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(safe_sql)
            rows = cursor.fetchall()
    return {"sql": safe_sql, "rows": rows, "row_count": len(rows)}


@app.get("/health")
def health() -> dict[str, Any]:
    status: dict[str, Any] = {"api": "ok", "model": CHAT_MODEL}
    try:
        status["ollama"] = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3).status_code
    except requests.RequestException:
        status["ollama"] = "unavailable"
    client = weaviate_client()
    try:
        status["weaviate"] = client.is_ready()
        status["context_index"] = client.collections.exists(COLLECTION)
    finally:
        client.close()
    return status


@app.post("/index")
def index_context() -> dict[str, str]:
    if not index_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A context refresh is already running")
    try:
        index_platform_context()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Context refresh failed: {exc}") from exc
    finally:
        index_lock.release()
    return {"status": "indexed"}


@app.post("/sql")
def sql_endpoint(request: SqlRequest) -> dict[str, Any]:
    try:
        return run_sql(request.sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except pymysql.MySQLError as exc:
        raise HTTPException(status_code=502, detail=f"StarRocks query failed: {exc}") from exc


@app.post("/ask")
def ask(request: Question) -> dict[str, Any]:
    sources = retrieve(request.question, request.max_sources)
    context = "\n\n".join(
        f"SOURCE {item['path']} ({item['kind']}):\n{item['content']}" for item in sources
    )
    planning_prompt = (
        "You are the local data-platform assistant. Use only the supplied context. "
        "For architecture or operational questions, answer directly and set sql to null. "
        "For questions requiring live numerical data, produce one read-only StarRocks SELECT "
        "using only analytics or marts tables. Never invent a table or column. Return strict "
        "JSON with keys answer and sql. The answer is a short preliminary explanation.\n\n"
        f"CONTEXT:\n{context}\n\nQUESTION:\n{request.question}"
    )
    raw_plan = ollama_chat(
        [
            {"role": "system", "content": "Return valid JSON only."},
            {"role": "user", "content": planning_prompt},
        ],
        json_mode=True,
    )
    try:
        plan = json.loads(raw_plan)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="The local model returned invalid JSON") from exc

    sql_result = None
    proposed_sql = plan.get("sql")
    if proposed_sql:
        try:
            sql_result = run_sql(str(proposed_sql))
        except (ValueError, pymysql.MySQLError) as exc:
            raise HTTPException(status_code=400, detail=f"Generated query was rejected: {exc}") from exc
        answer = ollama_chat(
            [
                {
                    "role": "system",
                    "content": "Answer from the supplied query results; do not invent values.",
                },
                {
                    "role": "user",
                    "content": (
                        f"Question: {request.question}\n"
                        f"SQL: {sql_result['sql']}\n"
                        f"Rows: {json.dumps(sql_result['rows'], default=str)}"
                    ),
                },
            ]
        )
    else:
        answer = str(plan.get("answer", "No answer was produced."))

    return {
        "answer": answer,
        "sql": sql_result,
        "sources": [
            {"path": item["path"], "kind": item["kind"], "distance": item["distance"]}
            for item in sources
        ],
        "model": CHAT_MODEL,
    }
