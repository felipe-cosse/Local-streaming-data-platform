from __future__ import annotations

import subprocess
import threading
from typing import Any

from fastapi import FastAPI, HTTPException


SCAN_COMMAND = [
    "soda",
    "scan",
    "-d",
    "starrocks",
    "-c",
    "/opt/platform/quality/configuration.yml",
    "/opt/platform/quality/checks.yml",
]

app = FastAPI(title="Soda quality runner", version="1.0.0")
scan_lock = threading.Lock()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/scan")
def scan() -> dict[str, Any]:
    if not scan_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A Soda scan is already running")
    try:
        result = subprocess.run(
            SCAN_COMMAND,
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="Soda scan timed out") from exc
    finally:
        scan_lock.release()

    output = "\n".join(part for part in (result.stdout, result.stderr) if part).strip()
    if result.returncode:
        raise HTTPException(
            status_code=502,
            detail={"exit_code": result.returncode, "output": output[-12000:]},
        )
    return {"status": "passed", "output": output[-12000:]}
