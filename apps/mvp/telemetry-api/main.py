import os
from contextlib import asynccontextmanager
from typing import List

import asyncpg
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)

app = FastAPI(title="Telemetry API", version="1.0.0")


@asynccontextmanager
async def lifespan(app_: FastAPI):
    app_.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    yield
    await app_.state.pool.close()

app.router.lifespan_context = lifespan

@app.get("/api/v1/devices/{device_id}/telemetry")
async def get_telemetry(device_id: str, limit: int = Query(10, ge=1, le=1000)):
    async with app.state.pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT device_id, value, type
            FROM telemetry
            WHERE device_id = $1
            ORDER BY created_at DESC
            LIMIT $2;
            """,
            device_id,
            limit
        )
    return JSONResponse([
        {
            "device_id": r["device_id"],
            "value": float(r["value"]),
            "type": r["type"]
        }
        for r in rows
    ])
