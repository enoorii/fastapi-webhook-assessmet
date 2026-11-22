import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel

from app.database import (
    DatabaseManager,
    IdempotencyConflictError,
)
from app.worker import delivery_worker

logger = logging.getLogger(__name__)

DELIVERY_CONCURRENCY = max(
    1,
    int(os.getenv("DELIVERY_CONCURRENCY", "8")),
)

PROCESS_POOL_WORKERS = max(
    1,
    int(os.getenv("PROCESS_POOL_WORKERS", "2")),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = DatabaseManager()
    await db.connect()

    process_pool = ProcessPoolExecutor(
        max_workers=PROCESS_POOL_WORKERS,
    )

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(5.0),
        limits=httpx.Limits(
            max_connections=DELIVERY_CONCURRENCY,
            max_keepalive_connections=DELIVERY_CONCURRENCY,
        ),
    )

    app.state.database = db

    # Create multiple independent delivery workers.
    worker_tasks = [
        asyncio.create_task(
            delivery_worker(
                db=db,
                process_pool=process_pool,
                http_client=http_client,
                worker_id=i + 1,
            ),
            name=f"delivery-worker-{i + 1}",
        )
        for i in range(DELIVERY_CONCURRENCY)
    ]

    logger.info(
        "Application started with %s delivery workers and %s CPU workers",
        DELIVERY_CONCURRENCY,
        PROCESS_POOL_WORKERS,
    )

    try:
        yield

    finally:
        logger.info("Stopping application")

        # Stop all delivery workers.
        for task in worker_tasks:
            task.cancel()

        await asyncio.gather(
            *worker_tasks,
            return_exceptions=True,
        )

        # Close HTTP connections.
        await http_client.aclose()

        # Shut down CPU processes outside the event loop.
        await asyncio.to_thread(
            process_pool.shutdown,
            wait=True,
            cancel_futures=True,
        )

        await db.close()

        logger.info("Application stopped")


app = FastAPI(
    title="Webhook Ingestion Service",
    lifespan=lifespan,
)


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post(
    "/webhooks/ingest",
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_webhook(
    request: Request,
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key or not x_idempotency_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Missing X-Idempotency-Key header",
        )

    idempotency_key = x_idempotency_key.strip()

    db: DatabaseManager = request.app.state.database

    try:
        created = await db.save_event(
            event_id=payload.event_id,
            tenant_id=payload.tenant_id,
            idempotency_key=idempotency_key,
            raw_content=payload.content,
        )

    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail=str(exc),
        ) from exc

    return {
        "status": "pending",
        "duplicate": not created,
        "idempotency_key": idempotency_key,
    }


@app.get("/webhooks/status")
async def get_status(
    request: Request,
    tenant_id: str,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key or not x_idempotency_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Missing X-Idempotency-Key header",
        )

    db: DatabaseManager = request.app.state.database

    event = await db.get_event(
        tenant_id=tenant_id,
        idempotency_key=x_idempotency_key.strip(),
    )

    if event is None:
        raise HTTPException(
            status_code=404,
            detail="Event not found",
        )

    return event
