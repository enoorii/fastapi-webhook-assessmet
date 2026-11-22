import asyncio
import logging
from concurrent.futures import Executor

import httpx

from app.database import DatabaseManager
from app.downstream import forward_event
from app.processor import scrub_and_fingerprint

logger = logging.getLogger(__name__)

POLL_INTERVAL = 0.2
LEASE_SECONDS = 10
DOWNSTREAM_TIMEOUT = 5


async def delivery_worker(
    db: DatabaseManager,
    process_pool: Executor,
    http_client: httpx.AsyncClient,
    worker_id: int,
):
    logger.info("Delivery worker %s started", worker_id)

    loop = asyncio.get_running_loop()

    while True:
        try:
            task = await db.get_due_task(lease_seconds=LEASE_SECONDS)

            if task is None:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            logger.info(
                "[Worker %s] Processing %s (attempt %s)",
                worker_id,
                task["event_id"],
                task["attempt_count"],
            )

            try:
                # Phase 1: CPU-bound preprocessing.
                if task["cleaned_text"] is None or task["fingerprint"] is None:
                    cleaned_text, fingerprint = await loop.run_in_executor(
                        process_pool,
                        scrub_and_fingerprint,
                        task["raw_content"],
                    )

                    saved = await db.save_processed_event(
                        task_id=task["id"],
                        claim_token=task["claim_token"],
                        cleaned_text=cleaned_text,
                        fingerprint=fingerprint,
                    )

                    if not saved:
                        logger.warning(
                            "[Worker %s] Lost ownership of %s during preprocessing",
                            worker_id,
                            task["event_id"],
                        )
                        continue

                    task["cleaned_text"] = cleaned_text
                    task["fingerprint"] = fingerprint

                # Phase 2: Downstream delivery.
                # Each worker can await its own HTTP request independently.
                async with asyncio.timeout(DOWNSTREAM_TIMEOUT):
                    await forward_event(
                        client=http_client,
                        event_id=task["event_id"],
                        cleaned_text=task["cleaned_text"],
                        fingerprint=task["fingerprint"],
                    )

            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code

                if status_code in (408, 429) or status_code >= 500:
                    await retry_task(db, task, exc)

                else:
                    await db.mark_failed(
                        task["id"],
                        task["claim_token"],
                        str(exc),
                    )

            except (httpx.RequestError, TimeoutError) as exc:
                await retry_task(db, task, exc)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.exception(
                    "[Worker %s] Unexpected error for %s",
                    worker_id,
                    task["event_id"],
                )

                await retry_task(db, task, exc)

            else:
                updated = await db.mark_delivered(
                    task["id"],
                    task["claim_token"],
                )

                if updated:
                    logger.info(
                        "[Worker %s] Delivered %s",
                        worker_id,
                        task["event_id"],
                    )
                else:
                    logger.warning(
                        "[Worker %s] Lost ownership of %s before marking delivered",
                        worker_id,
                        task["event_id"],
                    )

        except asyncio.CancelledError:
            logger.info(
                "Delivery worker %s shutting down",
                worker_id,
            )
            raise

        except Exception:
            logger.exception(
                "[Worker %s] Worker iteration failed",
                worker_id,
            )
            await asyncio.sleep(1)


async def retry_task(
    db: DatabaseManager,
    task: dict,
    error: Exception,
):
    attempt = task["attempt_count"]

    # 1, 2, 4, 8, 16, 32, 60, 60...
    delay = min(2 ** (attempt - 1), 60)

    logger.warning(
        "Delivery failed for event %s. Retrying in %s seconds: %s",
        task["event_id"],
        delay,
        error,
    )

    updated = await db.schedule_retry(
        task_id=task["id"],
        claim_token=task["claim_token"],
        delay_seconds=delay,
        error=str(error),
    )

    if not updated:
        logger.warning(
            "Could not schedule retry for event %s: claim was lost",
            task["event_id"],
        )
