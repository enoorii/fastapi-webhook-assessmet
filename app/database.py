import asyncio
import os
import uuid

import aiosqlite

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")
DB_TIMEOUT = 5


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is reused with a different payload."""


class DatabaseManager:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)
        self.connection: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()

    @property
    def db(self) -> aiosqlite.Connection:
        if self.connection is None:
            raise RuntimeError("DatabaseManager is not connected")
        return self.connection

    async def connect(self):
        """Open one persistent connection and initialize the database."""
        if self.connection is not None:
            return

        db = await aiosqlite.connect(
            self.db_path,
            timeout=DB_TIMEOUT,
        )

        try:
            db.row_factory = aiosqlite.Row

            await db.execute("PRAGMA busy_timeout = 5000")
            await db.execute("PRAGMA journal_mode = WAL")
            await db.execute("PRAGMA synchronous = NORMAL")

            await db.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,

                    event_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,

                    raw_content TEXT NOT NULL,
                    cleaned_text TEXT,
                    fingerprint TEXT,

                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending',
                            'processing',
                            'delivered',
                            'failed'
                        )),

                    created_at TEXT NOT NULL
                        DEFAULT (
                            strftime('%Y-%m-%d %H:%M:%f', 'now')
                        ),

                    picked_up_at TEXT,
                    claim_token TEXT,

                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_error TEXT,

                    UNIQUE (tenant_id, idempotency_key)
                )
            """)

            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_pending
                ON events (status, next_attempt_at, created_at, id)
            """)

            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_processing
                ON events (status, picked_up_at, id)
            """)

            await db.commit()

            self.connection = db

        except Exception:
            await db.close()
            raise

    async def close(self):
        """Close the persistent connection."""
        async with self.lock:
            if self.connection is not None:
                await self.connection.close()
                self.connection = None

    @staticmethod
    def _validate_existing(
        existing: aiosqlite.Row,
        event_id: str,
        raw_content: str,
    ):
        if existing["event_id"] != event_id or existing["raw_content"] != raw_content:
            raise IdempotencyConflictError(
                "Idempotency key was already used with a different payload"
            )

    async def save_event(
        self,
        event_id: str,
        tenant_id: str,
        idempotency_key: str,
        raw_content: str,
    ) -> bool:
        """
        Persist a raw event.

        Returns:
            True: A new event was inserted.
            False: An identical event already exists.

        Raises:
            IdempotencyConflictError: Existing key has different data.
        """
        async with self.lock:
            db = self.db

            try:
                # 1. Fast path: Look up the existing idempotency key.
                # The UNIQUE constraint provides the composite index.
                cursor = await db.execute(
                    """
                    SELECT event_id, raw_content
                    FROM events
                    WHERE tenant_id = ?
                      AND idempotency_key = ?
                    LIMIT 1
                    """,
                    (tenant_id, idempotency_key),
                )

                existing = await cursor.fetchone()
                await cursor.close()

                if existing is not None:
                    self._validate_existing(
                        existing,
                        event_id,
                        raw_content,
                    )
                    return False

                # 2. Slow path: The event does not exist.
                # Attempt to insert it.
                cursor = await db.execute(
                    """
                    INSERT INTO events (
                        event_id,
                        tenant_id,
                        idempotency_key,
                        raw_content,
                        status
                    )
                    VALUES (?, ?, ?, ?, 'pending')
                    ON CONFLICT(tenant_id, idempotency_key)
                    DO NOTHING
                    """,
                    (
                        event_id,
                        tenant_id,
                        idempotency_key,
                        raw_content,
                    ),
                )

                inserted = cursor.rowcount == 1
                await cursor.close()

                if inserted:
                    await db.commit()
                    return True

                # 3. Another process may have inserted the same key
                # between our initial SELECT and INSERT.
                cursor = await db.execute(
                    """
                    SELECT event_id, raw_content
                    FROM events
                    WHERE tenant_id = ?
                      AND idempotency_key = ?
                    LIMIT 1
                    """,
                    (tenant_id, idempotency_key),
                )

                existing = await cursor.fetchone()
                await cursor.close()

                await db.commit()

                if existing is None:
                    raise RuntimeError("Conflicting event disappeared during insertion")

                self._validate_existing(
                    existing,
                    event_id,
                    raw_content,
                )

                return False

            except Exception:
                await db.rollback()
                raise

    async def _select_due_task(
        self,
        lease_seconds: int,
    ) -> aiosqlite.Row | None:
        """Find the oldest task eligible for processing."""
        cursor = await self.db.execute(
            """
            SELECT *
            FROM events
            WHERE
                (
                    status = 'pending'
                    AND (
                        next_attempt_at IS NULL
                        OR julianday(next_attempt_at) <= julianday('now')
                    )
                )
                OR
                (
                    status = 'processing'
                    AND (
                        picked_up_at IS NULL
                        OR julianday(picked_up_at)
                            <= julianday('now', ?)
                    )
                )
            ORDER BY
                CASE
                    WHEN status = 'pending'
                    THEN julianday(
                        COALESCE(next_attempt_at, created_at)
                    )
                    ELSE julianday(picked_up_at)
                END ASC,
                id ASC
            LIMIT 1
            """,
            (f"-{lease_seconds} seconds",),
        )

        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def get_due_task(
        self,
        lease_seconds: int = 10,
    ) -> dict | None:
        """
        Atomically retrieve and claim an eligible task.

        An eligible task is:
          - pending and its retry time has arrived; or
          - processing with an expired lease.
        """
        async with self.lock:
            db = self.db

            # Read first. Avoid acquiring a SQLite write transaction
            # when there are no tasks to process.
            candidate = await self._select_due_task(lease_seconds)

            if candidate is None:
                return None

            try:
                # Acquire the write reservation and recheck.
                # This also coordinates different application processes.
                await db.execute("BEGIN IMMEDIATE")

                row = await self._select_due_task(lease_seconds)

                if row is None:
                    await db.commit()
                    return None

                task = dict(row)
                claim_token = uuid.uuid4().hex

                cursor = await db.execute(
                    """
                    UPDATE events
                    SET
                        status = 'processing',
                        picked_up_at = strftime(
                            '%Y-%m-%d %H:%M:%f',
                            'now'
                        ),
                        claim_token = ?,
                        attempt_count = attempt_count + 1,
                        next_attempt_at = NULL
                    WHERE id = ?
                    """,
                    (claim_token, task["id"]),
                )

                if cursor.rowcount != 1:
                    await db.rollback()
                    return None

                await db.commit()

                task["status"] = "processing"
                task["claim_token"] = claim_token
                task["attempt_count"] += 1

                return task

            except Exception:
                await db.rollback()
                raise

    async def save_processed_event(
        self,
        task_id: int,
        claim_token: str,
        cleaned_text: str,
        fingerprint: str,
    ) -> bool:
        """Save preprocessing results if the worker still owns the task."""
        async with self.lock:
            db = self.db

            try:
                cursor = await db.execute(
                    """
                    UPDATE events
                    SET
                        cleaned_text = ?,
                        fingerprint = ?
                    WHERE
                        id = ?
                        AND status = 'processing'
                        AND claim_token = ?
                    """,
                    (
                        cleaned_text,
                        fingerprint,
                        task_id,
                        claim_token,
                    ),
                )

                updated = cursor.rowcount == 1
                await db.commit()
                return updated

            except Exception:
                await db.rollback()
                raise

    async def mark_delivered(
        self,
        task_id: int,
        claim_token: str,
    ) -> bool:
        async with self.lock:
            db = self.db

            try:
                cursor = await db.execute(
                    """
                    UPDATE events
                    SET
                        status = 'delivered',
                        picked_up_at = NULL,
                        claim_token = NULL,
                        next_attempt_at = NULL,
                        last_error = NULL
                    WHERE
                        id = ?
                        AND status = 'processing'
                        AND claim_token = ?
                    """,
                    (task_id, claim_token),
                )

                updated = cursor.rowcount == 1
                await db.commit()
                return updated

            except Exception:
                await db.rollback()
                raise

    async def schedule_retry(
        self,
        task_id: int,
        claim_token: str,
        delay_seconds: int,
        error: str,
    ) -> bool:
        async with self.lock:
            db = self.db

            try:
                cursor = await db.execute(
                    """
                    UPDATE events
                    SET
                        status = 'pending',
                        picked_up_at = NULL,
                        claim_token = NULL,
                        next_attempt_at = strftime(
                            '%Y-%m-%d %H:%M:%f',
                            'now',
                            ?
                        ),
                        last_error = ?
                    WHERE
                        id = ?
                        AND status = 'processing'
                        AND claim_token = ?
                    """,
                    (
                        f"+{delay_seconds} seconds",
                        error[:1000],
                        task_id,
                        claim_token,
                    ),
                )

                updated = cursor.rowcount == 1
                await db.commit()
                return updated

            except Exception:
                await db.rollback()
                raise

    async def mark_failed(
        self,
        task_id: int,
        claim_token: str,
        error: str,
    ) -> bool:
        async with self.lock:
            db = self.db

            try:
                cursor = await db.execute(
                    """
                    UPDATE events
                    SET
                        status = 'failed',
                        picked_up_at = NULL,
                        claim_token = NULL,
                        next_attempt_at = NULL,
                        last_error = ?
                    WHERE
                        id = ?
                        AND status = 'processing'
                        AND claim_token = ?
                    """,
                    (error[:1000], task_id, claim_token),
                )

                updated = cursor.rowcount == 1
                await db.commit()
                return updated

            except Exception:
                await db.rollback()
                raise

    async def get_event(
        self,
        tenant_id: str,
        idempotency_key: str,
    ) -> dict | None:
        async with self.lock:
            cursor = await self.db.execute(
                """
                SELECT
                    id,
                    event_id,
                    tenant_id,
                    idempotency_key,
                    status,
                    attempt_count,
                    created_at,
                    last_error
                FROM events
                WHERE tenant_id = ?
                  AND idempotency_key = ?
                LIMIT 1
                """,
                (tenant_id, idempotency_key),
            )

            row = await cursor.fetchone()
            await cursor.close()

            return dict(row) if row is not None else None
