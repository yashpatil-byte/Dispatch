"""
Dispatch Worker — polls the queue, executes jobs, reports results back.

Features:
  - Exponential backoff on retries  (2^attempt seconds, capped at 60s)
  - Heartbeat every 5s to mark worker alive in DB
  - Graceful shutdown on SIGTERM / SIGINT
  - Pluggable job handler registry
"""
import asyncio
import os
import signal
import time
import uuid
import math
import logging
from typing import Any, Callable, Coroutine

import httpx
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import update

from models import Worker
from queue import JobQueue

logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://dispatch:dispatch@db:5432/dispatch")
REDIS_URL    = os.getenv("REDIS_URL", "redis://redis:6379")
API_URL      = os.getenv("API_URL", "http://api:8000")
QUEUE        = os.getenv("WORKER_QUEUE", "default")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))   # seconds
CONCURRENCY   = int(os.getenv("CONCURRENCY", "4"))

# ── Handler registry ──────────────────────────────────────────────────────────
Handler = Callable[[dict], Coroutine[Any, Any, dict]]
_handlers: dict[str, Handler] = {}

def register(job_name: str):
    """Decorator to register a handler for a named job."""
    def decorator(fn: Handler):
        _handlers[job_name] = fn
        return fn
    return decorator

# ── Built-in demo handlers ────────────────────────────────────────────────────
@register("send_email")
async def handle_send_email(payload: dict) -> dict:
    await asyncio.sleep(0.3)   # simulate SMTP call
    return {"sent_to": payload.get("to"), "status": "delivered"}

@register("resize_image")
async def handle_resize_image(payload: dict) -> dict:
    await asyncio.sleep(0.5)
    return {"url": payload.get("url"), "size": payload.get("size", "800x600"), "status": "resized"}

@register("generate_report")
async def handle_generate_report(payload: dict) -> dict:
    await asyncio.sleep(1.0)
    return {"report_id": str(uuid.uuid4()), "rows": payload.get("rows", 1000)}

@register("noop")
async def handle_noop(payload: dict) -> dict:
    await asyncio.sleep(0.1)
    return {"noop": True}

# ── Worker ────────────────────────────────────────────────────────────────────
class DispatchWorker:
    def __init__(self):
        self.worker_id  = f"worker-{uuid.uuid4().hex[:8]}"
        self.running    = True
        self.semaphore  = asyncio.Semaphore(CONCURRENCY)
        self.engine     = create_async_engine(DATABASE_URL, echo=False)
        self.Session    = async_sessionmaker(self.engine, expire_on_commit=False)
        self.http        : httpx.AsyncClient | None = None
        self.redis       : aioredis.Redis | None     = None
        self.queue       : JobQueue | None           = None

    async def start(self):
        self.http  = httpx.AsyncClient(base_url=API_URL, timeout=30)
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        self.queue = JobQueue(self.redis)

        await self._register_worker()

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._shutdown)

        await asyncio.gather(
            self._poll_loop(),
            self._heartbeat_loop(),
        )

    def _shutdown(self):
        log.info(f"{self.worker_id} shutting down…")
        self.running = False

    async def _register_worker(self):
        async with self.Session() as db:
            worker = Worker(id=self.worker_id, queue=QUEUE, status="idle")
            db.add(worker)
            await db.commit()
        log.info(f"{self.worker_id} registered on queue '{QUEUE}' (concurrency={CONCURRENCY})")

    async def _heartbeat_loop(self):
        while self.running:
            async with self.Session() as db:
                await db.execute(
                    update(Worker).where(Worker.id == self.worker_id)
                    .values(last_seen=__import__("datetime").datetime.utcnow())
                )
                await db.commit()
            await asyncio.sleep(5)

    async def _poll_loop(self):
        while self.running:
            job_id = await self.queue.dequeue(QUEUE, self.worker_id)
            if job_id:
                asyncio.create_task(self._execute(job_id))
            else:
                await asyncio.sleep(POLL_INTERVAL)

    async def _execute(self, job_id: str):
        async with self.semaphore:
            # Mark started
            await self.http.post(f"/internal/jobs/{job_id}/start", params={"worker_id": self.worker_id})

            # Fetch job details
            resp = await self.http.get(f"/jobs/{job_id}")
            job  = resp.json()

            handler = _handlers.get(job["name"])
            if handler is None:
                await self._fail(job_id, job, f"No handler registered for '{job['name']}'", requeue=False)
                return

            try:
                result = await asyncio.wait_for(handler(job["payload"]), timeout=300)
                await self.http.post(f"/internal/jobs/{job_id}/complete", json=result)
                log.info(f"✓ {job_id} ({job['name']}) done")
                await self._update_worker_stats(success=True)
            except asyncio.TimeoutError:
                await self._fail(job_id, job, "Job timed out after 300s", requeue=False)
            except Exception as exc:
                attempt  = job["attempts"]
                max_retries = job["max_retries"]
                requeue  = attempt < max_retries
                backoff  = min(2 ** attempt, 60)
                if requeue:
                    log.warning(f"✗ {job_id} failed (attempt {attempt}/{max_retries}), retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                else:
                    log.error(f"✗ {job_id} dead after {attempt} attempts: {exc}")
                await self._fail(job_id, job, str(exc), requeue=requeue)
                await self._update_worker_stats(success=False)

    async def _fail(self, job_id: str, job: dict, error: str, requeue: bool):
        await self.http.post(
            f"/internal/jobs/{job_id}/fail",
            params={"error": error, "requeue": str(requeue).lower()}
        )

    async def _update_worker_stats(self, success: bool):
        async with self.Session() as db:
            if success:
                await db.execute(update(Worker).where(Worker.id == self.worker_id).values(jobs_done=Worker.jobs_done + 1))
            else:
                await db.execute(update(Worker).where(Worker.id == self.worker_id).values(jobs_failed=Worker.jobs_failed + 1))
            await db.commit()

if __name__ == "__main__":
    asyncio.run(DispatchWorker().start())
