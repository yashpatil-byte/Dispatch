"""
Dispatch — Real-Time Distributed Job Orchestration Platform
REST API + WebSocket server (FastAPI)
"""
import asyncio
import os
from datetime import datetime, timezone
from typing import Any, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import select, func, update, text

from models import Base, Job, Worker
from queue import JobQueue
from ws_manager import manager

# ── Config ──────────────────────────────────────────────────────────────────
# Render provides postgres:// — asyncpg needs postgresql+asyncpg://
_raw_db_url  = os.getenv("DATABASE_URL", "postgresql+asyncpg://dispatch:dispatch@db:5432/dispatch")
DATABASE_URL = _raw_db_url.replace("postgres://", "postgresql+asyncpg://", 1)
REDIS_URL    = os.getenv("REDIS_URL", "redis://redis:6379")

engine       = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
redis_client : Optional[aioredis.Redis] = None
job_queue    : Optional[JobQueue]       = None

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="Dispatch", version="1.0.0", description="Distributed Job Orchestration Platform")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Lifespan ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global redis_client, job_queue
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    job_queue    = JobQueue(redis_client)
    asyncio.create_task(metrics_broadcast_loop())
    asyncio.create_task(embedded_worker_loop())

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()

# ── DB dependency ─────────────────────────────────────────────────────────────
async def get_db():
    async with SessionLocal() as session:
        yield session

# ── Schemas ──────────────────────────────────────────────────────────────────
class JobCreate(BaseModel):
    name:        str
    queue:       str        = "default"
    payload:     dict       = Field(default_factory=dict)
    priority:    int        = Field(default=5, ge=1, le=10)
    max_retries: int        = Field(default=3, ge=0, le=10)
    run_at:      Optional[datetime] = None   # scheduled execution time

class JobOut(BaseModel):
    id: str; name: str; queue: str; status: str; priority: int
    attempts: int; max_retries: int; payload: dict
    error: Optional[str]; result: Optional[Any]
    created_at: datetime; started_at: Optional[datetime]; finished_at: Optional[datetime]
    worker_id: Optional[str]

    class Config:
        from_attributes = True

# ── Job endpoints ─────────────────────────────────────────────────────────────
@app.post("/jobs", response_model=JobOut, status_code=201, tags=["Jobs"])
async def submit_job(body: JobCreate, db: AsyncSession = Depends(get_db)):
    """Submit a new job to the queue."""
    job = Job(
        name        = body.name,
        queue       = body.queue,
        payload     = body.payload,
        priority    = body.priority,
        max_retries = body.max_retries,
        run_at      = body.run_at,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    run_ts = body.run_at.timestamp() if body.run_at else None
    await job_queue.enqueue(job.id, body.queue, body.priority, run_ts)
    await manager.broadcast("job_created", _job_dict(job))
    return job

@app.get("/jobs", response_model=list[JobOut], tags=["Jobs"])
async def list_jobs(
    queue:  Optional[str] = None,
    status: Optional[str] = None,
    limit:  int           = Query(50, le=200),
    offset: int           = 0,
    db:     AsyncSession  = Depends(get_db),
):
    stmt = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
    if queue:
        stmt = stmt.where(Job.queue == queue)
    if status:
        stmt = stmt.where(Job.status == status)
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/jobs/{job_id}", response_model=JobOut, tags=["Jobs"])
async def get_job(job_id: str, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job

@app.delete("/jobs/{job_id}", tags=["Jobs"])
async def cancel_job(job_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel a pending job."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job.status not in ("pending",):
        raise HTTPException(400, f"Cannot cancel a job in '{job.status}' state")
    job.status = "cancelled"
    await db.commit()
    await manager.broadcast("job_cancelled", {"id": job_id})
    return {"cancelled": job_id}

# ── Worker endpoints ──────────────────────────────────────────────────────────
@app.get("/workers", tags=["Workers"])
async def list_workers(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Worker).order_by(Worker.started_at.desc()))
    workers = result.scalars().all()
    return [
        {
            "id": w.id, "queue": w.queue, "status": w.status,
            "jobs_done": w.jobs_done, "jobs_failed": w.jobs_failed,
            "last_seen": w.last_seen, "started_at": w.started_at,
        }
        for w in workers
    ]

# Internal: called by worker process
@app.post("/internal/jobs/{job_id}/start", include_in_schema=False)
async def mark_started(job_id: str, worker_id: str, db: AsyncSession = Depends(get_db)):
    now = datetime.now(timezone.utc)
    await db.execute(
        update(Job).where(Job.id == job_id)
        .values(status="running", started_at=now, worker_id=worker_id, attempts=Job.attempts + 1)
    )
    await db.commit()
    job = await db.get(Job, job_id)
    await manager.broadcast("job_started", _job_dict(job))
    return {"ok": True}

@app.post("/internal/jobs/{job_id}/complete", include_in_schema=False)
async def mark_complete(job_id: str, result: dict, db: AsyncSession = Depends(get_db)):
    now = datetime.now(timezone.utc)
    await db.execute(
        update(Job).where(Job.id == job_id)
        .values(status="done", finished_at=now, result=result)
    )
    await db.commit()
    job = await db.get(Job, job_id)
    await manager.broadcast("job_done", _job_dict(job))
    return {"ok": True}

@app.post("/internal/jobs/{job_id}/fail", include_in_schema=False)
async def mark_failed(job_id: str, error: str, requeue: bool, db: AsyncSession = Depends(get_db)):
    now  = datetime.now(timezone.utc)
    job  = await db.get(Job, job_id)
    new_status = "pending" if requeue else ("dead" if job.attempts >= job.max_retries else "failed")
    await db.execute(
        update(Job).where(Job.id == job_id)
        .values(status=new_status, finished_at=now, error=error)
    )
    await db.commit()
    job = await db.get(Job, job_id)
    if requeue:
        await job_queue.enqueue(job.id, job.queue, job.priority)
    elif new_status == "dead":
        await job_queue.send_to_dlq(job.id, job.queue, error)
    await manager.broadcast("job_failed", _job_dict(job))
    return {"ok": True}

# ── Metrics endpoint ──────────────────────────────────────────────────────────
@app.get("/metrics", tags=["Metrics"])
async def get_metrics(db: AsyncSession = Depends(get_db)):
    return await _compute_metrics(db)

# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, db: AsyncSession = Depends(get_db)):
    await manager.connect(ws)
    # Send current state on connect
    metrics = await _compute_metrics(db)
    await ws.send_text(__import__("json").dumps({"event": "init", "data": metrics}))
    try:
        while True:
            await ws.receive_text()   # keep alive; client may send pings
    except WebSocketDisconnect:
        await manager.disconnect(ws)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _job_dict(job: Job) -> dict:
    return {
        "id": job.id, "name": job.name, "queue": job.queue,
        "status": job.status, "priority": job.priority,
        "attempts": job.attempts, "error": job.error,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "worker_id": job.worker_id,
    }

async def _compute_metrics(db: AsyncSession) -> dict:
    status_counts = {}
    rows = await db.execute(
        select(Job.status, func.count().label("cnt")).group_by(Job.status)
    )
    for row in rows:
        status_counts[row.status] = row.cnt

    # Avg duration for completed jobs (last 1000)
    dur_rows = await db.execute(text(
        "SELECT AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) "
        "FROM jobs WHERE status = 'done' AND started_at IS NOT NULL "
        "AND finished_at IS NOT NULL LIMIT 1000"
    ))
    avg_dur = dur_rows.scalar() or 0

    workers_row = await db.execute(select(func.count()).select_from(Worker).where(Worker.status != "dead"))
    active_workers = workers_row.scalar() or 0

    q_depth = await job_queue.queue_depth("default") if job_queue else 0

    return {
        "pending":        status_counts.get("pending", 0),
        "running":        status_counts.get("running", 0),
        "done":           status_counts.get("done", 0),
        "failed":         status_counts.get("failed", 0),
        "dead":           status_counts.get("dead", 0),
        "avg_duration_s": round(avg_dur, 3),
        "active_workers": active_workers,
        "queue_depth":    q_depth,
    }

async def metrics_broadcast_loop():
    """Push metrics to all dashboard clients every 2 seconds."""
    async with SessionLocal() as db:
        while True:
            try:
                metrics = await _compute_metrics(db)
                await manager.broadcast("metrics", metrics)
            except Exception:
                pass
            await asyncio.sleep(2)

async def embedded_worker_loop():
    """
    Embedded worker that runs inside the API process (for free-tier hosting).
    Polls the queue and executes jobs with a small thread pool.
    For production scale, run worker.py as a separate process.
    """
    import importlib, sys
    # Dynamically import handlers from worker module
    try:
        worker_mod = importlib.import_module("worker")
        handlers = worker_mod._handlers
    except Exception:
        handlers = {}

    import uuid, asyncio as _asyncio
    worker_id = f"embedded-{uuid.uuid4().hex[:6]}"

    # Register this embedded worker in DB
    async with SessionLocal() as db:
        from models import Worker as WorkerModel
        from sqlalchemy import select
        existing = await db.execute(select(WorkerModel).where(WorkerModel.id == worker_id))
        if not existing.scalar_one_or_none():
            db.add(WorkerModel(id=worker_id, queue="default", status="idle"))
            await db.commit()

    sem = _asyncio.Semaphore(4)

    async def run_job(job_id: str):
        async with sem:
            now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            async with SessionLocal() as db:
                from sqlalchemy import update as _update
                job = await db.get(Job, job_id)
                if not job or job.status != "pending":
                    return
                await db.execute(_update(Job).where(Job.id == job_id).values(
                    status="running", started_at=now,
                    worker_id=worker_id, attempts=Job.attempts + 1
                ))
                await db.commit()
                await db.refresh(job)
                await manager.broadcast("job_started", _job_dict(job))

            handler = handlers.get(job.name)
            try:
                if handler is None:
                    raise ValueError(f"No handler for '{job.name}'")
                result = await _asyncio.wait_for(handler(job.payload), timeout=300)
                fin = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
                async with SessionLocal() as db:
                    from sqlalchemy import update as _update
                    await db.execute(_update(Job).where(Job.id == job_id).values(
                        status="done", finished_at=fin, result=result
                    ))
                    await db.commit()
                    j = await db.get(Job, job_id)
                    await manager.broadcast("job_done", _job_dict(j))
            except Exception as exc:
                fin = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
                attempt = job.attempts
                requeue = attempt < job.max_retries
                new_status = "pending" if requeue else "dead"
                async with SessionLocal() as db:
                    from sqlalchemy import update as _update
                    await db.execute(_update(Job).where(Job.id == job_id).values(
                        status=new_status, finished_at=fin, error=str(exc)
                    ))
                    await db.commit()
                    if requeue:
                        await job_queue.enqueue(job_id, job.queue, job.priority)
                    j = await db.get(Job, job_id)
                    await manager.broadcast("job_failed", _job_dict(j))

    while True:
        try:
            job_id = await job_queue.dequeue("default", worker_id)
            if job_id:
                _asyncio.create_task(run_job(job_id))
            else:
                await _asyncio.sleep(0.5)
        except Exception:
            await _asyncio.sleep(1)
