from sqlalchemy import Column, String, Integer, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

def gen_id():
    return str(uuid.uuid4())

class Job(Base):
    __tablename__ = "jobs"

    id          = Column(String, primary_key=True, default=gen_id)
    name        = Column(String, nullable=False)
    queue       = Column(String, nullable=False, default="default")
    payload     = Column(JSON, nullable=False, default=dict)
    status      = Column(String, nullable=False, default="pending")   # pending | running | done | failed | dead
    priority    = Column(Integer, nullable=False, default=5)          # 1 (highest) – 10 (lowest)
    attempts    = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    error       = Column(Text, nullable=True)
    result      = Column(JSON, nullable=True)
    run_at      = Column(DateTime(timezone=True), nullable=True)      # scheduled execution time
    started_at  = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    worker_id   = Column(String, nullable=True)


class Worker(Base):
    __tablename__ = "workers"

    id          = Column(String, primary_key=True)
    queue       = Column(String, nullable=False, default="default")
    status      = Column(String, nullable=False, default="idle")      # idle | busy | dead
    jobs_done   = Column(Integer, nullable=False, default=0)
    jobs_failed = Column(Integer, nullable=False, default=0)
    last_seen   = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    started_at  = Column(DateTime(timezone=True), server_default=func.now())
