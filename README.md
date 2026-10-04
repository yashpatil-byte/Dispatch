# Dispatch — Distributed Job Orchestration Platform

A production-grade distributed job queue and orchestration system with a real-time dashboard.

## Architecture

```
┌─────────────┐    REST/WS     ┌──────────────────────────────────┐
│  Dashboard  │ ◄────────────► │         FastAPI Server           │
│  (nginx)    │                │  - Job CRUD  (REST)              │
└─────────────┘                │  - Metrics   (REST + WebSocket)  │
                               │  - Worker mgmt                   │
┌─────────────┐                └──────────┬───────────────────────┘
│   Client    │ ─── POST /jobs ──────────►│                       │
│  (any HTTP) │                           │                       │
└─────────────┘                     ┌─────▼──────┐   ┌──────────┐
                                    │   Redis    │   │Postgres  │
                                    │ (priority  │   │(job state│
                                    │  queue)    │   │ history) │
                                    └─────┬──────┘   └──────────┘
                                          │ dequeue
                              ┌───────────┴───────────┐
                              │                       │
                         ┌────▼────┐            ┌────▼────┐
                         │Worker 1 │            │Worker 2 │
                         │(4 slots)│            │(4 slots)│
                         └─────────┘            └─────────┘
```

## Features

- **Priority Queue** — Redis sorted sets; priority 1 (critical) beats priority 10 (batch)
- **Distributed Workers** — Multiple worker processes, each with configurable concurrency slots
- **Exponential Backoff** — Failed jobs retry with `2^attempt` second delay, capped at 60s
- **Dead Letter Queue** — Jobs exhausting retries move to DLQ for inspection
- **Cron/Scheduled Jobs** — Submit jobs with a future `run_at` timestamp
- **Real-Time Dashboard** — WebSocket push updates on every job state change
- **Live Metrics** — Queue depth, throughput, avg duration, P99 latency
- **Worker Heartbeats** — Detect dead workers within 15s via last_seen checks
- **OpenAPI Docs** — Full Swagger UI at `/docs`

## Quickstart

```bash
docker compose up --scale worker=3
```

- Dashboard:  http://localhost:3000
- API docs:   http://localhost:8000/docs

## API

| Method | Endpoint             | Description             |
|--------|----------------------|-------------------------|
| POST   | /jobs                | Submit a job            |
| GET    | /jobs                | List jobs (filterable)  |
| GET    | /jobs/{id}           | Get job details         |
| DELETE | /jobs/{id}           | Cancel a pending job    |
| GET    | /workers             | List active workers     |
| GET    | /metrics             | Current queue metrics   |
| WS     | /ws                  | Real-time event stream  |

## Registering Custom Job Handlers

```python
from worker import register

@register("send_invoice")
async def handle_invoice(payload: dict) -> dict:
    # your logic here
    return {"invoice_id": payload["id"], "status": "sent"}
```

## Tech Stack

Python · FastAPI · PostgreSQL · Redis · Docker · WebSockets · SQLAlchemy (async)
