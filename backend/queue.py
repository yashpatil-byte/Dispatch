"""
Redis-backed priority queue using sorted sets.
Score = priority * 1e12 + enqueue_timestamp (lower score = higher priority).
"""
import time
import json
import redis.asyncio as aioredis
from typing import Optional

QUEUE_KEY   = "dispatch:queue:{queue}"
DLQ_KEY     = "dispatch:dlq:{queue}"
RUNNING_KEY = "dispatch:running"

class JobQueue:
    def __init__(self, redis: aioredis.Redis):
        self.r = redis

    def _key(self, queue: str) -> str:
        return QUEUE_KEY.format(queue=queue)

    def _dlq(self, queue: str) -> str:
        return DLQ_KEY.format(queue=queue)

    async def enqueue(self, job_id: str, queue: str, priority: int, run_at: Optional[float] = None) -> None:
        """Add a job to the sorted set. Score ensures priority ordering."""
        score_base = priority * 1e12
        score = score_base + (run_at or time.time())
        await self.r.zadd(self._key(queue), {job_id: score})

    async def dequeue(self, queue: str, worker_id: str) -> Optional[str]:
        """Atomically pop the highest-priority job and mark it running."""
        async with self.r.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(self._key(queue))
                    items = await pipe.zrangebyscore(
                        self._key(queue), "-inf", time.time() + 1e12, start=0, num=1
                    )
                    if not items:
                        await pipe.reset()
                        return None
                    job_id = items[0].decode() if isinstance(items[0], bytes) else items[0]
                    pipe.multi()
                    pipe.zrem(self._key(queue), job_id)
                    pipe.hset(RUNNING_KEY, job_id, worker_id)
                    await pipe.execute()
                    return job_id
                except aioredis.WatchError:
                    continue

    async def complete(self, job_id: str) -> None:
        await self.r.hdel(RUNNING_KEY, job_id)

    async def send_to_dlq(self, job_id: str, queue: str, reason: str) -> None:
        await self.r.hdel(RUNNING_KEY, job_id)
        await self.r.rpush(self._dlq(queue), json.dumps({"job_id": job_id, "reason": reason, "ts": time.time()}))

    async def queue_depth(self, queue: str) -> int:
        return await self.r.zcard(self._key(queue))

    async def running_count(self) -> int:
        return await self.r.hlen(RUNNING_KEY)

    async def dlq_depth(self, queue: str) -> int:
        return await self.r.llen(self._dlq(queue))
