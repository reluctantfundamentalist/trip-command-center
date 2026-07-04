"""
Redis caching layer with TTLs from the spec.

Cache key patterns and TTLs:
  ctx:{airline_id}                     — 1 hour  (account context for extraction)
  thread:{thread_id}                   — 4 hours (thread summary chain)
  guidance:{exec_id}:{airline_id}      — 7 days  (proactive guidance output)
  dashboard:{exec_id}:overview         — 5 min   (executive dashboard overview)
  digest:{week}                        — 30 days (weekly digest, immutable once generated)
"""

from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from config.settings import settings


# TTL constants in seconds
TTL_ACCOUNT_CONTEXT = 3600          # 1 hour
TTL_THREAD_SUMMARY = 14400         # 4 hours
TTL_GUIDANCE = 604800              # 7 days
TTL_DASHBOARD_OVERVIEW = 300       # 5 minutes
TTL_WEEKLY_DIGEST = 2592000        # 30 days


class RedisCache:
    """Async Redis cache wrapper with typed key helpers."""

    def __init__(self, redis_url: str | None = None) -> None:
        self._url = redis_url or settings.redis.url
        self._pool: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._pool = aioredis.from_url(
            self._url,
            decode_responses=True,
            max_connections=50,
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.aclose()

    @property
    def client(self) -> aioredis.Redis:
        if self._pool is None:
            raise RuntimeError("RedisCache not connected — call connect() first")
        return self._pool

    # ── Generic operations ────────────────────────────────────

    async def get(self, key: str) -> Any | None:
        raw = await self.client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    async def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        payload = json.dumps(value) if not isinstance(value, str) else value
        if ttl:
            await self.client.setex(key, ttl, payload)
        else:
            await self.client.set(key, payload)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self.client.exists(key))

    # ── Typed key helpers ─────────────────────────────────────

    # Account context cache
    async def get_account_context(self, airline_id: str) -> dict | None:
        return await self.get(f"ctx:{airline_id}")

    async def set_account_context(self, airline_id: str, context: dict) -> None:
        await self.set(f"ctx:{airline_id}", context, ttl=TTL_ACCOUNT_CONTEXT)

    async def invalidate_account_context(self, airline_id: str) -> None:
        await self.delete(f"ctx:{airline_id}")

    # Thread summary cache
    async def get_thread_summary(self, thread_id: str) -> list[str] | None:
        return await self.get(f"thread:{thread_id}")

    async def set_thread_summary(self, thread_id: str, summaries: list[str]) -> None:
        await self.set(f"thread:{thread_id}", summaries, ttl=TTL_THREAD_SUMMARY)

    async def invalidate_thread_summary(self, thread_id: str) -> None:
        await self.delete(f"thread:{thread_id}")

    # Proactive guidance cache
    async def get_guidance(self, exec_id: str, airline_id: str) -> dict | None:
        return await self.get(f"guidance:{exec_id}:{airline_id}")

    async def set_guidance(self, exec_id: str, airline_id: str, guidance: dict) -> None:
        await self.set(f"guidance:{exec_id}:{airline_id}", guidance, ttl=TTL_GUIDANCE)

    # Dashboard overview cache
    async def get_dashboard_overview(self, exec_id: str) -> dict | None:
        return await self.get(f"dashboard:{exec_id}:overview")

    async def set_dashboard_overview(self, exec_id: str, overview: dict) -> None:
        await self.set(f"dashboard:{exec_id}:overview", overview, ttl=TTL_DASHBOARD_OVERVIEW)

    async def invalidate_dashboard_overview(self, exec_id: str) -> None:
        await self.delete(f"dashboard:{exec_id}:overview")

    # Weekly digest cache
    async def get_weekly_digest(self, week: str) -> dict | None:
        return await self.get(f"digest:{week}")

    async def set_weekly_digest(self, week: str, digest: dict) -> None:
        await self.set(f"digest:{week}", digest, ttl=TTL_WEEKLY_DIGEST)

    # ── Write-through invalidation ────────────────────────────

    async def on_extraction_complete(self, airline_id: str, exec_id: str | None = None) -> None:
        """
        Called after a new meeting/offer/action_item is written.
        Invalidates the account context cache so subsequent extractions
        see fresh data.
        """
        await self.invalidate_account_context(airline_id)
        if exec_id:
            await self.invalidate_dashboard_overview(exec_id)


# Module-level singleton
cache = RedisCache()
