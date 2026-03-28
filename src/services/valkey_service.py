import json
import logging
import time
from typing import Any, Dict, Optional

import redis.asyncio as redis

from src.config.secrets import get_secrets_config

logger = logging.getLogger(__name__)


class ValkeyService:
    """Service to handle Valkey operations for metrics and live job state."""

    _instance: Optional["ValkeyService"] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ValkeyService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.config = get_secrets_config()
        self.valkey_url = self.config.VALKEY_URL
        self._initialized = True
        logger.info("[Valkey] Service initialized.")

    async def get_client(self) -> redis.Redis | None:
        """Get or create the Valkey client."""
        if self._client is None:
            if not self.valkey_url:
                logger.warning("[Valkey] VALKEY_URL not configured.")
                return None

            try:
                self._client = redis.from_url(self.valkey_url, decode_responses=True)
                await self._client.ping()
                logger.info("[Valkey] Connected to Aiven Valkey successfully.")
            except Exception as exc:
                logger.error("[Valkey] Connection failed: %s", exc)
                self._client = None

        return self._client

    async def get_stats(self) -> Dict[str, Any]:
        client = await self.get_client()
        if not client:
            return {}

        try:
            async with client.pipeline() as pipe:
                pipe.hgetall("metrics:events:total")
                pipe.hgetall("metrics:errors:by_component")
                pipe.hgetall("metrics:db:stats")
                results = await pipe.execute()

            return {
                "total_events": results[0],
                "errors_by_component": results[1],
                "db_stats": results[2],
            }
        except Exception as exc:
            logger.error("[Valkey] Error fetching stats: %s", exc)
            return {}

    async def record_throughput(self, topic: str):
        client = await self.get_client()
        if not client:
            return

        bucket = int(time.time() // 60)
        key = f"metrics:throughput:{topic}:{bucket}"

        try:
            async with client.pipeline() as pipe:
                pipe.incr(key)
                pipe.expire(key, 600)
                await pipe.execute()
        except Exception as exc:
            logger.error("[Valkey] Error recording throughput: %s", exc)

    async def get_throughput(self, topic: str) -> int:
        client = await self.get_client()
        if not client:
            return 0

        bucket = int(time.time() // 60)
        try:
            value = await client.get(f"metrics:throughput:{topic}:{bucket}")
            return int(value) if value else 0
        except Exception:
            return 0

    async def record_live_log(self, log_data: Dict[str, Any]):
        client = await self.get_client()
        if not client:
            return

        try:
            log_json = json.dumps(log_data) if isinstance(log_data, dict) else log_data
            async with client.pipeline() as pipe:
                pipe.lpush("metrics:live_logs", log_json)
                pipe.ltrim("metrics:live_logs", 0, 19)
                pipe.expire("metrics:live_logs", 3600)
                await pipe.execute()
        except Exception as exc:
            logger.error("[Valkey] Error recording live log: %s", exc)

    async def get_live_logs(self) -> list:
        client = await self.get_client()
        if not client:
            return []

        try:
            logs = await client.lrange("metrics:live_logs", 0, 19)
            return [json.loads(log) for log in logs]
        except Exception as exc:
            logger.error("[Valkey] Error fetching live logs: %s", exc)
            return []

    async def update_db_stats(self, stats: Dict[str, Any]):
        client = await self.get_client()
        if not client:
            return
        try:
            await client.hset("metrics:db:stats", mapping=stats)
        except Exception as exc:
            logger.error("[Valkey] Error updating DB stats: %s", exc)

    async def record_job_event(
        self,
        *,
        job_id: str,
        event_type: str,
        stage: str,
        progress: int,
        payload: Dict[str, Any],
    ) -> None:
        """Mirror the latest job state and recent event history into Valkey."""

        client = await self.get_client()
        if not client:
            return

        timestamp = str(int(time.time()))
        state_key = f"jobs:{job_id}:state"
        events_key = f"jobs:{job_id}:events"
        event = {
            "job_id": job_id,
            "event_type": event_type,
            "stage": stage,
            "progress": progress,
            "payload": payload,
            "created_at": timestamp,
        }
        state = {
            "job_id": job_id,
            "event_type": event_type,
            "stage": stage,
            "progress": str(progress),
            "updated_at": timestamp,
            "payload": json.dumps(payload),
        }
        status = payload.get("status")
        if status is not None:
            state["status"] = str(status)

        try:
            async with client.pipeline() as pipe:
                pipe.hset(state_key, mapping=state)
                pipe.expire(state_key, 86400)
                pipe.lpush(events_key, json.dumps(event))
                pipe.ltrim(events_key, 0, 49)
                pipe.expire(events_key, 86400)
                await pipe.execute()
        except Exception as exc:
            logger.error("[Valkey] Error recording job event: %s", exc)

    async def get_job_state(self, job_id: str) -> Dict[str, Any]:
        client = await self.get_client()
        if not client:
            return {}

        try:
            state = await client.hgetall(f"jobs:{job_id}:state")
            if not state:
                return {}
            payload = state.get("payload")
            if payload:
                try:
                    state["payload"] = json.loads(payload)
                except json.JSONDecodeError:
                    pass
            return state
        except Exception as exc:
            logger.error("[Valkey] Error fetching job state: %s", exc)
            return {}

    async def get_job_events(self, job_id: str, limit: int = 20) -> list[Dict[str, Any]]:
        client = await self.get_client()
        if not client:
            return []

        try:
            raw_events = await client.lrange(f"jobs:{job_id}:events", 0, max(limit - 1, 0))
            return [json.loads(event) for event in raw_events]
        except Exception as exc:
            logger.error("[Valkey] Error fetching job events: %s", exc)
            return []

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("[Valkey] Connection closed.")


_valkey_service = None


async def get_valkey_service() -> ValkeyService:
    global _valkey_service
    if _valkey_service is None:
        _valkey_service = ValkeyService()
    return _valkey_service
