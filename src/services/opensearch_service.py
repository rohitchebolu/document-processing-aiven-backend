import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse, unquote

from opensearchpy import OpenSearch, RequestsHttpConnection

from src.config.secrets import get_secrets_config

logger = logging.getLogger(__name__)


def _to_iso8601(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class OpenSearchService:
    """Service to index and query extracted jobs in Aiven OpenSearch."""

    _instance: Optional["OpenSearchService"] = None
    _client: Optional[OpenSearch] = None
    _index_initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OpenSearchService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.config = get_secrets_config()
        self.uri = self.config.OPENSEARCH_URI
        self.ca_cert_path = self.config.OPENSEARCH_CA_CERT_PATH
        self.index_name = self.config.OPENSEARCH_INDEX_NAME
        self._initialized = True
        logger.info("[OpenSearch] Service initialized. Index: %s", self.index_name)

    def is_enabled(self) -> bool:
        return bool(self.uri)

    async def _get_client(self) -> OpenSearch | None:
        if self._client is not None:
            return self._client

        if not self.uri:
            logger.info("[OpenSearch] OPENSEARCH_URI not configured; indexing disabled.")
            return None

        try:
            parsed = urlparse(self.uri)
            host: dict[str, Any] = {
                "host": parsed.hostname or "",
                "port": parsed.port or 443,
            }
            scheme = (parsed.scheme or "https").lower()
            use_ssl = scheme == "https"
            if use_ssl:
                host["scheme"] = "https"

            uri_user = unquote(parsed.username) if parsed.username else None
            uri_password = unquote(parsed.password) if parsed.password else None

            kwargs: dict[str, Any] = {
                "hosts": [host],
                "connection_class": RequestsHttpConnection,
                "timeout": 10,
                "use_ssl": use_ssl,
                "verify_certs": bool(self.ca_cert_path) if use_ssl else False,
            }
            if uri_user:
                kwargs["http_auth"] = (uri_user, uri_password or "")
            if self.ca_cert_path:
                kwargs["ca_certs"] = self.ca_cert_path

            self._client = OpenSearch(**kwargs)
            await asyncio.to_thread(self._client.ping)
            logger.info("[OpenSearch] Connected successfully.")
        except Exception as exc:
            logger.error("[OpenSearch] Connection failed: %s", exc)
            self._client = None
            return None

        return self._client

    async def _ensure_index(self) -> bool:
        if self._index_initialized:
            return True

        client = await self._get_client()
        if client is None:
            return False

        try:
            exists = await asyncio.to_thread(client.indices.exists, self.index_name)
            if not exists:
                body = {
                    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 1}},
                    "mappings": {
                        "properties": {
                            "job_id": {"type": "keyword"},
                            "filename": {"type": "text"},
                            "content_type": {"type": "keyword"},
                            "channel": {"type": "keyword"},
                            "status": {"type": "keyword"},
                            "prompt_name": {"type": "keyword"},
                            "order_count": {"type": "integer"},
                            "search_text": {"type": "text"},
                            "extraction_result": {"type": "object", "enabled": False},
                            "created_at": {"type": "date"},
                            "updated_at": {"type": "date"},
                            "processed_at": {"type": "date"},
                        }
                    },
                }
                await asyncio.to_thread(client.indices.create, self.index_name, body)
                logger.info("[OpenSearch] Created index: %s", self.index_name)

            self._index_initialized = True
            return True
        except Exception:
            logger.exception("[OpenSearch] Failed to ensure index %s", self.index_name)
            return False

    async def index_extraction_job(self, job: Any, extraction_result: list[dict[str, Any]]) -> None:
        if not await self._ensure_index():
            return

        client = await self._get_client()
        if client is None:
            return

        try:
            result_text = json.dumps(extraction_result, ensure_ascii=False)
            doc = {
                "job_id": job.job_id,
                "filename": job.original_filename,
                "content_type": job.content_type,
                "channel": job.channel,
                "status": job.status,
                "prompt_name": job.prompt_name,
                "order_count": len(extraction_result),
                "search_text": f"{job.original_filename} {result_text}",
                "extraction_result": extraction_result,
                "created_at": _to_iso8601(job.created_at),
                "updated_at": _to_iso8601(job.updated_at),
                "processed_at": _to_iso8601(job.processed_at),
            }
            await asyncio.to_thread(
                client.index,
                index=self.index_name,
                id=job.job_id,
                body=doc,
                refresh=True,
            )
            logger.info("[OpenSearch] Indexed extraction result: job_id=%s", job.job_id)
        except Exception:
            logger.exception("[OpenSearch] Failed to index job %s", job.job_id)

    async def search_jobs(
        self,
        *,
        query: str,
        limit: int = 20,
        status: str | None = None,
        channel: str | None = None,
    ) -> list[dict[str, Any]]:
        if not await self._ensure_index():
            raise RuntimeError("OpenSearch is not configured or unavailable.")

        client = await self._get_client()
        if client is None:
            raise RuntimeError("OpenSearch is not configured or unavailable.")

        must_filters: list[dict[str, Any]] = []
        if status:
            must_filters.append({"term": {"status": status}})
        if channel:
            must_filters.append({"term": {"channel": channel}})

        body = {
            "size": min(max(limit, 1), 100),
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "filename^2",
                                    "search_text",
                                ],
                                "operator": "and",
                            }
                        }
                    ],
                    "filter": must_filters,
                }
            },
            "_source": [
                "job_id",
                "filename",
                "channel",
                "status",
                "prompt_name",
                "order_count",
                "created_at",
                "updated_at",
                "processed_at",
            ],
        }

        try:
            response = await asyncio.to_thread(client.search, index=self.index_name, body=body)
            hits = response.get("hits", {}).get("hits", [])
            return [
                {
                    "score": hit.get("_score"),
                    **hit.get("_source", {}),
                }
                for hit in hits
            ]
        except Exception as exc:
            logger.error("[OpenSearch] Search failed: %s", exc)
            raise RuntimeError(f"OpenSearch query failed: {exc}") from exc

    async def close(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
            self._client = None
            self._index_initialized = False
            logger.info("[OpenSearch] Client closed.")


_opensearch_service: OpenSearchService | None = None


async def get_opensearch_service() -> OpenSearchService:
    global _opensearch_service
    if _opensearch_service is None:
        _opensearch_service = OpenSearchService()
    return _opensearch_service
