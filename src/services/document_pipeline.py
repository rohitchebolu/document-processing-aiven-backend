import asyncio
import base64
import json
import logging
import os
import ssl
import tempfile
import uuid
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaConsumer

from src.config.secrets import get_secrets_config
from src.core.extractors.pdf_extractor import pdf_extractor
from src.database import SessionLocal
from src.models.extraction_job import ExtractionJob
from src.models.job_event import JobEvent
from src.services.kafka_service import get_kafka_service
from src.services.llm.usage_tracker import current_extraction_job_id
from src.services.valkey_service import get_valkey_service

logger = logging.getLogger(__name__)


class DocumentPipeline:
    def __init__(self) -> None:
        config = get_secrets_config()
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.ssl_ca = config.KAFKA_SSL_CA_CERT_PATH
        self.ssl_cert = config.KAFKA_SSL_ACCESS_CERT_PATH
        self.ssl_key = config.KAFKA_SSL_ACCESS_KEY_PATH
        self.topic = os.getenv("KAFKA_UPLOAD_TOPIC", "document-extraction.uploads")
        self.group_id = os.getenv(
            "KAFKA_UPLOAD_CONSUMER_GROUP",
            "document-extraction-workers",
        )
        self.jobs_topic = os.getenv("KAFKA_JOBS_TOPIC", "document.jobs")
        self.progress_topic = os.getenv("KAFKA_PROGRESS_TOPIC", "document.progress")
        self.results_topic = os.getenv("KAFKA_RESULTS_TOPIC", "document.results")
        self.errors_topic = os.getenv("KAFKA_ERRORS_TOPIC", "document.errors")
        self.max_upload_bytes = int(os.getenv("UPLOAD_MAX_BYTES", str(5 * 1024 * 1024)))
        self._consumer: AIOKafkaConsumer | None = None
        self._consume_task: asyncio.Task | None = None

    async def start(self) -> None:
        await get_kafka_service()
        if not self.bootstrap_servers:
            logger.warning("Kafka bootstrap servers are not configured; queue consumer disabled.")
            return
        if self._consume_task is not None:
            return

        ssl_context = self._build_ssl_context()
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            security_protocol="SSL" if ssl_context else "PLAINTEXT",
            ssl_context=ssl_context,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        try:
            await asyncio.wait_for(self._consumer.start(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning(
                "Kafka consumer startup timed out while waiting for topic %s metadata. "
                "Create the topic and restart the app to enable background processing.",
                self.topic,
            )
            await self._consumer.stop()
            self._consumer = None
            return
        except Exception as exc:
            logger.warning(
                "Kafka consumer could not start for topic %s: %s. "
                "Create the topic and restart the app to enable background processing.",
                self.topic,
                exc,
            )
            if self._consumer is not None:
                await self._consumer.stop()
                self._consumer = None
            return

        self._consume_task = asyncio.create_task(self._consume_loop())
        logger.info("Document pipeline consumer started for topic %s", self.topic)

    async def stop(self) -> None:
        if self._consume_task is not None:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def enqueue_upload(
        self,
        *,
        filename: str,
        content_type: str | None,
        file_bytes: bytes,
        channel: str,
    ) -> ExtractionJob:
        if not self.bootstrap_servers:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not configured.")

        logger.info(
            "Enqueueing upload: filename=%s size_bytes=%s channel=%s",
            filename,
            len(file_bytes),
            channel or "api",
        )

        if len(file_bytes) > self.max_upload_bytes:
            raise ValueError(
                f"File exceeds UPLOAD_MAX_BYTES limit of {self.max_upload_bytes} bytes."
            )

        job = ExtractionJob(
            job_id=uuid.uuid4().hex,
            original_filename=filename,
            content_type=content_type,
            channel=channel or "api",
            status="queued",
        )

        db = SessionLocal()
        try:
            db.add(job)
            db.commit()
            db.refresh(job)
            logger.info(
                "Created extraction job row: job_id=%s db_id=%s status=%s",
                job.job_id,
                job.id,
                job.status,
            )
        finally:
            db.close()

        payload = {
            "job_id": job.job_id,
            "filename": filename,
            "content_type": content_type,
            "channel": channel,
            "file_data_base64": base64.b64encode(file_bytes).decode("utf-8"),
            "queued_at": datetime.now(timezone.utc).isoformat(),
        }

        kafka = await get_kafka_service()
        logger.info("Publishing extraction job to Kafka: job_id=%s topic=%s", job.job_id, self.topic)
        await kafka.publish(self.topic, payload, key=job.job_id)
        await self._record_event(
            job_id=job.job_id,
            event_type="job.uploaded",
            stage="uploaded",
            progress=5,
            payload={
                "status": "queued",
                "filename": filename,
                "content_type": content_type,
                "channel": channel,
            },
        )
        await self._record_event(
            job_id=job.job_id,
            event_type="job.queued",
            stage="queued",
            progress=10,
            payload={
                "status": "queued",
                "topic": self.topic,
                "channel": channel,
            },
        )
        return job

    async def get_job_snapshot(self, job_id: str) -> dict[str, Any] | None:
        db = SessionLocal()
        try:
            job = db.query(ExtractionJob).filter(ExtractionJob.job_id == job_id).first()
            if job is None:
                return None

            state = await self._get_job_cache(job_id)
            return {
                "job_id": job.job_id,
                "filename": job.original_filename,
                "content_type": job.content_type,
                "channel": job.channel,
                "status": job.status,
                "prompt_name": job.prompt_name,
                "error_message": job.error_message,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "updated_at": job.updated_at.isoformat() if job.updated_at else None,
                "processed_at": job.processed_at.isoformat() if job.processed_at else None,
                "live_state": state,
            }
        finally:
            db.close()

    async def get_job_events(self, job_id: str, limit: int = 50) -> list[dict[str, Any]]:
        db = SessionLocal()
        try:
            events = (
                db.query(JobEvent)
                .filter(JobEvent.job_id == job_id)
                .order_by(JobEvent.created_at.desc())
                .limit(limit)
                .all()
            )
            if events:
                return [
                    {
                        "id": event.id,
                        "event_type": event.event_type,
                        "stage": event.stage,
                        "progress": event.progress,
                        "payload": event.payload,
                        "created_at": event.created_at.isoformat(),
                    }
                    for event in events
                ]
        finally:
            db.close()

        return await self._get_cached_job_events(job_id, limit=limit)

    async def get_job_events_after(
        self,
        job_id: str,
        *,
        after_id: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        db = SessionLocal()
        try:
            query = db.query(JobEvent).filter(JobEvent.job_id == job_id)
            if after_id is not None:
                query = query.filter(JobEvent.id > after_id)

            events = query.order_by(JobEvent.id.asc()).limit(limit).all()
            return [
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "stage": event.stage,
                    "progress": event.progress,
                    "payload": event.payload,
                    "created_at": event.created_at.isoformat(),
                }
                for event in events
            ]
        finally:
            db.close()

    async def _consume_loop(self) -> None:
        if self._consumer is None:
            return

        try:
            async for message in self._consumer:
                logger.info(
                    "Kafka message received: topic=%s partition=%s offset=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                )
                await self._process_message(message.value)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Document pipeline consumer crashed.")

    async def _process_message(self, payload: dict[str, Any]) -> None:
        job_id = payload["job_id"]
        logger.info(
            "Starting background processing: job_id=%s filename=%s",
            job_id,
            payload.get("filename", "-"),
        )
        db = SessionLocal()
        try:
            job = db.query(ExtractionJob).filter(ExtractionJob.job_id == job_id).first()
            if job is None:
                logger.warning("Received job %s from Kafka but no database row exists.", job_id)
                return

            job.status = "processing"
            job.error_message = None
            db.commit()
            await self._record_event(
                job_id=job_id,
                event_type="job.processing_started",
                stage="processing",
                progress=20,
                payload={"status": "processing", "filename": job.original_filename},
            )

            file_bytes = base64.b64decode(payload["file_data_base64"])
            logger.info(
                "Decoded Kafka payload for processing: job_id=%s size_bytes=%s",
                job_id,
                len(file_bytes),
            )
            await self._record_event(
                job_id=job_id,
                event_type="job.grouping_started",
                stage="grouping",
                progress=30,
                payload={"status": "processing", "filename": job.original_filename},
            )
            await self._record_event(
                job_id=job_id,
                event_type="job.extraction_started",
                stage="extracting",
                progress=55,
                payload={"status": "processing", "filename": job.original_filename},
            )
            extracted_result = await asyncio.to_thread(
                self._extract_document,
                job.id,
                payload["filename"],
                file_bytes,
            )

            cleaned_result, prompt_name, prompt_template = self._split_processing_metadata(
                extracted_result
            )
            logger.info(
                "Extraction returned orders: job_id=%s order_count=%s prompt_name=%s",
                job_id,
                len(cleaned_result),
                prompt_name or "-",
            )

            await self._record_event(
                job_id=job_id,
                event_type="job.extraction_completed",
                stage="extracting",
                progress=85,
                payload={
                    "status": "processing",
                    "order_count": len(cleaned_result),
                    "prompt_name": prompt_name,
                },
            )

            job.status = "completed"
            job.extraction_result = cleaned_result
            job.prompt_name = prompt_name
            job.prompt_template = prompt_template
            job.processed_at = datetime.utcnow()
            job.updated_at = datetime.utcnow()
            db.commit()
            await self._record_event(
                job_id=job_id,
                event_type="job.completed",
                stage="completed",
                progress=100,
                payload={
                    "status": "completed",
                    "prompt_name": prompt_name,
                    "order_count": len(cleaned_result),
                    "result_preview": cleaned_result[0] if cleaned_result else {},
                },
            )
            logger.info("Job completed successfully: job_id=%s", job_id)
        except Exception as exc:
            db.rollback()
            logger.exception("Failed to process extraction job %s", job_id)
            job = db.query(ExtractionJob).filter(ExtractionJob.job_id == job_id).first()
            if job is not None:
                job.status = "failed"
                job.error_message = str(exc)
                job.updated_at = datetime.utcnow()
                db.commit()
                logger.info("Job marked failed in database: job_id=%s", job_id)
            await self._record_event(
                job_id=job_id,
                event_type="job.failed",
                stage="failed",
                progress=100,
                payload={"status": "failed", "message": str(exc)},
            )
        finally:
            db.close()

    def _extract_document(
        self,
        extraction_job_id: int,
        filename: str,
        file_bytes: bytes,
    ) -> list[dict[str, Any]]:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(file_bytes)
            temp_path = temp_file.name
        logger.info(
            "Temporary PDF created for extraction: extraction_job_id=%s path=%s size_bytes=%s",
            extraction_job_id,
            temp_path,
            len(file_bytes),
        )

        job_context = None
        try:
            job_context = current_extraction_job_id.set(extraction_job_id)
            logger.info(
                "Invoking pdf_extractor: extraction_job_id=%s filename=%s",
                extraction_job_id,
                filename,
            )
            result = pdf_extractor(
                filename=filename,
                pdf_path=temp_path,
            )
            if isinstance(result, list):
                logger.info(
                    "pdf_extractor finished: extraction_job_id=%s result_count=%s",
                    extraction_job_id,
                    len(result),
                )
                return result
            logger.info("pdf_extractor finished with single result: extraction_job_id=%s", extraction_job_id)
            return [result]
        finally:
            if job_context is not None:
                current_extraction_job_id.reset(job_context)
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.info(
                    "Temporary PDF removed after extraction: extraction_job_id=%s path=%s",
                    extraction_job_id,
                    temp_path,
                )

    def _split_processing_metadata(
        self, extracted_result: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], str | None, str | None]:
        cleaned_orders: list[dict[str, Any]] = []
        prompt_name: str | None = None
        prompt_template: str | None = None

        for order in extracted_result:
            order_copy = dict(order)
            order_prompt_name = order_copy.pop("_prompt_name", None)
            order_prompt_template = order_copy.pop("_prompt_template", None)
            cleaned_orders.append(order_copy)
            if prompt_name is None and order_prompt_name:
                prompt_name = order_prompt_name
            if prompt_template is None and order_prompt_template:
                prompt_template = order_prompt_template

        return cleaned_orders, prompt_name, prompt_template

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        if not self.ssl_ca:
            return None

        ssl_context = ssl.create_default_context(cafile=self.ssl_ca)
        if self.ssl_cert and self.ssl_key:
            ssl_context.load_cert_chain(certfile=self.ssl_cert, keyfile=self.ssl_key)
        return ssl_context

    async def _record_event(
        self,
        *,
        job_id: str,
        event_type: str,
        stage: str,
        progress: int,
        payload: dict[str, Any],
    ) -> None:
        await self._persist_job_event(
            job_id=job_id,
            event_type=event_type,
            stage=stage,
            progress=progress,
            payload=payload,
        )
        await self._update_job_cache(
            job_id=job_id,
            event_type=event_type,
            stage=stage,
            progress=progress,
            payload=payload,
        )
        await self._publish_job_event(
            job_id=job_id,
            event_type=event_type,
            stage=stage,
            progress=progress,
            payload=payload,
        )

    async def _persist_job_event(
        self,
        *,
        job_id: str,
        event_type: str,
        stage: str,
        progress: int,
        payload: dict[str, Any],
    ) -> None:
        db = SessionLocal()
        try:
            db.add(
                JobEvent(
                    job_id=job_id,
                    event_type=event_type,
                    stage=stage,
                    progress=progress,
                    payload=payload,
                )
            )
            db.commit()
        except Exception:
            db.rollback()
            logger.exception("Failed to persist job event: job_id=%s event_type=%s", job_id, event_type)
        finally:
            db.close()

    async def _publish_job_event(
        self,
        *,
        job_id: str,
        event_type: str,
        stage: str,
        progress: int,
        payload: dict[str, Any],
    ) -> None:
        if not self.bootstrap_servers:
            return

        envelope = {
            "event_id": uuid.uuid4().hex,
            "job_id": job_id,
            "event_type": event_type,
            "stage": stage,
            "progress": progress,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        topic = self._event_topic_for(event_type)
        kafka = await get_kafka_service()
        try:
            await kafka.publish(topic, envelope, key=job_id)
            logger.info(
                "Published job event to Kafka: job_id=%s event_type=%s topic=%s",
                job_id,
                event_type,
                topic,
            )
        except Exception as exc:
            logger.warning(
                "Could not publish job event to Kafka topic %s for job %s: %s",
                topic,
                job_id,
                exc,
            )

    def _event_topic_for(self, event_type: str) -> str:
        if event_type.endswith("failed"):
            return self.errors_topic
        if event_type.endswith("completed"):
            return self.results_topic
        if event_type in {"job.uploaded", "job.queued"}:
            return self.jobs_topic
        return self.progress_topic

    async def _update_job_cache(
        self,
        *,
        job_id: str,
        event_type: str,
        stage: str,
        progress: int,
        payload: dict[str, Any],
    ) -> None:
        valkey = await get_valkey_service()
        await valkey.record_job_event(
            job_id=job_id,
            event_type=event_type,
            stage=stage,
            progress=progress,
            payload=payload,
        )
        logger.info(
            "Valkey job cache updated: job_id=%s event_type=%s progress=%s",
            job_id,
            event_type,
            progress,
        )

    async def _get_job_cache(self, job_id: str) -> dict[str, Any]:
        valkey = await get_valkey_service()
        return await valkey.get_job_state(job_id)

    async def _get_cached_job_events(self, job_id: str, *, limit: int) -> list[dict[str, Any]]:
        valkey = await get_valkey_service()
        return await valkey.get_job_events(job_id, limit=limit)


document_pipeline = DocumentPipeline()
