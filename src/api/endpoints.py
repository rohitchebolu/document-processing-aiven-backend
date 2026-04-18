import asyncio
import json
import logging
from typing import AsyncIterator

from fastapi import File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse

from src.services.document_pipeline import document_pipeline
from src.services.opensearch_service import get_opensearch_service

logger = logging.getLogger(__name__)

STREAM_POLL_INTERVAL_SECONDS = 1.0
STREAM_HEARTBEAT_SECONDS = 15.0
TERMINAL_STATUSES = {"completed", "failed"}


def _encode_sse(
    *,
    event: str,
    data: dict,
    event_id: str | None = None,
    retry_ms: int | None = None,
) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event:
        lines.append(f"event: {event}")
    if retry_ms is not None:
        lines.append(f"retry: {retry_ms}")
    payload = json.dumps(data, separators=(",", ":"))
    for line in payload.splitlines():
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


async def process_pdf_endpoint(
    files: list[UploadFile] = File(...),
    channel: str = Form(default="api"),
):
    if not files:
        raise HTTPException(status_code=400, detail="At least one PDF file is required.")

    logger.info(
        "Upload request received: files=%s channel=%s",
        len(files),
        channel or "api",
    )

    queued_jobs = []
    for file in files:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Every uploaded file must have a filename.")
        if not file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"{file.filename} is not a PDF file.")

        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail=f"{file.filename} is empty.")
        logger.info(
            "Read upload file: filename=%s size_bytes=%s content_type=%s",
            file.filename,
            len(file_bytes),
            file.content_type or "-",
        )

        try:
            job = await document_pipeline.enqueue_upload(
                filename=file.filename,
                content_type=file.content_type,
                file_bytes=file_bytes,
                channel=channel,
            )
        except ValueError as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        queued_jobs.append(
            {
                "job_id": job.job_id,
                "filename": job.original_filename,
                "status": job.status,
            }
        )
        logger.info(
            "Upload queued successfully: job_id=%s filename=%s status=%s",
            job.job_id,
            job.original_filename,
            job.status,
        )

    return {
        "message": "Documents queued for extraction.",
        "topic": document_pipeline.topic,
        "event_topics": {
            "jobs": document_pipeline.jobs_topic,
            "progress": document_pipeline.progress_topic,
            "results": document_pipeline.results_topic,
            "errors": document_pipeline.errors_topic,
        },
        "jobs": queued_jobs,
    }


async def get_job_status_endpoint(job_id: str):
    snapshot = await document_pipeline.get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} was not found.")
    return snapshot


async def get_job_events_endpoint(job_id: str, limit: int = 50):
    snapshot = await document_pipeline.get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} was not found.")
    return {
        "job_id": job_id,
        "events": await document_pipeline.get_job_events(job_id, limit=limit),
    }


async def search_jobs_endpoint(
    q: str = Query(..., min_length=1, description="Search text"),
    limit: int = Query(default=20, ge=1, le=100),
    status: str | None = Query(default=None),
    channel: str | None = Query(default=None),
):
    opensearch = await get_opensearch_service()
    if not opensearch.is_enabled():
        raise HTTPException(
            status_code=503,
            detail="OpenSearch is not configured. Set OPENSEARCH_URI.",
        )

    try:
        hits = await opensearch.search_jobs(
            query=q,
            limit=limit,
            status=status,
            channel=channel,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "query": q,
        "limit": limit,
        "filters": {"status": status, "channel": channel},
        "count": len(hits),
        "results": hits,
    }


async def stream_job_events_endpoint(
    job_id: str,
    request: Request,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
):
    snapshot = await document_pipeline.get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} was not found.")

    async def event_stream() -> AsyncIterator[str]:
        last_seen_id: int | None = None
        if last_event_id:
            try:
                last_seen_id = int(last_event_id)
            except ValueError:
                logger.warning("Ignoring invalid Last-Event-ID for job %s: %s", job_id, last_event_id)

        initial_snapshot = await document_pipeline.get_job_snapshot(job_id)
        if initial_snapshot is not None:
            live_state = initial_snapshot.get("live_state") or {}
            initial_event_id = str(last_seen_id) if last_seen_id is not None else "snapshot"
            yield _encode_sse(
                event="snapshot",
                data={
                    "job_id": job_id,
                    "snapshot": initial_snapshot,
                    "live_state": live_state,
                },
                event_id=initial_event_id,
                retry_ms=3000,
            )

        last_heartbeat_at = asyncio.get_running_loop().time()
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: job_id=%s", job_id)
                break

            events = await document_pipeline.get_job_events_after(
                job_id,
                after_id=last_seen_id,
                limit=100,
            )
            if events:
                for event in events:
                    last_seen_id = event["id"]
                    yield _encode_sse(
                        event=event["event_type"],
                        data=event,
                        event_id=str(event["id"]),
                    )
                    if event.get("payload", {}).get("status") in TERMINAL_STATUSES:
                        yield ": stream-complete\n\n"
                        return

                last_heartbeat_at = asyncio.get_running_loop().time()
            else:
                snapshot = await document_pipeline.get_job_snapshot(job_id)
                status = (snapshot or {}).get("status")
                now = asyncio.get_running_loop().time()
                if now - last_heartbeat_at >= STREAM_HEARTBEAT_SECONDS:
                    yield ": keep-alive\n\n"
                    last_heartbeat_at = now
                if status in TERMINAL_STATUSES:
                    yield ": stream-complete\n\n"
                    return

            await asyncio.sleep(STREAM_POLL_INTERVAL_SECONDS)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
